"""撤销与完成撞车：行锁内复核保证恰好一方获胜；已完成结果保留且拒绝撤销。"""
import threading
import pytest
from fastapi import HTTPException
from src.models.entities import Drill
from src.schemas.drill import DrillInput
from src.services.drill_service import create_drill, cancel_drill, utcnow
from src.worker import claim_next, publish

OUTCOME = ('completed', 'completed', {'decision': 'approved', 'winner': 'hs-east', 'simulation': True})

def make_running(factory, scenario_id, label='race', **kw):
    with factory() as s:
        obj = create_drill(s, DrillInput(scenario_id=scenario_id, label=label, **kw))
        s.commit()
        did = obj.id
    with factory() as s:
        job = claim_next(s, utcnow())
        assert job.id == did
        s.commit()
        return did, job.claim_token

def fetch(factory, did):
    with factory() as s:
        return s.get(Drill, did)

def test_cancel_then_publish_discarded(factory, scenario_id):
    """撤销先落地：迟到的完成不得发布，结果为空。"""
    did, token = make_running(factory, scenario_id)
    with factory() as s:
        cancel_drill(s, did); s.commit()
    with factory() as s:
        assert publish(s, did, token, OUTCOME, utcnow()) == 'discarded'
        s.commit()
    row = fetch(factory, did)
    assert row.status == 'canceled' and row.result is None and row.finish_reason == 'user_cancel'

def test_publish_then_cancel_rejected(factory, scenario_id):
    """完成先落地：撤销被拒绝（409），结果保留。"""
    did, token = make_running(factory, scenario_id)
    with factory() as s:
        assert publish(s, did, token, OUTCOME, utcnow()) == 'completed'
        s.commit()
    with pytest.raises(HTTPException) as exc:
        with factory() as s:
            cancel_drill(s, did)
    assert exc.value.status_code == 409
    row = fetch(factory, did)
    assert row.status == 'completed' and row.result == OUTCOME[2]

def test_failed_result_kept_and_cancel_rejected(factory, scenario_id):
    """计算失败同样是保留的终态结果，拒绝撤销。"""
    did, token = make_running(factory, scenario_id)
    failed = ('failed', 'failed', {'error': '必须且只能配置一个原主节点', 'simulation': True})
    with factory() as s:
        assert publish(s, did, token, failed, utcnow()) == 'failed'
        s.commit()
    with pytest.raises(HTTPException) as exc:
        with factory() as s:
            cancel_drill(s, did)
    assert exc.value.status_code == 409
    row = fetch(factory, did)
    assert row.status == 'failed' and row.result['error']

def test_cancel_complete_race_exactly_one_winner(factory, scenario_id):
    """并发撤销与发布多轮对撞：每轮恰好一个终态，且状态、结果、原因自洽。"""
    for i in range(25):
        did, token = make_running(factory, scenario_id, label=f'race-{i}')
        barrier = threading.Barrier(2)
        outcome = {}
        def do_cancel():
            barrier.wait()
            with factory() as s:
                try:
                    cancel_drill(s, did); s.commit()
                    outcome['cancel'] = 'ok'
                except HTTPException as exc:
                    s.rollback(); outcome['cancel'] = exc.status_code
        def do_publish():
            barrier.wait()
            with factory() as s:
                outcome['publish'] = publish(s, did, token, OUTCOME, utcnow())
                s.commit()
        threads = [threading.Thread(target=do_cancel), threading.Thread(target=do_publish)]
        for t in threads: t.start()
        for t in threads: t.join()
        row = fetch(factory, did)
        if outcome['publish'] == 'completed':
            assert outcome['cancel'] == 409
            assert row.status == 'completed' and row.result is not None
        else:
            assert outcome['publish'] == 'discarded' and outcome['cancel'] == 'ok'
            assert row.status == 'canceled' and row.result is None
        assert row.finished_at is not None and row.finish_reason in ('completed', 'user_cancel')
