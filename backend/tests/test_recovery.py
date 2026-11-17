"""领走后崩溃：未到期任务沿用冻结输入恢复执行，过期任务终结；迟到计算不发布。"""
from datetime import timedelta
from src.models.entities import Drill
from src.schemas.drill import DrillInput
from src.services.drill_service import create_drill, utcnow, active_count
from src.worker import claim_next, publish, recover_orphans, run_one, CLAIM_TTL

OUTCOME = ('completed', 'completed', {'decision': 'approved', 'winner': 'hs-east', 'simulation': True})

def make_queued(factory, scenario_id, label='crash', **kw):
    with factory() as s:
        obj = create_drill(s, DrillInput(scenario_id=scenario_id, label=label, **kw))
        s.commit()
        return obj.id, obj.snapshot

def claim(factory, did):
    with factory() as s:
        job = claim_next(s, utcnow())
        assert job.id == did
        s.commit()
        return job.claim_token

def fetch(factory, did):
    with factory() as s:
        return s.get(Drill, did)

def test_crash_after_claim_recovers_with_frozen_input(factory, scenario_id):
    """认领后崩溃：恢复为排队，快照不变，随后由新认领完成计算。"""
    did, frozen = make_queued(factory, scenario_id, ttl_seconds=3600)
    token = claim(factory, did)
    assert token  # 执行进程在此崩溃：不认领新会话、不发布
    with factory() as s:
        recovered = recover_orphans(s, utcnow() + CLAIM_TTL + timedelta(seconds=1))
        s.commit()
        assert [r.id for r in recovered] == [did]
    row = fetch(factory, did)
    assert row.status == 'queued' and row.claim_token is None
    assert row.snapshot == frozen  # 沿用冻结输入
    assert run_one(factory) == 'completed'
    row = fetch(factory, did)
    assert row.status == 'completed' and row.result['input_sha256']
    assert row.snapshot == frozen

def test_crash_after_claim_expired_terminated(factory, scenario_id):
    """认领后崩溃且已过截止时间：恢复扫描直接终结为过期，名额归还。"""
    did, _ = make_queued(factory, scenario_id, ttl_seconds=10)
    claim(factory, did)
    with factory() as s:
        recovered = recover_orphans(s, utcnow() + CLAIM_TTL + timedelta(seconds=1))
        s.commit()
        assert [r.id for r in recovered] == [did]
    row = fetch(factory, did)
    assert row.status == 'expired' and row.finish_reason == 'deadline_expired'
    assert row.result is None and row.finished_at is not None
    with factory() as s:
        assert active_count(s) == 0

def test_fresh_claim_not_recovered(factory, scenario_id):
    """认领未超期的执行中任务不被误恢复。"""
    did, _ = make_queued(factory, scenario_id, ttl_seconds=3600)
    token = claim(factory, did)
    with factory() as s:
        assert recover_orphans(s, utcnow()) == []
        s.commit()
    row = fetch(factory, did)
    assert row.status == 'running' and row.claim_token == token

def test_late_result_not_published(factory, scenario_id):
    """计算完成时已过截止时间：不发布结果，终结为过期。"""
    did, _ = make_queued(factory, scenario_id, ttl_seconds=5)
    token = claim(factory, did)
    with factory() as s:
        assert publish(s, did, token, OUTCOME, utcnow() + timedelta(seconds=10)) == 'expired'
        s.commit()
    row = fetch(factory, did)
    assert row.status == 'expired' and row.result is None
    assert row.finish_reason == 'deadline_expired'

def test_stale_token_publish_discarded(factory, scenario_id):
    """崩溃恢复被重新认领后，旧认领令牌的迟到发布被丢弃。"""
    did, _ = make_queued(factory, scenario_id, ttl_seconds=3600)
    old_token = claim(factory, did)
    with factory() as s:
        recover_orphans(s, utcnow() + CLAIM_TTL + timedelta(seconds=1))
        s.commit()
    new_token = claim(factory, did)
    assert new_token != old_token
    with factory() as s:
        assert publish(s, did, old_token, OUTCOME, utcnow()) == 'discarded'
        s.commit()
    with factory() as s:
        assert publish(s, did, new_token, OUTCOME, utcnow()) == 'completed'
        s.commit()
    row = fetch(factory, did)
    assert row.status == 'completed' and row.result == OUTCOME[2]
