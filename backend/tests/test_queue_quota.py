"""满额竞争：并发接纳不超额，撤销/超时回收后不漏接，撤销重试不重复释放。"""
import threading
from datetime import timedelta
import pytest
from fastapi import HTTPException
from sqlalchemy import select, func
from src.constants.enums import ACTIVE_STATUSES
from src.models.entities import Drill
from src.schemas.drill import DrillInput
from src.services import drill_service
from src.services.drill_service import create_drill, cancel_drill, utcnow

def enqueue(factory, scenario_id, label, **kw):
    with factory() as s:
        obj = create_drill(s, DrillInput(scenario_id=scenario_id, label=label, **kw))
        s.commit()
        return obj.id

def active(factory):
    with factory() as s:
        return s.scalar(select(func.count(Drill.id)).where(Drill.status.in_(ACTIVE_STATUSES)))

def test_full_quota_contention(factory, scenario_id):
    """19 个在列，10 个线程同时抢最后 1 个名额：恰好 1 个入队，其余 429，总额不超 20。"""
    for i in range(19): enqueue(factory, scenario_id, f'pre-{i}')
    barrier = threading.Barrier(10)
    admitted, rejected = [], []
    def attempt(i):
        barrier.wait()
        with factory() as s:
            try:
                create_drill(s, DrillInput(scenario_id=scenario_id, label=f'race-{i}'))
                s.commit(); admitted.append(i)
            except HTTPException as exc:
                s.rollback()
                assert exc.status_code == 429
                rejected.append(i)
    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(admitted) == 1 and len(rejected) == 9
    assert active(factory) == 20

def test_cancel_releases_slot_and_retry_not_double(factory, scenario_id):
    """撤销立即归还名额；重试幂等，不重复释放；回收的名额可被后续接纳。"""
    ids = [enqueue(factory, scenario_id, f'pre-{i}') for i in range(20)]
    assert active(factory) == 20
    with factory() as s:
        _, changed = cancel_drill(s, ids[0]); assert changed
        s.commit()
    assert active(factory) == 19
    with factory() as s:
        obj, changed = cancel_drill(s, ids[0])  # 撤销重试
        assert not changed and obj.status == 'canceled'
        s.commit()
    assert active(factory) == 19  # 没有第二次释放
    enqueue(factory, scenario_id, 'after-cancel')
    assert active(factory) == 20
    with pytest.raises(HTTPException) as exc:
        with factory() as s:
            create_drill(s, DrillInput(scenario_id=scenario_id, label='overflow'))
    assert exc.value.status_code == 429

def test_freed_slots_all_reusable(factory, scenario_id):
    """20 个全部撤销后，20 个并发接纳全部成功，既不超额也不漏接。"""
    ids = [enqueue(factory, scenario_id, f'pre-{i}') for i in range(20)]
    barrier = threading.Barrier(20)
    def do_cancel(did):
        barrier.wait()
        with factory() as s:
            cancel_drill(s, did); s.commit()
    threads = [threading.Thread(target=do_cancel, args=(d,)) for d in ids]
    for t in threads: t.start()
    for t in threads: t.join()
    assert active(factory) == 0
    barrier = threading.Barrier(20)
    admitted = []
    def do_admit(i):
        barrier.wait()
        with factory() as s:
            try:
                create_drill(s, DrillInput(scenario_id=scenario_id, label=f'again-{i}'))
                s.commit(); admitted.append(i)
            except HTTPException:
                s.rollback()
    threads = [threading.Thread(target=do_admit, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(admitted) == 20
    assert active(factory) == 20

def test_concurrent_cancel_and_admit_never_exceeds(factory, scenario_id):
    """10 撤销与 10 接纳并发交错：活跃数永不超过 20，终态与计数自洽。"""
    ids = [enqueue(factory, scenario_id, f'pre-{i}') for i in range(20)]
    barrier = threading.Barrier(20)
    admitted = []
    errors = []
    def do_cancel(did):
        barrier.wait()
        with factory() as s:
            cancel_drill(s, did); s.commit()
    def do_admit(i):
        barrier.wait()
        with factory() as s:
            try:
                create_drill(s, DrillInput(scenario_id=scenario_id, label=f'mix-{i}'))
                s.commit(); admitted.append(i)
            except HTTPException as exc:
                s.rollback()
                if exc.status_code != 429: errors.append(exc)
    threads = ([threading.Thread(target=do_cancel, args=(d,)) for d in ids[:10]] +
               [threading.Thread(target=do_admit, args=(i,)) for i in range(10)])
    for t in threads: t.start()
    for t in threads: t.join()
    assert not errors
    final = active(factory)
    assert final == 10 + len(admitted)  # 10 个撤销必定生效，接纳数与终态一致
    assert final <= 20
    enqueue(factory, scenario_id, 'still-accepting')  # 有空位即可再接纳，不漏接

def test_expired_slots_reclaimed_on_admission(factory, scenario_id, monkeypatch):
    """满额且全部超时时，接纳路径同步清扫：超时名额立即归还，新演练得以入队。"""
    for i in range(20): enqueue(factory, scenario_id, f'ttl-{i}', ttl_seconds=60)
    assert active(factory) == 20
    later = utcnow() + timedelta(seconds=61)
    monkeypatch.setattr(drill_service, 'utcnow', lambda: later)
    new_id = enqueue(factory, scenario_id, 'after-expiry')
    assert active(factory) == 1
    with factory() as s:
        rows = s.scalars(select(Drill).where(Drill.status == 'expired')).all()
        assert len(rows) == 20
        assert all(r.finish_reason == 'deadline_expired' and r.finished_at is not None for r in rows)
        assert s.get(Drill, new_id).status == 'queued'
