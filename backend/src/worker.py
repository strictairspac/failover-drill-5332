import logging, time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4
from sqlalchemy import select, or_
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL, DRILL_CLAIM_TTL_SECONDS
from src.models.entities import Drill
from src.services.drill_service import sweep_expired, utcnow
from src.services.election import elect

CLAIM_TTL = timedelta(seconds=DRILL_CLAIM_TTL_SECONDS)

def claim_next(db, ts):
    """认领最早一条未过期的排队演练；认领令牌用于崩溃恢复与迟到发布的去重。"""
    job = db.scalar(select(Drill).where(Drill.status == 'queued',
                                        or_(Drill.deadline_at.is_(None), Drill.deadline_at > ts))
                    .order_by(Drill.id).with_for_update(skip_locked=True).limit(1))
    if job is None: return None
    job.status = 'running'
    job.claimed_at = ts
    job.claim_token = uuid4().hex
    return job

def publish(db, drill_id, token, outcome, ts):
    """终态写入的唯一入口：行锁内复核状态、认领令牌与截止时间，撤销/超时后迟到的计算不再发布。"""
    row = db.get(Drill, drill_id, with_for_update=True)
    if row is None: return 'discarded'
    if row.status != 'running' or row.claim_token != token:
        return 'discarded'  # 已被撤销或被崩溃回收重新认领，丢弃本次计算
    row.claim_token = None
    row.claimed_at = None
    if row.deadline_at is not None and row.deadline_at <= ts:
        row.status = 'expired'
        row.finish_reason = 'deadline_expired'
        row.finished_at = ts
        return 'expired'  # 超过截止时间的迟到计算不发布结果
    status, reason, result = outcome
    row.status = status
    row.finish_reason = reason
    row.result = result
    row.finished_at = ts
    return status

def recover_orphans(db, ts, claim_ttl=CLAIM_TTL):
    """执行进程崩溃后：认领超期且未到期的任务沿用冻结输入重新排队；已过期的终结。"""
    stale_before = ts - claim_ttl
    rows = db.scalars(select(Drill).where(Drill.status == 'running',
                                          Drill.claimed_at.is_not(None),
                                          Drill.claimed_at < stale_before)
                      .with_for_update(skip_locked=True)).all()
    for row in rows:
        if row.deadline_at is not None and row.deadline_at <= ts:
            row.status = 'expired'
            row.finish_reason = 'deadline_expired'
            row.finished_at = ts
        else:
            row.status = 'queued'  # 快照在入队时冻结，重新排队不改变计算输入
        row.claim_token = None
        row.claimed_at = None
    return rows

def run_one(session_factory=Session, compute=elect, ts=None):
    ts = ts or utcnow()
    with session_factory() as db:
        job = claim_next(db, ts)
        if job is None: return None
        db.commit()  # 认领先行落库：崩溃后留下 running 记录供恢复
        drill_id, token, snapshot = job.id, job.claim_token, job.snapshot
    try:
        outcome = ('completed', 'completed', compute(snapshot['nodes'], snapshot['scenario']))
    except ValueError as exc:
        outcome = ('failed', 'failed', {'error': str(exc), 'simulation': True})
    with session_factory() as db:
        final = publish(db, drill_id, token, outcome, utcnow())
        db.commit()
    logging.info('drill %s %s', drill_id, final)
    return final

def tick(session_factory=Session):
    with session_factory() as db:
        sweep_expired(db, utcnow())
        db.commit()
    with session_factory() as db:
        recover_orphans(db, utcnow())
        db.commit()
    handled = 0
    while handled < 64 and run_one(session_factory): handled += 1
    return handled

def loop():
    redis = Redis.from_url(REDIS_URL)
    # Redis leadership lock ensures only one worker processes the durable DB queue.
    while True:
        Path('/tmp/heartbeat').touch()
        lock = redis.lock('drill-worker', timeout=15, blocking_timeout=1)
        try:
            with lock:
                tick()
        except Exception:
            logging.exception('worker retry')
        time.sleep(1)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loop()
