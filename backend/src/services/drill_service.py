from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from src.config.settings import DRILL_MAX_ACTIVE, DRILL_MAX_TTL_SECONDS
from src.constants.enums import ACTIVE_STATUSES
from src.models.entities import Scenario, Drill, QueueState
from src.services.node_service import list_nodes
from src.utils.serialize import record

def utcnow(): return datetime.now(timezone.utc).replace(tzinfo=None)

def lock_queue(db):
    """接纳互斥：事务内锁定 queue_state 单行，串行化配额检查与入队。"""
    state = db.get(QueueState, 1, with_for_update=True)
    if state is None:
        try:
            with db.begin_nested():
                db.add(QueueState(id=1))
        except IntegrityError:
            pass
        state = db.get(QueueState, 1, with_for_update=True)
    return state

def sweep_expired(db, ts):
    """把已过截止时间的活跃演练终结为 expired，名额随状态翻转立即归还。"""
    rows = db.scalars(select(Drill).where(Drill.status.in_(ACTIVE_STATUSES),
                                          Drill.deadline_at.is_not(None),
                                          Drill.deadline_at <= ts)
                      .with_for_update(skip_locked=True)).all()
    for row in rows:
        row.status = 'expired'
        row.finish_reason = 'deadline_expired'
        row.finished_at = ts
        row.claim_token = None
        row.claimed_at = None
    return rows

def active_count(db):
    return db.scalar(select(func.count(Drill.id)).where(Drill.status.in_(ACTIVE_STATUSES)))

def resolve_deadline(payload, ts):
    if payload.deadline_at is not None:
        deadline = payload.deadline_at
        if deadline.tzinfo is not None:
            deadline = deadline.astimezone(timezone.utc).replace(tzinfo=None)
    elif payload.ttl_seconds is not None:
        deadline = ts + timedelta(seconds=payload.ttl_seconds)
    else:
        return None
    if deadline <= ts: raise HTTPException(422, '截止时间必须晚于当前时间')
    if (deadline - ts).total_seconds() > DRILL_MAX_TTL_SECONDS:
        raise HTTPException(422, '截止时间超出允许的最长跨度')
    return deadline

def create_drill(db, payload):
    scenario = db.get(Scenario, payload.scenario_id)
    if not scenario: raise HTTPException(404, '故障剧本不存在')
    ts = utcnow()
    deadline = resolve_deadline(payload, ts)
    lock_queue(db)
    sweep_expired(db, ts)  # 超时名额在接纳前同步回收，不因 worker 停摆而漏接
    if active_count(db) >= DRILL_MAX_ACTIVE:
        raise HTTPException(429, f'队列配额{DRILL_MAX_ACTIVE}已满，请稍后重试')
    obj = Drill(scenario_id=scenario.id, label=payload.label, status='queued', deadline_at=deadline,
                snapshot={'nodes': list_nodes(db), 'scenario': record(scenario)})
    db.add(obj); db.flush(); return obj

def cancel_drill(db, drill_id):
    """撤销排队或执行中的演练。重复撤销幂等返回；已终结的拒绝撤销。返回 (演练, 是否本次翻转)。"""
    obj = db.get(Drill, drill_id, with_for_update=True)
    if obj is None: raise HTTPException(404, '演练不存在')
    if obj.status in ACTIVE_STATUSES:
        obj.status = 'canceled'
        obj.finish_reason = 'user_cancel'
        obj.finished_at = utcnow()
        obj.claim_token = None
        obj.claimed_at = None
        return obj, True
    if obj.status == 'canceled':
        return obj, False  # 撤销重试：状态已翻转，不重复释放名额
    raise HTTPException(409, '演练已终结，结果保留，拒绝撤销')

def list_drills(db): return [record(d) for d in db.scalars(select(Drill).order_by(Drill.id.desc()).limit(100))]
def get_drill(db, drill_id):
    obj = db.get(Drill, drill_id)
    if obj is None: raise HTTPException(404, '演练不存在')
    return obj
