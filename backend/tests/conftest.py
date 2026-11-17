import os
os.environ.setdefault('DATABASE_URL', 'sqlite:///:memory:')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('DRILL_MAX_ACTIVE', '20')
import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import sessionmaker
from src.config.database import Base
from src.models.entities import User, Node, Scenario, QueueState
from src.utils.security import hash_password

PASSWORD_HASH = hash_password('Drill2026!')  # 测试内复用同一份哈希，避免重复 PBKDF2

@pytest.fixture()
def factory(tmp_path):
    """每个用例独立 SQLite 文件库；BEGIN IMMEDIATE 让事务在首个语句即取写锁，
    与 MySQL 上行锁/互斥行等效地串行化并发写入。"""
    engine = create_engine(f'sqlite:///{tmp_path}/test.db',
                           connect_args={'check_same_thread': False, 'timeout': 30})
    @event.listens_for(engine, 'begin')
    def _begin_immediate(conn): conn.exec_driver_sql('BEGIN IMMEDIATE')
    Base.metadata.create_all(engine)
    make = sessionmaker(bind=engine, expire_on_commit=False)
    with make() as s:
        s.add(QueueState(id=1))
        s.add(Scenario(name='晚班隔离演练', fail_primary=True, fence_primary=True,
                       max_data_loss=0, required_term=7))
        for name, role, pos in [('hs-primary', 'leader', 8100), ('hs-east', 'follower', 8100),
                                ('hs-west', 'follower', 8098)]:
            s.add(Node(name=name, role=role, term=6, log_position=pos,
                       available=True, fenced=False, vote_enabled=True))
        for role in ('admin', 'operator', 'viewer'):
            s.add(User(email=f'{role}@hengshi.local', role=role, password_hash=PASSWORD_HASH))
        s.commit()
    yield make
    engine.dispose()

@pytest.fixture()
def scenario_id(factory):
    with factory() as s:
        return s.scalar(select(Scenario.id))
