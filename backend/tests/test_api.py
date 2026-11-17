"""HTTP 层：撤销端点、权限（观察员只读）、截止时间与终态原因的展示字段。"""
from datetime import datetime, timedelta, timezone
import pytest
from fastapi.testclient import TestClient
from src.config.database import get_db
from src.main import app
from src.models.entities import Drill

@pytest.fixture()
def client(factory):
    def override():
        with factory() as s:
            yield s
    app.dependency_overrides[get_db] = override
    yield TestClient(app)
    app.dependency_overrides.clear()

def login(client, role):
    r = client.post('/auth/login', json={'email': f'{role}@hengshi.local', 'password': 'Drill2026!'})
    assert r.status_code == 200
    return {'Authorization': 'Bearer ' + r.json()['access_token']}

def create(client, headers, scenario_id, **extra):
    r = client.post('/drills', json={'scenario_id': scenario_id, 'label': '晚班切主核验', **extra}, headers=headers)
    assert r.status_code == 202, r.text
    return r.json()

def test_create_with_deadline_and_cancel_flow(client, scenario_id):
    op = login(client, 'operator')
    job = create(client, op, scenario_id, ttl_seconds=600)
    assert job['status'] == 'queued' and job['deadline_at'] is not None
    r = client.post(f"/drills/{job['id']}/cancel", headers=op)
    assert r.status_code == 200
    body = r.json()
    assert body['status'] == 'canceled' and body['finish_reason'] == 'user_cancel'
    assert body['finished_at'] is not None
    again = client.post(f"/drills/{job['id']}/cancel", headers=op)  # 撤销重试幂等
    assert again.status_code == 200 and again.json()['status'] == 'canceled'

def test_create_without_deadline_keeps_null(client, scenario_id):
    op = login(client, 'operator')
    job = create(client, op, scenario_id)
    assert job['deadline_at'] is None

def test_completed_rejects_cancel_and_keeps_result(client, factory, scenario_id):
    op = login(client, 'operator')
    job = create(client, op, scenario_id)
    with factory() as s:
        row = s.get(Drill, job['id'])
        row.status = 'completed'
        row.finish_reason = 'completed'
        row.result = {'decision': 'approved', 'winner': 'hs-east', 'simulation': True}
        s.commit()
    r = client.post(f"/drills/{job['id']}/cancel", headers=op)
    assert r.status_code == 409
    detail = client.get(f"/drills/{job['id']}", headers=op).json()
    assert detail['status'] == 'completed' and detail['result']['decision'] == 'approved'

def test_viewer_readonly(client, scenario_id):
    op = login(client, 'operator')
    viewer = login(client, 'viewer')
    job = create(client, op, scenario_id)
    assert client.post('/drills', json={'scenario_id': scenario_id, 'label': '观察员提交'}, headers=viewer).status_code == 403
    assert client.post(f"/drills/{job['id']}/cancel", headers=viewer).status_code == 403
    assert client.get('/drills', headers=viewer).status_code == 200
    assert client.get(f"/drills/{job['id']}", headers=viewer).status_code == 200

def test_deadline_validation(client, scenario_id):
    op = login(client, 'operator')
    assert client.post('/drills', json={'scenario_id': scenario_id, 'label': '零秒', 'ttl_seconds': 0}, headers=op).status_code == 422
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    r = client.post('/drills', json={'scenario_id': scenario_id, 'label': '过去时刻', 'deadline_at': past}, headers=op)
    assert r.status_code == 422
    future = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
    job = create(client, op, scenario_id, deadline_at=future)
    assert job['deadline_at'] is not None

def test_cancel_unknown_drill_404(client):
    op = login(client, 'operator')
    assert client.post('/drills/9999/cancel', headers=op).status_code == 404
