"""接口层回归：验证排队入口、静态路径顺序与错误翻译，不重复服务层场景。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.store import Store


@pytest.fixture()
def fresh(monkeypatch) -> Store:
    """路由与服务共用同一份全新仓库，避免测试之间残留排队记录。"""
    store = Store()
    for target in (
        "app.services.scheduling.store",
        "app.services.taskplan.store",
        "app.services.throttle.store",
        "app.services.alert.store",
    ):
        monkeypatch.setattr(target, store, raising=False)
    return store


@pytest.fixture()
def client(fresh) -> TestClient:
    return TestClient(app)


def test_static_queue_paths_not_shadowed(client):
    """/queue 等静态路径不能被 /{entry_id} 抢走。"""
    for path in ("/api/throttle/queue", "/api/throttle/queue/waiting", "/api/throttle/queue/records"):
        response = client.get(path)
        assert response.status_code == 200, path


def test_submission_reports_position_and_duplicate(client):
    payload = client.post("/api/taskplan/1/queue/submissions", json={"values": {}}).json()
    assert payload["ok"] is True
    assert payload["排队位置"] == 1
    again = client.post("/api/taskplan/1/queue/submissions", json={"values": {}}).json()
    assert again["duplicate"] is True
    assert again["record"]["id"] == payload["record"]["id"]


def test_round_board_exposes_order_and_degradation(client):
    for pid in (4, 3, 2, 1):
        client.post(f"/api/taskplan/{pid}/queue/submissions", json={"values": {}})
    board = client.post(
        "/api/throttle/1/queue/rounds",
        json={"values": {"now": "2026-11-13T08:00:10", "auto_advance": False}},
    ).json()["策略"][0]
    assert board["额度"]["已消费额度"] == 3
    assert {item["计划编号"] for item in board["运行中"]} == {"TASK-0001", "TASK-0002", "TASK-0003"}
    assert board["等待中"][0]["排队位置"] == 1
    assert board["等待中"][0]["等待原因"].startswith("并发名额已占满")

    waiting = client.get("/api/taskplan/queue/waiting").json()
    assert waiting["total"] == 1
    assert waiting["items"][0]["计划编号"] == "TASK-0004"


def test_closed_record_cannot_reopen_via_api(client):
    client.post("/api/throttle/1/queue/rounds", json={"values": {"now": "2026-11-13T08:00:00"}})
    client.post("/api/taskplan/1/queue/submissions", json={"values": {}})
    record_id = client.get("/api/throttle/queue/records").json()["items"][0]["id"]
    steps = [
        ("开始请求", "2026-11-13T08:00:05"),
        ("释放请求", "2026-11-13T08:00:30"),
        ("收口请求", "2026-11-13T08:01:10"),
    ]
    for action, now in steps:
        assert client.post(
            f"/api/throttle/queue/records/{record_id}/actions",
            json={"action": action, "now": now},
        ).json()["ok"] is True

    denied = client.post(
        f"/api/throttle/queue/records/{record_id}/actions",
        json={"action": "切换优先级", "priority": "低"},
    ).json()
    assert denied["ok"] is False
    assert "已收口" in denied["message"]

    restored = client.post(
        f"/api/throttle/queue/records/{record_id}/actions",
        json={"action": "恢复排队", "now": "2026-11-13T08:02:00"},
    ).json()
    assert restored["entry"]["状态"] == "等待中"
    original = client.get(f"/api/throttle/queue/records/{record_id}").json()
    assert original["状态"] == "已收口"


def test_alert_confirm_action(client, fresh):
    """排队超时提醒可在告警中心确认，确认后状态为已确认且不能重复确认。"""
    strategy = fresh.find("throttle", 1)
    strategy["等待超时轮数"] = "1"
    strategy["等待超时秒数"] = "30"
    strategy["日请求上限"] = "1"
    strategy["并发数量"] = "2"
    client.post("/api/taskplan/1/queue/submissions", json={"values": {"now": "2026-11-13T08:00:00"}})
    client.post("/api/taskplan/2/queue/submissions", json={"values": {"now": "2026-11-13T08:00:00"}})
    client.post("/api/throttle/1/queue/rounds", json={"values": {"now": "2026-11-13T08:00:40"}})

    alerts = client.get("/api/alert/queue").json()
    assert alerts["total"] == 1
    alert = alerts["items"][0]
    assert alert["关联计划"] == "TASK-0002"
    confirmed = client.post(f"/api/alert/{alert['id']}/actions", json={"values": {"action": "确认提醒"}}).json()
    assert confirmed["ok"] is True
    assert confirmed["entry"]["status"] == "已确认"
    again = client.post(f"/api/alert/{alert['id']}/actions", json={"values": {"action": "确认提醒"}}).json()
    assert again["ok"] is False

    # 再推进一轮也不会重复提醒。
    client.post("/api/throttle/1/queue/rounds", json={"values": {"now": "2026-11-13T08:05:00"}})
    assert client.get("/api/alert/queue").json()["total"] == 1
