"""排队调度核心场景测试：额度、唯一排队位置、唯一占用、连续占用上限与生命周期。

测试直接调用服务层，保证状态机在同一把锁内的判定不被路由层参数掩盖；
时间用伪时钟回放，避免依赖真实等待。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest

from app.services.scheduling import (
    CLOSED,
    LIFECYCLE,
    RELEASING,
    RUNNING,
    WAITING,
    QueueService,
    ScheduleError,
    strategy_limits,
)
from app.store import Store

@pytest.fixture()
def fresh(monkeypatch):
    """每个用例一份全新的内存仓库，避免排队记录跨用例串味。"""
    fresh_store = Store()
    monkeypatch.setattr("app.services.scheduling.store", fresh_store)
    monkeypatch.setattr("app.services.taskplan.store", fresh_store)
    monkeypatch.setattr("app.services.throttle.store", fresh_store)
    monkeypatch.setattr("app.services.alert.store", fresh_store)
    return fresh_store


def _values(store: Store, module: str, entry_id: int) -> dict:
    return store.find(module, entry_id)


def _now(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def test_priority_order_and_daily_quota(fresh):
    """额度内按优先级分配，剩余计划按稳定优先级排队并给出位置。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["并发数量"] = "1"
    service.submit(plan_id=4, strategy_id=1, now=_now("2026-11-13T08:00:00"))  # 低
    service.submit(plan_id=3, strategy_id=1, now=_now("2026-11-13T08:01:00"))  # 中
    service.submit(plan_id=2, strategy_id=1, now=_now("2026-11-13T08:02:00"))  # 高

    board = service.run_round(strategy_id=1, now=_now("2026-11-13T08:03:00"))["策略"][0]
    assert board["额度"]["已消费额度"] == 1
    assert board["额度"]["日请求上限"] == 1200
    assert [entry["计划编号"] for entry in board["运行中"]] == ["TASK-0002"]
    assert [entry["计划编号"] for entry in board["等待中"]] == ["TASK-0003", "TASK-0004"]
    assert [entry["排队位置"] for entry in board["等待中"]] == [1, 2]
    assert board["等待中"][0]["预计释放时间"]


def test_daily_quota_exhausted_blocks_everyone(fresh):
    """日请求上限是硬约束：额度用完后即使紧急计划也只能等待，并给出释放时间。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["日请求上限"] = "2"
    _values(fresh, "throttle", 1)["并发数量"] = "3"
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=2, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=3, strategy_id=1, now=_now("2026-11-13T08:00:00"))

    board = service.run_round(strategy_id=1, now=_now("2026-11-13T08:00:00"))["策略"][0]
    assert board["额度"]["已消费额度"] == 2
    assert board["额度"]["耗尽"] is True
    assert len(board["等待中"]) == 1
    waiting = board["等待中"][0]
    assert waiting["计划编号"] == "TASK-0003"
    assert waiting["等待原因"].startswith("日请求上限已耗尽")
    assert waiting["预计释放时间"].startswith("2026-11-14T00:00:00")


def test_duplicate_submission_keeps_single_slot(fresh):
    """同一计划重复提交、同一 request_key 重复提交都只保留一个等待位置。"""
    service = QueueService()
    first = service.submit(plan_id=1, strategy_id=1, request_key="REQ-1", now=_now("2026-11-13T08:00:00"))
    second = service.submit(plan_id=1, strategy_id=1, request_key="REQ-1", now=_now("2026-11-13T08:01:00"))
    third = service.submit(plan_id=1, strategy_id=1, request_key="REQ-2", now=_now("2026-11-13T08:02:00"))

    assert second["duplicate"] is True
    assert third["duplicate"] is True
    assert second["record"]["id"] == first["record"]["id"] == third["record"]["id"]
    assert service.records(status=WAITING)["total"] == 1


def test_concurrent_submissions_share_last_slot(fresh):
    """两个线程同时争抢最后一个名额，只接受唯一占用，另一个被明确拦下。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["日请求上限"] = "2"
    _values(fresh, "throttle", 1)["并发数量"] = "1"
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=2, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    ids = [row["id"] for row in service.records(status=WAITING)["items"]]
    assert len(ids) == 2

    results: list[tuple[str, str]] = []
    barrier = threading.Barrier(2)

    def attempt(record_id: int) -> None:
        barrier.wait()
        try:
            action = service.transition(record_id, "开始请求", now=_now("2026-11-13T08:00:05"))
            results.append(("ok", action["record"]["状态"]))
        except ScheduleError as exc:
            results.append(("error", str(exc)))

    threads = [threading.Thread(target=attempt, args=(record_id,)) for record_id in ids]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(kind for kind, _ in results) == ["error", "ok"]
    assert [state for kind, state in results if kind == "ok"] == [RUNNING]
    assert service.records(status=RUNNING)["total"] == 1
    assert service.board(1, now=_now("2026-11-13T08:00:05"))["额度"]["已消费额度"] == 1


def test_concurrent_rounds_only_use_last_slot_once(fresh):
    """两个轮次同时推进，争抢最后一个名额时只接受唯一占用。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["日请求上限"] = "3"
    _values(fresh, "throttle", 1)["并发数量"] = "2"
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=2, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=3, strategy_id=1, now=_now("2026-11-13T08:00:00"))

    results: list[int] = []
    barrier = threading.Barrier(2)

    def round_once() -> None:
        barrier.wait()
        board = service.run_round(strategy_id=1, now=_now("2026-11-13T08:00:10"))["策略"][0]
        results.append(board["额度"]["已消费额度"])

    threads = [threading.Thread(target=round_once) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # 两个轮次都看到累计额度 2，说明最后一个名额只被占用了一次；
    # 如果锁没生效，两个线程会各自读到一个空位，累计额度会变成 3。
    assert results == [2, 2]
    assert service.records(status=RUNNING)["total"] == 2
    assert service.records(status=WAITING)["total"] == 1
    assert service.board(1, now=_now("2026-11-13T08:00:10"))["额度"]["已消费额度"] == 2


def test_high_priority_consecutive_cap_degrades(fresh):
    """高优先计划连续占满上限后必须让位，并记录被降级原因。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["并发数量"] = "1"
    _values(fresh, "throttle", 1)["连续占用上限"] = "1"
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=4, strategy_id=1, now=_now("2026-11-13T08:00:00"))

    first = service.run_round(strategy_id=1, now=_now("2026-11-13T08:00:00"))["策略"][0]
    assert first["本轮"]["进入运行"]
    running = first["运行中"]
    assert running and running[0]["计划编号"] == "TASK-0001"

    # 下一轮两个计划都再次提交请求；高优先计划连续占用已达上限，必须让位。
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:01:00"))
    second = service.run_round(strategy_id=1, now=_now("2026-11-13T08:01:00"))["策略"][0]
    assert [entry["计划编号"] for entry in second["运行中"]] == ["TASK-0004"]
    degraded = second["被降级的计划"]
    assert any(item["计划编号"] == "TASK-0001" for item in degraded)
    assert any("连续占用" in item["降级原因"] for item in degraded)


def test_lifecycle_only_forward_and_closed_is_terminal(fresh):
    """状态只允许 等待中→运行中→释放中→已收口；已收口不能被恢复或切换优先级拉回。"""
    service = QueueService()
    payload = service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    record_id = payload["record"]["id"]

    service.transition(record_id, "开始请求", now=_now("2026-11-13T08:00:01"))
    assert service.get_record(record_id)["状态"] == RUNNING
    service.transition(record_id, "释放请求", now=_now("2026-11-13T08:00:30"))
    assert service.get_record(record_id)["状态"] == RELEASING
    service.transition(record_id, "收口请求", now=_now("2026-11-13T08:01:00"))
    record = service.get_record(record_id)
    assert record["状态"] == CLOSED
    assert record["生命周期"] == LIFECYCLE

    with pytest.raises(ScheduleError):
        service.set_priority(record_id, "紧急", now=_now("2026-11-13T08:02:00"))
    with pytest.raises(ScheduleError):
        service.transition(record_id, "开始请求", now=_now("2026-11-13T08:02:00"))

    restored = service.restore(record_id, now=_now("2026-11-13T08:02:00"))
    assert restored["record"]["id"] != record_id
    assert restored["record"]["恢复自"] == record_id
    assert service.get_record(record_id)["状态"] == CLOSED
    assert restored["record"]["状态"] == WAITING


def test_backward_transition_rejected(fresh):
    """不能从释放中回到运行中，也不能提前收口。"""
    service = QueueService()
    payload = service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    record_id = payload["record"]["id"]
    service.transition(record_id, "开始请求", now=_now("2026-11-13T08:00:01"))
    service.transition(record_id, "释放请求", now=_now("2026-11-13T08:00:10"))
    with pytest.raises(ScheduleError):
        service.transition(record_id, "开始请求", now=_now("2026-11-13T08:00:20"))
    with pytest.raises(ScheduleError):
        service.transition(record_id, "收口请求", now=_now("2026-11-13T08:00:05"))
    with pytest.raises(ScheduleError):
        service.transition(record_id, "释放请求", now=_now("2026-11-13T08:00:30"))


def test_wait_timeout_alerts_once(fresh):
    """等待超时在告警中心只生成一次提醒。"""
    service = QueueService()
    strategy = _values(fresh, "throttle", 1)
    strategy["等待超时轮数"] = "1"
    strategy["等待超时秒数"] = "30"
    strategy["日请求上限"] = "1"
    strategy["并发数量"] = "2"
    service.submit(plan_id=1, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=2, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.run_round(strategy_id=1, now=_now("2026-11-13T08:00:30"))

    alerts = [row for row in fresh.rows("alert") if row.get("提醒类型") == "排队等待超时"]
    assert len(alerts) == 1
    assert alerts[0]["关联计划"] == "TASK-0002"

    service.run_round(strategy_id=1, now=_now("2026-11-13T08:02:00"))
    alerts = [row for row in fresh.rows("alert") if row.get("提醒类型") == "排队等待超时"]
    assert len(alerts) == 1
    assert alerts[0]["重复提醒"] is False

    plan = fresh.find("taskplan", 2)
    assert plan["排队状态"] == WAITING
    assert plan["abnormal"] is True


def test_plan_list_follows_rounds(fresh):
    """计划清单与待办跟随轮次更新，等待中的计划显示排队位置。"""
    service = QueueService()
    _values(fresh, "throttle", 1)["并发数量"] = "1"
    service.submit(plan_id=4, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.submit(plan_id=3, strategy_id=1, now=_now("2026-11-13T08:00:00"))
    service.run_round(strategy_id=1, now=_now("2026-11-13T08:00:00"))

    plan3 = fresh.find("taskplan", 3)
    plan4 = fresh.find("taskplan", 4)
    assert plan3["排队状态"] == RUNNING
    assert plan4["排队状态"] == WAITING
    assert plan4["排队位置"] == 1
    assert plan4["预计释放时间"]
    assert "排队位置 1" in plan4["待办"]
    assert plan3["待办"].endswith("等待释放")


def test_strategy_limits_parse_text_fields():
    """策略字段是文本时也能解析出额度、并发和排队策略。"""
    limits = strategy_limits({"日请求上限": "1200 次", "并发数量": "3 个", "优先级权重": "2 倍",
                              "请求间隔": "30 秒", "排队策略": "优先级加权"})
    assert limits["daily_limit"] == 1200
    assert limits["concurrency"] == 3
    assert limits["weight"] == 2
    assert limits["interval_seconds"] == 30
    assert limits["strategy"] == "priority"
