"""采集请求排队调度：把频率策略的日上限、并发、优先级权重与排队策略落到轮次上。

判定口径（冲突时按顺序取第一条为准）：

1. 日请求上限是硬约束：额度耗尽后，无论优先级多高都不再分配名额，等待队列
   给出明确的排队位置和预计释放时间（次日额度恢复）。
2. 稳定优先级：额度之内，默认按「计划优先级 × 策略优先级权重」排序；同分时按
   提交时间、排队记录编号做稳定排序，保证同一份数据每次调度结果一致。
3. 公平性兜底：等待轮数会带来有限的优先级补偿；任何计划连续占用达到
   「连续占用上限」后，只要还有别的计划在等，就必须让出一轮并记为降级。
4. 唯一排队位置：同一计划在同一策略下最多保留一条「等待中」记录；运行中 /
   释放中的请求不算排队位置，计划可以提前提交下一轮请求，但等待队列仍只占一个位置。
5. 状态生命周期：等待中 → 运行中 → 释放中 → 已收口，只允许向前流转。
   已收口是终态；切换优先级只更新当前活动记录，恢复排队会新建等待记录，
   历史已收口记录不会被改回任何活动阶段。

所有会改状态的入口都在同一把可重入锁里完成「检查额度 → 占名额 → 写记录」，
因此两个轮次同时争抢最后一个名额时只可能有一个请求拿到占用。
"""
from __future__ import annotations

import threading
from datetime import datetime, time, timedelta, timezone
from typing import Any

from app.store import store

WAITING = "等待中"
RUNNING = "运行中"
RELEASING = "释放中"
CLOSED = "已收口"
LIFECYCLE = [WAITING, RUNNING, RELEASING, CLOSED]
ACTIVE_STATES = {WAITING, RUNNING, RELEASING}
# 只有运行中的请求占用并发名额：进入释放中表示请求已经结束、额度已释放，
# 只是还在等收口确认，不再挡住下一轮。
OCCUPYING_STATES = {RUNNING}

DEFAULT_INTERVAL_SECONDS = 60
DEFAULT_DAILY_LIMIT = 100
DEFAULT_CONCURRENCY = 1
DEFAULT_WEIGHT = 1
DEFAULT_MAX_CONSECUTIVE_ROUNDS = 2
DEFAULT_WAIT_TIMEOUT_ROUNDS = 3
DEFAULT_WAIT_TIMEOUT_SECONDS = 1800
AGING_ROUNDS = 3
MAX_AGING_BOOST = 2

PRIORITY_SCORES = {"紧急": 5, "最高": 4, "高": 3, "中": 2, "低": 1, "最低": 1}
STRATEGY_FIFO = "fifo"
STRATEGY_ROUND_ROBIN = "round_robin"
STRATEGY_PRIORITY = "priority"
STRATEGY_LABELS = {
    STRATEGY_FIFO: "先进先出",
    STRATEGY_ROUND_ROBIN: "轮转",
    STRATEGY_PRIORITY: "优先级加权",
}

QUEUE_MODULE = "_queue_records"
QUOTA_MODULE = "_queue_quota"
STATE_MODULE = "_queue_plan_state"
ROUND_MODULE = "_queue_rounds"

_ACTION_ALIASES = {
    "开始请求": "start",
    "开始": "start",
    "start": "start",
    "占用名额": "start",
    "释放请求": "release",
    "释放": "release",
    "release": "release",
    "收口请求": "close",
    "收口": "close",
    "close": "close",
    "恢复排队": "restore",
    "恢复": "restore",
    "restore": "restore",
    "切换优先级": "priority",
    "priority": "priority",
}

# 调度状态是整个服务共享的，锁也必须共享：不同路由即使各自实例化 QueueService，
# 也不能绕开同一把锁去抢最后一个名额。
_SCHEDULER_LOCK = threading.RLock()


class ScheduleError(Exception):
    """排队调度里可预期的业务错误，路由层直接转成可读提示。"""


# --------------------------------------------------------------------------- 基础工具
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_now(value: Any = None) -> datetime:
    """把接口传入的 now / 时间字符串统一成带时区的 UTC 时间。"""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ScheduleError("时间「" + value + "」无法解析，请使用 ISO 8601 格式") from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    if value is not None and not isinstance(value, str):
        raise ScheduleError("时间「" + str(value) + "」无法解析，请使用 ISO 8601 格式")
    return _utcnow()


def _iso(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _coerce_now(str(value))
    except ScheduleError:
        return None


def _day_key(now: datetime) -> str:
    return now.astimezone(timezone.utc).date().isoformat()


def _next_day_start(now: datetime) -> datetime:
    day = now.astimezone(timezone.utc).date()
    return datetime.combine(day + timedelta(days=1), time.min, tzinfo=timezone.utc)


def _digit_runs(text: Any) -> list[str]:
    runs: list[str] = []
    current: list[str] = []
    for char in str(text or ""):
        if char.isdigit():
            current.append(char)
        elif current:
            runs.append("".join(current))
            current = []
    if current:
        runs.append("".join(current))
    return runs


def _first_int(text: Any, default: int, minimum: int = 1) -> int:
    runs = _digit_runs(text)
    if not runs:
        return default
    return max(int(runs[0]), minimum)


def _parse_interval_seconds(text: Any) -> int:
    raw = str(text or "").strip()
    if not raw:
        return DEFAULT_INTERVAL_SECONDS
    runs = _digit_runs(raw)
    if not runs:
        return DEFAULT_INTERVAL_SECONDS
    number = float(runs[0])
    lowered = raw.lower()
    if "毫秒" in lowered or "ms" in lowered:
        return max(int(number / 1000), 1)
    if "小时" in lowered or "h" in lowered:
        return max(int(number * 3600), 1)
    if "分钟" in lowered or "min" in lowered:
        return max(int(number * 60), 1)
    return max(int(number), 1)


def priority_score(text: Any) -> int:
    raw = str(text or "").strip()
    if raw in PRIORITY_SCORES:
        return PRIORITY_SCORES[raw]
    runs = _digit_runs(raw)
    if runs:
        return max(int(runs[0]), 1)
    return PRIORITY_SCORES["中"]


def strategy_key(text: Any) -> str:
    raw = str(text or "").strip().lower()
    if "轮转" in raw or "round" in raw or raw == "rr":
        return STRATEGY_ROUND_ROBIN
    if "先进" in raw or "fifo" in raw or "先到" in raw:
        return STRATEGY_FIFO
    return STRATEGY_PRIORITY


def strategy_label(key: str) -> str:
    return STRATEGY_LABELS.get(key, STRATEGY_LABELS[STRATEGY_PRIORITY])


# --------------------------------------------------------------------------- 数据访问
def _record_id() -> int:
    rows = store.rows(QUEUE_MODULE)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _find_record(record_id: int) -> dict[str, Any] | None:
    return store.find(QUEUE_MODULE, record_id)


def _find_plan(plan_id: int) -> dict[str, Any] | None:
    return store.find("taskplan", plan_id)


def _find_strategy(strategy_id: int) -> dict[str, Any] | None:
    return store.find("throttle", strategy_id)


def _resolve_strategy(plan: dict[str, Any], strategy_id: int | None) -> dict[str, Any]:
    """按策略编号 → 目标站点 → 唯一启用策略的顺序绑定，保证计划有确定的额度来源。"""
    if strategy_id is not None:
        strategy = _find_strategy(strategy_id)
        if strategy is None:
            raise ScheduleError("频率策略 " + str(strategy_id) + " 不存在或已归档")
        return strategy
    site = str(plan.get("目标站点") or "").strip()
    candidates = [
        row for row in store.rows("throttle")
        if row.get("status") in ("已启用", "待校验") and str(row.get("目标站点") or "").strip() == site
    ]
    if candidates:
        enabled = [row for row in candidates if row.get("status") == "已启用"]
        return sorted(enabled or candidates, key=lambda row: int(row.get("id", 0)))[0]
    enabled_any = [row for row in store.rows("throttle") if row.get("status") == "已启用"]
    if len(enabled_any) == 1:
        return enabled_any[0]
    raise ScheduleError(
        "采集计划 " + str(plan.get("计划编号") or plan.get("id")) + " 没有可用的频率策略，请先指定策略编号"
    )


def strategy_limits(strategy: dict[str, Any]) -> dict[str, Any]:
    """把策略字段解析成可执行的额度、并发与排序参数，缺省值保持可用。"""
    daily_limit = _first_int(strategy.get("日请求上限"), DEFAULT_DAILY_LIMIT, minimum=0)
    concurrency = _first_int(strategy.get("并发数量"), DEFAULT_CONCURRENCY)
    weight = _first_int(strategy.get("优先级权重"), DEFAULT_WEIGHT)
    interval = _parse_interval_seconds(strategy.get("请求间隔"))
    max_consecutive = _first_int(strategy.get("连续占用上限"), DEFAULT_MAX_CONSECUTIVE_ROUNDS)
    timeout_rounds = _first_int(strategy.get("等待超时轮数"), DEFAULT_WAIT_TIMEOUT_ROUNDS)
    timeout_seconds = _first_int(strategy.get("等待超时秒数"), DEFAULT_WAIT_TIMEOUT_SECONDS)
    key = strategy_key(strategy.get("排队策略"))
    return {
        "daily_limit": daily_limit,
        "concurrency": concurrency,
        "weight": weight,
        "interval_seconds": interval,
        "strategy": key,
        "strategy_label": strategy_label(key),
        "max_consecutive": max_consecutive,
        "timeout_rounds": timeout_rounds,
        "timeout_seconds": timeout_seconds,
    }


def _quota_row(strategy_id: int, day: str) -> dict[str, Any]:
    for row in store.rows(QUOTA_MODULE):
        if int(row.get("strategy_id", 0)) == strategy_id and row.get("day") == day:
            return row
    row = {"id": len(store.rows(QUOTA_MODULE)) + 1, "strategy_id": strategy_id, "day": day, "used": 0}
    store.rows(QUOTA_MODULE).append(row)
    return row


def _plan_state(strategy_id: int, plan_id: int) -> dict[str, Any]:
    for row in store.rows(STATE_MODULE):
        if int(row.get("strategy_id", 0)) == strategy_id and int(row.get("plan_id", 0)) == plan_id:
            return row
    row = {
        "id": len(store.rows(STATE_MODULE)) + 1,
        "strategy_id": strategy_id,
        "plan_id": plan_id,
        "consecutive": 0,
        "rounds": 0,
        "last_started_at": "",
    }
    store.rows(STATE_MODULE).append(row)
    return row


def _round_row(strategy_id: int) -> dict[str, Any]:
    for row in store.rows(ROUND_MODULE):
        if int(row.get("strategy_id", 0)) == strategy_id:
            return row
    row = {
        "id": len(store.rows(ROUND_MODULE)) + 1,
        "strategy_id": strategy_id,
        "round": 0,
        "at": "",
        "consumed_this_round": 0,
        "admitted": [],
        "degraded": [],
    }
    store.rows(ROUND_MODULE).append(row)
    return row


def _waiting_rows(strategy_id: int) -> list[dict[str, Any]]:
    return [
        row for row in store.rows(QUEUE_MODULE)
        if int(row.get("strategy_id", 0)) == strategy_id and row.get("状态") == WAITING
    ]


def _occupying_rows(strategy_id: int) -> list[dict[str, Any]]:
    return [
        row for row in store.rows(QUEUE_MODULE)
        if int(row.get("strategy_id", 0)) == strategy_id and row.get("状态") in OCCUPYING_STATES
    ]


def _quota_snapshot(strategy_id: int, now: datetime, limits: dict[str, Any]) -> dict[str, Any]:
    quota = _quota_row(strategy_id, _day_key(now))
    used = int(quota.get("used", 0))
    limit = int(limits["daily_limit"])
    return {
        "day": quota["day"],
        "日上限": limit,
        "日请求上限": limit,
        "已消费额度": used,
        "剩余额度": max(limit - used, 0),
        "重置时间": _iso(_next_day_start(now)),
        "耗尽": used >= limit,
    }


def _estimate_release(strategy_id: int, now: datetime, limits: dict[str, Any]) -> datetime:
    """估算下一个名额释放时间：并发占满时取最早完成的占用记录。"""
    candidates: list[datetime] = []
    for row in _occupying_rows(strategy_id):
        if row.get("状态") == RUNNING:
            started = _parse_time(row.get("开始时间")) or now
            candidates.append(started + timedelta(seconds=limits["interval_seconds"]))
        else:
            released = _parse_time(row.get("释放时间")) or now
            candidates.append(released + timedelta(seconds=limits["interval_seconds"]))
    if candidates:
        return min(candidates)
    return now + timedelta(seconds=limits["interval_seconds"])


def _effective_score(row: dict[str, Any], limits: dict[str, Any]) -> float:
    weight = int(row.get("优先级权重") or limits["weight"] or 1)
    base = priority_score(row.get("计划优先级")) * max(weight, 1)
    aging = min(int(row.get("等待轮数", 0)) // AGING_ROUNDS, MAX_AGING_BOOST)
    return float(base + aging)


def _sort_key(limits: dict[str, Any], now: datetime):
    def key(row: dict[str, Any]):
        submitted = _parse_time(row.get("提交时间")) or now
        score = _effective_score(row, limits)
        consecutive = int(_plan_state(int(row["strategy_id"]), int(row.get("plan_id", 0))).get("consecutive", 0))
        if limits["strategy"] == STRATEGY_FIFO:
            return (submitted, int(row.get("id", 0)))
        if limits["strategy"] == STRATEGY_ROUND_ROBIN:
            return (consecutive, -score, submitted, int(row.get("id", 0)))
        return (-score, consecutive, submitted, int(row.get("id", 0)))
    return key


def _mark_degraded(row: dict[str, Any], reason: str, at: str) -> None:
    row["被降级"] = True
    row["降级原因"] = reason
    entries = row.get("降级记录")
    if not isinstance(entries, list):
        entries = []
    entries.append({"原因": reason, "发生时间": at})
    row["降级记录"] = entries[-5:]


def _degraded_ids(round_row: dict[str, Any] | None) -> list[int]:
    if round_row is None:
        return []
    ids = round_row.get("degraded")
    return ids if isinstance(ids, list) else []


def _record_degraded(round_row: dict[str, Any] | None, record_id: int) -> None:
    if round_row is None:
        return
    ids = list(_degraded_ids(round_row))
    if record_id not in ids:
        ids.append(record_id)
    round_row["degraded"] = ids


def _waiting_view(
    strategy_id: int,
    now: datetime,
    limits: dict[str, Any],
) -> list[dict[str, Any]]:
    """按排队策略排序等待记录，并把位置、等待原因、预计释放时间写回记录。

    返回的顺序就是可观察的等待顺序：位置 1 是下一轮最先拿到名额的计划。
    """
    ordered = sorted(_waiting_rows(strategy_id), key=_sort_key(limits, now))
    quota = _quota_snapshot(strategy_id, now, limits)
    occupying = _occupying_rows(strategy_id)
    free_slots = max(int(limits["concurrency"]) - len(occupying), 0)
    release_at = _estimate_release(strategy_id, now, limits)
    cap = int(limits["max_consecutive"])
    view: list[dict[str, Any]] = []
    for index, row in enumerate(ordered, start=1):
        state = _plan_state(strategy_id, int(row.get("plan_id", 0)))
        consecutive = int(state.get("consecutive", 0))
        blocked_by_cap = consecutive >= cap
        if quota["耗尽"]:
            reason = "日请求上限已耗尽，等待次日额度恢复"
            estimated = quota["重置时间"]
        elif free_slots <= 0:
            reason = "并发名额已占满，等待运行中的请求释放"
            estimated = _iso(release_at)
        elif blocked_by_cap:
            reason = "连续占用已达上限 " + str(cap) + " 轮，本轮让出名额"
            estimated = _iso(now + timedelta(seconds=limits["interval_seconds"]))
        elif index > free_slots:
            reason = "本轮名额已分配给优先级更高的计划"
            estimated = _iso(now + timedelta(seconds=limits["interval_seconds"]))
        else:
            reason = "等待轮次调度分配名额"
            estimated = _iso(now)
        if row.get("被降级") and row.get("降级原因") and not quota["耗尽"]:
            reason = str(row["降级原因"])
        row["排队位置"] = index
        row["等待原因"] = reason
        row["预计释放时间"] = estimated
        row["排队策略"] = limits["strategy_label"]
        row["日请求上限"] = limits["daily_limit"]
        row["并发数量"] = limits["concurrency"]
        submitted = _parse_time(row.get("提交时间")) or now
        view.append({
            "record_id": row.get("id"),
            "plan_id": row.get("plan_id"),
            "计划编号": row.get("计划编号"),
            "策略编号": row.get("策略编号"),
            "状态": WAITING,
            "计划优先级": row.get("计划优先级"),
            "有效优先级": round(_effective_score(row, limits), 2),
            "排队位置": index,
            "等待轮数": int(row.get("等待轮数", 0)),
            "等待秒数": max(int((now - submitted).total_seconds()), 0),
            "等待原因": reason,
            "预计释放时间": estimated,
            "被降级": bool(row.get("被降级")),
            "降级原因": row.get("降级原因") or "",
            "超时提醒": bool(row.get("超时提醒")),
            "下一次流转": row.get("下一次流转", ""),
        })
    return view


def _sync_plan(row: dict[str, Any], clock: datetime) -> None:
    """把排队状态写回采集计划清单，让计划待办跟着轮次更新。"""
    plan = _find_plan(int(row.get("plan_id", 0)))
    if plan is None:
        return
    state = row.get("状态")
    plan["排队记录"] = row.get("id")
    plan["排队状态"] = state
    plan["排队位置"] = int(row.get("排队位置", 0) or 0) if state == WAITING else 0
    plan["预计释放时间"] = row.get("预计释放时间", "")
    plan["等待轮数"] = int(row.get("等待轮数", 0))
    plan["关联策略"] = row.get("策略编号")
    plan["最近流转"] = _iso(clock)
    if state == WAITING:
        plan["pending"] = True
        plan["待办"] = "等待第 " + str(int(row.get("等待轮数", 0)) + 1) + " 轮调度，当前排队位置 " + str(plan["排队位置"])
        plan["abnormal"] = bool(row.get("超时提醒"))
    elif state == RUNNING:
        plan["pending"] = True
        plan["待办"] = "第 " + str(int(row.get("轮次", 0))) + " 轮运行中，等待释放"
        plan["abnormal"] = False
    elif state == RELEASING:
        plan["pending"] = True
        plan["待办"] = "第 " + str(int(row.get("轮次", 0))) + " 轮释放中，等待收口"
        plan["abnormal"] = False
    else:
        plan["pending"] = False
        plan["待办"] = "本轮已收口"
        plan["abnormal"] = False


class QueueService:
    """排队调度入口：提交、轮次推进、生命周期流转与可观察的队列快照。"""

    def __init__(self) -> None:
        self._lock = _SCHEDULER_LOCK

    # ------------------------------------------------------------------ 提交
    def submit(
        self,
        *,
        plan_id: int,
        strategy_id: int | None = None,
        request_key: str | None = None,
        priority: str | None = None,
        now: Any = None,
        auto_allocate: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            plan = _find_plan(plan_id)
            if plan is None:
                raise ScheduleError("采集计划 " + str(plan_id) + " 不存在或已归档")
            strategy = _resolve_strategy(plan, strategy_id)
            sid = int(strategy["id"])
            limits = strategy_limits(strategy)
            key = str(request_key or "").strip()

            if key:
                duplicate = next(
                    (row for row in store.rows(QUEUE_MODULE)
                     if int(row.get("strategy_id", 0)) == sid
                     and row.get("request_key") == key
                     and row.get("状态") in ACTIVE_STATES),
                    None,
                )
                if duplicate is not None:
                    self._refresh(sid, clock, limits)
                    payload = self._present(duplicate, limits, now=clock)
                    payload["duplicate"] = True
                    payload["message"] = "同一请求已占用排队位置，未重复入队"
                    return payload

            existing = next(
                (row for row in _waiting_rows(sid) if int(row.get("plan_id", 0)) == int(plan_id)),
                None,
            )
            if existing is not None:
                existing["request_key"] = existing.get("request_key") or key
                existing["计划优先级"] = priority or existing.get("计划优先级")
                self._refresh(sid, clock, limits)
                payload = self._present(existing, limits, now=clock)
                payload["duplicate"] = True
                payload["message"] = "该计划已有排队位置，沿用原等待顺序"
                return payload

            record: dict[str, Any] = {
                "id": _record_id(),
                "plan_id": plan_id,
                "plan_name": plan.get("计划编号") or str(plan_id),
                "计划编号": plan.get("计划编号") or str(plan_id),
                "strategy_id": sid,
                "策略编号": strategy.get("策略编号") or str(sid),
                "目标站点": strategy.get("目标站点") or plan.get("目标站点"),
                "request_key": key,
                "计划优先级": priority or plan.get("优先级") or "中",
                "优先级权重": limits["weight"],
                "排队策略": limits["strategy_label"],
                "状态": WAITING,
                "轮次": 0,
                "提交时间": _iso(clock),
                "开始时间": "",
                "释放时间": "",
                "收口时间": "",
                "等待轮数": 0,
                "排队位置": 0,
                "预计释放时间": "",
                "等待原因": "等待分配名额",
                "已消费额度": 0,
                "被降级": False,
                "降级原因": "",
                "降级记录": [],
                "超时提醒": False,
                "提醒编号": "",
                "生命周期": list(LIFECYCLE),
                "下一次流转": RUNNING,
                "本轮更新时间": _iso(clock),
            }
            store.rows(QUEUE_MODULE).append(record)
            if auto_allocate:
                self._admit(sid, clock, limits)
            self._refresh(sid, clock, limits)
            payload = self._present(record, limits, now=clock)
            payload["duplicate"] = False
            payload["message"] = "已进入等待队列"
            return payload

    # ------------------------------------------------------------------ 轮次
    def run_round(
        self,
        *,
        strategy_id: int | None = None,
        now: Any = None,
        auto_advance: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            strategies = self._strategies(strategy_id)
            boards: list[dict[str, Any]] = []
            for strategy in strategies:
                sid = int(strategy["id"])
                limits = strategy_limits(strategy)
                round_row = _round_row(sid)
                round_row["round"] = int(round_row.get("round", 0)) + 1
                round_row["at"] = _iso(clock)
                round_row["consumed_this_round"] = 0
                round_row["admitted"] = []
                round_row["degraded"] = []
                if auto_advance:
                    self._advance_lifecycle(sid, clock, limits)
                self._admit(sid, clock, limits, round_row=round_row)
                for row in _waiting_rows(sid):
                    row["等待轮数"] = int(row.get("等待轮数", 0)) + 1
                    row["本轮更新时间"] = _iso(clock)
                self._check_timeouts(sid, clock, limits)
                self._refresh(sid, clock, limits, round_row=round_row)
                boards.append(self._board(sid, clock, limits, round_row=round_row))
            return {
                "ok": True,
                "轮次": boards[0]["轮次"] if len(boards) == 1 else max((item["轮次"] for item in boards), default=0),
                "策略": boards,
            }

    def _advance_lifecycle(self, sid: int, clock: datetime, limits: dict[str, Any]) -> None:
        for row in store.rows(QUEUE_MODULE):
            if int(row.get("strategy_id", 0)) != sid:
                continue
            if row.get("状态") == RUNNING:
                started = _parse_time(row.get("开始时间")) or clock
                if clock >= started + timedelta(seconds=limits["interval_seconds"]):
                    row["状态"] = RELEASING
                    row["释放时间"] = _iso(clock)
                    row["下一次流转"] = CLOSED
                    row["本轮更新时间"] = _iso(clock)
            elif row.get("状态") == RELEASING:
                released = _parse_time(row.get("释放时间")) or clock
                if clock >= released + timedelta(seconds=limits["interval_seconds"]):
                    row["状态"] = CLOSED
                    row["收口时间"] = _iso(clock)
                    row["下一次流转"] = ""
                    row["本轮更新时间"] = _iso(clock)

    def _admit(
        self,
        sid: int,
        clock: datetime,
        limits: dict[str, Any],
        *,
        round_row: dict[str, Any] | None = None,
    ) -> list[int]:
        """在额度与并发允许的范围内按顺序分配名额；调用方必须持有调度锁。"""
        admitted: list[int] = []
        for _ in range(1000):
            quota = _quota_snapshot(sid, clock, limits)
            occupying = _occupying_rows(sid)
            if quota["耗尽"] or len(occupying) >= int(limits["concurrency"]):
                break
            ordered = sorted(_waiting_rows(sid), key=_sort_key(limits, clock))
            if not ordered:
                break
            candidate = self._pick_candidate(sid, ordered, limits, round_row, clock)
            if candidate is None:
                break
            quota_row = _quota_row(sid, _day_key(clock))
            quota_row["used"] = int(quota_row.get("used", 0)) + 1
            candidate["状态"] = RUNNING
            candidate["开始时间"] = _iso(clock)
            candidate["轮次"] = int(_round_row(sid).get("round", 0))
            candidate["已消费额度"] = int(candidate.get("已消费额度", 0)) + 1
            candidate["等待原因"] = ""
            candidate["预计释放时间"] = _iso(clock + timedelta(seconds=limits["interval_seconds"]))
            candidate["下一次流转"] = RELEASING
            candidate["本轮更新时间"] = _iso(clock)
            state = _plan_state(sid, int(candidate.get("plan_id", 0)))
            state["consecutive"] = int(state.get("consecutive", 0)) + 1
            state["rounds"] = int(state.get("rounds", 0)) + 1
            state["last_started_at"] = _iso(clock)
            for other in store.rows(STATE_MODULE):
                if int(other.get("strategy_id", 0)) == sid and int(other.get("plan_id", 0)) != int(candidate.get("plan_id", 0)):
                    other["consecutive"] = 0
            admitted.append(candidate["id"])
            if round_row is not None:
                round_row["admitted"] = list(round_row.get("admitted", [])) + [candidate["id"]]
                round_row["consumed_this_round"] = int(round_row.get("consumed_this_round", 0)) + 1
            _sync_plan(candidate, clock)
        return admitted

    def _pick_candidate(
        self,
        sid: int,
        ordered: list[dict[str, Any]],
        limits: dict[str, Any],
        round_row: dict[str, Any] | None,
        clock: datetime,
    ) -> dict[str, Any] | None:
        """高优先计划连续占满上限时让位；排序本身是稳定优先级。"""
        cap = int(limits["max_consecutive"])
        eligible: list[dict[str, Any]] = []
        capped: list[dict[str, Any]] = []
        for row in ordered:
            consecutive = int(_plan_state(sid, int(row.get("plan_id", 0))).get("consecutive", 0))
            (capped if consecutive >= cap else eligible).append(row)
        for row in capped:
            if len(ordered) > 1:
                _mark_degraded(row, "连续占用已达上限 " + str(cap) + " 轮，本轮让出名额", _iso(clock))
                _record_degraded(round_row, int(row["id"]))
        if eligible:
            candidate = eligible[0]
            for row in eligible[1:]:
                row["等待原因"] = "本轮名额已分配给优先级更高的计划"
            return candidate
        if len(ordered) == 1:
            return capped[0]
        return None

    # ------------------------------------------------------------------ 流转
    def transition(self, record_id: int, action: str, *, now: Any = None) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            record = self._require_record(record_id)
            strategy = _find_strategy(int(record["strategy_id"]))
            if strategy is None:
                raise ScheduleError("排队记录关联的频率策略已归档，无法继续流转")
            limits = strategy_limits(strategy)
            normalized = str(action or "").strip()
            op = _ACTION_ALIASES.get(normalized)
            if op is None:
                raise ScheduleError("动作「" + normalized + "」不属于排队调度可执行范围")
            if op == "restore":
                return self.restore(record_id, now=clock)
            if op == "priority":
                raise ScheduleError("切换优先级请同时提交目标优先级字段")
            current = record.get("状态")
            if current == CLOSED:
                raise ScheduleError("排队记录已收口，不能再回到任何活动阶段")
            if op == "start":
                if current != WAITING:
                    raise ScheduleError("只有等待中的记录可以开始请求，当前状态：" + str(current))
                self._start_exclusive(record, clock, limits)
            elif op == "release":
                if current != RUNNING:
                    raise ScheduleError("只有运行中的记录可以释放请求，当前状态：" + str(current))
                started = _parse_time(record.get("开始时间"))
                if started is not None and clock < started:
                    raise ScheduleError("释放时间早于开始时间，状态不能回退")
                record["状态"] = RELEASING
                record["释放时间"] = _iso(clock)
                record["下一次流转"] = CLOSED
            else:  # close
                if current != RELEASING:
                    raise ScheduleError("只有释放中的记录可以收口，当前状态：" + str(current))
                released = _parse_time(record.get("释放时间"))
                if released is not None and clock < released:
                    raise ScheduleError("收口时间早于释放时间，状态不能回退")
                record["状态"] = CLOSED
                record["收口时间"] = _iso(clock)
                record["下一次流转"] = ""
            record["本轮更新时间"] = _iso(clock)
            sid = int(record["strategy_id"])
            _sync_plan(record, clock)
            self._refresh(sid, clock, limits)
            payload = self._present(record, limits, now=clock)
            payload["message"] = "排队记录已" + normalized
            return payload

    def _start_exclusive(self, record: dict[str, Any], clock: datetime, limits: dict[str, Any]) -> None:
        """唯一占用：检查与写入在同一把锁内完成，最后一个名额只会被一个请求拿到。"""
        sid = int(record["strategy_id"])
        quota = _quota_snapshot(sid, clock, limits)
        if quota["耗尽"]:
            raise ScheduleError("日请求上限已耗尽，无法开始请求，请等待 " + quota["重置时间"] + " 额度恢复")
        occupying = _occupying_rows(sid)
        if len(occupying) >= int(limits["concurrency"]):
            raise ScheduleError("并发名额已占满，最后一个名额已被其他请求占用")
        cap = int(limits["max_consecutive"])
        state = _plan_state(sid, int(record.get("plan_id", 0)))
        if int(state.get("consecutive", 0)) >= cap:
            others = [row for row in _waiting_rows(sid) if int(row.get("id", 0)) != int(record["id"])]
            if others:
                _mark_degraded(record, "连续占用已达上限 " + str(cap) + " 轮，本轮让出名额", _iso(clock))
                raise ScheduleError("连续占用已达上限 " + str(cap) + " 轮，本轮应让出名额")
        ordered = sorted(_waiting_rows(sid), key=_sort_key(limits, clock))
        ahead = [row for row in ordered if int(row.get("id", 0)) != int(record["id"])]
        if ahead and limits["strategy"] != STRATEGY_FIFO:
            best = ahead[0]
            if _effective_score(best, limits) > _effective_score(record, limits):
                raise ScheduleError("本轮名额应分配给优先级更高的计划 " + str(best.get("计划编号")))
        quota_row = _quota_row(sid, _day_key(clock))
        quota_row["used"] = int(quota_row.get("used", 0)) + 1
        record["状态"] = RUNNING
        record["开始时间"] = _iso(clock)
        record["轮次"] = int(_round_row(sid).get("round", 0))
        record["已消费额度"] = int(record.get("已消费额度", 0)) + 1
        record["预计释放时间"] = _iso(clock + timedelta(seconds=limits["interval_seconds"]))
        record["下一次流转"] = RELEASING
        state["consecutive"] = int(state.get("consecutive", 0)) + 1
        state["rounds"] = int(state.get("rounds", 0)) + 1
        state["last_started_at"] = _iso(clock)
        for other in store.rows(STATE_MODULE):
            if int(other.get("strategy_id", 0)) == sid and int(other.get("plan_id", 0)) != int(record.get("plan_id", 0)):
                other["consecutive"] = 0

    def restore(self, record_id: int, *, now: Any = None) -> dict[str, Any]:
        """恢复排队：已收口记录保持收口，另起一条等待记录承接新轮次。"""
        with self._lock:
            clock = _coerce_now(now)
            record = self._require_record(record_id)
            strategy = _find_strategy(int(record["strategy_id"]))
            if strategy is None:
                raise ScheduleError("关联频率策略已归档，无法恢复排队")
            if record.get("状态") in ACTIVE_STATES:
                limits = strategy_limits(strategy)
                payload = self._present(record, limits, now=clock)
                payload["message"] = "记录仍在活动阶段，无需恢复排队"
                return payload
            plan = _find_plan(int(record["plan_id"]))
            if plan is None:
                raise ScheduleError("关联采集计划已归档，无法恢复排队")
            payload = self.submit(
                plan_id=int(record["plan_id"]),
                strategy_id=int(record["strategy_id"]),
                priority=record.get("计划优先级"),
                now=clock,
                auto_allocate=False,
            )
            payload["record"]["恢复自"] = record_id
            payload["message"] = "已新建等待记录，原记录保持已收口"
            return payload

    def set_priority(self, record_id: int, priority: str, *, now: Any = None) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            record = self._require_record(record_id)
            if record.get("状态") == CLOSED:
                raise ScheduleError("排队记录已收口，不能再切换优先级")
            if not str(priority or "").strip():
                raise ScheduleError("缺少目标优先级")
            plan = _find_plan(int(record["plan_id"]))
            if plan is not None:
                plan["优先级"] = priority
            record["计划优先级"] = priority
            record["本轮更新时间"] = _iso(clock)
            strategy = _find_strategy(int(record["strategy_id"]))
            limits = strategy_limits(strategy) if strategy else strategy_limits({})
            self._refresh(int(record["strategy_id"]), clock, limits)
            payload = self._present(record, limits, now=clock)
            payload["message"] = "优先级已切换为 " + str(priority)
            return payload

    # ------------------------------------------------------------------ 快照
    def _refresh(
        self,
        sid: int,
        clock: datetime,
        limits: dict[str, Any],
        *,
        round_row: dict[str, Any] | None = None,
    ) -> None:
        view = _waiting_view(sid, clock, limits)
        for index, item in enumerate(view, start=1):
            record = _find_record(int(item["record_id"]))
            if record is not None:
                record["排队位置"] = index
                record["等待原因"] = item["等待原因"]
                record["预计释放时间"] = item["预计释放时间"]
        for row in store.rows(QUEUE_MODULE):
            if int(row.get("strategy_id", 0)) == sid:
                _sync_plan(row, clock)
        strategy = _find_strategy(sid)
        if strategy is not None:
            strategy["排队中数量"] = len(view)
            strategy["运行中数量"] = len(_occupying_rows(sid))
            strategy["今日已消费额度"] = _quota_snapshot(sid, clock, limits)["已消费额度"]
            strategy["本轮已消费额度"] = int(_round_row(sid).get("consumed_this_round", 0))
            strategy["日请求上限"] = limits["daily_limit"]

    def _board(
        self,
        sid: int,
        clock: datetime,
        limits: dict[str, Any],
        *,
        round_row: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        strategy = _find_strategy(sid) or {}
        quota = _quota_snapshot(sid, clock, limits)
        round_row = round_row or _round_row(sid)
        waiting = _waiting_view(sid, clock, limits)
        running = []
        for row in _occupying_rows(sid):
            running.append({
                "record_id": row.get("id"),
                "计划编号": row.get("计划编号"),
                "状态": row.get("状态"),
                "轮次": int(row.get("轮次", 0)),
                "开始时间": row.get("开始时间"),
                "释放时间": row.get("释放时间"),
                "预计释放时间": row.get("预计释放时间", ""),
                "已消费额度": int(row.get("已消费额度", 0)),
                "下一次流转": row.get("下一次流转", ""),
            })
        return {
            "strategy_id": sid,
            "策略编号": strategy.get("策略编号") or str(sid),
            "目标站点": strategy.get("目标站点"),
            "排队策略": limits["strategy_label"],
            "排队策略值": limits["strategy"],
            "优先级权重": limits["weight"],
            "日请求上限": limits["daily_limit"],
            "并发数量": limits["concurrency"],
            "连续占用上限": limits["max_consecutive"],
            "轮次": int(round_row.get("round", 0)),
            "本轮": {
                "轮次": int(round_row.get("round", 0)),
                "时间": round_row.get("at") or _iso(clock),
                "已消费额度": int(round_row.get("consumed_this_round", 0)),
                "进入运行": list(round_row.get("admitted", [])),
                "被降级": list(round_row.get("degraded", [])),
            },
            "本轮已消费额度": int(round_row.get("consumed_this_round", 0)),
            "额度": quota,
            "运行中": running,
            "等待中": waiting,
            "被降级的计划": [item for item in waiting if item["被降级"]],
            "生命周期": list(LIFECYCLE),
            "可观察顺序": [item["record_id"] for item in waiting],
        }

    def _check_timeouts(self, sid: int, clock: datetime, limits: dict[str, Any]) -> None:
        """等待超时只提醒一次：提醒写入告警中心，记录打上超时标记后不再重复生成。"""
        strategy = _find_strategy(sid) or {}
        for row in _waiting_rows(sid):
            if row.get("超时提醒"):
                continue
            submitted = _parse_time(row.get("提交时间")) or clock
            waited_seconds = max(int((clock - submitted).total_seconds()), 0)
            waited_rounds = int(row.get("等待轮数", 0))
            over_rounds = waited_rounds >= int(limits["timeout_rounds"])
            over_seconds = waited_seconds >= int(limits["timeout_seconds"])
            if not (over_rounds or over_seconds):
                continue
            reason = "等待轮数达到上限" if over_rounds else "等待时长达到上限"
            alert = self._create_alert(row, strategy, reason, clock, waited_seconds, waited_rounds)
            row["超时提醒"] = True
            row["提醒编号"] = alert.get("规则编号")
            row["超时原因"] = reason
            row["超时提醒时间"] = _iso(clock)
            _sync_plan(row, clock)

    def _create_alert(
        self,
        row: dict[str, Any],
        strategy: dict[str, Any],
        reason: str,
        clock: datetime,
        waited_seconds: int,
        waited_rounds: int,
    ) -> dict[str, Any]:
        rows = store.rows("alert")
        alert_id = max((int(item.get("id", 0)) for item in rows), default=0) + 1
        alert = {
            "id": alert_id,
            "status": "待校验",
            "pending": True,
            "abnormal": True,
            "规则编号": "ALER-QUEUE-" + str(alert_id).rjust(4, "0"),
            "监控指标": "排队等待超时",
            "告警级别": "高",
            "触发条件": "等待超时：" + reason,
            "通知渠道": "站内提醒",
            "静默时间": _iso(clock)[:10],
            "升级策略": "等待 " + str(waited_rounds) + " 轮 / " + str(waited_seconds) + " 秒后升级到值班负责人",
            "是否启用": "是",
            "关联计划": row.get("计划编号"),
            "关联策略": strategy.get("策略编号"),
            "关联排队记录": row.get("id"),
            "提醒类型": "排队等待超时",
            "提醒原因": reason,
            "提醒时间": _iso(clock),
            "已消费额度": int(row.get("已消费额度", 0)),
            "排队位置": int(row.get("排队位置", 0)),
            "等待轮数": waited_rounds,
            "等待秒数": waited_seconds,
            "重复提醒": False,
        }
        rows.append(alert)
        return alert

    def _present(self, record: dict[str, Any], limits: dict[str, Any], *, now: datetime) -> dict[str, Any]:
        sid = int(record["strategy_id"])
        return {
            "ok": True,
            "record": dict(record),
            "排队位置": int(record.get("排队位置", 0) or 0),
            "预计释放时间": record.get("预计释放时间", ""),
            "本轮已消费额度": int(_round_row(sid).get("consumed_this_round", 0)),
            "今日已消费额度": _quota_snapshot(sid, now, limits)["已消费额度"],
        }

    def _strategies(self, strategy_id: int | None) -> list[dict[str, Any]]:
        if strategy_id is not None:
            strategy = _find_strategy(strategy_id)
            if strategy is None:
                raise ScheduleError("频率策略 " + str(strategy_id) + " 不存在或已归档")
            return [strategy]
        return [row for row in store.rows("throttle") if row.get("status") == "已启用"]

    def _require_record(self, record_id: int) -> dict[str, Any]:
        record = _find_record(record_id)
        if record is None:
            raise ScheduleError("排队记录 " + str(record_id) + " 不存在")
        return record

    def board(self, strategy_id: int, *, now: Any = None) -> dict[str, Any]:
        with self._lock:
            strategy = _find_strategy(strategy_id)
            if strategy is None:
                raise ScheduleError("频率策略 " + str(strategy_id) + " 不存在或已归档")
            clock = _coerce_now(now)
            limits = strategy_limits(strategy)
            self._refresh(strategy_id, clock, limits)
            return self._board(strategy_id, clock, limits)

    def boards(self, *, now: Any = None) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            result = []
            for strategy in self._strategies(None):
                sid = int(strategy["id"])
                limits = strategy_limits(strategy)
                self._refresh(sid, clock, limits)
                result.append(self._board(sid, clock, limits))
            return {"items": result, "total": len(result), "generated_at": _iso(clock)}

    def waiting_records(self, *, now: Any = None) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            rows = []
            for strategy in self._strategies(None):
                sid = int(strategy["id"])
                limits = strategy_limits(strategy)
                self._refresh(sid, clock, limits)
                for item in _waiting_view(sid, clock, limits):
                    rows.append(item)
            rows.sort(key=lambda item: (str(item.get("策略编号") or ""), int(item.get("排队位置") or 0),
                                        int(item.get("record_id") or 0)))
            return {"items": rows, "total": len(rows), "generated_at": _iso(clock)}

    def records(
        self,
        *,
        status: str | None = None,
        strategy_id: int | None = None,
        plan_id: int | None = None,
        now: Any = None,
    ) -> dict[str, Any]:
        with self._lock:
            clock = _coerce_now(now)
            rows = []
            for row in store.rows(QUEUE_MODULE):
                if status and row.get("状态") != status:
                    continue
                if strategy_id is not None and int(row.get("strategy_id", 0)) != strategy_id:
                    continue
                if plan_id is not None and int(row.get("plan_id", 0)) != plan_id:
                    continue
                view = dict(row)
                view["排队记录"] = row.get("id")
                if row.get("状态") == WAITING:
                    submitted = _parse_time(row.get("提交时间")) or clock
                    view["等待秒数"] = max(int((clock - submitted).total_seconds()), 0)
                rows.append(view)
            rows.sort(key=lambda item: int(item.get("id", 0)))
            return {"items": rows, "total": len(rows)}

    def get_record(self, record_id: int) -> dict[str, Any]:
        with self._lock:
            return dict(self._require_record(record_id))


queue_service = QueueService()
