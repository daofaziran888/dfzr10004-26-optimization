"""采集任务编排业务规则：状态流转、排队绑定与筛选口径都收在这里。

计划清单会带上排队信息（排队状态、排队位置、预计释放时间、计划待办），
计划详情与列表看到的是同一份数据，避免前端各拼一份。
"""
from __future__ import annotations

from typing import Any

from app.services.scheduling import ScheduleError, queue_service
from app.store import store

MODULE = "taskplan"
REQUIRED_FIELDS = ["计划编号", "目标站点", "采集频率"]
STATUS_ORDER = ["草稿", "运行中", "已暂停", "已完成"]
ACTION_RULES = {"启动计划": "运行中", "暂停计划": "已暂停", "恢复计划": "运行中", "归档计划": "已完成"}
ACTION_SOURCES = {
    "启动计划": {"草稿", "已暂停"},
    "暂停计划": {"运行中"},
    "恢复计划": {"已暂停"},
    "归档计划": {"已暂停", "已完成"},
}
NEGATIVE_ACTIONS = ["暂停计划"]
QUEUE_FIELDS = ["排队状态", "排队位置", "预计释放时间", "等待轮数", "待办", "排队记录"]


class TaskplanService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("计划编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def pending_rows(self) -> list[dict[str, Any]]:
        """计划待办：等待、运行、释放中的计划，按排队位置排序。"""
        rows = [
            row for row in store.rows(MODULE)
            if row.get("排队状态") in ("等待中", "运行中", "释放中")
        ]
        rows.sort(key=lambda row: (
            {"等待中": 0, "运行中": 1, "释放中": 2}.get(str(row.get("排队状态")), 3),
            int(row.get("排队位置") or 0),
            int(row.get("id", 0)),
        ))
        return rows

    def waiting_entries(self) -> list[dict[str, Any]]:
        """待等待清单：只取等待中的计划，按所属策略和排队位置排序。"""
        snapshot = queue_service.waiting_records()
        plan_index = {int(row.get("id", 0)): row for row in store.rows(MODULE)}
        items = []
        for item in snapshot["items"]:
            plan = plan_index.get(int(item.get("plan_id") or 0))
            merged = dict(item)
            if plan is not None:
                merged.update({
                    "目标站点": plan.get("目标站点"),
                    "采集频率": plan.get("采集频率"),
                    "计划状态": plan.get("status"),
                    "待办": plan.get("待办"),
                })
            items.append(merged)
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in ("数据范围", "优先级", "资源配额", "预计用时"):
            if values.get(field):
                entry[field] = values.get(field)
        entry.setdefault("优先级", "中")
        entry["status"] = STATUS_ORDER[0]
        entry["计划状态"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["待办"] = "等待第 1 轮调度"
        rows.append(entry)
        return entry, []

    def submit(self, entry_id: int, values: dict[str, Any]) -> dict[str, Any]:
        """提交采集请求：同一计划只保留一个等待位置，真正分配由轮次调度决定。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise ScheduleError(f"采集计划 {entry_id} 不存在或已归档")
        if entry.get("status") == "已完成":
            raise ScheduleError(f"采集计划 {entry_id} 已归档，不能提交请求")
        if entry.get("status") == "已暂停":
            raise ScheduleError(f"采集计划 {entry_id} 已暂停，请先恢复计划再提交请求")
        strategy_id = values.get("strategy_id") or values.get("策略编号")
        try:
            strategy_id = int(strategy_id) if strategy_id not in (None, "") else None
        except (TypeError, ValueError) as exc:
            raise ScheduleError("策略编号必须是整数") from exc
        return queue_service.submit(
            plan_id=entry_id,
            strategy_id=strategy_id,
            request_key=values.get("request_key") or values.get("请求编号"),
            priority=values.get("priority") or values.get("优先级"),
            now=values.get("now"),
            auto_allocate=bool(values.get("auto_allocate", False)),
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集计划 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集任务编排可执行范围"
        current = str(entry.get("status") or "")
        allowed = ACTION_SOURCES.get(action, set())
        if current not in allowed:
            return None, f"采集计划当前状态「{current}」不能执行「{action}」"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["计划状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        entry["待办"] = {
            "启动计划": "等待第 1 轮调度",
            "暂停计划": "已暂停，排队记录保留",
            "恢复计划": "等待下一轮调度",
            "归档计划": "已归档",
        }[action]
        return entry, f"采集计划已{action}"
