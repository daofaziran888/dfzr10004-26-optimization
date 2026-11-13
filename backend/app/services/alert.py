"""采集告警中心业务规则：状态流转、排队超时提醒与筛选口径都收在这里。

排队等待超时的提醒由 app.services.scheduling 在轮次推进时写入本模块，
每条等待记录只会生成一次提醒；这里只负责让提醒可查、可确认、可关闭。
"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "alert"
REQUIRED_FIELDS = ["规则编号", "监控指标", "告警级别"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用规则": "已启用", "停用规则": "已停用", "确认提醒": "已确认"}
ACTION_SOURCES = {
    "提交校验": {"草稿", "待校验"},
    "启用规则": {"草稿", "待校验", "已确认"},
    "停用规则": {"已启用", "已确认", "待校验"},
    "确认提醒": {"待校验"},
}
NEGATIVE_ACTIONS = ["停用规则"]
QUEUE_ALERT_FIELDS = ["提醒类型", "关联计划", "关联策略", "排队位置", "等待轮数", "等待秒数", "提醒时间"]


class AlertService:
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
            rows = [
                row for row in rows
                if keyword in str(row.get("规则编号", "")) or keyword in str(row.get("关联计划", ""))
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def queue_alerts(self) -> list[dict[str, Any]]:
        """排队等待超时提醒：只保留排队调度生成的提醒，按提醒时间倒序。"""
        rows = [row for row in store.rows(MODULE) if row.get("提醒类型") == "排队等待超时"]
        rows.sort(key=lambda row: (str(row.get("提醒时间") or ""), int(row.get("id", 0)),), reverse=True)
        return rows

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for field in ("触发条件", "通知渠道", "静默时间", "升级策略", "是否启用"):
            if values.get(field):
                entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"告警规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集告警中心可执行范围"
        current = str(entry.get("status") or "")
        allowed = ACTION_SOURCES.get(action, set())
        if current not in allowed:
            return None, f"告警规则当前状态「{current}」不能执行「{action}」"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target in ("草稿", "待校验", "已启用")
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "确认提醒":
            entry["提醒状态"] = "已确认"
        return entry, f"告警规则已{action}"
