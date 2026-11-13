"""采集任务编排接口：维护采集计划，并把计划接入频率策略的排队调度。

- POST /{id}/queue/submissions 提交采集请求：同一计划只保留一个排队位置，
  同一 request_key 重复提交返回原位置，不会占用第二个名额。
- GET /queue/waiting 待等待清单：计划清单补入的等待视图，带当前排队位置。
- GET /{id}/queue 单个计划的排队明细，计划待办会跟着轮次更新。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult, QueueSubmitPayload
from app.services.scheduling import ScheduleError, queue_service
from app.services.taskplan import TaskplanService

router = APIRouter(prefix="/api/taskplan", tags=["采集任务编排"])

service = TaskplanService()

LIST_FIELDS = ["计划编号", "目标站点", "采集频率", "数据范围", "优先级", "资源配额", "预计用时", "计划状态"]
STATUSES = ["草稿", "运行中", "已暂停", "已完成"]


@router.get("/queue/waiting", response_model=dict)
def list_waiting(now: str | None = Query(default=None, description="可选，按指定时间生成快照")) -> dict:
    """待等待清单：所有等待中的采集计划，按排队位置给出可观察的等待顺序。"""
    try:
        items = service.waiting_entries()
    except ScheduleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"items": items, "total": len(items), "statuses": ["等待中"]}


@router.get("/pending", response_model=dict)
def list_pending(now: str | None = Query(default=None, description="可选，按指定时间生成快照")) -> dict:
    """计划待办：等待、运行、释放三个阶段的计划待办跟随轮次刷新。"""
    rows = service.pending_rows()
    return {"items": rows, "total": len(rows)}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按计划编号检索"),
    status: str | None = Query(default=None, description="草稿、运行中、已暂停、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按计划编号与状态过滤采集任务编排列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条采集计划，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="采集计划已登记", entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集任务编排清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "taskplan", "total": total, "items": items}


@router.get("/{entry_id}/queue", response_model=dict)
def get_plan_queue(entry_id: int, now: str | None = Query(default=None)) -> dict:
    """单个计划的排队明细：排队位置、等待原因、预计释放时间与轮次历史。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采集计划 {entry_id} 不存在或已归档")
    records = queue_service.records(plan_id=entry_id)["items"]
    board = None
    if entry.get("排队记录"):
        record = next((item for item in records if int(item["id"]) == int(entry["排队记录"])), None)
        if record is not None:
            try:
                board = queue_service.board(int(record["strategy_id"]), now=now)
            except ScheduleError:
                board = None
    return {"plan": entry, "records": records, "board": board}


@router.post("/{entry_id}/queue/submissions", response_model=dict)
def submit_request(entry_id: int, payload: QueueSubmitPayload | None = None) -> dict:
    """提交采集请求：同一计划只保留一个排队位置，重复提交沿用原位置。"""
    values = payload.values if payload else {}
    try:
        return service.submit(entry_id, values)
    except ScheduleError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条采集计划明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"采集计划 {entry_id} 不存在或已归档")
    return entry


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条采集计划执行启动计划、暂停计划、恢复计划、归档计划；动作与状态不匹配会被拦下。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
