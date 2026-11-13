"""采集频率控制接口：维护频率策略，并把策略额度落到排队轮次上。

路由层只做参数整理和错误翻译，状态流转全部在 app.services 里完成。
静态路径（/queue、/export）必须排在 /{entry_id} 前面，否则会被当成策略编号。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult, QueueActionPayload, QueueSubmitPayload
from app.services.scheduling import LIFECYCLE, QueueService, ScheduleError
from app.services.throttle import ThrottleService

router = APIRouter(prefix="/api/throttle", tags=["采集频率控制"])

service = ThrottleService()
queue = QueueService()

LIST_FIELDS = ["策略编号", "目标站点", "请求间隔", "日请求上限", "并发数量", "排队策略", "优先级权重", "是否启用"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]


def _queue_error(exc: ScheduleError) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("/queue", response_model=dict)
def list_queues(now: str | None = Query(default=None, description="可选，按指定时间生成快照")) -> dict:
    """查看所有已启用策略的排队看板：额度、运行中、等待顺序与被降级计划。"""
    try:
        return queue.boards(now=now)
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.get("/queue/waiting", response_model=dict)
def list_waiting(now: str | None = Query(default=None, description="可选，按指定时间生成快照")) -> dict:
    """待等待清单：跨策略汇总所有等待中的计划，附带排队位置与预计释放时间。"""
    try:
        return queue.waiting_records(now=now)
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.get("/queue/records", response_model=dict)
def list_records(
    status: str | None = Query(default=None, description="等待中、运行中、释放中、已收口"),
    strategy_id: int | None = Query(default=None, description="按频率策略过滤"),
    plan_id: int | None = Query(default=None, description="按采集计划过滤"),
) -> dict:
    """排队记录流水：按生命周期阶段过滤，便于核对轮次历史。"""
    if status and status not in LIFECYCLE:
        raise HTTPException(status_code=400, detail="排队状态只允许：等待中、运行中、释放中、已收口")
    return queue.records(status=status, strategy_id=strategy_id, plan_id=plan_id)


@router.get("/queue/records/{record_id}", response_model=dict)
def get_record(record_id: int) -> dict:
    """读取单条排队记录：状态、轮次、消费额度与降级原因都在这里。"""
    try:
        return queue.get_record(record_id)
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.post("/queue/records/{record_id}/actions", response_model=ActionResult)
def run_record_action(record_id: int, payload: QueueActionPayload) -> ActionResult:
    """流转排队记录：开始请求、释放请求、收口请求、恢复排队、切换优先级。

    已收口的记录不能回到任何活动阶段；恢复排队会新建等待记录，旧记录保持已收口。
    切换优先级只更新当前活动记录，不会把一个已收口的轮次拉回等待中。
    """
    try:
        if str(payload.action or "").strip() in ("切换优先级", "priority"):
            result = queue.set_priority(record_id, payload.priority or "", now=payload.now)
        else:
            result = queue.transition(record_id, payload.action, now=payload.now)
    except ScheduleError as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(ok=True, message=result["message"], entry=result["record"])


@router.post("/queue/rounds", response_model=dict)
def run_all_rounds(payload: QueueSubmitPayload | None = None) -> dict:
    """推进所有已启用策略的一个轮次：释放到期名额、按策略分配新名额。"""
    values = payload.values if payload else {}
    try:
        return queue.run_round(now=values.get("now"), auto_advance=bool(values.get("auto_advance", True)))
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集频率控制清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "throttle", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤采集频率控制列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条频率策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="频率策略已登记", entry=entry)


@router.get("/{entry_id}/queue", response_model=dict)
def get_queue(entry_id: int, now: str | None = Query(default=None)) -> dict:
    """单个频率策略的排队看板：本轮已消费额度、等待顺序、预计释放时间。"""
    try:
        return queue.board(entry_id, now=now)
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.post("/{entry_id}/queue/rounds", response_model=dict)
def run_round(entry_id: int, payload: QueueSubmitPayload | None = None) -> dict:
    """推进单个策略的轮次；返回本轮进入运行、被降级的计划和额度快照。"""
    values = payload.values if payload else {}
    try:
        return queue.run_round(strategy_id=entry_id, now=values.get("now"),
                               auto_advance=bool(values.get("auto_advance", True)))
    except ScheduleError as exc:
        raise _queue_error(exc) from exc


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条频率策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"频率策略 {entry_id} 不存在或已归档")
    return entry


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条频率策略执行提交校验、启用策略、停用策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
