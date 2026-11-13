"""任务依赖编排接口：维护依赖图节点，覆盖启动节点、暂停节点、归档节点等动作。

动作与批次事件都会驱动共用路径引擎做增量重算，返回值里带上批次信息，
便于前端展示受影响路径、复用路径与新摘要版本。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.dag import DagService

router = APIRouter(prefix="/api/dag", tags=["任务依赖编排"])

service = DagService()

LIST_FIELDS = ["节点编号", "节点名称", "上游节点", "下游节点", "执行条件", "超时分钟", "失败策略", "节点状态", "最近产出"]
STATUSES = ["草稿", "运行中", "已暂停", "已完成"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按节点编号检索"),
    status: str | None = Query(default=None, description="草稿、运行中、已暂停、已完成"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与状态过滤任务依赖编排列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条依赖图节点明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"依赖图节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条依赖图节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="依赖图节点已登记", entry=entry)


@router.post("/events/batch")
def submit_event_batch(payload: EntryPayload) -> dict[str, Any]:
    """批量提交节点状态事件：同批次多节点以同一快照计算优先级与增量路径。

    values 结构：
      events: [{node, status, 超时分钟?, 上游节点?, 下游节点?, 最近产出?}]
      idempotency_key?: 幂等键，相同键重复提交直接返回原批次
      fail_injection?: {scope: path|node, target: 编号}，用于演示断点续算
    """
    values = payload.values
    events = values.get("events")
    if not isinstance(events, list) or not events:
        raise HTTPException(status_code=400, detail="events 必须是非空数组")
    try:
        return service.submit_event_batch(
            events,
            idempotency_key=values.get("idempotency_key"),
            fail_injection=values.get("fail_injection"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/resume")
def resume_batch(batch_id: str) -> dict[str, Any]:
    """失败批次从受影响路径的断点继续，已完成路径不重算。"""
    try:
        return service.resume_batch(batch_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条依赖图节点执行动作；同幂等键重复动作不会重复缩短承诺时间。"""
    action = str(payload.values.get("action") or "").strip()
    idempotency_key = payload.values.get("idempotency_key")
    entry, message, batch = service.run_action(
        entry_id, action, idempotency_key=idempotency_key
    )
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出任务依赖编排清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dag", "total": total, "items": items}
