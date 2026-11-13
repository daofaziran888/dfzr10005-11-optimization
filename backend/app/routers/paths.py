"""路径计算中心接口：基线与 SLA 共用的唯一路径结果出口。

这里收拢路径版本、承诺窗口、批次增量记录、口径版本与三个可追溯视图
（已完成节点、历史基线、当前 SLA 风险），任何页面都不自行解释节点状态。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import EntryPayload
from app.services.path_engine import path_engine

router = APIRouter(prefix="/api/paths", tags=["路径计算中心"])


@router.get("/center")
def path_center() -> dict[str, Any]:
    """路径总览：最新摘要、批次记录与三个可追溯视图一次取齐。"""
    return path_engine.path_center()


@router.get("/summaries")
def latest_summaries() -> dict[str, Any]:
    """每条路径当前生效的摘要快照（深拷贝，调用方改动不影响引擎）。"""
    return {"items": path_engine.latest_summaries()}


@router.get("/{path_id}/versions")
def path_versions(path_id: str) -> dict[str, Any]:
    """单条路径的全部历史版本：保留路径版本与承诺窗口的对应关系。"""
    versions = path_engine.path_versions(path_id)
    if not versions:
        raise HTTPException(status_code=404, detail=f"路径 {path_id} 不存在或尚未计算")
    return {"path_id": path_id, "versions": versions}


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    """状态事件批次记录：受影响路径、复用路径、优先级与断点。"""
    return {"items": path_engine.list_batches()}


@router.post("/calibers")
def create_caliber(payload: EntryPayload) -> dict[str, Any]:
    """发布新基线口径：旧路径按原口径保留，之后的重算按新版本计算。"""
    values = payload.values
    try:
        buffer_minutes = int(values.get("promise_buffer_minutes", 30))
        lead_minutes = int(values.get("warn_lead_minutes", 20))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="口径缓冲分钟必须是整数") from exc
    return path_engine.new_caliber(
        str(values.get("label") or ""),
        promise_buffer_minutes=buffer_minutes,
        warn_lead_minutes=lead_minutes,
        risk_thresholds=values.get("risk_thresholds"),
    )
