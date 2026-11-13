"""SLA告警管理业务规则。

SLA 台账与基线监控共用同一个路径引擎、同一份路径摘要：风险等级、承诺时间、
预警时间不允许两套口径各自计算。DAG 状态变化触发增量重算后，本页无需人工刷新
规则本身即可读到与基线完全一致的新风险等级。
"""
from __future__ import annotations

from typing import Any

from app.services.path_engine import path_engine
from app.store import store

MODULE = "sla"
REQUIRED_FIELDS = ["规则编号", "监控对象", "产出时间"]
STATUS_ORDER = ["草稿", "监控中", "已触发", "已关闭"]
ACTION_RULES = {"启用监控": "草稿", "处理告警": "监控中", "关闭规则": "已触发"}
NEGATIVE_ACTIONS = []


class SlaService:
    def _enriched_rows(self) -> list[dict[str, Any]]:
        return path_engine.sla_view()

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._enriched_rows()
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("规则编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        for row in self._enriched_rows():
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        if values.get("path_id"):
            entry["path_id"] = values["path_id"]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"SLA规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于SLA告警管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"SLA规则已{action}"
