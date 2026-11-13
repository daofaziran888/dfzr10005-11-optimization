"""查询性能分析业务规则：状态流转、字段校验与筛选口径都收在这里。

BENC 报告对路径摘要的引用走引擎的冻结副本：引用时快照定型，之后依赖图节点
状态再变化也不会被报告重新解释，保证分析结论可追溯。
"""
from __future__ import annotations

from typing import Any

from app.services.path_engine import path_engine
from app.store import store

MODULE = "benchmark"
REQUIRED_FIELDS = ["报告编号", "查询语句", "执行耗时"]
STATUS_ORDER = ["草稿", "分析中", "已生成", "已优化"]
ACTION_RULES = {"开始分析": "草稿", "应用优化": "已生成", "归档报告": "已优化"}
NEGATIVE_ACTIONS = []


class BenchmarkService:
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
            rows = [row for row in rows if keyword in str(row.get("报告编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

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
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"性能报告 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于查询性能分析可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"性能报告已{action}"

    def path_reference(self, benchmark_id: str) -> dict[str, Any]:
        """返回 BENC 引用的路径摘要冻结副本（不重新解释节点状态）。"""
        return path_engine.benchmark_reference(benchmark_id)
