"""任务依赖编排业务规则：状态流转、字段校验与筛选口径都收在这里。

状态动作不再只改内存行：每次状态变化都会作为「状态事件」提交给共用路径引擎，
由引擎以批次快照驱动增量重算，基线与 SLA 随后即可读到同一份新结果。
"""
from __future__ import annotations

from typing import Any

from app.services.path_engine import DONE_STATUS, path_engine
from app.store import store

MODULE = "dag"
REQUIRED_FIELDS = ["节点编号", "节点名称", "上游节点"]
STATUS_ORDER = ["草稿", "运行中", "已暂停", "已完成"]
ACTION_RULES = {"启动节点": "草稿", "暂停节点": "运行中", "归档节点": "已暂停", "完成节点": "已完成"}
NEGATIVE_ACTIONS = []
ACTION_TO_EVENT = {
    "启动节点": "运行中",
    "暂停节点": "已暂停",
    "完成节点": "已完成",
}


class DagService:
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
            rows = [row for row in rows if keyword in str(row.get("节点编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def find_by_code(self, node_code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("节点编号")) == node_code:
                return row
        return None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        for extra in ("下游节点", "执行条件", "超时分钟", "失败策略", "最近产出"):
            if extra in values:
                entry[extra] = values[extra]
        entry["status"] = STATUS_ORDER[0]
        entry["节点状态"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 新节点可能改变拓扑（上游/下游/超时分钟共同决定增量路径）：重建图并补算新路径摘要
        path_engine.reload_graph()
        path_engine.submit_events([{"node": entry["节点编号"], "status": STATUS_ORDER[0]}])
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        *,
        idempotency_key: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"依赖图节点 {entry_id} 不存在或已归档", None
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于任务依赖编排可执行范围", None
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里", None
        event_status = ACTION_TO_EVENT.get(action, target)
        event: dict[str, Any] = {"node": entry["节点编号"], "status": event_status}
        if event_status == DONE_STATUS and not entry.get("最近产出"):
            event["最近产出"] = path_engine._now_text()
        # 引擎内部对同幂等键去重：重复事件不会多次缩短承诺时间
        batch = path_engine.submit_events([event], idempotency_key=idempotency_key)
        # 引擎已更新 dag 表；重新读取拿到最新行
        entry = store.find(MODULE, entry_id)
        assert entry is not None
        return entry, f"依赖图节点已{action}", batch

    def submit_event_batch(
        self,
        events: list[dict[str, Any]],
        *,
        idempotency_key: str | None = None,
        fail_injection: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """两个及以上节点同时变化：同一批次快照统一计算优先级与受影响路径。"""
        return path_engine.submit_events(
            events, idempotency_key=idempotency_key, fail_injection=fail_injection
        )

    def resume_batch(self, batch_id: str) -> dict[str, Any]:
        """失败后从受影响路径的断点继续。"""
        return path_engine.resume_batch(batch_id)
