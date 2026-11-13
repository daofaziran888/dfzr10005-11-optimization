"""共用路径计算引擎：基线与 SLA 唯一的路径取数来源（同一份实现、同一份结果）。

设计要点（对应增量重算诉求）：

1. 图口径：依赖图节点的「上游节点、下游节点、超时分钟」共同决定增量路径；
   节点状态变化后只重算变更节点及其下游所在路径，未受影响路径原样复用摘要。
2. 摘要快照：每条路径的计算结果是不可变快照（深拷贝对外），刷新、重新进入或
   返回列表都拿到同一份；BENC 引用的是摘要快照副本，不重新解释节点状态。
3. 版本与水位：摘要按路径自增版本号，保存上一版超时预算与最近计算水位；
   基线口径（caliber）变化后旧路径按原口径保留，新结果按新版本重算。
4. 幂等：相同状态事件（同一幂等键）重复到达直接返回原批次，不会多次缩短承诺时间。
5. 批次一致性：同一批次内多个节点同时变化，以同一份批次快照计算受影响范围与优先级。
6. 断点续算：某条路径失败时记录断点，resume 从受影响路径的断点继续，已完成路径不重算。
7. 并发：路径之间相互独立，用有界线程池并发重算；共享状态全部在锁内变更。

只依赖标准库，内存态，便于克隆即跑与单元测试。
"""
from __future__ import annotations

import copy
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable

from app.store import store

DAG_MODULE = "dag"
BASELINE_MODULE = "baseline"
SLA_MODULE = "sla"
BENCHMARK_MODULE = "benchmark"

# 并发重算的路径级并发度：路径之间无共享写，只在落库摘要时短暂持锁。
MAX_PATH_WORKERS = 4
# 已完成节点不再被状态事件回退，保证「已完成节点」视图可追溯。
DONE_STATUS = "已完成"


def parse_time(value: Any) -> datetime | None:
    """把「最近产出/产出时间」解析成 datetime；无法解析时返回 None。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def to_int(value: Any, default: int = 0) -> int:
    """宽松地把超时分钟等字段转成整数；占位文本等非法值回落到 default。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def split_refs(value: Any) -> list[str]:
    """上游/下游节点字段支持逗号、顿号、分号、空白分隔，统一成编号列表。"""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value)
    for sep in ("，", "、", ";", "；", ",", "|", " "):
        text = text.replace(sep, ",")
    return [part.strip() for part in text.split(",") if part.strip()]


@dataclass
class Caliber:
    """基线口径版本：承诺/预警缓冲等计算参数随版本演进，旧摘要保留旧口径。"""

    version: int
    label: str
    # 承诺时间 = 末端最近产出 + 超时预算总和 + 承诺缓冲（分钟）
    promise_buffer_minutes: int
    # 预警时间相对承诺时间提前的分钟数
    warn_lead_minutes: int
    # 风险等级阈值：剩余时间（分钟）落入各档；从高风险到低风险判定
    risk_thresholds: dict[str, int]
    created_at: str


@dataclass
class Batch:
    """一次状态事件批次：同一批次内的所有节点共享同一份图快照。"""

    batch_id: str
    idempotency_key: str
    watermark: int
    caliber_version: int
    events: list[dict[str, Any]]
    snapshot: dict[str, dict[str, Any]]
    affected_paths: list[str]
    # 优先级：同批次多节点同时变化时，按批次快照统一打分排序
    priorities: list[dict[str, Any]]
    status: str  # processing / completed / failed
    checkpoints: list[dict[str, Any]] = field(default_factory=list)
    reused_paths: list[str] = field(default_factory=list)
    retired_paths: list[str] = field(default_factory=list)
    new_versions: list[str] = field(default_factory=list)
    error: str = ""
    created_at: str = ""
    finished_at: str = ""


class PathEngine:
    """路径计算的唯一实现。进程内单例，见模块尾部 ``path_engine``。"""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._lock = threading.RLock()
        self._clock = clock or datetime.now
        self._initialized = False
        self._watermark = 0
        self._caliber_version = 1
        self._calibers: list[Caliber] = []
        # path_id -> 不可变摘要列表（按版本追加，永不覆盖）
        self._summaries: dict[str, list[dict[str, Any]]] = {}
        self._paths: list[dict[str, Any]] = []
        self._node_index: dict[str, dict[str, Any]] = {}
        self._batches: dict[str, Batch] = {}
        self._idempotency: dict[str, str] = {}
        # 一次性失败注入：{scope: path|node, target: 编号}；触发后清除
        self._fail_injection: dict[str, str] | None = None
        self._executor = ThreadPoolExecutor(
            max_workers=MAX_PATH_WORKERS, thread_name_prefix="path-recalc"
        )

    # ------------------------------------------------------------------
    # 初始化与图构建
    # ------------------------------------------------------------------
    def bootstrap(self) -> None:
        """首次加载：建图、枚举路径、产出 v1 摘要（基线/SLA 都复用这份）。"""
        with self._lock:
            if self._initialized:
                return
            self._calibers.append(self._default_caliber())
            self._reload_graph_locked()
            self._watermark += 1
            caliber = self._calibers[0]
            for path in self._paths:
                node_snapshot = {
                    nid: dict(self._node_index[nid]) for nid in path["node_order"]
                }
                summary = self._compute_summary(
                    path,
                    node_snapshot,
                    version=1,
                    base_watermark=self._watermark,
                    caliber=caliber,
                    previous_budget=None,
                    batch_id="BOOTSTRAP",
                )
                self._summaries[path["path_id"]] = [summary]
            self._initialized = True

    def reload_graph(self) -> None:
        """外部新增/删除 dag 行后显式重建图索引；为新发现的路径补算首版摘要，
        既有摘要版本保持不变。"""
        with self._lock:
            self.bootstrap()
            self._reload_graph_locked()
            caliber = self._current_caliber_locked()
            for path in self._paths:
                if path["path_id"] in self._summaries:
                    continue
                node_snapshot = {
                    nid: dict(self._node_index[nid]) for nid in path["node_order"]
                }
                summary = self._compute_summary(
                    path, node_snapshot,
                    version=1,
                    base_watermark=self._watermark,
                    caliber=caliber,
                    previous_budget=None,
                    batch_id="GRAPH-RELOAD",
                )
                self._summaries[path["path_id"]] = [summary]

    def _default_caliber(self) -> Caliber:
        return Caliber(
            version=1,
            label="基线口径 v1（承诺=末端产出+超时预算+30分钟缓冲）",
            promise_buffer_minutes=30,
            warn_lead_minutes=20,
            risk_thresholds={"高风险": 30, "中风险": 60},
            created_at=self._now_text(),
        )

    def _now_text(self) -> str:
        return self._clock().strftime("%Y-%m-%d %H:%M:%S")

    def _reload_graph_locked(self) -> None:
        """从 store 的 dag 表重建图与路径枚举（拓扑口径来自上游/下游/超时分钟）。"""
        rows = store.rows(DAG_MODULE)
        self._node_index = {str(row["节点编号"]): row for row in rows}
        self._paths = self._enumerate_paths_locked()

    def _enumerate_paths_locked(self) -> list[dict[str, Any]]:
        """以「无上游」为根、沿下游边 DFS 枚举全部根->末端路径。

        边同时参考节点的上游与下游字段并做双向校正，保证口径一致；
        节点编号排序保证枚举结果稳定可复现。
        """
        node_ids = sorted(self._node_index)
        down_edges: dict[str, list[str]] = {nid: [] for nid in node_ids}
        up_edges: dict[str, list[str]] = {nid: [] for nid in node_ids}
        for nid in node_ids:
            node = self._node_index[nid]
            for down in split_refs(node.get("下游节点")):
                if down in self._node_index and down not in down_edges[nid]:
                    down_edges[nid].append(down)
            for up in split_refs(node.get("上游节点")):
                if up in self._node_index and nid not in down_edges[up]:
                    down_edges[up].append(nid)
            up_edges[nid] = split_refs(node.get("上游节点"))
        for nid in node_ids:
            down_edges[nid] = sorted(set(down_edges[nid]))

        roots = sorted(
            nid for nid in node_ids
            if not [u for u in up_edges[nid] if u in self._node_index]
        )
        paths: list[dict[str, Any]] = []

        def walk(current: str, chain: list[str], visiting: set[str]) -> None:
            children = down_edges.get(current, [])
            open_children = [child for child in children if child not in visiting]
            if not open_children:
                paths.append(self._make_path_meta_locked(chain))
                return
            for child in open_children:
                visiting.add(child)
                walk(child, [*chain, child], visiting)
                visiting.discard(child)

        for root in roots:
            walk(root, [root], {root})

        # 孤立节点（既无上游也无下游）自身构成一条单节点路径，避免漏算
        covered = {nid for path in paths for nid in path["node_order"]}
        for nid in node_ids:
            if nid not in covered:
                paths.append(self._make_path_meta_locked([nid]))
        paths.sort(key=lambda item: item["path_id"])
        return paths

    def _make_path_meta_locked(self, node_order: list[str]) -> dict[str, Any]:
        path_id = "PATH-" + "->".join(node_order)
        return {
            "path_id": path_id,
            "node_order": node_order,
            "root": node_order[0],
            "sink": node_order[-1],
            "fingerprint": self._fingerprint(node_order),
        }

    @staticmethod
    def _fingerprint(node_order: list[str]) -> str:
        # 拓扑指纹：只要上游顺序/成员不变，指纹就不变，便于识别未受影响路径
        return "fp:" + "|".join(node_order)

    # ------------------------------------------------------------------
    # 摘要计算（路径计算收拢的共用规则，基线与 SLA 都走这一个方法）
    # ------------------------------------------------------------------
    def _output_lag(self, total_timeout: int, latest_output: Any) -> int | None:
        """路径级：超时预算与最近产出（距当前分钟数）的差值。"""
        produced = parse_time(latest_output)
        if produced is None:
            return None
        elapsed = int((self._clock() - produced).total_seconds() // 60)
        return elapsed - total_timeout

    def _risk_level(self, summary: dict[str, Any], caliber: Caliber) -> str:
        """统一风险口径：承诺窗口剩余分钟数对照口径阈值分档。"""
        promise_dt = parse_time(summary.get("promise_time"))
        if promise_dt is None:
            return "无承诺窗口"
        remaining = int((promise_dt - self._clock()).total_seconds() // 60)
        if summary.get("all_nodes_done"):
            return "已完成"
        if remaining <= caliber.risk_thresholds["高风险"]:
            return "高风险"
        if remaining <= caliber.risk_thresholds["中风险"]:
            return "中风险"
        return "低风险"

    # ------------------------------------------------------------------
    # 状态事件批次（幂等 + 快照 + 增量重算）
    # ------------------------------------------------------------------
    def submit_events(
        self,
        events: list[dict[str, Any]],
        *,
        idempotency_key: str | None = None,
        fail_injection: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """提交一批节点状态变化事件，返回批次结果。

        相同 idempotency_key 的重复请求直接返回原批次，不重复计算、不重复缩短承诺。
        """
        with self._lock:
            self.bootstrap()
            key = idempotency_key or self._auto_key(events)
            if key in self._idempotency:
                batch = self._batches[self._idempotency[key]]
                return self._batch_result(batch, reused_idempotent=True)

            # 1. 批次开始时先拍快照：同批次多节点变化以同一份快照决定受影响范围
            snapshot = {nid: dict(row) for nid, row in self._node_index.items()}
            changed_nodes = self._validate_events_locked(events)
            affected_paths = self._affected_paths_locked(changed_nodes, snapshot)
            priorities = self._prioritize_locked(changed_nodes, affected_paths, snapshot)

            self._watermark += 1
            batch = Batch(
                batch_id=f"BATCH-{self._watermark:04d}",
                idempotency_key=key,
                watermark=self._watermark,
                caliber_version=self._caliber_version,
                events=copy.deepcopy(events),
                snapshot=snapshot,
                affected_paths=affected_paths,
                priorities=priorities,
                status="processing",
                created_at=self._now_text(),
            )
            self._batches[batch.batch_id] = batch
            self._idempotency[key] = batch.batch_id
            if fail_injection:
                self._fail_injection = dict(fail_injection)

            # 2. 在快照口径下应用事件到 dag 表（已完成节点不允许回退）
            for event in events:
                self._apply_event_locked(event)
            # 结构（上游/下游/超时分钟）可能变化，重建图索引
            self._reload_graph_locked()

            # 2.1 拓扑变化可能产生新路径：新路径没有任何历史摘要，必须纳入本批重算，
            #     不能漏算，也不允许沿用旧图口径
            existing = set(batch.affected_paths)
            new_paths = [
                path for path in self._paths
                if path["path_id"] not in existing and path["path_id"] not in self._summaries
            ]
            if new_paths:
                extra_ids = sorted(path["path_id"] for path in new_paths)
                batch.affected_paths = sorted(existing | set(extra_ids))
                batch.priorities = batch.priorities + [
                    {
                        "path_id": path["path_id"],
                        "timeout_budget_minutes": sum(
                            to_int(self._node_index[nid].get("超时分钟"))
                            for nid in path["node_order"]
                        ),
                        "changed_nodes": [
                            nid for nid in changed_nodes if nid in path["node_order"]
                        ],
                        "score": 0,
                    }
                    for path in new_paths
                ]

            # 3. 仅重算受影响路径；未受影响路径复用原摘要
            return self._recalc_locked(batch)

    def _validate_events_locked(self, events: list[dict[str, Any]]) -> list[str]:
        if not events:
            raise ValueError("状态事件批次不能为空")
        changed: list[str] = []
        for event in events:
            nid = str(event.get("node") or event.get("节点编号") or "").strip()
            if nid not in self._node_index:
                raise ValueError(f"状态事件引用了不存在的依赖图节点：{nid}")
            changed.append(nid)
        # 同批次去重但保留顺序
        return list(dict.fromkeys(changed))

    def _affected_paths_locked(
        self, changed_nodes: list[str], snapshot: dict[str, dict[str, Any]]
    ) -> list[str]:
        """变更节点及其下游节点覆盖到的路径才需要重算。"""
        impacted: set[str] = set(changed_nodes)
        # 在批次快照上沿下游边扩散，保证与同批次其它节点使用同一口径
        queue = list(changed_nodes)
        while queue:
            current = queue.pop(0)
            row = snapshot.get(current, {})
            for down in split_refs(row.get("下游节点")):
                if down in snapshot and down not in impacted:
                    impacted.add(down)
                    queue.append(down)
        affected = [
            path["path_id"] for path in self._paths
            if any(nid in impacted for nid in path["node_order"])
        ]
        return sorted(affected)

    def _prioritize_locked(
        self,
        changed_nodes: list[str],
        affected_paths: list[str],
        snapshot: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """同批次快照下统一打分：超时预算越大、包含变更节点越靠前的路径先算。"""
        scored: list[dict[str, Any]] = []
        for path in self._paths:
            if path["path_id"] not in affected_paths:
                continue
            budget = sum(
                to_int(snapshot[nid].get("超时分钟")) for nid in path["node_order"]
            )
            hit_changed = [nid for nid in changed_nodes if nid in path["node_order"]]
            scored.append({
                "path_id": path["path_id"],
                "timeout_budget_minutes": budget,
                "changed_nodes": hit_changed,
                "score": budget + len(hit_changed) * 1000,
            })
        scored.sort(key=lambda item: (-item["score"], item["path_id"]))
        return scored

    def _apply_event_locked(self, event: dict[str, Any]) -> None:
        nid = str(event.get("node") or event["节点编号"]).strip()
        node = self._node_index[nid]
        new_status = str(event.get("status") or event.get("节点状态") or "").strip()
        if new_status:
            # 已完成节点状态可追溯且不可回退
            if node.get("节点状态") == DONE_STATUS and new_status != DONE_STATUS:
                raise ValueError(f"节点 {nid} 已完成，状态不可回退")
            node["节点状态"] = new_status
            node["status"] = new_status
            node["pending"] = new_status != DONE_STATUS
        if "超时分钟" in event:
            node["超时分钟"] = event["超时分钟"]
        if "上游节点" in event:
            node["上游节点"] = event["上游节点"]
        if "下游节点" in event:
            node["下游节点"] = event["下游节点"]
        latest_output = event.get("最近产出")
        if latest_output:
            node["最近产出"] = latest_output
        elif new_status == DONE_STATUS and not node.get("最近产出"):
            node["最近产出"] = self._now_text()

    def _recalc_locked(self, batch: Batch) -> dict[str, Any]:
        """按优先级并发重算受影响路径，未受影响路径复用原摘要。

        工作线程只做无共享状态的纯计算（读取节点快照副本），所有摘要落库、
        断点更新与失败注入状态变更都在本方法的锁内完成。
        """
        caliber = self._current_caliber_locked()
        path_map = {path["path_id"]: path for path in self._paths}
        # 拓扑变化后部分受影响路径可能已从新图消失：旧摘要标记为已取代并保留历史，不重算
        retired_paths = sorted(
            pid for pid in batch.affected_paths
            if pid not in path_map and pid in self._summaries
        )
        for pid in retired_paths:
            self._summaries[pid][-1]["superseded_at_watermark"] = batch.watermark
            self._summaries[pid][-1]["superseded_by_batch"] = batch.batch_id
        order = [
            item["path_id"] for item in batch.priorities
            if item["path_id"] in path_map
        ]
        prior_checkpoints = {
            cp["path_id"]: cp for cp in batch.checkpoints
        }

        def compute(path_id: str) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
            # 纯计算：每个任务拿自己需要的副本，不触碰引擎共享可变状态
            path = path_map[path_id]
            history = self._summaries.get(path_id, [])
            previous = copy.deepcopy(history[-1]) if history else None
            previous_budget = previous["timeout_budget_minutes"] if previous else None
            version = (previous["version_no"] + 1) if previous else 1
            prior = prior_checkpoints.get(path_id)
            done_nodes = list(prior["done_nodes"]) if prior else []
            remaining = list(prior["remaining_nodes"]) if prior else list(path["node_order"])
            node_snapshot = {nid: dict(self._node_index[nid]) for nid in path["node_order"]}
            try:
                for nid in list(remaining):
                    if self._should_fail(path_id, nid):
                        raise RuntimeError(
                            f"注入失败：节点 {nid} 中断（路径 {path_id}）"
                        )
                    done_nodes.append(nid)
                    remaining = remaining[1:]
                summary = self._compute_summary(
                    path, node_snapshot,
                    version=version,
                    base_watermark=batch.watermark,
                    caliber=caliber,
                    previous_budget=previous_budget,
                    batch_id=batch.batch_id,
                    done_nodes=done_nodes,
                )
                self._enforce_no_shorten(previous, summary)
                return path_id, summary, None
            except Exception as exc:
                return path_id, None, {
                    "path_id": path_id,
                    "done_nodes": done_nodes,
                    "remaining_nodes": remaining,
                    "error": str(exc),
                }

        failures: list[dict[str, Any]] = []
        new_versions: list[str] = []
        futures = [self._executor.submit(compute, pid) for pid in order]
        for future in futures:
            path_id, summary, fail_cp = future.result()
            if fail_cp is not None:
                failures.append(fail_cp)
                continue
            assert summary is not None
            self._summaries.setdefault(path_id, []).append(summary)
            new_versions.append(f"{path_id}@{summary['path_version']}")

        # 一次性失败注入在计算完成后清除（无论是否命中，保证不污染后续批次）
        self._fail_injection = None
        reused_paths = sorted(set(self._summaries) - set(batch.affected_paths))
        batch.reused_paths = reused_paths
        batch.retired_paths = retired_paths
        batch.new_versions = new_versions
        batch.checkpoints = failures
        if failures:
            batch.status = "failed"
            batch.error = "；".join(f"{cp['path_id']}: {cp['error']}" for cp in failures)
        else:
            batch.status = "completed"
        batch.finished_at = self._now_text()
        return self._batch_result(batch)

    def _should_fail(self, path_id: str, node_id: str) -> bool:
        injection = self._fail_injection
        if not injection:
            return False
        scope, target = injection.get("scope"), injection.get("target")
        return (scope == "path" and target == path_id) or (scope == "node" and target == node_id)

    @staticmethod
    def _enforce_no_shorten(
        previous: dict[str, Any] | None, summary: dict[str, Any]
    ) -> None:
        """承诺/预警窗口单调不缩短：重复事件不得多次缩短承诺时间。"""
        if previous is None:
            return
        prev_promise = parse_time(previous.get("promise_time"))
        new_promise = parse_time(summary.get("promise_time"))
        if prev_promise and new_promise and new_promise < prev_promise:
            summary["promise_time"] = previous["promise_time"]
            summary["warn_time"] = previous["warn_time"]
            summary["window_kept_from_version"] = previous["path_version"]

    def _compute_summary(
        self,
        path: dict[str, Any],
        node_snapshot: dict[str, dict[str, Any]],
        *,
        version: int,
        base_watermark: int,
        caliber: Caliber,
        previous_budget: int | None,
        batch_id: str,
        done_nodes: list[str] | None = None,
    ) -> dict[str, Any]:
        """无共享状态的摘要纯计算，可安全在线程池中执行。"""
        node_order: list[str] = path["node_order"]
        if done_nodes is None:
            done_nodes = list(node_order)
        nodes_payload: list[dict[str, Any]] = []
        total_timeout = 0
        all_done = True

        for position, nid in enumerate(node_order):
            node = node_snapshot[nid]
            timeout = to_int(node.get("超时分钟"))
            total_timeout += timeout
            upstream = [u for u in split_refs(node.get("上游节点")) if u in node_snapshot]
            downstream = [d for d in split_refs(node.get("下游节点")) if d in node_snapshot]
            produced_at = parse_time(node.get("最近产出"))
            lag_minutes: int | None = None
            if produced_at is not None:
                lag_minutes = int((self._clock() - produced_at).total_seconds() // 60) - timeout
            status = str(node.get("节点状态") or node.get("status") or "")
            if status != DONE_STATUS:
                all_done = False
            nodes_payload.append({
                "节点编号": nid,
                "节点名称": node.get("节点名称", ""),
                "上游节点顺序": upstream,
                "下游节点集合": downstream,
                "超时分钟": timeout,
                "节点状态": status,
                "最近产出": node.get("最近产出"),
                "超时与最近产出差值分钟": lag_minutes,
                "路径内序号": position,
            })

        sink = node_snapshot[path["sink"]]
        sink_produced = parse_time(sink.get("最近产出"))
        promise_dt: datetime | None = None
        warn_dt: datetime | None = None
        if sink_produced is not None:
            promise_dt = sink_produced + _minutes(total_timeout + caliber.promise_buffer_minutes)
            warn_dt = promise_dt - _minutes(caliber.warn_lead_minutes)

        downstream_set = sorted({
            d
            for nid in node_order
            for d in split_refs(node_snapshot[nid].get("下游节点"))
            if d in node_snapshot
        })
        return {
            "path_id": path["path_id"],
            "path_version": f"v{version}",
            "version_no": version,
            "fingerprint": path["fingerprint"],
            "watermark": base_watermark,
            "batch_id": batch_id,
            "caliber_version": caliber.version,
            "caliber_label": caliber.label,
            "root": path["root"],
            "sink": path["sink"],
            "node_order": node_order,
            "upstream_order": node_order[:-1],
            "downstream_set": downstream_set,
            "nodes": nodes_payload,
            "timeout_budget_minutes": total_timeout,
            "previous_timeout_budget_minutes": previous_budget,
            "promise_time": promise_dt.strftime("%Y-%m-%d %H:%M") if promise_dt else None,
            "warn_time": warn_dt.strftime("%Y-%m-%d %H:%M") if warn_dt else None,
            "sink_latest_output": sink.get("最近产出"),
            "output_lag_minutes": self._output_lag(total_timeout, sink.get("最近产出")),
            "all_nodes_done": all_done and all(nid in done_nodes for nid in node_order),
            "computed_at": self._now_text(),
            "frozen": True,
        }

    def resume_batch(self, batch_id: str) -> dict[str, Any]:
        """失败批次从受影响路径的断点继续；已完成路径与复用路径不重算。"""
        with self._lock:
            batch = self._batches.get(batch_id)
            if batch is None:
                raise ValueError(f"批次 {batch_id} 不存在")
            if batch.status != "failed":
                return self._batch_result(batch)
            # 续算沿用失败时的图口径；当前 dag 表即上次已应用事件后的状态
            batch.status = "processing"
            batch.error = ""
            return self._recalc_locked(batch)

    # ------------------------------------------------------------------
    # 口径版本
    # ------------------------------------------------------------------
    def new_caliber(
        self,
        label: str,
        *,
        promise_buffer_minutes: int,
        warn_lead_minutes: int,
        risk_thresholds: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """发布新基线口径：旧摘要保留旧口径，之后的重算按新版本计算。"""
        with self._lock:
            self.bootstrap()
            version = self._caliber_version + 1
            caliber = Caliber(
                version=version,
                label=label or f"基线口径 v{version}",
                promise_buffer_minutes=promise_buffer_minutes,
                warn_lead_minutes=warn_lead_minutes,
                risk_thresholds=risk_thresholds or {"高风险": 30, "中风险": 60},
                created_at=self._now_text(),
            )
            self._calibers.append(caliber)
            self._caliber_version = version
            return self._caliber_payload(caliber)

    def _current_caliber_locked(self) -> Caliber:
        return next(c for c in self._calibers if c.version == self._caliber_version)

    # ------------------------------------------------------------------
    # 对外只读视图：基线、SLA、BENC、路径中心共用
    # ------------------------------------------------------------------
    def latest_summaries(self) -> list[dict[str, Any]]:
        """返回每条路径当前摘要的深拷贝（不可变快照，调用方无法污染引擎状态）。"""
        with self._lock:
            self.bootstrap()
            result = []
            for path in self._paths:
                history = self._summaries.get(path["path_id"], [])
                if history:
                    result.append(copy.deepcopy(history[-1]))
            return result

    def summary_for_path(self, path_id: str) -> dict[str, Any] | None:
        with self._lock:
            self.bootstrap()
            history = self._summaries.get(path_id)
            return copy.deepcopy(history[-1]) if history else None

    def path_versions(self, path_id: str) -> list[dict[str, Any]]:
        """单条路径的全部历史版本（历史基线视图可追溯）。"""
        with self._lock:
            self.bootstrap()
            return copy.deepcopy(self._summaries.get(path_id, []))

    def baseline_view(self) -> list[dict[str, Any]]:
        """基线配置清单：业务结论反映到清单上，字段与路径摘要同口径、同结果。"""
        with self._lock:
            self.bootstrap()
            caliber = self._current_caliber_locked()
            summaries = {
                path_id: history[-1]
                for path_id, history in self._summaries.items() if history
            }
            view: list[dict[str, Any]] = []
            for row in store.rows(BASELINE_MODULE):
                summary = summaries.get(str(row.get("path_id") or ""))
                view.append(self._enrich_baseline_locked(row, summary, caliber))
            return view

    def _enrich_baseline_locked(
        self, row: dict[str, Any], summary: dict[str, Any] | None, caliber: Caliber
    ) -> dict[str, Any]:
        item = dict(row)
        if summary is None:
            item.update({
                "path_version": None, "watermark": None, "风险等级": "未关联路径",
                "关键路径版本": None, "承诺窗口对应版本": None,
            })
            return item
        risk = self._risk_level(summary, caliber)
        item.update({
            "关键路径": summary["path_id"],
            "关键路径版本": summary["path_version"],
            "path_version": summary["path_version"],
            "watermark": summary["watermark"],
            "caliber_version": summary["caliber_version"],
            "承诺时间": summary["promise_time"],
            "预警时间": summary["warn_time"],
            "最近产出": summary["sink_latest_output"],
            "超时预算分钟": summary["timeout_budget_minutes"],
            "上一版超时预算分钟": summary["previous_timeout_budget_minutes"],
            "产出滞后分钟": summary["output_lag_minutes"],
            "依赖范围": "->".join(summary["node_order"]),
            "风险等级": risk,
            "承诺窗口对应版本": f"{summary['path_id']}@{summary['path_version']}",
            "摘要批次": summary["batch_id"],
        })
        return item

    def sla_view(self) -> list[dict[str, Any]]:
        """SLA 台账：与基线共用同一份路径结果，风险等级同步一致。"""
        with self._lock:
            self.bootstrap()
            caliber = self._current_caliber_locked()
            summaries = {
                path_id: history[-1]
                for path_id, history in self._summaries.items() if history
            }
            view: list[dict[str, Any]] = []
            for row in store.rows(SLA_MODULE):
                summary = summaries.get(str(row.get("path_id") or ""))
                view.append(self._enrich_sla_locked(row, summary, caliber))
            return view

    def _enrich_sla_locked(
        self, row: dict[str, Any], summary: dict[str, Any] | None, caliber: Caliber
    ) -> dict[str, Any]:
        item = dict(row)
        if summary is None:
            item.update({"风险等级": "未关联路径", "path_version": None})
            return item
        risk = self._risk_level(summary, caliber)
        item.update({
            "监控对象": summary["path_id"],
            "产出时间": summary["sink_latest_output"],
            "风险等级": risk,
            "path_version": summary["path_version"],
            "watermark": summary["watermark"],
            "caliber_version": summary["caliber_version"],
            "承诺时间": summary["promise_time"],
            "预警时间": summary["warn_time"],
            "延迟阈值分钟": summary["timeout_budget_minutes"],
            "承诺窗口对应版本": f"{summary['path_id']}@{summary['path_version']}",
        })
        return item

    def benchmark_reference(self, benchmark_id: str) -> dict[str, Any]:
        """BENC 引用路径摘要：返回冻结副本，不重新解释节点状态。"""
        with self._lock:
            self.bootstrap()
            bench = next(
                (row for row in store.rows(BENCHMARK_MODULE)
                 if str(row.get("报告编号")) == benchmark_id),
                None,
            )
            if bench is None:
                raise ValueError(f"性能报告 {benchmark_id} 不存在")
            path_id = str(bench.get("path_id") or "")
            summary = self.summary_for_path(path_id)
            if summary is None:
                raise ValueError(f"性能报告 {benchmark_id} 未关联有效路径")
            # 冻结引用：记录引用时的版本，后续节点变化不影响这份副本
            return {
                "报告编号": benchmark_id,
                "引用路径": path_id,
                "引用时版本": summary["path_version"],
                "frozen_reference": True,
                "path_summary": summary,
            }

    def path_center(self) -> dict[str, Any]:
        """路径中心：三个可追溯视图（已完成节点 / 历史基线 / 当前 SLA 风险）汇总。"""
        with self._lock:
            self.bootstrap()
            caliber = self._current_caliber_locked()
            latest = self.latest_summaries()
            completed_nodes = [
                {
                    "节点编号": nid,
                    "节点状态": row.get("节点状态"),
                    "最近产出": row.get("最近产出"),
                    "超时分钟": to_int(row.get("超时分钟")),
                }
                for nid, row in sorted(self._node_index.items())
                if row.get("节点状态") == DONE_STATUS
            ]
            historical_baselines = [
                {
                    "path_id": path_id,
                    "versions": [
                        {
                            "path_version": item["path_version"],
                            "caliber_version": item["caliber_version"],
                            "watermark": item["watermark"],
                            "promise_time": item["promise_time"],
                            "warn_time": item["warn_time"],
                            "timeout_budget_minutes": item["timeout_budget_minutes"],
                            "batch_id": item["batch_id"],
                            "computed_at": item["computed_at"],
                        }
                        for item in history
                    ],
                }
                for path_id, history in sorted(self._summaries.items())
            ]
            current_sla_risks = [
                {
                    "path_id": summary["path_id"],
                    "path_version": summary["path_version"],
                    "风险等级": self._risk_level(summary, caliber),
                    "promise_time": summary["promise_time"],
                    "warn_time": summary["warn_time"],
                    "watermark": summary["watermark"],
                }
                for summary in latest
            ]
            return {
                "watermark": self._watermark,
                "caliber_version": self._caliber_version,
                "calibers": [self._caliber_payload(c) for c in self._calibers],
                "paths": latest,
                "batches": [self._batch_payload(b) for b in self._batches.values()],
                "traceability": {
                    "completed_nodes_view": completed_nodes,
                    "historical_baseline_view": historical_baselines,
                    "current_sla_risk_view": current_sla_risks,
                },
            }

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            self.bootstrap()
            return [self._batch_payload(b) for b in self._batches.values()]

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self._lock:
            self.bootstrap()
            batch = self._batches.get(batch_id)
            if batch is None:
                raise ValueError(f"批次 {batch_id} 不存在")
            return self._batch_payload(batch)

    # ------------------------------------------------------------------
    # 序列化辅助
    # ------------------------------------------------------------------
    def _batch_result(self, batch: Batch, *, reused_idempotent: bool = False) -> dict[str, Any]:
        payload = self._batch_payload(batch)
        payload["idempotent_replay"] = reused_idempotent
        return payload

    def _batch_payload(self, batch: Batch) -> dict[str, Any]:
        return {
            "batch_id": batch.batch_id,
            "idempotency_key": batch.idempotency_key,
            "watermark": batch.watermark,
            "caliber_version": batch.caliber_version,
            "status": batch.status,
            "events": copy.deepcopy(batch.events),
            "affected_paths": list(batch.affected_paths),
            "priorities": copy.deepcopy(batch.priorities),
            "reused_paths": list(batch.reused_paths),
            "retired_paths": list(batch.retired_paths),
            "new_versions": list(batch.new_versions),
            "checkpoints": copy.deepcopy(batch.checkpoints),
            "error": batch.error,
            "created_at": batch.created_at,
            "finished_at": batch.finished_at,
        }

    def _caliber_payload(self, caliber: Caliber) -> dict[str, Any]:
        return {
            "version": caliber.version,
            "label": caliber.label,
            "promise_buffer_minutes": caliber.promise_buffer_minutes,
            "warn_lead_minutes": caliber.warn_lead_minutes,
            "risk_thresholds": dict(caliber.risk_thresholds),
            "created_at": caliber.created_at,
        }

    @staticmethod
    def _auto_key(events: list[dict[str, Any]]) -> str:
        raw = "|".join(
            f"{e.get('node') or e.get('节点编号')}:{e.get('status') or e.get('节点状态')}"
            for e in events
        )
        return f"auto:{raw}"


def _minutes(value: int):
    from datetime import timedelta

    return timedelta(minutes=value)


# 进程内单例：基线、SLA、BENC、DAG 动作全部共用这一个引擎实例与同一份结果
path_engine = PathEngine()
