"""路径引擎增量重算端到端验证（纯标准库，python tests/test_path_engine.py 直接跑）。

覆盖诉求：
1. 初始取数：DAG-0002 路径的超时分钟、承诺/预警时间、最近产出正确
2. 增量重算：仅变更节点及其下游路径重算，未受影响路径复用原摘要
3. 幂等：相同状态事件重复到达不多次缩短承诺时间
4. 批次快照：两个节点同时变化以同一批次快照计算优先级
5. 失败续算：从受影响路径断点继续
6. 口径版本：旧路径保留旧口径，新结果按新版本计算
7. 基线与 SLA 共用同一结果、风险等级一致
8. BENC 冻结引用不重新解释节点状态
9. 摘要保留上游顺序、下游集合、超时分钟与最近产出差值
10. 路径版本与承诺窗口对应关系、三个可追溯视图
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.path_engine import PathEngine, parse_time, split_refs, to_int  # noqa: E402
from app.store import store  # noqa: E402

MAIN_PATH = "PATH-DAG-0001->DAG-0002->DAG-0003"
SIDE_PATH = "PATH-DAG-0001->DAG-0004->DAG-0005"
QUALITY_PATH = "PATH-DAG-0006->DAG-0007"
FIXED_NOW = datetime(2026, 11, 13, 4, 0, 0)
passed: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        raise AssertionError(f"[失败] {name} {detail}")
    passed.append(name)


def fresh_engine() -> PathEngine:
    # 每个用例拿一个全新 store 与引擎，时钟固定，结果可复现
    store.__init__()
    return PathEngine(clock=lambda: FIXED_NOW)


def test_bootstrap_and_example_fields() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    summaries = {s["path_id"]: s for s in engine.latest_summaries()}
    check("枚举到三条主路径", set(summaries) == {MAIN_PATH, SIDE_PATH, QUALITY_PATH}, str(summaries))

    target = summaries[MAIN_PATH]
    # DAG-0001=30 + DAG-0002=45 + DAG-0003=60
    check("DAG-0002链路超时预算=135分钟", target["timeout_budget_minutes"] == 135)
    check("上一版超时预算初始为空", target["previous_timeout_budget_minutes"] is None)
    check("水位初始为1", target["watermark"] == 1 and target["path_version"] == "v1")
    # 末端 DAG-0003 最近产出 2026-11-12 04:40 + 135 + 30 缓冲
    expected_promise = parse_time("2026-11-12 04:40") + timedelta(minutes=165)
    check("承诺时间=末端产出+超时预算+缓冲",
          parse_time(target["promise_time"]) == expected_promise, target["promise_time"])
    expected_warn = expected_promise - timedelta(minutes=20)
    check("预警时间=承诺-20分钟", parse_time(target["warn_time"]) == expected_warn)
    check("最近产出取末端节点", target["sink_latest_output"] == "2026-11-12 04:40")

    # 摘要保留上游节点顺序、下游节点集合
    check("上游节点顺序保留", target["node_order"] == ["DAG-0001", "DAG-0002", "DAG-0003"])
    node2 = next(n for n in target["nodes"] if n["节点编号"] == "DAG-0002")
    check("节点上游顺序", node2["上游节点顺序"] == ["DAG-0001"])
    check("节点下游集合", node2["下游节点集合"] == ["DAG-0003"])
    check("节点超时分钟", node2["超时分钟"] == 45)
    # 超时分钟与最近产出差值：固定时钟 04:00，DAG-0002 产出 03:10 -> 50-45=5
    check("超时分钟与最近产出差值", node2["超时与最近产出差值分钟"] == 5, str(node2))
    check("路径级产出滞后差值", target["output_lag_minutes"] is not None)

    # 快照不可变：外部修改深拷贝不污染引擎
    target["nodes"][0]["超时分钟"] = 9999
    again = engine.summary_for_path(MAIN_PATH)
    check("摘要快照对外不可变", again["nodes"][0]["超时分钟"] == 30)


def test_incremental_recalc_and_reuse() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    before_side = engine.summary_for_path(SIDE_PATH)
    before_main = engine.summary_for_path(MAIN_PATH)

    # DAG-0002 状态变化：受影响路径只有主链路（变更节点+下游 DAG-0003）
    batch = engine.submit_events([{"node": "DAG-0002", "status": "已暂停"}])
    check("受影响路径仅主链路", batch["affected_paths"] == [MAIN_PATH], str(batch["affected_paths"]))
    check("未受影响路径被复用", SIDE_PATH in batch["reused_paths"] and QUALITY_PATH in batch["reused_paths"])
    check("受影响路径升版本v2", f"{MAIN_PATH}@v2" in batch["new_versions"])

    after_side = engine.summary_for_path(SIDE_PATH)
    after_main = engine.summary_for_path(MAIN_PATH)
    check("复用路径版本与computed_at不变",
          after_side["path_version"] == "v1" and after_side["computed_at"] == before_side["computed_at"])
    check("重算路径版本递增", after_main["path_version"] == "v2")
    check("保存上一版超时预算", after_main["previous_timeout_budget_minutes"] == 135)
    check("水位递增到2", after_main["watermark"] == 2)
    # 状态变化后不得沿用旧摘要
    check("状态变化后摘要已更新", after_main["batch_id"] == batch["batch_id"] != before_main["batch_id"])


def test_idempotency_no_repeated_shorten() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    first = engine.submit_events(
        [{"node": "DAG-0003", "status": "运行中", "最近产出": "2026-11-13 03:30"}],
        idempotency_key="evt-1",
    )
    promise_v2 = engine.summary_for_path(MAIN_PATH)["promise_time"]
    versions_before = len(engine.path_versions(MAIN_PATH))
    # 同幂等键重复到达
    replay = engine.submit_events(
        [{"node": "DAG-0003", "status": "运行中", "最近产出": "2026-11-13 03:30"}],
        idempotency_key="evt-1",
    )
    check("重复事件幂等命中", replay["idempotent_replay"] is True)
    check("重复事件不产生新版本",
          replay["batch_id"] == first["batch_id"]
          and len(engine.path_versions(MAIN_PATH)) == versions_before)
    check("承诺时间未被重复缩短", engine.summary_for_path(MAIN_PATH)["promise_time"] == promise_v2)

    # 无幂等键但内容相同的自动键同样去重
    auto_payload = [{"node": "DAG-0007", "status": "已暂停"}]
    engine.submit_events(auto_payload)
    auto_replay = engine.submit_events(auto_payload)
    check("自动幂等键同样生效", auto_replay["idempotent_replay"] is True)


def test_promise_window_monotonic() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    original = engine.summary_for_path(MAIN_PATH)["promise_time"]
    # 新产出时间更早，若直接重算会缩短承诺窗口；引擎必须保留原窗口
    engine.submit_events([
        {"node": "DAG-0003", "status": "运行中", "最近产出": "2026-11-11 00:00"}
    ])
    kept = engine.summary_for_path(MAIN_PATH)
    check("承诺窗口单调不缩短", kept["promise_time"] == original)
    check("记录窗口沿用版本", kept.get("window_kept_from_version") == "v1")


def test_batch_snapshot_priority() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    # 两个节点同时变化：DAG-0002（主链路）与 DAG-0004（旁链路），同一批次快照
    batch = engine.submit_events([
        {"node": "DAG-0002", "status": "已暂停"},
        {"node": "DAG-0004", "status": "运行中"},
    ], idempotency_key="batch-dual")
    check("一个批次覆盖两条受影响路径",
          set(batch["affected_paths"]) == {MAIN_PATH, SIDE_PATH}, str(batch["affected_paths"]))
    check("同批次快照统一打分排序", [p["path_id"] for p in batch["priorities"]]
          == [MAIN_PATH, SIDE_PATH])  # 主链路预算135 > 旁链路预算90
    main_score = batch["priorities"][0]
    check("优先级含变更节点与预算", main_score["changed_nodes"] == ["DAG-0002"]
          and main_score["timeout_budget_minutes"] == 135)
    check("质量链路未受影响被复用", QUALITY_PATH in batch["reused_paths"])
    check("同批次水位一致", engine.summary_for_path(MAIN_PATH)["watermark"]
          == engine.summary_for_path(SIDE_PATH)["watermark"] == 2)


def test_failure_resume_from_checkpoint() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    # 注入：主链路在 DAG-0002 处失败
    failed = engine.submit_events(
        [{"node": "DAG-0002", "status": "已暂停"}],
        idempotency_key="fail-batch",
        fail_injection={"scope": "node", "target": "DAG-0002"},
    )
    check("批次标记失败", failed["status"] == "failed")
    cp = failed["checkpoints"][0]
    check("断点记录已完成与剩余节点",
          cp["done_nodes"] == ["DAG-0001"] and cp["remaining_nodes"] == ["DAG-0002", "DAG-0003"], str(cp))
    check("失败路径未产生新版本", engine.summary_for_path(MAIN_PATH)["path_version"] == "v1")

    # 续算：从断点继续，已完成节点不重算
    resumed = engine.resume_batch(failed["batch_id"])
    check("续算成功", resumed["status"] == "completed", resumed.get("error"))
    check("续算后升版v2", engine.summary_for_path(MAIN_PATH)["path_version"] == "v2")
    check("续算无断点残留", resumed["checkpoints"] == [])


def test_caliber_versioning() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    old = engine.summary_for_path(MAIN_PATH)
    check("初始口径v1", old["caliber_version"] == 1)
    old_promise = old["promise_time"]

    engine.new_caliber("放宽缓冲口径v2", promise_buffer_minutes=60, warn_lead_minutes=15)
    # 发布口径本身不重算；下一次事件按新口径，旧摘要保留旧口径
    unchanged = engine.summary_for_path(MAIN_PATH)
    check("旧路径按原口径保留", unchanged["caliber_version"] == 1
          and unchanged["promise_time"] == old_promise)

    engine.submit_events([{"node": "DAG-0002", "status": "运行中"}])
    new = engine.summary_for_path(MAIN_PATH)
    check("新结果按新口径版本", new["caliber_version"] == 2 and new["path_version"] == "v2")
    expected = parse_time("2026-11-12 04:40") + timedelta(minutes=135 + 60)
    check("新口径承诺窗口重算", parse_time(new["promise_time"]) == expected)
    # 历史版本仍可追溯到旧口径与旧承诺窗口
    versions = engine.path_versions(MAIN_PATH)
    check("版本与承诺窗口对应关系保留",
          versions[0]["caliber_version"] == 1 and versions[0]["promise_time"] == old_promise
          and versions[1]["caliber_version"] == 2 and versions[1]["promise_time"] == new["promise_time"])


def test_baseline_sla_share_same_result() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    engine.submit_events([{"node": "DAG-0002", "status": "已暂停"}])
    baseline = {row["基线编号"]: row for row in engine.baseline_view()}
    sla = {row["规则编号"]: row for row in engine.sla_view()}

    b2, s2 = baseline["BASL-0002"], sla["SLA-0002"]
    check("基线SLA承诺时间一致", b2["承诺时间"] == s2["承诺时间"])
    check("基线SLA预警时间一致", b2["预警时间"] == s2["预警时间"])
    check("基线SLA风险等级同步", b2["风险等级"] == s2["风险等级"])
    check("基线SLA路径版本一致", b2["path_version"] == s2["path_version"] == "v2")
    check("承诺窗口对应版本字段", b2["承诺窗口对应版本"] == f"{MAIN_PATH}@v2")
    check("业务结论反映到基线清单", b2["关键路径"] == MAIN_PATH and b2["超时预算分钟"] == 135)

    # 刷新/重新进入：再次读取仍是同一份快照
    again_b = {row["基线编号"]: row for row in engine.baseline_view()}["BASL-0002"]
    check("刷新后路径快照保持", again_b["承诺窗口对应版本"] == b2["承诺窗口对应版本"]
          and again_b["承诺时间"] == b2["承诺时间"])


def test_benc_frozen_reference() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    ref1 = engine.benchmark_reference("BENC-0002")
    check("BENC引用v1摘要", ref1["引用时版本"] == "v1" and ref1["frozen_reference"] is True)
    frozen_status = ref1["path_summary"]["nodes"][1]["节点状态"]

    # 节点状态随后变化并升版
    engine.submit_events([{"node": "DAG-0002", "status": "已暂停"}])
    check("引擎内摘要已升版", engine.summary_for_path(MAIN_PATH)["path_version"] == "v2")
    # BENC 已持有的冻结副本不被重新解释
    check("BENC不重新解释节点状态",
          ref1["path_summary"]["nodes"][1]["节点状态"] == frozen_status
          and ref1["引用时版本"] == "v1")
    # 新的引用拿到新版本
    ref2 = engine.benchmark_reference("BENC-0002")
    check("新引用按新版本快照", ref2["引用时版本"] == "v2")


def test_traceability_views() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    engine.submit_events([{"node": "DAG-0002", "status": "已暂停"}])
    center = engine.path_center()
    trace = center["traceability"]

    done_codes = {item["节点编号"] for item in trace["completed_nodes_view"]}
    check("已完成节点视图", {"DAG-0001", "DAG-0006"} <= done_codes and "DAG-0002" not in done_codes)

    hist = {item["path_id"]: item["versions"] for item in trace["historical_baseline_view"]}
    check("历史基线视图含版本链",
          [v["path_version"] for v in hist[MAIN_PATH]] == ["v1", "v2"])
    risks = {item["path_id"]: item["风险等级"] for item in trace["current_sla_risk_view"]}
    check("当前SLA风险视图可追溯", MAIN_PATH in risks and risks[MAIN_PATH] in
          {"高风险", "中风险", "低风险", "已完成", "无承诺窗口"})
    check("最近计算水位", center["watermark"] == 2)


def test_done_node_not_reverted() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    try:
        engine.submit_events([{"node": "DAG-0001", "status": "运行中"}], idempotency_key="revert")
    except ValueError:
        pass
    else:
        raise AssertionError("已完成节点回退应被拒绝")
    node = next(r for r in store.rows("dag") if r["节点编号"] == "DAG-0001")
    check("已完成节点状态保持", node["节点状态"] == "已完成")


def test_timeout_change_recalc() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    before = engine.summary_for_path(MAIN_PATH)
    # 超时分钟是增量路径口径之一：DAG-0002 从 45 调整为 90
    batch = engine.submit_events([
        {"node": "DAG-0002", "status": "运行中", "超时分钟": 90}
    ], idempotency_key="timeout-change")
    check("超时变化触发主链路重算", MAIN_PATH in batch["affected_paths"])
    after = engine.summary_for_path(MAIN_PATH)
    check("超时预算更新为180", after["timeout_budget_minutes"] == 180)
    check("上一版预算135保留", after["previous_timeout_budget_minutes"] == 135)
    check("承诺窗口随预算顺延",
          parse_time(after["promise_time"]) > parse_time(before["promise_time"]))


def test_topology_change_new_path_not_lost() -> None:
    engine = fresh_engine()
    engine.bootstrap()
    initial_paths = {s["path_id"] for s in engine.latest_summaries()}
    check("初始无孤立新链路", initial_paths == {MAIN_PATH, SIDE_PATH, QUALITY_PATH})
    # 预置一个孤立节点（自身成一条单节点路径），随后把它接到 DAG-0003 下游：
    # 旧主链路保持不变，同时产生一条延伸的新路径
    isolated = {
        "id": 98, "status": "草稿", "pending": True, "abnormal": False,
        "节点编号": "DAG-0098", "节点名称": "新增末端", "上游节点": "",
        "下游节点": "", "执行条件": "测试", "超时分钟": 15,
        "失败策略": "告警", "节点状态": "草稿",
        "最近产出": "2026-11-13 01:00",
    }
    store.rows("dag").append(isolated)
    engine.reload_graph()
    check("孤立节点自身成路径",
          engine.summary_for_path("PATH-DAG-0098") is not None)

    batch = engine.submit_events([
        {"node": "DAG-0003", "status": "运行中", "下游节点": "DAG-0098"},
        {"node": "DAG-0098", "status": "运行中", "上游节点": "DAG-0003"},
    ], idempotency_key="topo-change")
    new_path = "PATH-DAG-0001->DAG-0002->DAG-0003->DAG-0098"
    check("新路径纳入受影响集合", new_path in batch["affected_paths"], str(batch["affected_paths"]))
    check("新路径产出首版摘要", engine.summary_for_path(new_path) is not None)
    check("新路径预算含新增节点",
          engine.summary_for_path(new_path)["timeout_budget_minutes"] == 135 + 15)
    # 旧主链路仍然存在并被重算（其末端节点下游集合变化）
    check("旧主链路仍可追溯", engine.summary_for_path(MAIN_PATH) is not None)
    # 未受影响的质量链路仍复用
    check("质量链路复用旧摘要", QUALITY_PATH in batch["reused_paths"])


def test_helpers() -> None:
    check("split_refs多分隔符", split_refs("A，B、C;D") == ["A", "B", "C", "D"])
    check("to_int占位文本回落", to_int("任务依赖编排样例1") == 0 and to_int("45") == 45)
    check("parse_time兼容日期", parse_time("2026-11-13") is not None)


def main() -> None:
    tests = [
        test_helpers,
        test_bootstrap_and_example_fields,
        test_incremental_recalc_and_reuse,
        test_idempotency_no_repeated_shorten,
        test_promise_window_monotonic,
        test_batch_snapshot_priority,
        test_failure_resume_from_checkpoint,
        test_caliber_versioning,
        test_baseline_sla_share_same_result,
        test_benc_frozen_reference,
        test_traceability_views,
        test_done_node_not_reverted,
        test_timeout_change_recalc,
        test_topology_change_new_path_not_lost,
    ]
    for test in tests:
        test()
    print(f"\n全部 {len(passed)} 项断言通过：")
    for name in passed:
        print(f"  ✓ {name}")


if __name__ == "__main__":
    main()
