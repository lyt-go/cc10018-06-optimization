"""聚合计划索引表专项测试。

覆盖验收场景：
1. 连续读取未变更配置走索引快速路径，不再重走完整依赖树；
2. 修改一个子指标只让受它影响的计算步骤失效，共用父指标的两份输出表节点互不串用；
3. 公式/口径变更后能通过计划索引表定位受影响计划，SQL 版本与计划版本清单严格对应；
4. 同一指标域被两份聚合配置使用、计划生成与派生指标更正并发时，依赖更正节点的计划
   不执行旧公式（两阶段构建重试），另一份计划继续复用不受影响的依赖；
5. 两个不同输出表改动前后的计划差异、预览 SQL 与实际重算范围对比。
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.store import store
from app.engine.plan_registry import plan_registry, parse_sources
from app.engine.metric_calculator import MetricCalculator


def reset_store() -> None:
    """重新装载 seed 数据并清空计划索引。"""
    store._tables = {}
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    plan_registry.reset()


def find_metric(code: str) -> dict:
    return next(r for r in store.rows("derived_metric") if r.get("指标编号") == code)


def make_agg(entry_id: int, code: str, source: str, output_table: str) -> dict:
    rows = store.rows("data_aggregate")
    entry = {
        "id": entry_id,
        "status": "已生效", "pending": False, "abnormal": False,
        "配置编号": code, "数据来源": source, "聚合维度": "dt,shop_id",
        "聚合函数": "SUM(gmv)", "过滤条件": "dt>=2026-01-01",
        "输出表名": output_table, "增量键": "dt", "更新策略": "增量覆盖",
    }
    rows.append(entry)
    return entry


# ───────────────────────────────────────────────
# 场景 1：快速路径，连续读取不重走依赖树
# ───────────────────────────────────────────────

def test_fast_path_no_tree_walk_on_unchanged_config():
    reset_store()
    calc = MetricCalculator()

    plan1 = calc.build_compute_plan(1)
    assert plan1["ok"]
    stats = plan_registry.get_stats()
    assert stats["tree_walks"] == 1
    assert stats["fast_path_hits"] == 0
    first_version = plan1["plan_version"]

    # 连续读取同一未变更配置：详情、计划接口、预览 SQL 都应命中快速路径
    plan2 = calc.build_compute_plan(1)
    plan3 = calc.build_compute_plan(1)
    calc.preview_sql(1)
    stats = plan_registry.get_stats()
    assert stats["tree_walks"] == 1, "未变更配置不应重走依赖树"
    assert stats["fast_path_hits"] == 3
    assert plan2["plan_version"] == first_version
    assert plan3["root_fingerprint"] == plan1["root_fingerprint"]

    # 强制重建才会再走一次依赖树
    calc.build_compute_plan(1, force=True)
    assert plan_registry.get_stats()["tree_walks"] == 2
    print("  [PASS] 场景1: 连续读取未变更配置命中快速路径，不重走依赖树")


# ───────────────────────────────────────────────
# 场景 2：子指标更正只失效受影响节点；两份输出表节点隔离
# ───────────────────────────────────────────────

def test_selective_invalidation_and_node_isolation():
    reset_store()
    calc = MetricCalculator()

    # 两份输出表共用交易域与 DERI-GMV，其中 A 表还依赖 DERI-ARPU（父指标为 DERI-GMV）
    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-ARPU", "ads_table_a")
    make_agg(102, "AGG-B-102", "METR-TRADE,DERI-GMV", "ads_table_b")

    plan_a = calc.build_compute_plan(101)
    plan_b = calc.build_compute_plan(102)

    # 两份计划的节点 id 必须带 aggregate 命名空间，不能共用同一节点
    nodes_a = {s["node_id"] for s in plan_a["steps"]}
    nodes_b = {s["node_id"] for s in plan_b["steps"]}
    assert nodes_a.isdisjoint(nodes_b), "两个输出表不得串用对方节点"
    assert all(nid.startswith("agg101:") for nid in nodes_a)
    assert all(nid.startswith("agg102:") for nid in nodes_b)

    # 索引表中同一来源在两个命名空间下各有一份节点
    index = plan_registry.index_listing()
    gmv_nodes = [n for n in index["node_index"] if n["code"] == "DERI-GMV"]
    assert len(gmv_nodes) == 2 and {n["aggregate_id"] for n in gmv_nodes} == {101, 102}

    # 更正子指标 DERI-GMV 的公式
    gmv = find_metric("DERI-GMV")
    old_formula = gmv["计算公式"]
    impact = plan_registry.notify_source_change  # 先验证影响面定位
    affected = plan_registry.find_affected_plans("DERI-GMV")
    assert {p["aggregate_id"] for p in affected} == {101, 102}

    gmv["计算公式"] = "SUM(订单金额_含税)"
    change = plan_registry.notify_source_change("DERI-GMV", "公式更正")
    assert change["changed"] is True
    assert change["change"]["old_version"] == 1
    assert change["change"]["new_version"] == 2
    assert {p["aggregate_id"] for p in change["affected_plans"]} == {101, 102}

    # 重建 A：GMV 与其下游 ARPU 失效，指标域节点继续复用
    plan_a2 = calc.build_compute_plan(101)
    scope_a = plan_a2["recompute_scope"]
    rebuilt_codes = {d["code"] for d in scope_a["rebuilt_steps"]}
    assert "DERI-GMV" in rebuilt_codes
    assert "DERI-ARPU" in rebuilt_codes, "父指标更正必须让依赖它的子指标节点失效"
    assert "METR-TRADE" not in rebuilt_codes, "未变更的指标域节点应继续复用"
    reused = set(scope_a["reused_node_ids"])
    assert any("METR-TRADE" in nid for nid in reused)

    # 重建 B：只有 GMV 与聚合节点失效，指标域同样复用
    plan_b2 = calc.build_compute_plan(102)
    scope_b = plan_b2["recompute_scope"]
    rebuilt_b = {d["code"] for d in scope_b["rebuilt_steps"]}
    assert rebuilt_b == {"DERI-GMV", "AGG:102"}

    # A 的重建不会污染 B 的节点：B 重建后再次读取 A 仍保持自己的版本与节点
    plan_a3 = calc.build_compute_plan(101)
    assert plan_a3["plan_version"] == plan_a2["plan_version"]
    assert {s["node_id"] for s in plan_a3["steps"]} == nodes_a

    # 恢复公式后版本继续递增，且同样可被索引表追踪
    gmv["计算公式"] = old_formula
    change3 = plan_registry.notify_source_change("DERI-GMV", "公式回滚")
    assert change3["change"]["new_version"] == 3
    print("  [PASS] 场景2: 子指标更正选择性失效，两个输出表节点隔离不串用")


# ───────────────────────────────────────────────
# 场景 3：另一份计划继续重用不受影响的依赖（共用指标域）
# ───────────────────────────────────────────────

def test_unaffected_plan_reuses_shared_domain():
    reset_store()
    calc = MetricCalculator()

    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-GMV", "ads_table_a")
    make_agg(102, "AGG-B-102", "METR-USER,DERI-DAU", "ads_table_b")
    calc.build_compute_plan(101)
    calc.build_compute_plan(102)

    # 仅更正用户域下的 DAU；交易域计划完全不受影响
    dau = find_metric("DERI-DAU")
    dau["计算公式"] = "COUNT(DISTINCT active_user_id)"
    plan_registry.notify_source_change("DERI-DAU", "口径更正")

    plan_b2 = calc.build_compute_plan(102)
    rebuilt_b = {d["code"] for d in plan_b2["recompute_scope"]["rebuilt_steps"]}
    assert "DERI-DAU" in rebuilt_b
    assert "METR-USER" not in rebuilt_b, "同计划内未变更的指标域应复用"

    # A 计划读取应走快速路径（stale 为空，根指纹未变）：
    # 返回的是已缓存计划快照，验证点是不重走依赖树、命中快速路径
    before_hits = plan_registry.get_stats()["fast_path_hits"]
    before_walks = plan_registry.get_stats()["tree_walks"]
    plan_a2 = calc.build_compute_plan(101)
    stats = plan_registry.get_stats()
    assert plan_a2["plan_version"] == 1
    assert stats["fast_path_hits"] == before_hits + 1
    assert stats["tree_walks"] == before_walks

    # 索引表反查：DAU 只命中 B，GMV 只命中 A
    assert {p["aggregate_id"] for p in plan_registry.find_affected_plans("DERI-DAU")} == {102}
    assert {p["aggregate_id"] for p in plan_registry.find_affected_plans("DERI-GMV")} == {101}
    print("  [PASS] 场景3: 不受影响的另一份计划继续复用原依赖节点")


# ───────────────────────────────────────────────
# 场景 4：SQL 版本与计划版本清单严格对应
# ───────────────────────────────────────────────

def test_preview_sql_matches_plan_versions():
    reset_store()
    calc = MetricCalculator()
    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-ARPU", "ads_table_a")

    plan = calc.build_compute_plan(101)
    sql = calc.preview_sql(101)
    manifest = {i["code"]: i["version"] for i in plan["version_manifest"]}

    # SQL 头部固化了计划中每个来源的版本
    for code, ver in manifest.items():
        assert f"-- @version {code} v{ver}" in sql
    assert f"plan-v{plan['plan_version']}" in sql
    assert "ads_table_a" in sql

    check = plan_registry.verify_sql_versions(101)
    assert check["ok"] is True
    assert check["embedded_versions"] == manifest

    # 公式更正后，旧 SQL 必须与新版本对不上（防止执行旧公式）
    find_metric("DERI-GMV")["计算公式"] = "SUM(订单金额_含税)"
    plan_registry.notify_source_change("DERI-GMV", "公式更正")
    new_plan = calc.build_compute_plan(101)
    new_sql = calc.preview_sql(101)
    assert new_plan["version_manifest"] != plan["version_manifest"]
    assert f"-- @version DERI-GMV v2" in new_sql
    stale_check = plan_registry.verify_sql_versions(101, sql)
    assert stale_check["ok"] is False
    mismatch_codes = {m["code"] for m in stale_check["mismatches"]}
    assert "DERI-GMV" in mismatch_codes
    # ARPU 自身口径未变（来源版本仍是 v1），但它依赖 GMV，其节点应因依赖版本变化而失效
    arpu_step = next(s for s in new_plan["steps"] if s.get("metric_code") == "DERI-ARPU")
    assert arpu_step["dependency_versions"] == {"DERI-GMV": 2}
    arpu_node = plan_registry.index_listing()["node_index"]
    arpu_entry = next(n for n in arpu_node if n["code"] == "DERI-ARPU")
    assert arpu_entry["dependency_versions"] == {"DERI-GMV": 2}

    # 新 SQL 自身校验通过
    assert plan_registry.verify_sql_versions(101, new_sql)["ok"] is True
    print("  [PASS] 场景4: 预览SQL版本与计划版本清单严格对应，旧SQL可被识别")


# ───────────────────────────────────────────────
# 场景 5：改动前后计划差异与实际重算范围对比
# ───────────────────────────────────────────────

def test_plan_diff_between_versions():
    reset_store()
    calc = MetricCalculator()
    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-ARPU", "ads_table_a")

    v1 = calc.build_compute_plan(101)
    assert v1["plan_version"] == 1
    scope_v1 = v1["recompute_scope"]
    assert scope_v1["full_recompute"] is True  # 首次生成全部节点重建

    find_metric("DERI-GMV")["计算公式"] = "SUM(订单金额_含税)"
    plan_registry.notify_source_change("DERI-GMV", "公式更正")
    v2 = calc.build_compute_plan(101)
    assert v2["plan_version"] == 2
    assert v2["recompute_scope"]["full_recompute"] is False

    diff = plan_registry.diff_plans(101, 1, 2)
    assert diff["ok"]
    assert diff["output_table"] == "ads_table_a"
    changed_ids = {c["node_id"] for c in diff["changed_nodes"]}
    assert any("DERI-GMV" in nid for nid in changed_ids)
    assert any("DERI-ARPU" in nid for nid in changed_ids)
    assert all("METR-TRADE" not in nid for nid in changed_ids)
    assert diff["sql_changed"] is True
    # GMV 来源版本 1→2；ARPU 自身口径未变（版本保持 1），但其节点指纹随依赖版本变化
    assert diff["version_changes"] == [{"code": "DERI-GMV", "from": 1, "to": 2}]
    assert diff["from_recompute_scope"]["rebuilt_count"] > diff["to_recompute_scope"]["rebuilt_count"]

    # 重算范围查询接口等价于计划内字段
    scope = plan_registry.get_recompute_scope(101)
    assert scope["rebuilt_node_ids"] == v2["recompute_scope"]["rebuilt_node_ids"]
    print("  [PASS] 场景5: 改动前后计划差异、SQL变化与重算范围可对比")


# ───────────────────────────────────────────────
# 场景 6：两阶段构建——生成计划与派生指标更正并发
# ───────────────────────────────────────────────

def test_concurrent_correction_rebuilds_with_new_formula():
    reset_store()
    calc = MetricCalculator()
    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-GMV", "ads_table_a")
    make_agg(102, "AGG-B-102", "METR-TRADE,DERI-GMV", "ads_table_b")
    calc.build_compute_plan(101)
    calc.build_compute_plan(102)

    gmv = find_metric("DERI-GMV")
    fired = threading.Event()

    def hook(phase: str, aggregate_id: int) -> None:
        # 在 A 计划构建的快照阶段（锁外）并发更正公式
        if phase == "snapshot" and aggregate_id == 101 and not fired.is_set():
            fired.set()
            gmv["计算公式"] = "SUM(订单金额_并发更正)"
            plan_registry.notify_source_change("DERI-GMV", "并发公式更正")

    plan_registry.set_build_hook(hook)
    try:
        # A 标记为待重建后触发构建
        plan_registry.notify_aggregate_change(101, "测试：强制进入构建")
        plan = calc.build_compute_plan(101)
    finally:
        plan_registry.set_build_hook(None)

    assert fired.is_set()
    assert plan_registry.get_stats()["build_retries"] == 1, "检测到并发更正必须重试"
    # 提交的计划绑定的是更正后的新版本与新公式，绝不带旧公式
    gmv_manifest = next(i for i in plan["version_manifest"] if i["code"] == "DERI-GMV")
    assert gmv_manifest["version"] == 2
    gmv_step = next(s for s in plan["steps"] if s.get("metric_code") == "DERI-GMV")
    assert gmv_step["formula"] == "SUM(订单金额_并发更正)"
    assert gmv_step["source_version"] == 2
    sql = calc.preview_sql(101)
    assert "-- @version DERI-GMV v2" in sql
    assert plan_registry.verify_sql_versions(101)["ok"] is True

    # B 计划：共用同一指标域，域节点不受更正影响，只有 GMV 链需要重建
    plan_b = calc.build_compute_plan(102)
    rebuilt_b = {d["code"] for d in plan_b["recompute_scope"]["rebuilt_steps"]}
    assert "METR-TRADE" not in rebuilt_b, "另一份计划应继续复用不受影响的指标域依赖"
    assert "DERI-GMV" in rebuilt_b
    assert plan_registry.verify_sql_versions(102)["ok"] is True
    print("  [PASS] 场景6: 并发更正触发重试，提交新公式；另一份计划复用未受影响依赖")


# ───────────────────────────────────────────────
# 场景 7：服务层更新入口与索引表查询
# ───────────────────────────────────────────────

def test_service_update_drives_version_and_index():
    reset_store()
    from app.services.derived_metric import DerivedMetricService
    from app.services.data_aggregate import DataAggregateService

    dm_service = DerivedMetricService()
    agg_service = DataAggregateService()
    calc = MetricCalculator()
    make_agg(101, "AGG-A-101", "METR-TRADE,DERI-GMV", "ads_table_a")
    calc.build_compute_plan(101)

    # 通过服务层修改公式（DERI-GMV 的 seed id=1）
    entry, message = dm_service.update_entry(1, {"计算公式": "SUM(订单金额_服务层更正)"})
    assert entry is not None and "来源版本 v1→v2" in message
    plan = calc.build_compute_plan(101)
    step = next(s for s in plan["steps"] if s.get("metric_code") == "DERI-GMV")
    assert step["formula"] == "SUM(订单金额_服务层更正)" and step["source_version"] == 2

    # 索引表可直接列出来源版本、节点与计划行
    listing = agg_service.get_plan_index()
    versions = {r["code"]: r["version"] for r in listing["source_versions"]}
    assert versions["DERI-GMV"] == 2
    assert any(row["aggregate_id"] == 101 for row in listing["plan_index"])

    affected = agg_service.get_affected_plans("DERI-GMV")
    assert affected["plans"][0]["aggregate_id"] == 101

    # 聚合配置自身修改只影响自己这份计划
    entry2, msg2 = agg_service.update_entry(101, {"聚合维度": "dt,shop_id,channel"})
    assert entry2 is not None
    rebuilt = calc.build_compute_plan(101)
    assert any(d["reason"].startswith("配置字段变更") for d in rebuilt["recompute_scope"]["rebuilt_steps"])
    print("  [PASS] 场景7: 服务层变更驱动版本递增，索引表可查询定位")


def test_parse_sources_helper():
    reset_store()
    domains, metrics = parse_sources("METR-TRADE, DERI-GMV,DERI-ARPU ,")
    assert domains == ["METR-TRADE"]
    assert metrics == ["DERI-GMV", "DERI-ARPU"]
    print("  [PASS] 辅助: 数据来源解析")


def run_all() -> bool:
    tests = [
        test_fast_path_no_tree_walk_on_unchanged_config,
        test_selective_invalidation_and_node_isolation,
        test_unaffected_plan_reuses_shared_domain,
        test_preview_sql_matches_plan_versions,
        test_plan_diff_between_versions,
        test_concurrent_correction_rebuilds_with_new_formula,
        test_service_update_drives_version_and_index,
        test_parse_sources_helper,
    ]
    print("\n=== 聚合计划索引表专项测试 ===")
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  [FAIL] {t.__name__}: {e}")
            failed += 1
    print(f"\n=== 结果: {passed} 通过, {failed} 失败, 共 {len(tests)} 项 ===")
    return failed == 0


if __name__ == "__main__":
    sys.exit(0 if run_all() else 1)
