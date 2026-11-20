"""聚合计划索引表：来源版本管理、节点级复用索引与可复用聚合计划。

设计目标
--------
1. 聚合配置详情与计划预览反复从同样的指标域/派生指标生成计算步骤，本模块把生成结果
   沉淀为「聚合计划索引表」，连续读取未变更配置时直接命中索引，不再重走依赖树。
2. 可复用节点绑定实际来源版本与依赖版本：指标域节点绑定域行指纹，派生指标节点绑定
   指标行指纹 + 全部父依赖版本，任一变化才使对应节点失效；父指标变更只会让依赖它的
   下游节点失效，不相关节点继续复用。
3. 节点按聚合配置隔离（node_id 带 aggregate_id），两份输出表即使共用同一父指标，也
   各自持有独立节点，不会串用对方的计算步骤。
4. 派生指标公式/口径变更后，可通过计划索引表反查受影响的计划与实际重算范围；生成的
   聚合 SQL 带版本清单，可校验 SQL 与本次计划列出的版本严格对应。
5. 计划生成采用「快照 → 构建 → 提交」两阶段：构建期间来源发生更正会被检测到并重试，
   依赖更正节点的计划不会带着旧公式提交，未受影响的另一份计划继续复用原节点。
"""
from __future__ import annotations

import hashlib
import threading
from copy import deepcopy
from typing import Any, Callable

from app.store import store
from app.engine.formula import MetricFormula

DOMAIN_PREFIX = "METR-"
METRIC_PREFIX = "DERI-"

HISTORY_LIMIT = 20


def _fingerprint(payload: str) -> str:
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def parse_sources(source: str) -> tuple[list[str], list[str]]:
    """把聚合配置的「数据来源」拆成指标域编号与派生指标编号。"""
    domain_codes: list[str] = []
    metric_codes: list[str] = []
    for part in (source or "").split(","):
        code = part.strip()
        if code.startswith(DOMAIN_PREFIX):
            domain_codes.append(code)
        elif code.startswith(METRIC_PREFIX):
            metric_codes.append(code)
    return domain_codes, metric_codes


class SourceVersionManager:
    """来源版本表：记录每个指标域/派生指标的实际版本与内容指纹。

    版本不依赖人工维护的「口径版本」字段，而是由行内容指纹驱动：公式、口径、父指标、
    数据源表等实质内容发生变化时版本 +1，仅改备注等不进指纹的字段不会产生新版本。
    """

    DOMAIN_FIELDS = ("域编号", "数据源表", "域状态", "status", "更新频率")
    METRIC_FIELDS = ("指标编号", "父指标编号", "计算公式", "口径版本", "统计粒度", "业务限定", "status")

    def __init__(self) -> None:
        self._versions: dict[str, int] = {}
        self._fingerprints: dict[str, str] = {}
        self._kinds: dict[str, str] = {}
        self._store_token: int | None = None

    def _current_token(self) -> int:
        # store 被整体重置（测试重新装载 seed）时表容器对象会换身份
        return id(store._tables)

    def _find_row(self, code: str) -> dict[str, Any] | None:
        if code.startswith(DOMAIN_PREFIX):
            return next((r for r in store.rows("metric_domain") if r.get("域编号") == code), None)
        if code.startswith(METRIC_PREFIX):
            return next((r for r in store.rows("derived_metric") if r.get("指标编号") == code), None)
        return None

    def kind_of(self, code: str) -> str:
        self.ensure_initialized()
        return self._kinds.get(code, "unknown")

    def ensure_initialized(self) -> None:
        token = self._current_token()
        if self._store_token == token and self._versions:
            return
        self._versions = {}
        self._fingerprints = {}
        self._kinds = {}
        for row in store.rows("metric_domain"):
            code = str(row.get("域编号", "")).strip()
            if code:
                self._kinds[code] = "domain"
                self._versions[code] = 1
                self._fingerprints[code] = self.compute_fingerprint(code)
        for row in store.rows("derived_metric"):
            code = str(row.get("指标编号", "")).strip()
            if code:
                self._kinds[code] = "metric"
                self._versions[code] = 1
                self._fingerprints[code] = self.compute_fingerprint(code)
        self._store_token = token

    def compute_fingerprint(self, code: str) -> str:
        row = self._find_row(code)
        if row is None:
            return _fingerprint(f"missing:{code}")
        if code.startswith(DOMAIN_PREFIX):
            payload = "|".join(str(row.get(f, "")) for f in self.DOMAIN_FIELDS)
        else:
            payload = "|".join(str(row.get(f, "")) for f in self.METRIC_FIELDS)
        return _fingerprint(payload)

    def register(self, code: str, kind: str) -> None:
        """登记新建来源，初始版本为 1。"""
        self.ensure_initialized()
        if code and code not in self._versions:
            self._kinds[code] = kind
            self._versions[code] = 1
            self._fingerprints[code] = self.compute_fingerprint(code)

    def bump(self, code: str, reason: str) -> dict[str, Any] | None:
        """内容变更后比对指纹，实质变化才递增版本；返回变更记录。"""
        self.ensure_initialized()
        new_fp = self.compute_fingerprint(code)
        old_fp = self._fingerprints.get(code)
        if old_fp == new_fp and code in self._versions:
            return None
        old_version = self._versions.get(code, 0)
        if code not in self._versions:
            self._kinds[code] = "domain" if code.startswith(DOMAIN_PREFIX) else "metric"
        new_version = max(old_version, 0) + 1
        self._versions[code] = new_version
        self._fingerprints[code] = new_fp
        return {
            "code": code,
            "kind": self._kinds.get(code, "unknown"),
            "old_version": old_version,
            "new_version": new_version,
            "old_fingerprint": old_fp,
            "new_fingerprint": new_fp,
            "reason": reason,
        }

    def get_version(self, code: str) -> int:
        self.ensure_initialized()
        return self._versions.get(code, 0)

    def get_fingerprint(self, code: str) -> str:
        self.ensure_initialized()
        return self._fingerprints.get(code, "")

    def snapshot(self) -> dict[str, int]:
        self.ensure_initialized()
        return dict(self._versions)

    def listing(self) -> list[dict[str, Any]]:
        self.ensure_initialized()
        return [
            {
                "code": code,
                "kind": self._kinds.get(code, "unknown"),
                "version": self._versions[code],
                "fingerprint": self._fingerprints.get(code, ""),
            }
            for code in sorted(self._versions)
        ]


class PlanRegistry:
    """聚合计划索引表（进程内单例，见模块末尾 plan_registry）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.sources = SourceVersionManager()
        # 节点索引：node_id -> 节点（节点自带 aggregate_id，跨输出表天然隔离）
        self._nodes: dict[str, dict[str, Any]] = {}
        # 计划索引：aggregate_id -> 计划索引行
        self._plans: dict[int, dict[str, Any]] = {}
        self._store_token: int | None = None
        # 观测计数：验证「不再重走完整依赖树」与节点复用率
        self._stats = {
            "plan_requests": 0,
            "full_builds": 0,
            "tree_walks": 0,
            "fast_path_hits": 0,
            "node_cache_hits": 0,
            "node_rebuilds": 0,
            "build_retries": 0,
            "committed_builds": 0,
        }
        # 并发测试钩子：在两阶段构建的快照阶段回调（此时锁已释放）
        self._build_hook: Callable[[str, int], None] | None = None

    # ── 基础设施 ──────────────────────────────────────────────

    def set_build_hook(self, hook: Callable[[str, int], None] | None) -> None:
        self._build_hook = hook

    def _invoke_hook(self, phase: str, aggregate_id: int) -> None:
        if self._build_hook is not None:
            self._build_hook(phase, aggregate_id)

    def _current_token(self) -> int:
        return id(store._tables)

    def _reset_if_store_replaced(self) -> None:
        token = self._current_token()
        if self._store_token == token:
            return
        self._nodes.clear()
        self._plans.clear()
        self._store_token = token
        for key in self._stats:
            self._stats[key] = 0
        self.sources = SourceVersionManager()

    def reset(self) -> None:
        """清空索引（测试用）。"""
        with self._lock:
            self._nodes.clear()
            self._plans.clear()
            self._store_token = None
            self.sources = SourceVersionManager()
            for key in self._stats:
                self._stats[key] = 0

    def get_stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._stats)

    @staticmethod
    def _node_id(aggregate_id: int, kind: str, code: str) -> str:
        # 聚合配置作为命名空间：共用父指标的两份输出表得到不同节点，杜绝串用
        return f"agg{aggregate_id}:{kind}:{code}"

    # ── 依赖图 ────────────────────────────────────────────────

    def _parent_of(self, metric_code: str) -> str:
        row = self.sources._find_row(metric_code)
        if not row:
            return ""
        parent = str(row.get("父指标编号", "")).strip()
        return "" if parent == metric_code else parent

    def _metric_closure(self, metric_codes: list[str]) -> list[str]:
        """派生指标沿父指标展开的拓扑序（父在前），带循环依赖保护。"""
        visited: set[str] = set()
        order: list[str] = []
        stack: set[str] = set()

        def dfs(code: str) -> None:
            if code in visited or code in stack:
                return
            stack.add(code)
            parent = self._parent_of(code)
            if parent.startswith(METRIC_PREFIX):
                dfs(parent)
            stack.discard(code)
            visited.add(code)
            order.append(code)

        for code in metric_codes:
            dfs(code)
        return order

    def _plan_closure(self, agg: dict[str, Any]) -> list[str]:
        domain_codes, metric_codes = parse_sources(agg.get("数据来源", ""))
        return sorted(domain_codes) + self._metric_closure(metric_codes)

    def _descendants(self, code: str) -> set[str]:
        """反查：所有（传递）依赖该来源的派生指标。"""
        result: set[str] = set()
        frontier = [code]
        while frontier:
            current = frontier.pop()
            for row in store.rows("derived_metric"):
                child = str(row.get("指标编号", "")).strip()
                parent = str(row.get("父指标编号", "")).strip()
                if parent == current and child and child not in result:
                    result.add(child)
                    frontier.append(child)
        return result

    # ── 指纹 ──────────────────────────────────────────────────

    def _config_fingerprint(self, agg: dict[str, Any]) -> str:
        fields = ("配置编号", "数据来源", "聚合维度", "聚合函数", "过滤条件", "输出表名", "增量键", "更新策略")
        return _fingerprint("|".join(str(agg.get(f, "")) for f in fields))

    def _root_fingerprint(self, agg: dict[str, Any], closure: list[str], versions: dict[str, int]) -> str:
        # 只纳入本计划闭包内的来源版本：闭包外的指标更正不得使本计划根指纹变化
        parts = [self._config_fingerprint(agg)]
        for code in closure:
            parts.append(f"{code}@{versions.get(code, 0)}")
        return _fingerprint("|".join(parts))

    # ── 变更通知与影响面定位 ──────────────────────────────────

    def notify_source_change(self, code: str, reason: str) -> dict[str, Any]:
        """来源（指标域/派生指标）发生更正：递增版本并标记受影响计划。

        返回受影响的聚合计划清单，可直接用于「通过索引表定位受影响计划」。
        """
        with self._lock:
            self._reset_if_store_replaced()
            change = self.sources.bump(code, reason)
            if change is None:
                return {"changed": False, "code": code, "affected_plans": []}
            impacted = {code, *self._descendants(code)}
            affected: list[dict[str, Any]] = []
            for aggregate_id, entry in self._plans.items():
                agg = store.find("data_aggregate", aggregate_id)
                if agg is None:
                    continue
                domain_codes, metric_codes = parse_sources(agg.get("数据来源", ""))
                # 计划真正绑定的来源：直接引用的指标域/派生指标 + 派生指标的传递父链
                bound = set(domain_codes) | set(metric_codes) | set(self._metric_closure(metric_codes))
                hit = impacted & bound
                if not hit:
                    continue
                for hit_code in hit:
                    entry["stale"][hit_code] = reason
                affected.append({
                    "aggregate_id": aggregate_id,
                    "输出表名": agg.get("输出表名", ""),
                    "plan_version": entry["plan_version"],
                    "impacted_nodes": sorted(hit),
                    "change": change,
                })
            return {"changed": True, "change": change, "affected_plans": affected}

    def notify_aggregate_change(self, aggregate_id: int, reason: str) -> None:
        """聚合配置自身字段变更：标记该计划配置节点失效。"""
        with self._lock:
            self._reset_if_store_replaced()
            entry = self._plans.get(aggregate_id)
            if entry is not None:
                entry["stale"]["__config__"] = reason

    def find_affected_plans(self, code: str) -> list[dict[str, Any]]:
        """通过计划索引表定位引用某来源（含传递依赖）的全部计划。"""
        with self._lock:
            self._reset_if_store_replaced()
            closure = {code, *self._descendants(code)}
            result: list[dict[str, Any]] = []
            for aggregate_id, entry in self._plans.items():
                agg = store.find("data_aggregate", aggregate_id)
                if agg is None:
                    continue
                plan_codes = set(self._plan_closure(agg))
                hit = closure & plan_codes
                if not hit:
                    continue
                result.append({
                    "aggregate_id": aggregate_id,
                    "输出表名": agg.get("输出表名", ""),
                    "plan_version": entry["plan_version"],
                    "matched_nodes": sorted(hit),
                    "node_ids": [
                        nid for nid in entry["node_ids"]
                        if any(f":{c}" in nid for c in hit)
                    ],
                })
            return result

    # ── 计划获取（快速路径 + 两阶段构建）──────────────────────

    def get_plan(self, aggregate_id: int, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            self._reset_if_store_replaced()
            self.sources.ensure_initialized()
            self._stats["plan_requests"] += 1
            agg = store.find("data_aggregate", aggregate_id)
            if agg is None:
                return {"ok": False, "aggregate_id": aggregate_id, "error": "聚合配置不存在"}
            entry = self._plans.get(aggregate_id)
            versions = self.sources.snapshot()
            if entry and not force and not entry["stale"]:
                # 闭包按当前配置实时计算；根指纹只含闭包内版本，
                # 闭包外的来源更正不会让本计划错过快速路径
                closure = self._plan_closure(agg)
                if entry["root_fingerprint"] == self._root_fingerprint(agg, closure, versions):
                    self._stats["fast_path_hits"] += 1
                    return deepcopy(entry["plan"])
            # 需要构建：进入两阶段流程（最多重试 1 次）
        for attempt in range(2):
            plan = self._build_plan(aggregate_id, attempt)
            if plan.get("_retry"):
                continue
            return plan
        return plan  # 重试后仍漂移则返回最后一次构建结果

    def _build_plan(self, aggregate_id: int, attempt: int) -> dict[str, Any]:
        # 阶段 1：锁内取快照
        with self._lock:
            self._reset_if_store_replaced()
            self.sources.ensure_initialized()
            agg = store.find("data_aggregate", aggregate_id)
            if agg is None:
                return {"ok": False, "aggregate_id": aggregate_id, "error": "聚合配置不存在"}
            snapshot = self.sources.snapshot()
            entry = self._plans.get(aggregate_id)
            stale = dict(entry["stale"]) if entry else {}

        # 阶段 2：锁外读取来源、解析依赖、组装节点（并发更正可能在此期间发生）
        self._invoke_hook("snapshot", aggregate_id)
        assembled = self._assemble(aggregate_id, agg, snapshot, stale)

        # 阶段 3：锁内校验版本未漂移后提交
        with self._lock:
            current = self.sources.snapshot()
            drifted = {
                code for code, ver in snapshot.items()
                if current.get(code) != ver
            }
            if drifted and attempt == 0:
                # 构建期间来源被更正：放弃本次带旧版本的结果，整体重试。
                # 只标记当前计划中真正被更正触及的节点，另一份共用域的计划不受牵连。
                self._stats["build_retries"] += 1
                entry2 = self._plans.get(aggregate_id)
                if entry2 is not None:
                    entry2.setdefault("stale", {}).update(
                        {code: "构建期间检测到并发更正，已重试"
                         for code in drifted if code in set(entry2["closure"])}
                    )
                return {"ok": False, "_retry": True, "drifted": sorted(drifted)}

            plan, touched_nodes, root_fp, closure = assembled
            for node in touched_nodes:
                self._nodes[node["node_id"]] = node
            old_entry = self._plans.get(aggregate_id)
            plan_version = (old_entry["plan_version"] + 1) if old_entry else 1
            plan["plan_version"] = plan_version
            plan["root_fingerprint"] = root_fp
            sql = self.render_sql(plan)
            plan["preview_sql"] = sql
            history = list(old_entry["history"]) if old_entry else []
            history.append(deepcopy(plan))
            history = history[-HISTORY_LIMIT:]
            self._plans[aggregate_id] = {
                "aggregate_id": aggregate_id,
                "plan_version": plan_version,
                "root_fingerprint": root_fp,
                "closure": closure,
                "node_ids": [node["node_id"] for node in touched_nodes],
                "stale": {},
                "plan": deepcopy(plan),
                "history": history,
            }
            self._stats["committed_builds"] += 1
            return deepcopy(plan)

    # ── 节点组装 ──────────────────────────────────────────────

    def _assemble(
        self,
        aggregate_id: int,
        agg: dict[str, Any],
        versions: dict[str, int],
        stale: dict[str, str],
    ) -> tuple[dict[str, Any], list[dict[str, Any]], str, list[str]]:
        self._stats["full_builds"] += 1
        self._stats["tree_walks"] += 1

        domain_codes, metric_codes = parse_sources(agg.get("数据来源", ""))
        ordered_metrics = self._metric_closure(metric_codes)
        closure = sorted(domain_codes) + ordered_metrics

        steps: list[dict[str, Any]] = []
        touched: list[dict[str, Any]] = []
        rebuilt_nodes: list[str] = []
        reused_nodes: list[str] = []
        rebuild_details: list[dict[str, Any]] = []
        manifest: list[dict[str, Any]] = []
        step_no = 1

        # 1) 指标域加载节点
        for code in domain_codes:
            node_id = self._node_id(aggregate_id, "load_domain", code)
            source_ver = versions.get(code, 0)
            source_fp = self.sources.get_fingerprint(code)
            row = self.sources._find_row(code)
            payload = {
                "type": "load_domain",
                "domain_code": code,
                "source_table": (row or {}).get("数据源表", ""),
                "description": f"从指标域 {code} 加载原子数据",
            }
            node, reused, reason = self._resolve_node(
                node_id, aggregate_id, "load_domain", code, source_ver, source_fp,
                payload, {}, stale,
            )
            touched.append(node)
            (reused_nodes if reused else rebuilt_nodes).append(node_id)
            if not reused:
                rebuild_details.append({"node_id": node_id, "code": code, "kind": "load_domain", "reason": reason})
            step = {"step": step_no, **payload, "node_id": node_id,
                    "source_version": source_ver, "fingerprint": node["fingerprint"]}
            steps.append(step)
            manifest.append({"code": code, "kind": "load_domain", "version": source_ver,
                             "source_table": (row or {}).get("数据源表", ""),
                             "fingerprint": source_fp, "node_id": node_id})
            step_no += 1

        # 2) 派生指标计算节点（拓扑序，父节点在前）
        for code in ordered_metrics:
            node_id = self._node_id(aggregate_id, "compute_derived", code)
            source_ver = versions.get(code, 0)
            source_fp = self.sources.get_fingerprint(code)
            row = self.sources._find_row(code)
            formula = MetricFormula((row or {}).get("计算公式", ""))
            parent = (row or {}).get("父指标编号", "")
            dep_versions = {parent: versions.get(parent, 0)} if parent else {}
            payload = {
                "type": "compute_derived",
                "metric_code": code,
                "parent_code": parent,
                "formula": formula.raw,
                "caliber_version": (row or {}).get("口径版本", ""),
                "sql_expr": formula.to_sql_expr(),
                "description": f"计算派生指标 {code}: {formula.raw}",
            }
            node, reused, reason = self._resolve_node(
                node_id, aggregate_id, "compute_derived", code, source_ver, source_fp,
                payload, dep_versions, stale,
            )
            touched.append(node)
            (reused_nodes if reused else rebuilt_nodes).append(node_id)
            if not reused:
                rebuild_details.append({"node_id": node_id, "code": code,
                                        "kind": "compute_derived", "reason": reason})
            step = {"step": step_no, **payload, "node_id": node_id,
                    "source_version": source_ver, "dependency_versions": dep_versions,
                    "fingerprint": node["fingerprint"]}
            steps.append(step)
            manifest.append({"code": code, "kind": "compute_derived", "version": source_ver,
                             "formula": formula.raw, "caliber_version": (row or {}).get("口径版本", ""),
                             "dependency_versions": dep_versions, "fingerprint": source_fp,
                             "node_id": node_id})
            step_no += 1

        # 3) 聚合输出节点
        agg_node_id = self._node_id(aggregate_id, "aggregate", str(aggregate_id))
        config_fp = self._config_fingerprint(agg)
        agg_payload = {
            "type": "aggregate",
            "dimensions": agg.get("聚合维度", ""),
            "functions": agg.get("聚合函数", ""),
            "output_table": agg.get("输出表名", ""),
            "description": f"按 {agg.get('聚合维度', '')} 聚合，函数 {agg.get('聚合函数', '')}",
        }
        agg_dep_versions = {code: versions.get(code, 0) for code in closure}
        agg_node, agg_reused, agg_reason = self._resolve_node(
            agg_node_id, aggregate_id, "aggregate", str(aggregate_id), 0, config_fp,
            agg_payload, agg_dep_versions, stale, config_fp=config_fp,
        )
        touched.append(agg_node)
        (reused_nodes if agg_reused else rebuilt_nodes).append(agg_node_id)
        if not agg_reused:
            rebuild_details.append({"node_id": agg_node_id, "code": f"AGG:{aggregate_id}",
                                    "kind": "aggregate", "reason": agg_reason})
        steps.append({"step": step_no, **agg_payload, "node_id": agg_node_id,
                      "fingerprint": agg_node["fingerprint"]})

        recompute_scope = {
            "aggregate_id": aggregate_id,
            "输出表名": agg.get("输出表名", ""),
            "full_recompute": len(rebuilt_nodes) == len(touched),
            "rebuilt_node_ids": rebuilt_nodes,
            "reused_node_ids": reused_nodes,
            "rebuilt_steps": rebuild_details,
            "rebuilt_count": len(rebuilt_nodes),
            "reused_count": len(reused_nodes),
        }
        plan = {
            "ok": True,
            "aggregate_id": aggregate_id,
            "steps": steps,
            "version_manifest": manifest,
            "recompute_scope": recompute_scope,
            "reused_nodes": len(reused_nodes),
            "rebuilt_nodes": len(rebuilt_nodes),
        }
        root_fp = self._root_fingerprint(agg, closure, versions)
        return plan, touched, root_fp, closure

    def _resolve_node(
        self,
        node_id: str,
        aggregate_id: int,
        kind: str,
        code: str,
        source_version: int,
        source_fingerprint: str,
        payload: dict[str, Any],
        dependency_versions: dict[str, int],
        stale: dict[str, str],
        *,
        config_fp: str | None = None,
    ) -> tuple[dict[str, Any], bool, str]:
        """返回 (节点, 是否复用旧节点, 失效原因)。"""
        fp_material = config_fp if kind == "aggregate" else source_fingerprint
        fingerprint = _fingerprint(
            fp_material + "|" + "|".join(f"{k}={v}" for k, v in sorted(dependency_versions.items()))
        )
        existing = self._nodes.get(node_id)
        if existing is not None and existing["fingerprint"] == fingerprint:
            self._stats["node_cache_hits"] += 1
            return existing, True, ""

        reason = self._invalidation_reason(kind, code, existing, source_version, dependency_versions, stale)
        self._stats["node_rebuilds"] += 1
        node = {
            "node_id": node_id,
            "aggregate_id": aggregate_id,
            "kind": kind,
            "code": code,
            "source_version": source_version,
            "source_fingerprint": source_fingerprint,
            "dependency_versions": dict(dependency_versions),
            "fingerprint": fingerprint,
            "payload": deepcopy(payload),
        }
        return node, False, reason

    @staticmethod
    def _invalidation_reason(
        kind: str,
        code: str,
        existing: dict[str, Any] | None,
        source_version: int,
        dependency_versions: dict[str, int],
        stale: dict[str, str],
    ) -> str:
        if code in stale:
            return stale[code]
        if "__config__" in stale and kind == "aggregate":
            return stale["__config__"]
        if existing is None:
            return "首次构建"
        if existing.get("source_version") != source_version and kind != "aggregate":
            return f"来源版本变更 v{existing.get('source_version', 0)}→v{source_version}"
        if existing.get("dependency_versions") != dependency_versions:
            return "依赖版本变更"
        return "节点指纹变更"

    # ── SQL 生成与版本校验 ────────────────────────────────────

    def render_sql(self, plan: dict[str, Any]) -> str:
        """按计划列出的版本生成聚合 SQL，每个 CTE 标注节点与来源版本。"""
        lines = [
            "-- 指标聚合计算SQL（由聚合计划索引表生成）",
            f"-- 计划版本: plan-v{plan.get('plan_version', 0)}  根指纹: {plan.get('root_fingerprint', '')}",
        ]
        for item in plan.get("version_manifest", []):
            lines.append(f"-- @version {item['code']} v{item['version']} {item['fingerprint']}")
        lines.append("WITH")
        ctes: list[str] = []
        aggregate_step: dict[str, Any] | None = None
        for step in plan["steps"]:
            kind = step["type"]
            node_id = step["node_id"].replace(":", "_").replace("-", "_")
            ver = step.get("source_version", 0)
            if kind == "load_domain":
                ctes.append(
                    f"  {node_id} AS (\n"
                    f"    SELECT * FROM source_table WHERE domain_id = '{step['domain_code']}'\n"
                    f"  ) -- {step['node_id']} 来源版本 v{ver}"
                )
            elif kind == "compute_derived":
                ctes.append(
                    f"  {node_id} AS (\n"
                    f"    SELECT {step['sql_expr']} AS val FROM domain_base\n"
                    f"  ) -- {step['node_id']} 来源版本 v{ver}"
                )
            else:
                aggregate_step = step
        lines.append(",\n".join(ctes))
        if aggregate_step:
            lines.append(
                f"\nSELECT {aggregate_step['dimensions']}, {aggregate_step['functions']}\n"
                f"FROM aggregated\nGROUP BY {aggregate_step['dimensions']};"
            )
            lines.append(f"\n-- 输出目标表: {aggregate_step['output_table']}")
        return "\n".join(lines)

    def verify_sql_versions(self, aggregate_id: int, sql: str | None = None) -> dict[str, Any]:
        """校验 SQL 中固化的版本清单与当前来源版本、计划版本清单完全一致。"""
        with self._lock:
            entry = self._plans.get(aggregate_id)
            if entry is None:
                return {"ok": False, "error": "计划尚未生成", "mismatches": []}
            plan = entry["plan"]
            sql = sql or plan.get("preview_sql") or self.render_sql(plan)
            current = self.sources.snapshot()
            embedded: dict[str, int] = {}
            for line in sql.splitlines():
                marker = line.split("-- @version", 1)
                if len(marker) == 2:
                    parts = marker[1].strip().split()
                    if len(parts) >= 2 and parts[1].startswith("v"):
                        embedded[parts[0]] = int(parts[1][1:])
            mismatches: list[dict[str, Any]] = []
            for item in plan["version_manifest"]:
                code, ver = item["code"], item["version"]
                if embedded.get(code) != ver:
                    mismatches.append({"code": code, "plan_version": ver, "sql_version": embedded.get(code)})
                if current.get(code) != ver:
                    mismatches.append({"code": code, "plan_version": ver,
                                       "current_version": current.get(code), "type": "来源已更正"})
            return {
                "ok": not mismatches,
                "aggregate_id": aggregate_id,
                "plan_version": plan["plan_version"],
                "embedded_versions": embedded,
                "manifest_versions": {i["code"]: i["version"] for i in plan["version_manifest"]},
                "current_versions": {c: current.get(c) for c in embedded},
                "mismatches": mismatches,
            }

    # ── 计划对比、重算范围与索引查询 ──────────────────────────

    def get_recompute_scope(self, aggregate_id: int) -> dict[str, Any]:
        with self._lock:
            entry = self._plans.get(aggregate_id)
            if entry is None:
                return {"ok": False, "error": "计划尚未生成", "aggregate_id": aggregate_id}
            scope = deepcopy(entry["plan"]["recompute_scope"])
            scope["ok"] = True
            scope["pending_stale"] = dict(entry["stale"])
            return scope

    def diff_plans(self, aggregate_id: int, from_version: int, to_version: int) -> dict[str, Any]:
        """比较同一聚合配置两个计划版本的步骤、版本清单与重算范围。"""
        with self._lock:
            entry = self._plans.get(aggregate_id)
            if entry is None:
                return {"ok": False, "error": "计划尚未生成", "aggregate_id": aggregate_id}
            old = next((p for p in entry["history"] if p["plan_version"] == from_version), None)
            new = next((p for p in entry["history"] if p["plan_version"] == to_version), None)
            if old is None or new is None:
                return {"ok": False, "error": "指定的计划版本不存在",
                        "available": [p["plan_version"] for p in entry["history"]]}
            old_nodes = {s["node_id"]: s for s in old["steps"]}
            new_nodes = {s["node_id"]: s for s in new["steps"]}
            changed = [
                {"node_id": nid, "from": old_nodes[nid].get("fingerprint"),
                 "to": new_nodes[nid].get("fingerprint")}
                for nid in old_nodes.keys() & new_nodes.keys()
                if old_nodes[nid].get("fingerprint") != new_nodes[nid].get("fingerprint")
            ]
            old_manifest = {i["code"]: i["version"] for i in old["version_manifest"]}
            new_manifest = {i["code"]: i["version"] for i in new["version_manifest"]}
            version_changes = [
                {"code": code, "from": old_manifest.get(code), "to": new_manifest.get(code)}
                for code in sorted(set(old_manifest) | set(new_manifest))
                if old_manifest.get(code) != new_manifest.get(code)
            ]
            return {
                "ok": True,
                "aggregate_id": aggregate_id,
                "from_version": from_version,
                "to_version": to_version,
                "output_table": new["steps"][-1].get("output_table", ""),
                "added_nodes": sorted(new_nodes.keys() - old_nodes.keys()),
                "removed_nodes": sorted(old_nodes.keys() - new_nodes.keys()),
                "changed_nodes": changed,
                "version_changes": version_changes,
                "from_recompute_scope": old["recompute_scope"],
                "to_recompute_scope": new["recompute_scope"],
                "sql_changed": old.get("preview_sql") != new.get("preview_sql"),
            }

    def index_listing(self) -> dict[str, Any]:
        """聚合计划索引表全貌：来源版本、节点索引、计划索引三类行。"""
        with self._lock:
            self._reset_if_store_replaced()
            self.sources.ensure_initialized()
            return {
                "source_versions": self.sources.listing(),
                "node_index": [
                    {
                        "node_id": nid,
                        "aggregate_id": node["aggregate_id"],
                        "kind": node["kind"],
                        "code": node["code"],
                        "source_version": node["source_version"],
                        "dependency_versions": node["dependency_versions"],
                        "fingerprint": node["fingerprint"],
                    }
                    for nid, node in sorted(self._nodes.items())
                ],
                "plan_index": [
                    {
                        "aggregate_id": aggregate_id,
                        "plan_version": entry["plan_version"],
                        "root_fingerprint": entry["root_fingerprint"],
                        "closure": entry["closure"],
                        "node_ids": entry["node_ids"],
                        "stale": dict(entry["stale"]),
                    }
                    for aggregate_id, entry in sorted(self._plans.items())
                ],
                "stats": dict(self._stats),
            }


# 进程内单例：所有 MetricCalculator / 服务 / 路由共用同一份索引
plan_registry = PlanRegistry()
