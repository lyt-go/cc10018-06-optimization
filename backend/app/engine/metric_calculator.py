"""指标计算引擎：支持原子指标→派生指标→聚合指标的链式计算。

核心能力：
1. 公式解析：将「SUM(订单金额)/COUNT(DISTINCT 用户ID)」解析为可执行结构
2. 口径版本控制：同一指标编号支持多版本，来源变更由聚合计划索引表按指纹递增版本
3. 依赖追踪：记录派生指标对原子指标、聚合配置对派生指标的依赖关系
4. 计算编排：按依赖拓扑排序执行，避免循环依赖

性能与复用：计算计划的生成、节点复用、失效判定与 SQL 版本绑定统一委托给
``app.engine.plan_registry``（进程内单例索引表），本类保留原有公开 API 作为门面，
连续读取未变更配置时走索引快速路径，不再重走完整依赖树。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.engine.formula import MetricFormula
from app.engine.plan_registry import plan_registry


class MetricCalculator:
    """指标计算引擎（门面）；有状态的索引能力收敛在 plan_registry 单例中。"""

    def __init__(self) -> None:
        # 保留属性以兼容历史调用方；实际缓存由全局索引表管理
        self._cache: dict[str, Any] = {}
        self.registry = plan_registry

    def list_active_domains(self) -> list[dict[str, Any]]:
        """获取所有已启用的指标域。"""
        return [r for r in store.rows("metric_domain") if r.get("status") == "已启用"]

    def list_effective_metrics(self) -> list[dict[str, Any]]:
        """获取所有已生效的派生指标。"""
        return [r for r in store.rows("derived_metric") if r.get("status") == "已生效"]

    def get_metric_by_code(self, code: str) -> dict[str, Any] | None:
        for r in store.rows("derived_metric"):
            if r.get("指标编号") == code:
                return r
        return None

    def parse_formula(self, formula_raw: str) -> MetricFormula:
        return MetricFormula(formula_raw)

    def compute_dependencies(self, metric_code: str) -> list[str]:
        """获取指定指标的直接依赖（父指标编号列表）。"""
        metric = self.get_metric_by_code(metric_code)
        if not metric:
            return []
        parent = metric.get("父指标编号", "")
        if parent and parent != metric_code:
            return [parent]
        return []

    def topological_sort(self, metric_codes: list[str]) -> list[str]:
        """按依赖拓扑排序指标，保证先计算原子指标。"""
        visited: set[str] = set()
        result: list[str] = []

        def dfs(code: str) -> None:
            if code in visited:
                return
            visited.add(code)
            for dep in self.compute_dependencies(code):
                dfs(dep)
            result.append(code)

        for code in metric_codes:
            dfs(code)
        return result

    def build_compute_plan(self, aggregate_config_id: int, *, force: bool = False) -> dict[str, Any]:
        """为一条聚合配置生成（或从索引表复用）计算计划。"""
        return self.registry.get_plan(aggregate_config_id, force=force)

    def preview_sql(self, aggregate_config_id: int) -> str:
        """为聚合配置生成预览SQL；SQL 中固化本次计划列出的全部来源版本。"""
        plan = self.registry.get_plan(aggregate_config_id)
        if not plan.get("ok"):
            return "-- 计算计划生成失败"
        return plan.get("preview_sql") or self.registry.render_sql(plan)

    def validate_metric_chain(self, domain_code: str, metric_code: str) -> dict[str, Any]:
        """校验从指标域到派生指标的链路完整性。"""
        domain = next((r for r in store.rows("metric_domain") if r.get("域编号") == domain_code), None)
        metric = self.get_metric_by_code(metric_code)

        errors = []
        if not domain:
            errors.append(f"指标域 {domain_code} 不存在")
        elif domain.get("status") != "已启用":
            errors.append(f"指标域 {domain_code} 状态为 {domain.get('status')}，非已启用")

        if not metric:
            errors.append(f"派生指标 {metric_code} 不存在")
        elif metric.get("status") != "已生效":
            errors.append(f"派生指标 {metric_code} 状态为 {metric.get('status')}，非已生效")

        if domain and metric:
            # 校验父指标是否属于该域（沿父链回溯到指标域）
            parent = metric.get("父指标编号", "")
            root = parent
            seen: set[str] = set()
            while root and root.startswith("DERI-") and root not in seen:
                seen.add(root)
                parent_row = self.get_metric_by_code(root)
                root = (parent_row or {}).get("父指标编号", "")
            if parent.startswith("METR-") and parent != domain_code:
                errors.append(f"父指标 {parent} 不属于指标域 {domain_code}")
            elif root.startswith("METR-") and root != domain_code:
                errors.append(f"祖先指标域 {root} 与指定指标域 {domain_code} 不一致")

        return {
            "ok": len(errors) == 0,
            "domain_code": domain_code,
            "metric_code": metric_code,
            "errors": errors,
        }
