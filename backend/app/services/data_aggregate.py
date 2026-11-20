"""数据聚合业务规则：状态流转、字段校验、跨模块依赖校验与计算计划生成。

计算计划与预览 SQL 统一通过聚合计划索引表（plan_registry）获取：首次生成后按
aggregate_id 隔离缓存节点，连续读取未变更配置走快速路径，不再重走完整依赖树。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.engine.metric_calculator import MetricCalculator
from app.engine.plan_registry import plan_registry

MODULE = "data_aggregate"
LIST_FIELDS = ["配置编号", "数据来源", "聚合维度", "聚合函数", "过滤条件", "输出表名", "增量键", "更新策略"]
REQUIRED_FIELDS = ["配置编号", "数据来源", "聚合维度"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]


class DataAggregateService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("配置编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry:
            # 附加计算计划预览（命中索引表快速路径时不重走依赖树）
            calc = MetricCalculator()
            entry["_compute_plan"] = calc.build_compute_plan(entry_id)
            entry["_preview_sql"] = calc.preview_sql(entry_id)
        return entry

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing

        # 校验数据来源中的指标域和派生指标是否存在
        source = values.get("数据来源", "")
        errors = self._validate_source(source)
        if errors:
            return None, errors

        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """修改聚合配置；实质字段变更只使本配置的计划失效，不影响其他输出表。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合配置 {entry_id} 不存在或已归档"
        if "数据来源" in values and values.get("数据来源") is not None:
            errors = self._validate_source(str(values.get("数据来源", "")))
            if errors:
                return None, "；".join(errors)
        before = {field: entry.get(field) for field in LIST_FIELDS}
        for field in LIST_FIELDS:
            if field in values and values.get(field) is not None:
                entry[field] = values.get(field)
        changed_fields = [
            field for field in LIST_FIELDS
            if str(before.get(field, "")) != str(entry.get(field, ""))
        ]
        if not changed_fields:
            return entry, "聚合配置无实质变更"
        plan_registry.notify_aggregate_change(entry_id, f"配置字段变更：{'、'.join(changed_fields)}")
        return entry, f"聚合配置 {entry_id} 已更新，原计划标记失效，下次读取将按新版本重建"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据聚合可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"聚合配置已{action}"

    def _validate_source(self, source: str) -> list[str]:
        errors = []
        calc = MetricCalculator()
        for part in source.split(","):
            part = part.strip()
            if part.startswith("METR-"):
                domain = next((r for r in store.rows("metric_domain") if r.get("域编号") == part), None)
                if not domain:
                    errors.append(f"指标域 {part} 不存在")
                elif domain.get("status") != "已启用":
                    errors.append(f"指标域 {part} 未启用")
            elif part.startswith("DERI-"):
                metric = calc.get_metric_by_code(part)
                if not metric:
                    errors.append(f"派生指标 {part} 不存在")
                elif metric.get("status") != "已生效":
                    errors.append(f"派生指标 {part} 未生效")
        return errors

    def get_compute_plan(self, entry_id: int) -> dict[str, Any]:
        calc = MetricCalculator()
        return calc.build_compute_plan(entry_id)

    def get_preview_sql(self, entry_id: int) -> dict[str, Any]:
        calc = MetricCalculator()
        plan = calc.build_compute_plan(entry_id)
        sql = calc.preview_sql(entry_id)
        verification = plan_registry.verify_sql_versions(entry_id, sql)
        return {
            "aggregate_id": entry_id,
            "plan_version": plan.get("plan_version"),
            "version_manifest": plan.get("version_manifest", []),
            "sql": sql,
            "version_check": verification,
        }

    def get_recompute_scope(self, entry_id: int) -> dict[str, Any]:
        return plan_registry.get_recompute_scope(entry_id)

    def get_plan_index(self) -> dict[str, Any]:
        return plan_registry.index_listing()

    def get_affected_plans(self, source_code: str) -> dict[str, Any]:
        return {"source_code": source_code, "plans": plan_registry.find_affected_plans(source_code)}

    def diff_plans(self, entry_id: int, from_version: int, to_version: int) -> dict[str, Any]:
        return plan_registry.diff_plans(entry_id, from_version, to_version)
