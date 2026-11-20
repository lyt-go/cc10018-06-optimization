"""派生指标业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.engine.plan_registry import plan_registry

MODULE = "derived_metric"
LIST_FIELDS = ["指标编号", "父指标编号", "计算公式", "统计周期", "业务限定", "统计粒度", "精度位数", "口径版本"]
REQUIRED_FIELDS = ["指标编号", "父指标编号", "计算公式"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
NEGATIVE_ACTIONS = []


class DerivedMetricService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("指标编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 新指标纳入来源版本表（初始 v1）
        plan_registry.sources.register(str(entry.get("指标编号", "")).strip(), "metric")
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """修改派生指标；公式/口径/父指标等实质内容变更驱动来源版本递增。

        版本递增后由聚合计划索引表标记并返回受影响的计划，只让依赖该指标（含传递
        依赖）的计算节点失效，不相关计划不受影响。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"派生指标 {entry_id} 不存在或已归档"
        before = {field: entry.get(field) for field in LIST_FIELDS}
        for field in LIST_FIELDS:
            if field in values and values.get(field) is not None:
                entry[field] = values.get(field)
        changed_fields = [
            field for field in LIST_FIELDS
            if str(before.get(field, "")) != str(entry.get(field, ""))
        ]
        if not changed_fields:
            return entry, "派生指标无实质变更"
        code = str(entry.get("指标编号", "")).strip()
        impact = plan_registry.notify_source_change(code, f"字段变更：{'、'.join(changed_fields)}")
        if impact.get("changed"):
            change = impact["change"]
            entry["口径版本"] = (
                f"{entry.get('口径版本', '')}→v{change['new_version']}"
                if entry.get("口径版本") else f"v{change['new_version']}"
            )
            return entry, (
                f"派生指标 {code} 来源版本 v{change['old_version']}→v{change['new_version']}，"
                f"受影响聚合计划 {len(impact['affected_plans'])} 份"
            )
        return entry, "派生指标已更新（未产生新版本）"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"派生指标 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于派生指标可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        # 状态（生效/废弃）参与指纹，状态流转同样产生来源版本并标记受影响计划
        plan_registry.notify_source_change(
            str(entry.get("指标编号", "")).strip(), f"状态流转为{target}")
        return entry, f"派生指标已{action}"
