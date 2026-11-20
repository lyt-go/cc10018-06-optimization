"""指标域管理业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.engine.plan_registry import plan_registry

MODULE = "metric_domain"
LIST_FIELDS = ["域编号", "域名称", "业务域描述", "数据源表", "更新频率", "负责人", "域状态", "创建时间"]
REQUIRED_FIELDS = ["域编号", "域名称", "业务域描述"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "开启域": "已启用", "停用域": "已停用"}
NEGATIVE_ACTIONS = ["停用域"]


class MetricDomainService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("域编号", "")).casefold()]
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
        plan_registry.sources.register(str(entry.get("域编号", "")).strip(), "domain")
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """修改指标域；数据源表/域状态等实质内容变更驱动来源版本递增。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"指标域 {entry_id} 不存在或已归档"
        before = {field: entry.get(field) for field in LIST_FIELDS}
        for field in LIST_FIELDS:
            if field in values and values.get(field) is not None:
                entry[field] = values.get(field)
        changed_fields = [
            field for field in LIST_FIELDS
            if str(before.get(field, "")) != str(entry.get(field, ""))
        ]
        if not changed_fields:
            return entry, "指标域无实质变更"
        code = str(entry.get("域编号", "")).strip()
        impact = plan_registry.notify_source_change(code, f"字段变更：{'、'.join(changed_fields)}")
        if impact.get("changed"):
            change = impact["change"]
            return entry, (
                f"指标域 {code} 来源版本 v{change['old_version']}→v{change['new_version']}，"
                f"受影响聚合计划 {len(impact['affected_plans'])} 份"
            )
        return entry, "指标域已更新（未产生新版本）"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"指标域 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于指标域管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        plan_registry.notify_source_change(
            str(entry.get("域编号", "")).strip(), f"状态流转为{target}")
        return entry, f"指标域已{action}"
