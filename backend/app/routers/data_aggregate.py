"""数据聚合接口：维护聚合配置，覆盖提交校验、标记生效、停用配置等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.data_aggregate import DataAggregateService

router = APIRouter(prefix="/api/data_aggregate", tags=["数据聚合"])

service = DataAggregateService()

LIST_FIELDS = ["配置编号", "数据来源", "聚合维度", "聚合函数", "过滤条件", "输出表名", "增量键", "更新策略"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、配置编号关键字与状态过滤数据聚合列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="data_aggregate.csv"'})


@router.get("/plan-index", response_model=dict)
def get_plan_index() -> dict:
    """聚合计划索引表：来源版本、节点复用索引、计划索引与命中统计。"""
    return service.get_plan_index()


@router.get("/affected-plans", response_model=dict)
def get_affected_plans(source_code: str = Query(..., description="指标域或派生指标编号")) -> dict:
    """通过计划索引表定位引用某来源（含传递依赖）的全部聚合计划。"""
    return service.get_affected_plans(source_code)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条聚合配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合配置 {entry_id} 不存在或已归档")
    return entry


@router.put("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: dict[str, object]) -> ActionResult:
    """修改聚合配置；实质字段变更只失效本配置的计划节点。"""
    entry, message = service.update_entry(entry_id, payload)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条聚合配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条聚合配置执行提交校验、标记生效、停用配置；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/{entry_id}/compute-plan")
def get_compute_plan(entry_id: int, force: bool = Query(default=False, description="强制重建，跳过索引快速路径")) -> dict:
    """获取聚合配置的计算计划（指标域→派生指标→聚合的完整链路）。"""
    from app.engine.plan_registry import plan_registry
    plan = plan_registry.get_plan(entry_id, force=force)
    if not plan.get("ok"):
        raise HTTPException(status_code=404, detail=plan)
    return plan


@router.get("/{entry_id}/preview-sql")
def get_preview_sql(entry_id: int) -> dict:
    """获取聚合配置的预览SQL（固化本次计划列出的来源版本并给出版本校验结果）。"""
    return service.get_preview_sql(entry_id)


@router.get("/{entry_id}/recompute-scope")
def get_recompute_scope(entry_id: int) -> dict:
    """获取最近一次计划生成的实际重算范围（重建/复用节点明细）。"""
    scope = service.get_recompute_scope(entry_id)
    if not scope.get("ok"):
        raise HTTPException(status_code=404, detail=scope)
    return scope


@router.get("/{entry_id}/plan-diff")
def get_plan_diff(
    entry_id: int,
    from_version: int = Query(..., description="旧计划版本"),
    to_version: int = Query(..., description="新计划版本"),
) -> dict:
    """比较同一输出表改动前后两个计划版本的步骤、版本清单与重算范围。"""
    diff = service.diff_plans(entry_id, from_version, to_version)
    if not diff.get("ok"):
        raise HTTPException(status_code=404, detail=diff)
    return diff
