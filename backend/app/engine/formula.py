"""派生指标公式解析：从 metric_calculator 抽出，避免与聚合计划索引表循环导入。"""
from __future__ import annotations

import re


class MetricFormula:
    """单个公式的解析表示。"""

    def __init__(self, raw: str) -> None:
        self.raw = (raw or "").strip()
        self.agg_func: str | None = None
        self.agg_field: str | None = None
        self.operators: list[str] = []
        self.operands: list[str] = []
        self._parse()

    def _parse(self) -> None:
        raw = self.raw
        # 提取聚合函数
        m = re.match(r"(\w+)\(([^)]+)\)", raw)
        if m:
            self.agg_func = m.group(1).upper()
            self.agg_field = m.group(2).strip()
        # 提取四则运算
        parts = re.split(r"([+\-*/])", raw)
        for p in parts:
            p = p.strip()
            if p in {"+", "-", "*", "/"}:
                self.operators.append(p)
            elif p:
                self.operands.append(p)

    def to_sql_expr(self, table_alias: str = "t") -> str:
        if self.agg_func and self.agg_field:
            if self.agg_func == "COUNT_DISTINCT":
                return f"COUNT(DISTINCT {table_alias}.{self.agg_field})"
            return f"{self.agg_func}({table_alias}.{self.agg_field})"
        return self.raw
