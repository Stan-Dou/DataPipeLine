"""红利利差/ERP 分析计算模块

组合 IndexDAO（指数估值） + MacroDAO（国债收益率）的数据，
计算两个核心指标：
    1. 红利利差 = 指数股息率 - 10年国债收益率
    2. ERP = 1/PE - 10年国债收益率

用法：
    from analysis.spread_analysis import SpreadAnalyzer
    sa = SpreadAnalyzer(db_path="data/options.db")
    spread = sa.calc_dividend_spread("000922")
    erp = sa.calc_erp("000922")
"""

from typing import Optional, List, Dict, Any
from DAO.index_dao import IndexDAO
from DAO.macro_dao import MacroDAO


class SpreadAnalyzer:
    """红利利差与股权风险溢价分析器"""

    def __init__(self, db_path: str = "data/options.db"):
        self.db_path = db_path
        self.macro_dao = MacroDAO(db_path)
        # IndexDAO 通过 lazy 方式创建（只在需要时打开）

    def calc_dividend_spread(self, index_code: str,
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """计算红利利差 = 指数股息率 - 10年国债收益率

        Args:
            index_code: 指数代码，如 "000922"(中证红利)

        Returns:
            [{"date", "dividend_yield", "bond_yield_10y", "spread"}, ...]
        """
        sql = """
            SELECT v.date, v.dividend_yield, b.yield_10y,
                   (v.dividend_yield - b.yield_10y) AS spread
            FROM index_valuation v
            JOIN bond_yield b ON v.date = b.date
            WHERE v.index_code = ?
              AND v.dividend_yield IS NOT NULL
              AND b.yield_10y IS NOT NULL
        """
        params: list = [index_code]
        if start_date:
            sql += " AND v.date>=?"
            params.append(start_date)
        if end_date:
            sql += " AND v.date<=?"
            params.append(end_date)
        sql += " ORDER BY v.date"
        return [dict(row) for row in self.macro_dao.conn.execute(sql, params).fetchall()]

    def calc_erp(self, index_code: str,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """计算股权风险溢价 ERP = 1/PE - 10年国债收益率

        Args:
            index_code: 指数代码，如 "000922"(中证红利)

        Returns:
            [{"date", "pe", "bond_yield_10y", "erp"}, ...]
        """
        sql = """
            SELECT v.date, v.pe, b.yield_10y,
                   (1.0/v.pe - b.yield_10y/100) AS erp
            FROM index_valuation v
            JOIN bond_yield b ON v.date = b.date
            WHERE v.index_code = ?
              AND v.pe IS NOT NULL AND v.pe > 0
              AND b.yield_10y IS NOT NULL
        """
        params: list = [index_code]
        if start_date:
            sql += " AND v.date>=?"
            params.append(start_date)
        if end_date:
            sql += " AND v.date<=?"
            params.append(end_date)
        sql += " ORDER BY v.date"
        return [dict(row) for row in self.macro_dao.conn.execute(sql, params).fetchall()]

    def summary(self, index_code: str = "000922") -> Dict[str, Any]:
        """生成当前利差和ERP的摘要报告

        Returns:
            {"spread": {...}, "erp": {...}}
        """
        def percentile(values, current):
            if not values:
                return 50.0
            return round(sum(1 for v in values if v <= current) / len(values) * 100, 1)

        result = {}

        # 红利利差
        spread_data = self.calc_dividend_spread(index_code)
        if spread_data:
            spreads = [s["spread"] for s in spread_data]
            dy = [s["dividend_yield"] for s in spread_data]
            latest = spread_data[-1]
            result["spread"] = {
                "date": latest["date"],
                "dividend_yield": latest["dividend_yield"],
                "bond_yield_10y": latest["yield_10y"],
                "spread": latest["spread"],
                "spread_percentile": percentile(spreads, latest["spread"]),
                "spread_min": min(spreads),
                "spread_max": max(spreads),
                "spread_avg": round(sum(spreads) / len(spreads), 4),
                "dy_percentile": percentile(dy, latest["dividend_yield"]),
                "record_count": len(spread_data),
                "note": "",
            }
            # 标记数据限制
            if len(spread_data) < 100:
                result["spread"]["note"] = "[注意] 股息率历史数据不足100条，利差百分位参考价值有限"

        # ERP
        erp_data = self.calc_erp(index_code)
        if erp_data:
            erp_vals = [e["erp"] for e in erp_data]
            latest_erp = erp_data[-1]
            result["erp"] = {
                "date": latest_erp["date"],
                "pe": latest_erp["pe"],
                "bond_yield_10y": latest_erp["yield_10y"],
                "erp": latest_erp["erp"],
                "erp_percentile": percentile(erp_vals, latest_erp["erp"]),
                "erp_min": min(erp_vals),
                "erp_max": max(erp_vals),
                "erp_avg": round(sum(erp_vals) / len(erp_vals), 4),
                "record_count": len(erp_data),
            }

        return result

    def close(self):
        self.macro_dao.close()
