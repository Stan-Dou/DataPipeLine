"""数据加载管线：从数据源到数据库

每个函数完成一条完整的数据加载链路：
  获取(源) → 清洗 → 转Record → 写入(DAO)

用法：
    from Scraper.loader import load_index_daily
    from DAO.index_dao import IndexDAO

    dao = IndexDAO("data/options.db")
    count = load_index_daily("000922", dao)
    print(f"写入 {count} 条")
"""

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from Scraper import _akshare
from DAO.index_dao import IndexDAO, IndexRecord, IndexValuationRecord
from DAO.macro_dao import MacroDAO, BondYieldRecord

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  指数行情加载
# ═══════════════════════════════════════════════════════════

# 常用指数代码与名称
KNOWN_INDICES = {
    "000922": "中证红利",
    "H30269": "中证红利低波",
    "930955": "中证红利低波100",
    "000015": "上证红利",
    "399324": "深证红利",
    "399986": "中证银行",
    "000966": "中证公用",
    "399998": "中证煤炭",
    "H30171": "中证交通运输",
    "000932": "中证消费",
}

_AKSHARE_COL_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
}


def load_index_daily(index_code: str, dao: IndexDAO,
                     start_date: str = "20100101",
                     end_date: Optional[str] = None,
                     index_name: Optional[str] = None) -> int:
    """拉取指数日线行情 → 写入数据库

    Args:
        index_code: 指数代码
        dao: IndexDAO 实例
        start_date: 起始日期
        end_date: 截止日期（默认今天）
        index_name: 指数名称（不传则从 KNOWN_INDICES 自动查找）

    Returns:
        写入的记录数
    """
    name = index_name or KNOWN_INDICES.get(index_code, index_code)
    end = end_date or datetime.now().strftime("%Y%m%d")

    # 1. 获取
    df = _akshare.get_index_daily(index_code, start_date, end)
    if df.empty:
        print(f"  [loader] {name}({index_code}): 无数据")
        return 0

    # 2. 清洗 + 转Record
    records = []
    for _, row in df.iterrows():
        date_val = row.get("日期") or row.get("date")
        if date_val is None:
            continue

        records.append(IndexRecord(
            date=str(date_val)[:10],
            index_code=index_code,
            index_name=name,
            open=float(row.get("开盘") or row.get("open") or 0),
            high=float(row.get("最高") or row.get("high") or 0),
            low=float(row.get("最低") or row.get("low") or 0),
            close=float(row.get("收盘") or row.get("close") or 0),
            volume=float(row.get("成交量") or row.get("volume") or 0),
            amount=float(row.get("成交额") or row.get("amount") or 0),
            source="akshare",
        ))

    # 3. 写入
    dao.upsert_index(index_code, name)
    count = dao.upsert_daily_batch(records)
    print(f"  [loader] {name}({index_code}): {count} 条行情")
    return count


# ═══════════════════════════════════════════════════════════
#  指数估值加载
# ═══════════════════════════════════════════════════════════

def load_index_valuation(index_code: str, dao: IndexDAO) -> int:
    """拉取指数估值数据（PE/股息率） → 写入数据库

    Args:
        index_code: 指数代码
        dao: IndexDAO 实例

    Returns:
        写入的记录数
    """
    name = KNOWN_INDICES.get(index_code, index_code)

    # 1. 获取
    df = _akshare.get_index_valuation(index_code)
    if df.empty:
        print(f"  [loader] {name}({index_code}): 估值数据为空")
        return 0

    # 2. 清洗 + 转Record
    records = []
    for _, row in df.iterrows():
        if pd.isna(row["日期"]):
            continue

        date_str = row["日期"].strftime("%Y-%m-%d")
        pe_val = None
        if pd.notna(row.get("市盈率2")):
            pe_val = float(row["市盈率2"])
        elif pd.notna(row.get("市盈率1")):
            pe_val = float(row["市盈率1"])

        dy_val = None
        if pd.notna(row.get("股息率2")):
            dy_val = float(row["股息率2"])
        elif pd.notna(row.get("股息率1")):
            dy_val = float(row["股息率1"])

        records.append(IndexValuationRecord(
            date=date_str,
            index_code=index_code,
            pe=pe_val,
            dividend_yield=dy_val,
            source="csi_official",
        ))

    # 3. 写入
    count = dao.upsert_valuation_batch(records)
    print(f"  [loader] {name}({index_code}): {count} 条估值")
    return count


# ═══════════════════════════════════════════════════════════
#  批量加载行业指数
# ═══════════════════════════════════════════════════════════

# 红利低波50成分股对应的行业指数
INDUSTRY_INDICES = {
    "399986": "中证银行",
    "000966": "中证公用",
    "399998": "中证煤炭",
    "H30171": "中证交通运输",
    "000932": "中证消费",
}


def load_industry_indices(dao: IndexDAO,
                          start_date: str = "20100101") -> dict:
    """批量拉取所有行业指数的行情 + 估值

    Args:
        dao: IndexDAO 实例
        start_date: 起始日期

    Returns:
        {"daily": 总行情条数, "valuation": 总估值条数}
    """
    total_daily = 0
    total_val = 0
    results = {}

    for code, name in INDUSTRY_INDICES.items():
        print(f"\n[{name} ({code})]")
        d = load_index_daily(code, dao, start_date, index_name=name)
        v = load_index_valuation(code, dao)
        total_daily += d
        total_val += v
        results[code] = {"name": name, "daily": d, "valuation": v}

    print(f"\n===== 行业指数汇总 =====")
    print(f"  行情总计: {total_daily} 条")
    print(f"  估值总计: {total_val} 条")
    for code, r in results.items():
        print(f"  {r['name']}({code}): 行情{r['daily']}条  估值{r['valuation']}条")
    return results


# ═══════════════════════════════════════════════════════════
#  国债收益率加载
# ═══════════════════════════════════════════════════════════

# AKShare bond_zh_us_rate 的中文列名 → 英文字段映射
_BOND_COL_MAP = {
    "中国国债收益率2年": "yield_2y",
    "中国国债收益率5年": "yield_5y",
    "中国国债收益率10年": "yield_10y",
    "中国国债收益率30年": "yield_30y",
    "中国国债收益率10年-2年": "spread_10y_2y",
    "美国国债收益率2年": "us_yield_2y",
    "美国国债收益率5年": "us_yield_5y",
    "美国国债收益率10年": "us_yield_10y",
    "美国国债收益率30年": "us_yield_30y",
    "美国国债收益率10年-2年": "us_spread_10y_2y",
}


def load_bond_yield(dao: MacroDAO, full: bool = False) -> int:
    """拉取国债收益率 → 写入数据库

    Args:
        dao: MacroDAO 实例
        full: 是否强制全量拉取（默认 False，增量更新）

    Returns:
        写入的记录数
    """
    # 1. 获取
    df = _akshare.get_bond_yield_history()
    if df.empty:
        print("  [loader] 国债收益率: 无数据")
        return 0

    # 2. 清洗 + 转Record
    all_records = []
    for _, row in df.iterrows():
        date_val = row.get("日期")
        if date_val is None:
            continue

        date_str = str(date_val)[:10]
        kwargs = {"date": date_str, "source": "akshare"}
        has_cn = False

        for cn_name, field_name in _BOND_COL_MAP.items():
            val = row.get(cn_name)
            if val is not None and not (isinstance(val, float) and val != val):
                kwargs[field_name] = round(float(val), 4)
                if cn_name.startswith("中国"):
                    has_cn = True

        if has_cn:
            all_records.append(BondYieldRecord(**kwargs))

    # 3. 增量过滤
    if not full:
        latest = dao.get_latest_date()
        if latest:
            to_save = [r for r in all_records if r.date > latest]
        else:
            to_save = all_records
    else:
        to_save = all_records

    if not to_save:
        print("  [loader] 国债收益率: 无新增数据")
        return 0

    # 4. 写入
    count = dao.upsert_batch(to_save)
    print(f"  [loader] 国债收益率: {count} 条 (增量)" if not full else f"  [loader] 国债收益率: {count} 条 (全量)")
    return count


# ═══════════════════════════════════════════════════════════
#  便捷一次性加载
# ═══════════════════════════════════════════════════════════

def load_all_indices(dao: IndexDAO, start_date: str = "20100101") -> dict:
    """拉取所有已知指数的行情 + 估值"""
    total = {"daily": 0, "valuation": 0}
    for code, name in KNOWN_INDICES.items():
        print(f"\n[{name} ({code})]")
        d = load_index_daily(code, dao, start_date, index_name=name)
        v = load_index_valuation(code, dao)
        total["daily"] += d
        total["valuation"] += v
    return total
