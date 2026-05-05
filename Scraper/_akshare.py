"""AKShare 底层调用封装（私有模块，不对外使用）

职责：
  - 封装 AKShare 函数调用，统一错误处理
  - 返回 pandas DataFrame

注意：
  - _ 前缀表示此模块是内部实现细节，对外使用走 loader.py
  - 不在这里做任何数据清洗或字段映射
"""

import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def get_index_daily(index_code: str,
                    start_date: str = "20100101",
                    end_date: Optional[str] = None) -> pd.DataFrame:
    """获取指数日线行情

    Args:
        index_code: 指数代码，如 "000922"(中证红利)
        start_date: 起始日期 "20200101"
        end_date:   截止日期，默认今天

    Returns:
        DataFrame with columns: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额
    """
    try:
        import akshare as ak
    except ImportError:
        raise ImportError("请安装 akshare: pip install akshare")

    from datetime import datetime
    end = end_date or datetime.now().strftime("%Y%m%d")

    try:
        df = ak.index_zh_a_hist(
            symbol=index_code,
            period="daily",
            start_date=start_date,
            end_date=end,
        )
    except Exception as e:
        logger.error(f"[AKShare] 获取 {index_code} 日线失败: {e}")
        return pd.DataFrame()

    return df


def get_index_valuation(index_code: str) -> pd.DataFrame:
    """获取指数估值数据（PE/股息率）

    数据来源：中证指数官网 xls 文件
    包含字段：市盈率1, 市盈率2, 股息率1, 股息率2

    Args:
        index_code: 指数代码

    Returns:
        DataFrame
    """
    url = (
        "https://oss-ch.csindex.com.cn/static/"
        f"html/csindex/public/uploads/file/autofile/indicator/{index_code}indicator.xls"
    )
    try:
        df = pd.read_excel(url)
        if df.empty:
            return df
        df.columns = [
            "日期", "指数代码", "指数中文全称", "指数中文简称",
            "指数英文全称", "指数英文简称",
            "市盈率1", "市盈率2", "股息率1", "股息率2",
        ]
        df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d", errors="coerce")
        return df
    except Exception as e:
        logger.warning(f"[AKShare/CSI] 获取 {index_code} 估值失败: {e}")
        return pd.DataFrame()


def get_bond_yield_history() -> pd.DataFrame:
    """获取中美国债收益率历史

    Returns:
        DataFrame
    """
    try:
        import akshare as ak
    except ImportError:
        raise ImportError("请安装 akshare: pip install akshare")

    try:
        df = ak.bond_zh_us_rate()
        return df
    except Exception as e:
        logger.error(f"[AKShare] 获取国债收益率失败: {e}")
        return pd.DataFrame()


def get_bond_yield_latest() -> Optional[float]:
    """获取最新中国10年期国债收益率

    Returns:
        收益率百分比，如 1.75 表示 1.75%
    """
    df = get_bond_yield_history()
    if df.empty:
        return None
    last = df["中国国债收益率10年"].dropna()
    if last.empty:
        return None
    return round(float(last.iloc[-1]), 4)
