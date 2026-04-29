"""中证红利类指数爬虫 - 支持 CSI官网源 和 AKShare源

用法：
    scraper = CSIDividend50Scraper(source="akshare")   # AKShare源（推荐）
    scraper = CSIDividend50Scraper(source="csi")        # 中证官网源

    # 获取行情数据
    records = scraper.run(start_date="20200101", end_date="20260429")

    # 获取估值数据（优先使用中证官方源）
    valuations = scraper.fetch_valuation()
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from Scraper.base_scraper import BaseScraper
from DAO.index_dao import IndexRecord, IndexValuationRecord

logger = logging.getLogger(__name__)

# 常用红利类指数代码表
KNOWN_INDICES = {
    "000922": "中证红利",
    "H30269": "中证红利低波",
    "930955": "中证红利低波100",
    "000015": "上证红利",
    "399324": "深证红利",
    "000821": "沪深300红利",
    "H30533": "中国互联网50",
}

DEFAULT_INDEX_CODE = "000922"
DEFAULT_INDEX_NAME = "中证红利"


class CSIDividend50Scraper(BaseScraper):
    """中证红利类指数双源爬虫（默认中证红利，可切换到任意中证指数）

    支持两个数据源:
        - "akshare": 通过AKShare库获取（推荐，数据全、稳定）
        - "csi":     直接请求中证指数官网API
    """

    def __init__(self, source: str = "akshare",
                 index_code: str = DEFAULT_INDEX_CODE,
                 index_name: Optional[str] = None):
        """
        Args:
            source:     数据源，"akshare" 或 "csi"
            index_code: 指数代码，默认 "000922"(中证红利)
                        其他常用: "H30269"(红利低波), "930955"(红利低波100)
            index_name: 指数名称（不传则从KNOWN_INDICES查）
        """
        super().__init__()
        if source not in ("akshare", "csi"):
            raise ValueError(f"不支持的数据源: {source}，请使用 'akshare' 或 'csi'")
        self.source = source
        self.index_code = index_code
        self.index_name = index_name or KNOWN_INDICES.get(index_code, index_code)

    # ══════════════════════════════════════════════════════════
    #  BaseScraper 接口（CSI官网源使用）
    # ══════════════════════════════════════════════════════════

    def build_request(self, **kwargs):
        """构建CSI官网API请求"""
        start_date = kwargs.get("start_date", "20130101")
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))
        url = (
            f"https://www.csindex.com.cn/csindex-home/perf/index-perf"
            f"?indexCode={self.index_code}"
            f"&startDate={start_date}"
            f"&endDate={end_date}"
        )
        return {"url": url, "headers": {"Accept": "application/json"}}

    def parse(self, response) -> List[IndexRecord]:
        """解析CSI官网返回的JSON"""
        try:
            data = response.json()
        except Exception as e:
            logger.error(f"CSI官网响应解析失败: {e}")
            return []

        records = []
        items = data if isinstance(data, list) else data.get("data", [])

        for item in items:
            try:
                # CSI API 返回字段可能是 tradeDate/tradingDay, close/idxClose 等
                date_str = item.get("tradeDate") or item.get("tradingDay", "")
                if len(date_str) == 8:
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                official_name = item.get("indexNameCn") or item.get("indexNameCnAll") or self.index_name

                records.append(IndexRecord(
                    date=date_str,
                    index_code=self.index_code,
                    index_name=official_name,
                    open=float(item.get("open") or item.get("idxOpen") or 0),
                    high=float(item.get("high") or item.get("idxHigh") or 0),
                    low=float(item.get("low") or item.get("idxLow") or 0),
                    close=float(item.get("close") or item.get("idxClose") or 0),
                    volume=float(item.get("tradingVol") or item.get("volume") or 0),
                    amount=float(item.get("tradingValue") or item.get("amount") or 0),
                    source="csi_official",
                ))
            except (ValueError, TypeError) as e:
                logger.warning(f"跳过无效数据行: {e}")
                continue

        logger.info(f"[CSI官网] 解析到 {len(records)} 条行情数据")
        return records

    # ══════════════════════════════════════════════════════════
    #  AKShare 源
    # ══════════════════════════════════════════════════════════

    def _fetch_akshare(self, start_date: str, end_date: str) -> List[IndexRecord]:
        """通过AKShare获取日K数据"""
        try:
            import akshare as ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

        print(f"[AKShare] 获取 {self.index_name}({self.index_code}) "
              f"{start_date} ~ {end_date}")

        try:
            df = ak.index_zh_a_hist(
                symbol=self.index_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as e:
            print(f"[AKShare] 网络请求失败: {e}")
            print("  提示: 如果在公司网络下，可能需要设置代理或使用CSI官网源 --source csi")
            return []

        if df is None or df.empty:
            print("[AKShare] 未获取到数据")
            return []

        records = []
        for _, row in df.iterrows():
            date_val = row.get("日期") or row.get("date")
            records.append(IndexRecord(
                date=str(date_val)[:10],
                index_code=self.index_code,
                index_name=self.index_name,
                open=float(row.get("开盘") or row.get("open") or 0),
                high=float(row.get("最高") or row.get("high") or 0),
                low=float(row.get("最低") or row.get("low") or 0),
                close=float(row.get("收盘") or row.get("close") or 0),
                volume=float(row.get("成交量") or row.get("volume") or 0),
                amount=float(row.get("成交额") or row.get("amount") or 0),
                source="akshare",
            ))

        print(f"[AKShare] 获取到 {len(records)} 条行情数据")
        return records

    # ══════════════════════════════════════════════════════════
    #  估值数据（PE/PB/股息率）
    # ══════════════════════════════════════════════════════════

    def fetch_valuation(self, indicator: str = "all") -> List[IndexValuationRecord]:
        """获取估值数据（优先使用中证官方长周期数据）

        Args:
            indicator: "pe" / "pb" / "dividend_yield" / "all"
                       当前稳定支持 PE 和股息率；PB 取决于官方是否提供

        Returns:
            IndexValuationRecord 列表
        """
        return self._fetch_valuation_official(indicator)

    def _fetch_valuation_official(self, indicator: str = "all") -> List[IndexValuationRecord]:
        """通过中证官方接口和静态文件获取估值数据

        说明:
            - 长周期 PE: 复用中证官方 index-perf 接口中的滚动市盈率字段
            - 近期股息率: 读取中证官方估值 xls 文件
            - PB: 官方当前未稳定提供，可能为空
        """
        records_map: Dict[str, IndexValuationRecord] = {}

        if indicator not in ("all", "pe", "pb", "dividend_yield"):
            raise ValueError(f"不支持的估值指标: {indicator}")

        perf_req = self.build_request(start_date="20100101", end_date=datetime.now().strftime("%Y%m%d"))
        response = self.fetch(**perf_req)
        if response is not None and indicator in ("all", "pe"):
            try:
                data = response.json()
                items = data if isinstance(data, list) else data.get("data", [])
                for item in items:
                    date_str = item.get("tradeDate") or item.get("tradingDay", "")
                    if len(date_str) == 8:
                        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                    if date_str not in records_map:
                        records_map[date_str] = IndexValuationRecord(
                            date=date_str,
                            index_code=self.index_code,
                            source="csi_official",
                        )

                    pe_value = item.get("peg")
                    if pe_value not in (None, ""):
                        try:
                            records_map[date_str].pe = float(pe_value)
                        except (TypeError, ValueError):
                            pass
            except Exception as e:
                logger.warning(f"[CSI官网] 长周期PE获取失败: {e}")

        if indicator in ("all", "dividend_yield", "pe"):
            try:
                import pandas as pd

                valuation_url = (
                    "https://oss-ch.csindex.com.cn/static/"
                    f"html/csindex/public/uploads/file/autofile/indicator/{self.index_code}indicator.xls"
                )
                df = pd.read_excel(valuation_url)
                if df is not None and not df.empty:
                    df.columns = [
                        "日期",
                        "指数代码",
                        "指数中文全称",
                        "指数中文简称",
                        "指数英文全称",
                        "指数英文简称",
                        "市盈率1",
                        "市盈率2",
                        "股息率1",
                        "股息率2",
                    ]
                    df["日期"] = pd.to_datetime(df["日期"], format="%Y%m%d", errors="coerce")

                    for _, row in df.iterrows():
                        if pd.isna(row["日期"]):
                            continue

                        date_str = row["日期"].strftime("%Y-%m-%d")
                        if date_str not in records_map:
                            records_map[date_str] = IndexValuationRecord(
                                date=date_str,
                                index_code=self.index_code,
                                source="csi_official",
                            )

                        rec = records_map[date_str]
                        if indicator in ("all", "pe") and rec.pe is None:
                            for pe_col in ("市盈率2", "市盈率1"):
                                pe_value = row.get(pe_col)
                                if pd.notna(pe_value):
                                    rec.pe = float(pe_value)
                                    break

                        if indicator in ("all", "dividend_yield"):
                            for dy_col in ("股息率2", "股息率1"):
                                dy_value = row.get(dy_col)
                                if pd.notna(dy_value):
                                    rec.dividend_yield = float(dy_value)
                                    break
            except Exception as e:
                logger.warning(f"[CSI官网] 估值静态文件获取失败: {e}")

        records = sorted(records_map.values(), key=lambda r: r.date)
        logger.info(f"[CSI官网] 估值数据共 {len(records)} 条")
        return records

    def _fetch_valuation_csi(self) -> List[IndexValuationRecord]:
        """兼容旧调用，统一转到官方稳定估值源。"""
        return self._fetch_valuation_official(indicator="all")

    # ══════════════════════════════════════════════════════════
    #  统一入口
    # ══════════════════════════════════════════════════════════

    def run(self, **kwargs) -> Optional[List[IndexRecord]]:
        """获取行情数据（统一入口）

        Args:
            start_date: 起始日期，格式 "20200101"（默认 "20130101"）
            end_date:   截止日期，格式 "20260429"（默认今天）

        Returns:
            IndexRecord 列表
        """
        start_date = kwargs.get("start_date", "20130101")
        end_date = kwargs.get("end_date", datetime.now().strftime("%Y%m%d"))

        if self.source == "akshare":
            return self._fetch_akshare(start_date, end_date)
        else:
            # CSI官网走 BaseScraper 的 build_request → fetch → parse 流程
            req = self.build_request(start_date=start_date, end_date=end_date)
            response = self.fetch(**req)
            if response is None:
                return None
            return self.parse(response)

    def run_all(self, **kwargs) -> Dict[str, Any]:
        """一次性获取行情 + 估值数据

        Returns:
            {"daily": [IndexRecord, ...], "valuation": [IndexValuationRecord, ...]}
        """
        daily = self.run(**kwargs) or []
        valuation = self.fetch_valuation()
        return {"daily": daily, "valuation": valuation}
