"""中证红利50指数爬虫 - 支持 CSI官网源 和 AKShare源

用法：
    scraper = CSIDividend50Scraper(source="akshare")   # AKShare源（推荐）
    scraper = CSIDividend50Scraper(source="csi")        # 中证官网源

    # 获取行情数据
    records = scraper.run(start_date="20200101", end_date="20260429")

    # 获取估值数据（仅AKShare源支持）
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
    "000922": "中证红利50",
    "H30269": "中证红利低波",
    "930955": "中证红利低波100",
    "000015": "上证红利",
    "399324": "深证红利",
    "000821": "高股息",
    "H30533": "中证红利100",
}

DEFAULT_INDEX_CODE = "000922"
DEFAULT_INDEX_NAME = "中证红利50"


class CSIDividend50Scraper(BaseScraper):
    """中证系列指数双源爬虫（默认中证红利50，可切换到任意中证指数）

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
            index_code: 指数代码，默认 "000922"(中证红50)
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

                records.append(IndexRecord(
                    date=date_str,
                    index_code=self.index_code,
                    index_name=self.index_name,
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
        """获取估值数据（PE/PB/股息率）

        Args:
            indicator: "pe" / "pb" / "dividend_yield" / "all"
                       当 source="akshare" 时支持全部
                       当 source="csi" 时仅支持有限字段

        Returns:
            IndexValuationRecord 列表
        """
        if self.source == "akshare":
            return self._fetch_valuation_akshare(indicator)
        else:
            return self._fetch_valuation_csi()

    def _fetch_valuation_akshare(self, indicator: str = "all") -> List[IndexValuationRecord]:
        """通过AKShare获取估值数据

        使用 index_value_hist_funddb 接口获取 韭圈儿 的估值数据
        """
        try:
            import akshare as ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

        records_map: Dict[str, IndexValuationRecord] = {}

        indicator_map = {
            "pe": "市盈率",
            "pb": "市净率",
            "dividend_yield": "股息率",
        }

        targets = list(indicator_map.items()) if indicator == "all" else \
            [(indicator, indicator_map[indicator])]

        for field_name, cn_name in targets:
            try:
                logger.info(f"[AKShare] 获取 {self.index_name} {cn_name} ...")
                # 韭圈儿上的名称可能不同，尝试几种
                df = None
                for name_try in ["中证红利", "中证红利50"]:
                    try:
                        df = ak.index_value_hist_funddb(
                            symbol=name_try,
                            indicator=cn_name,
                        )
                        if df is not None and not df.empty:
                            break
                    except Exception:
                        continue

                if df is None or df.empty:
                    logger.warning(f"[AKShare] {cn_name} 数据为空，跳过")
                    continue

                for _, row in df.iterrows():
                    date_val = row.get("日期") or row.get("date")
                    date_str = str(date_val)[:10]
                    value = float(row.get(cn_name) or row.get("value") or 0)

                    if date_str not in records_map:
                        records_map[date_str] = IndexValuationRecord(
                            date=date_str,
                            index_code=self.index_code,
                            source="akshare_funddb",
                        )

                    rec = records_map[date_str]
                    if field_name == "pe":
                        rec.pe = value
                    elif field_name == "pb":
                        rec.pb = value
                    elif field_name == "dividend_yield":
                        rec.dividend_yield = value

                logger.info(f"[AKShare] {cn_name}: {len(df)} 条")

            except Exception as e:
                logger.warning(f"[AKShare] 获取{cn_name}失败: {e}")
                continue

        records = sorted(records_map.values(), key=lambda r: r.date)
        logger.info(f"[AKShare] 估值数据共 {len(records)} 条")
        return records

    def _fetch_valuation_csi(self) -> List[IndexValuationRecord]:
        """从CSI官网获取估值数据

        中证官网有指数估值（PE/PB等）的API，但接口可能有变动
        """
        url = (
            f"https://www.csindex.com.cn/csindex-home/perf/index-perf-value"
            f"?indexCode={self.index_code}"
        )
        response = self.fetch(url, headers={"Accept": "application/json"})
        if response is None:
            return []

        try:
            data = response.json()
        except Exception as e:
            logger.error(f"CSI估值响应解析失败: {e}")
            return []

        records = []
        items = data if isinstance(data, list) else data.get("data", [])

        for item in items:
            try:
                date_str = item.get("tradeDate", "")
                if len(date_str) == 8:
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

                records.append(IndexValuationRecord(
                    date=date_str,
                    index_code=self.index_code,
                    pe=float(item.get("pe") or 0) or None,
                    pb=float(item.get("pb") or 0) or None,
                    dividend_yield=float(item.get("dividendYield") or 0) or None,
                    source="csi_official",
                ))
            except (ValueError, TypeError):
                continue

        logger.info(f"[CSI官网] 估值数据 {len(records)} 条")
        return records

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
