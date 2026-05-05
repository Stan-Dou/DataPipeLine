"""国债收益率爬虫 - 通过AKShare获取中美国债收益率日线

数据源：
    AKShare bond_zh_us_rate() — 包含中国/美国多个期限的国债收益率，1990年至今

用法：
    scraper = BondYieldScraper()
    records = scraper.run()              # 获取全量历史 + 增量更新
    records = scraper.run(full=True)     # 强制全量拉取
"""

import logging
from datetime import datetime
from typing import List, Optional

from Scraper.base_scraper import BaseScraper
from DAO.macro_dao import BondYieldRecord, MacroDAO

logger = logging.getLogger(__name__)


# AKShare 返回的列名映射（原始中文名 → 英文字段）
COLUMN_MAP = {
    "中国国债收益率2年":       "yield_2y",
    "中国国债收益率5年":       "yield_5y",
    "中国国债收益率10年":      "yield_10y",
    "中国国债收益率30年":      "yield_30y",
    "中国国债收益率10年-2年":  "spread_10y_2y",
    "美国国债收益率2年":       "us_yield_2y",
    "美国国债收益率5年":       "us_yield_5y",
    "美国国债收益率10年":      "us_yield_10y",
    "美国国债收益率30年":      "us_yield_30y",
    "美国国债收益率10年-2年":  "us_spread_10y_2y",
}


class BondYieldScraper(BaseScraper):
    """国债收益率爬虫（AKShare 数据源）"""

    def __init__(self, db_path: str = "data/options.db"):
        super().__init__()
        self.db_path = db_path

    # ── BaseScraper 接口（AKShare 不走 HTTP fetch，留空占位） ──

    def build_request(self, **kwargs):
        return {"url": "", "method": "GET"}

    def parse(self, response) -> List[BondYieldRecord]:
        return []

    # ── AKShare 数据获取 ──

    def fetch_bond_yields(self) -> List[BondYieldRecord]:
        """从 AKShare 获取全量中美国债收益率历史"""
        try:
            import akshare as ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

        print(f"[BondYield] 正在通过 AKShare 拉取中美国债收益率...")
        df = ak.bond_zh_us_rate()

        if df is None or df.empty:
            print("[BondYield] 未获取到数据")
            return []

        records = []
        for _, row in df.iterrows():
            date_val = row.get("日期")
            if date_val is None:
                continue

            date_str = str(date_val)[:10]

            # 跳过中国数据全部为空的日期（如美国假日）
            has_cn_data = False
            kwargs = {"date": date_str, "source": "akshare"}

            for cn_name, field_name in COLUMN_MAP.items():
                val = row.get(cn_name)
                if val is not None and not (isinstance(val, float) and val != val):  # 非 NaN
                    kwargs[field_name] = round(float(val), 4)
                    if cn_name.startswith("中国"):
                        has_cn_data = True

            # 只保存有中国数据的记录（没有中国数据对我们没意义）
            if has_cn_data:
                records.append(BondYieldRecord(**kwargs))

        print(f"[BondYield] AKShare 返回 {len(df)} 行，解析出 {len(records)} 条有效记录")
        return records

    # ── 主入口 ──

    def run(self, full: bool = False, **kwargs) -> List[BondYieldRecord]:
        """拉取国债收益率并做增量更新

        Args:
            full: 是否强制全量拉取（默认 False，只拉取数据库中缺失的日期）

        Returns:
            本次新增/更新的记录数
        """
        all_records = self.fetch_bond_yields()
        if not all_records:
            return []

        if full:
            to_save = all_records
        else:
            # 增量更新：只保存数据库中还没有的日期
            dao = MacroDAO(self.db_path)
            latest = dao.get_latest_date()
            dao.close()
            if latest:
                to_save = [r for r in all_records if r.date > latest]
                print(f"[BondYield] 增量更新: 已有数据至 {latest}，本次新增 {len(to_save)} 条")
            else:
                to_save = all_records

        if not to_save:
            print("[BondYield] 没有需要新增的数据")
            return []

        dao = MacroDAO(self.db_path)
        count = dao.upsert_batch(to_save)
        dao.close()
        print(f"[BondYield] 已写入 {count} 条数据到数据库")
        return to_save


# ── 独立运行 ──

if __name__ == "__main__":
    import sys
    full = "--full" in sys.argv
    scraper = BondYieldScraper()
    result = scraper.run(full=full)
    print(f"完成: {len(result)} 条")
