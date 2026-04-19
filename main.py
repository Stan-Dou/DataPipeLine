"""每日运行：爬取港交所期权数据 → 存入数据库

用法：
    python main.py                          # 抓取最近一个交易日
    python main.py 2026-04-16               # 抓取指定日期
    python main.py 2026-03-01 2026-04-17    # 抓取日期范围
"""

import sys
import time
from datetime import datetime, timedelta, date
from Scraper.hkex_options_scraper import HKEXOptionsScraper
from DAO.options_dao import OptionsDAO, OptionRecord

# 配置
DB_PATH = "data/options.db"
WATCHLIST = ["泡泡", "小米"]


def get_trading_dates(start_str: str, end_str: str) -> list:
    """生成日期范围内的所有交易日（跳过周末）"""
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()
    dates = []
    d = start
    while d <= end:
        if d.weekday() < 5:  # 周一到周五
            dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def get_last_trading_date() -> str:
    """获取最近一个交易日（跳过周末）"""
    d = datetime.now() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def fetch_one_day(date_str: str, dao: OptionsDAO) -> int:
    """爬取并存储一天的数据，返回记录数"""
    year, month, day = map(int, date_str.split("-"))
    scraper = HKEXOptionsScraper(year, month, day, watchlist=WATCHLIST)
    raw_records = scraper.run()

    if not raw_records:
        return 0

    records = [
        OptionRecord(
            date=date_str,
            stock_code=r["stock_code"],
            closing_price=r["closing_price"],
            expiry=r["expiry"],
            strike=r["strike"],
            type=r["type"],
            open=r["open"],
            high=r["high"],
            low=r["low"],
            settlement=r["settlement"],
            change=r["change"],
            iv=r["iv"],
            volume=r["volume"],
            oi=r["oi"],
            oi_change=r["oi_change"],
        )
        for r in raw_records
    ]

    return dao.upsert_batch(records)


def main():
    dao = OptionsDAO(DB_PATH)

    # 判断运行模式
    if len(sys.argv) == 3:
        # 日期范围模式: python main.py 2026-03-01 2026-04-17
        dates = get_trading_dates(sys.argv[1], sys.argv[2])
        print(f"=== 批量爬取: {sys.argv[1]} → {sys.argv[2]} ({len(dates)}个交易日) ===\n")
        total = 0
        for i, date_str in enumerate(dates, 1):
            print(f"[{i}/{len(dates)}] {date_str} ... ", end="", flush=True)
            count = fetch_one_day(date_str, dao)
            if count > 0:
                print(f"{count} 条")
                total += count
            else:
                print("无数据(可能是假日)")
        print(f"\n=== 完成: {total} 条数据, {len(dates)} 天 ===")

    elif len(sys.argv) == 2:
        # 单日模式: python main.py 2026-04-16
        date_str = sys.argv[1]
        print(f"=== 爬取 {date_str} ===")
        count = fetch_one_day(date_str, dao)
        print(f"写入: {count} 条")

    else:
        # 默认：最近交易日
        date_str = get_last_trading_date()
        print(f"=== 爬取 {date_str} ===")
        count = fetch_one_day(date_str, dao)
        print(f"写入: {count} 条")

    dao.close()


if __name__ == "__main__":
    main()
