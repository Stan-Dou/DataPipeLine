"""HKEX期权数据爬虫 - 从港交所每日报告抓取期权OI/成交量"""

import re
from Scraper.base_scraper import BaseScraper


# 名称 → 股票代码
STOCK_NAME_MAP = {
    "小米": "01810",
    "泡泡": "09992",
    "泡泡玛特": "09992",
}

# 股票代码 → HKEX期权简写代码（报告中用这个定位）
STOCK_TO_HKATS = {
    "09992": "POP",
    "01810": "MIU",
}


def resolve_stock_code(name_or_code: str) -> str:
    """名称/别名 → 股票代码。已经是代码则原样返回。"""
    return STOCK_NAME_MAP.get(name_or_code, name_or_code)


class HKEXOptionsScraper(BaseScraper):
    def __init__(self, year, month, day, watchlist=None):
        super().__init__()
        self.option_date = f"{year:04d}{month:02d}{day:02d}"
        if watchlist:
            self.watchlist = [resolve_stock_code(w) for w in watchlist]
        else:
            self.watchlist = None  # None = 解析所有已知股票

    def build_request(self, **kwargs):
        short_option_date = self.option_date[2:]
        url = f"https://www.hkex.com.hk/eng/stat/dmstat/dayrpt/dqe{short_option_date}.htm"
        return {"url": url}

    def parse(self, response):
        html = response.text
        results = []

        # 决定要解析哪些股票
        codes = self.watchlist if self.watchlist else list(STOCK_TO_HKATS.keys())

        for stock_code in codes:
            hkats = STOCK_TO_HKATS.get(stock_code)
            if not hkats:
                continue
            records = self._parse_stock(html, stock_code, hkats)
            results.extend(records)

        return results

    def _parse_stock(self, html, stock_code, hkats_code):
        """从HTML中提取某只股票的全部期权合约数据"""
        records = []

        # 定位数据段：从 NAME="POP" 到下一个 NAME="
        start_marker = f'NAME="{hkats_code}"'
        start = html.find(start_marker)
        if start == -1:
            return records

        next_marker = html.find('NAME="', start + len(start_marker))
        section = html[start:next_marker] if next_marker != -1 else html[start:]

        # 提取收盘价: "CLOSING PRICE HK$  164.80"
        closing_match = re.search(r'CLOSING PRICE HK\$\s+([\d.]+)', section)
        closing_price = float(closing_match.group(1)) if closing_match else 0.0

        # 每行格式（固定宽度）:
        # 29APR26   142.50 C     25.28     25.28     25.28     22.73      +1.27   47       10         348            0
        # 到期日    行权价 C/P   开盘      最高      最低      结算价     变动    IV%    成交量      OI          OI变动
        pattern = re.compile(
            r'(\d{2}[A-Z]{3}\d{2})'   # 到期日: 29APR26
            r'\s+([\d.]+)'             # 行权价: 142.50
            r'\s+([CP])'              # 类型: C或P
            r'\s+([\d.]+)'            # 开盘价
            r'\s+([\d.]+)'            # 最高价
            r'\s+([\d.]+)'            # 最低价
            r'\s+([\d.]+)'            # 结算价
            r'\s+([+-]?[\d.]+)'       # 结算价变动
            r'\s+(\d+)'              # IV%
            r'\s+([\d,]+)'            # 成交量
            r'\s+([\d,]+)'            # OI
            r'\s+([+-]?[\d,]+)'       # OI变动
        )

        for m in pattern.finditer(section):
            records.append({
                "stock_code": stock_code,
                "closing_price": closing_price,  # 标的股票收盘价
                "expiry": m.group(1),
                "strike": float(m.group(2)),
                "type": m.group(3),             # "C" or "P"
                "open": float(m.group(4)),
                "high": float(m.group(5)),
                "low": float(m.group(6)),
                "settlement": float(m.group(7)),
                "change": m.group(8),
                "iv": int(m.group(9)),
                "volume": int(m.group(10).replace(",", "")),
                # OI = 当日未平仓合约数（日报原始值）
                "oi": int(m.group(11).replace(",", "")),
                "oi_change": int(m.group(12).replace(",", "").replace("+", "")),
            })

        return records
