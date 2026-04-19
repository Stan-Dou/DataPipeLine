"""期权数据的数据对象 + DAO"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path


@dataclass
class OptionRecord:
    """一条期权合约在某一天的数据"""
    date: str           # "2026-04-16"
    stock_code: str     # "09992"
    closing_price: float # 标的股票收盘价，如 164.80
    expiry: str         # "29APR26"
    strike: float       # 142.5
    type: str           # "C" or "P"
    open: float
    high: float
    low: float
    settlement: float
    change: str
    iv: int
    volume: int
    oi: int
    oi_change: int


class OptionsDAO:
    """期权数据的增删查"""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # 查询结果可以用列名访问
        self._create_tables()

    def _create_tables(self):
        """建表，已存在则跳过。BCNF设计：stocks + stock_aliases + options_daily"""

        # 1. 股票信息表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                stock_code  TEXT PRIMARY KEY,   -- "09992"
                stock_name  TEXT NOT NULL,       -- "泡泡玛特"
                hkats_code  TEXT UNIQUE          -- "POP"
            )
        """)

        # 2. 股票别名表（多个名字映射同一代码）
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_aliases (
                alias       TEXT PRIMARY KEY,                        -- "泡泡" / "泡泡玛特"
                stock_code  TEXT NOT NULL REFERENCES stocks(stock_code)
            )
        """)

        # 3. 期权日报表（事实表）
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS options_daily (
                date          TEXT    NOT NULL,
                stock_code    TEXT    NOT NULL REFERENCES stocks(stock_code),
                closing_price REAL,
                expiry        TEXT    NOT NULL,
                strike      REAL    NOT NULL,
                type        TEXT    NOT NULL CHECK(type IN ('C', 'P')),
                open        REAL,
                high        REAL,
                low         REAL,
                settlement  REAL,
                change      TEXT,
                iv          INTEGER,
                volume      INTEGER,
                oi          INTEGER,
                oi_change   INTEGER,
                PRIMARY KEY (date, stock_code, expiry, strike, type)
            )
        """)

        # 索引
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_date 
            ON options_daily(stock_code, date)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_expiry 
            ON options_daily(expiry)
        """)

        # 中文视图：JOIN stocks表显示股票名称
        self.conn.execute("""
            CREATE VIEW IF NOT EXISTS 期权日报 AS
            SELECT
                o.date          AS 日期,
                s.stock_name    AS 股票名称,
                o.stock_code    AS 股票代码,
                o.closing_price AS 收盘价,
                o.expiry        AS 到期日,
                o.strike     AS 行权价,
                o.type       AS 类型,
                o.open       AS 开盘价,
                o.high       AS 最高价,
                o.low        AS 最低价,
                o.settlement AS 结算价,
                o.change     AS 涨跌,
                o.iv         AS 隐含波动率,
                o.volume     AS 成交量,
                o.oi         AS 持仓量,
                o.oi_change  AS 持仓变动
            FROM options_daily o
            LEFT JOIN stocks s ON o.stock_code = s.stock_code
        """)

        # 预置数据
        self.conn.executemany(
            "INSERT OR IGNORE INTO stocks VALUES (?,?,?)",
            [
                ("09992", "泡泡玛特", "POP"),
                ("01810", "小米集团", "MIU"),
            ]
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO stock_aliases VALUES (?,?)",
            [
                ("泡泡", "09992"),
                ("泡泡玛特", "09992"),
                ("POP MART", "09992"),
                ("小米", "01810"),
                ("小米集团", "01810"),
                ("Xiaomi", "01810"),
            ]
        )

        self.conn.commit()

    def upsert(self, record: OptionRecord) -> bool:
        """插入一条记录，主键冲突则更新。

        Args:
            record: OptionRecord对象，包含一条期权合约的完整数据。
                    主键为 (date, stock_code, expiry, strike, type)，
                    若该主键已存在，则用新数据覆盖旧数据。

        Returns:
            True 表示写入成功。

        Example:
            r = OptionRecord(date='2026-04-16', stock_code='09992',
                             expiry='29APR26', strike=150.0, type='P', ...)
            dao.upsert(r)
        """
        self.conn.execute("""
            INSERT INTO options_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, stock_code, expiry, strike, type)
            DO UPDATE SET
                closing_price=excluded.closing_price,
                open=excluded.open, high=excluded.high, low=excluded.low,
                settlement=excluded.settlement, change=excluded.change,
                iv=excluded.iv, volume=excluded.volume,
                oi=excluded.oi, oi_change=excluded.oi_change
        """, (
            record.date, record.stock_code, record.closing_price,
            record.expiry, record.strike, record.type,
            record.open, record.high, record.low, record.settlement, record.change,
            record.iv, record.volume, record.oi, record.oi_change,
        ))
        self.conn.commit()
        return True

    def upsert_batch(self, records: List[OptionRecord]) -> int:
        """批量插入，一次commit，比逐条upsert快几十倍。

        Args:
            records: OptionRecord列表，通常是parse返回的全部数据。

        Returns:
            成功写入的记录数。

        Example:
            records = [OptionRecord(...), OptionRecord(...), ...]
            count = dao.upsert_batch(records)  # 1614条一次性写入
        """
        sql = """
            INSERT INTO options_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, stock_code, expiry, strike, type)
            DO UPDATE SET
                closing_price=excluded.closing_price,
                open=excluded.open, high=excluded.high, low=excluded.low,
                settlement=excluded.settlement, change=excluded.change,
                iv=excluded.iv, volume=excluded.volume,
                oi=excluded.oi, oi_change=excluded.oi_change
        """
        rows = [
            (r.date, r.stock_code, r.closing_price,
             r.expiry, r.strike, r.type,
             r.open, r.high, r.low, r.settlement, r.change,
             r.iv, r.volume, r.oi, r.oi_change)
            for r in records
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def get(self, date: str, stock_code: str, expiry: str, strike: float, type: str) -> Optional[OptionRecord]:
        """按主键精确查询一条记录。

        Args:
            date:       交易日期，格式 "2026-04-16"
            stock_code: 港交所股票代码，如 "09992"
            expiry:     期权到期日，格式 "29APR26"（日+月缩写+年后两位）
            strike:     行权价，如 150.0
            type:       期权类型，"C"(看涨) 或 "P"(看跌)

        Returns:
            找到则返回 OptionRecord，找不到返回 None。

        Example:
            r = dao.get('2026-04-16', '09992', '29APR26', 150.0, 'P')
            if r:
                print(r.settlement, r.oi)
        """
        row = self.conn.execute(
            "SELECT * FROM options_daily WHERE date=? AND stock_code=? AND expiry=? AND strike=? AND type=?",
            (date, stock_code, expiry, strike, type)
        ).fetchone()
        if row is None:
            return None
        return OptionRecord(**dict(row))

    def delete(self, date: str, stock_code: str, expiry: str, strike: float, type: str) -> bool:
        """按主键删除一条记录。参数含义同 get()。

        Returns:
            True 表示确实删除了一条，False 表示没找到该记录。
        """
        cursor = self.conn.execute(
            "DELETE FROM options_daily WHERE date=? AND stock_code=? AND expiry=? AND strike=? AND type=?",
            (date, stock_code, expiry, strike, type)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def close(self):
        self.conn.close()

    # -- 别名相关 --

    def resolve_alias(self, name_or_code: str) -> Optional[str]:
        """将股票别名或名称转换为股票代码。

        支持传入中文名、英文名、别名、或直接传代码。

        Args:
            name_or_code: 股票的任意标识，如 "泡泡"、"POP MART"、"09992"

        Returns:
            对应的股票代码（如 "09992"），找不到返回 None。

        Example:
            dao.resolve_alias('泡泡')     # → '09992'
            dao.resolve_alias('Xiaomi')   # → '01810'
            dao.resolve_alias('09992')    # → '09992'
            dao.resolve_alias('不存在')   # → None
        """
        row = self.conn.execute(
            "SELECT stock_code FROM stock_aliases WHERE alias=?",
            (name_or_code,)
        ).fetchone()
        if row:
            return row[0]
        # 可能直接传的就是代码
        row = self.conn.execute(
            "SELECT stock_code FROM stocks WHERE stock_code=?",
            (name_or_code,)
        ).fetchone()
        return row[0] if row else None

    def add_stock(self, stock_code: str, stock_name: str, hkats_code: str, aliases: List[str] = None):
        """添加一只新股票到数据库，可同时添加别名。

        Args:
            stock_code: 港交所股票代码，5位数字字符串，如 "00700"
            stock_name: 股票中文名称，如 "腾讯控股"
            hkats_code: 港交所期权系统的3字母缩写，如 "TCH"。
                        在HKEX期权日报中用这个代码定位数据段。
            aliases:    别名列表（可选），支持之后用 resolve_alias() 查找。
                        如 ["腾讯", "Tencent", "微信"]。

        Example:
            dao.add_stock('00700', '腾讯控股', 'TCH',
                          aliases=['腾讯', 'Tencent'])
            dao.resolve_alias('腾讯')  # → '00700'
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO stocks VALUES (?,?,?)",
            (stock_code, stock_name, hkats_code)
        )
        if aliases:
            self.conn.executemany(
                "INSERT OR IGNORE INTO stock_aliases VALUES (?,?)",
                [(a, stock_code) for a in aliases]
            )
        self.conn.commit()

    # -- View / API queries --

    def get_company_metadata(self) -> List[Dict[str, Any]]:
        """Return company-level metadata for the local view layer."""
        companies = self.conn.execute(
            """
            SELECT
                s.stock_code,
                s.stock_name,
                s.hkats_code,
                (
                    SELECT closing_price
                    FROM options_daily latest
                    WHERE latest.stock_code = s.stock_code
                    ORDER BY latest.date DESC
                    LIMIT 1
                ) AS latest_closing_price,
                MIN(o.date) AS start_date,
                MAX(o.date) AS end_date,
                COUNT(DISTINCT o.date) AS trading_days
            FROM stocks s
            JOIN options_daily o ON o.stock_code = s.stock_code
            GROUP BY s.stock_code, s.stock_name, s.hkats_code
            ORDER BY s.hkats_code
            """
        ).fetchall()

        result = []
        for company in companies:
            stock_code = company["stock_code"]
            expiries = self.conn.execute(
                "SELECT DISTINCT expiry FROM options_daily WHERE stock_code=?",
                (stock_code,)
            ).fetchall()
            strikes = self.conn.execute(
                "SELECT DISTINCT strike FROM options_daily WHERE stock_code=? ORDER BY strike",
                (stock_code,)
            ).fetchall()
            option_types = self.conn.execute(
                "SELECT DISTINCT type FROM options_daily WHERE stock_code=? ORDER BY type",
                (stock_code,)
            ).fetchall()
            contract_rows = self.conn.execute(
                """
                SELECT expiry, type, strike
                FROM options_daily
                WHERE stock_code=?
                GROUP BY expiry, type, strike
                ORDER BY strike
                """,
                (stock_code,)
            ).fetchall()

            contract_index: Dict[str, Dict[str, List[float]]] = {}
            for row in contract_rows:
                expiry = row["expiry"]
                option_type = row["type"]
                strike = row["strike"]
                contract_index.setdefault(expiry, {}).setdefault(option_type, []).append(strike)

            result.append({
                "stock_code": company["stock_code"],
                "stock_name": company["stock_name"],
                "hkats_code": company["hkats_code"],
                "latest_closing_price": company["latest_closing_price"],
                "start_date": company["start_date"],
                "end_date": company["end_date"],
                "trading_days": company["trading_days"],
                "expiries": sorted((row[0] for row in expiries), key=self._parse_expiry),
                "strikes": [row[0] for row in strikes],
                "types": [row[0] for row in option_types],
                "contract_index": contract_index,
            })

        return result

    @staticmethod
    def _parse_expiry(expiry: str) -> datetime:
        """Convert HKEX expiry strings like 29APR26 into a sortable datetime."""
        return datetime.strptime(expiry, "%d%b%y")

    def get_stock_identity(self, identifier: str) -> Optional[Dict[str, str]]:
        """Resolve stock identity from HKATS code, stock code, alias, or name."""
        row = self.conn.execute(
            "SELECT stock_code, stock_name, hkats_code FROM stocks WHERE hkats_code=? OR stock_code=?",
            (identifier, identifier)
        ).fetchone()
        if row:
            return dict(row)

        stock_code = self.resolve_alias(identifier)
        if stock_code is None:
            return None

        row = self.conn.execute(
            "SELECT stock_code, stock_name, hkats_code FROM stocks WHERE stock_code=?",
            (stock_code,)
        ).fetchone()
        return dict(row) if row else None

    def get_closing_price_series(self, identifier: str) -> List[Dict[str, Any]]:
        """Return the stock closing price series for one company."""
        identity = self.get_stock_identity(identifier)
        if identity is None:
            return []

        rows = self.conn.execute(
            """
            SELECT date, MAX(closing_price) AS closing_price
            FROM options_daily
            WHERE stock_code=?
            GROUP BY date
            ORDER BY date
            """,
            (identity["stock_code"],)
        ).fetchall()

        return [
            {
                "date": row["date"],
                "closing_price": row["closing_price"],
            }
            for row in rows
        ]

    def get_contract_series(
        self,
        identifier: str,
        expiry: str,
        strike: float,
        option_type: str,
    ) -> Dict[str, Any]:
        """Return time series data for a single contract line."""
        identity = self.get_stock_identity(identifier)
        if identity is None:
            return {}

        rows = self.conn.execute(
            """
            SELECT
                date,
                closing_price,
                expiry,
                strike,
                type,
                settlement,
                open,
                high,
                low,
                change,
                volume,
                oi,
                oi_change,
                iv
            FROM options_daily
            WHERE stock_code=? AND expiry=? AND strike=? AND type=?
            ORDER BY date
            """,
            (identity["stock_code"], expiry, strike, option_type)
        ).fetchall()

        line_name = f"{identity['hkats_code']}-{expiry}-{'Call' if option_type == 'C' else 'Put'}-{strike}"
        return {
            "company": identity,
            "line_name": line_name,
            "expiry": expiry,
            "strike": strike,
            "type": option_type,
            "points": [
                {
                    "date": row["date"],
                    "closing_price": row["closing_price"],
                    "settlement": row["settlement"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "change": row["change"],
                    "volume": row["volume"],
                    "oi": row["oi"],
                    "oi_change": row["oi_change"],
                    "iv": row["iv"],
                }
                for row in rows
            ],
        }
