"""指数数据的数据对象 + DAO（支持中证红利50等A股指数）"""

import sqlite3
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from pathlib import Path


@dataclass
class IndexRecord:
    """一条指数在某一天的数据"""
    date: str            # "2026-04-28"
    index_code: str      # "000922"
    index_name: str      # "中证红利50"
    open: float
    high: float
    low: float
    close: float
    volume: float        # 成交量
    amount: float        # 成交额
    source: str          # "akshare" / "csi_official"


@dataclass
class IndexValuationRecord:
    """指数估值数据（PE/PB/股息率）"""
    date: str
    index_code: str
    pe: Optional[float] = None         # 市盈率
    pe_percentile: Optional[float] = None  # PE历史分位数(0-100)
    pb: Optional[float] = None         # 市净率
    pb_percentile: Optional[float] = None
    dividend_yield: Optional[float] = None  # 股息率(%)
    dy_percentile: Optional[float] = None
    source: str = ""


class IndexDAO:
    """指数行情 + 估值数据的增删查"""

    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        # 1. 指数信息表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS indices (
                index_code  TEXT PRIMARY KEY,
                index_name  TEXT NOT NULL
            )
        """)

        # 2. 指数日行情表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_daily (
                date        TEXT    NOT NULL,
                index_code  TEXT    NOT NULL REFERENCES indices(index_code),
                index_name  TEXT,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL    NOT NULL,
                volume      REAL,
                amount      REAL,
                source      TEXT,
                PRIMARY KEY (date, index_code)
            )
        """)

        # 3. 指数估值表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS index_valuation (
                date            TEXT    NOT NULL,
                index_code      TEXT    NOT NULL REFERENCES indices(index_code),
                pe              REAL,
                pe_percentile   REAL,
                pb              REAL,
                pb_percentile   REAL,
                dividend_yield  REAL,
                dy_percentile   REAL,
                source          TEXT,
                PRIMARY KEY (date, index_code)
            )
        """)

        # 索引
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_index_daily_code_date
            ON index_daily(index_code, date)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_index_valuation_code_date
            ON index_valuation(index_code, date)
        """)

        # 预置中证红利50
        self.conn.execute(
            "INSERT OR IGNORE INTO indices VALUES (?,?)",
            ("000922", "中证红利50")
        )
        self.conn.commit()

    # ── 行情数据 ──

    def upsert_daily(self, record: IndexRecord) -> bool:
        self.conn.execute("""
            INSERT INTO index_daily VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, index_code)
            DO UPDATE SET
                index_name=excluded.index_name,
                open=excluded.open, high=excluded.high,
                low=excluded.low, close=excluded.close,
                volume=excluded.volume, amount=excluded.amount,
                source=excluded.source
        """, (
            record.date, record.index_code, record.index_name,
            record.open, record.high, record.low, record.close,
            record.volume, record.amount, record.source,
        ))
        self.conn.commit()
        return True

    def upsert_daily_batch(self, records: List[IndexRecord]) -> int:
        sql = """
            INSERT INTO index_daily VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, index_code)
            DO UPDATE SET
                index_name=excluded.index_name,
                open=excluded.open, high=excluded.high,
                low=excluded.low, close=excluded.close,
                volume=excluded.volume, amount=excluded.amount,
                source=excluded.source
        """
        rows = [
            (r.date, r.index_code, r.index_name,
             r.open, r.high, r.low, r.close,
             r.volume, r.amount, r.source)
            for r in records
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    # ── 估值数据 ──

    def upsert_valuation(self, record: IndexValuationRecord) -> bool:
        self.conn.execute("""
            INSERT INTO index_valuation VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, index_code)
            DO UPDATE SET
                pe=excluded.pe, pe_percentile=excluded.pe_percentile,
                pb=excluded.pb, pb_percentile=excluded.pb_percentile,
                dividend_yield=excluded.dividend_yield,
                dy_percentile=excluded.dy_percentile,
                source=excluded.source
        """, (
            record.date, record.index_code,
            record.pe, record.pe_percentile,
            record.pb, record.pb_percentile,
            record.dividend_yield, record.dy_percentile,
            record.source,
        ))
        self.conn.commit()
        return True

    def upsert_valuation_batch(self, records: List[IndexValuationRecord]) -> int:
        sql = """
            INSERT INTO index_valuation VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date, index_code)
            DO UPDATE SET
                pe=excluded.pe, pe_percentile=excluded.pe_percentile,
                pb=excluded.pb, pb_percentile=excluded.pb_percentile,
                dividend_yield=excluded.dividend_yield,
                dy_percentile=excluded.dy_percentile,
                source=excluded.source
        """
        rows = [
            (r.date, r.index_code,
             r.pe, r.pe_percentile,
             r.pb, r.pb_percentile,
             r.dividend_yield, r.dy_percentile,
             r.source)
            for r in records
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    # ── 查询 ──

    def get_daily_series(self, index_code: str,
                         start_date: str = None,
                         end_date: str = None) -> List[Dict[str, Any]]:
        """获取指数日行情序列"""
        sql = "SELECT * FROM index_daily WHERE index_code=?"
        params: list = [index_code]
        if start_date:
            sql += " AND date>=?"
            params.append(start_date)
        if end_date:
            sql += " AND date<=?"
            params.append(end_date)
        sql += " ORDER BY date"
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def get_valuation_series(self, index_code: str,
                             start_date: str = None,
                             end_date: str = None) -> List[Dict[str, Any]]:
        """获取指数估值序列"""
        sql = "SELECT * FROM index_valuation WHERE index_code=?"
        params: list = [index_code]
        if start_date:
            sql += " AND date>=?"
            params.append(start_date)
        if end_date:
            sql += " AND date<=?"
            params.append(end_date)
        sql += " ORDER BY date"
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def get_latest_date(self, index_code: str) -> Optional[str]:
        """获取已有数据的最新日期"""
        row = self.conn.execute(
            "SELECT MAX(date) FROM index_daily WHERE index_code=?",
            (index_code,)
        ).fetchone()
        return row[0] if row and row[0] else None

    def get_record_count(self, index_code: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM index_daily WHERE index_code=?",
            (index_code,)
        ).fetchone()
        return row[0] if row else 0

    def close(self):
        self.conn.close()
