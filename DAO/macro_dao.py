"""宏观/债券数据的 DataClass + DAO（与 index_dao 共用 options.db）"""

import sqlite3
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from pathlib import Path


@dataclass
class BondYieldRecord:
    """某一日的国债收益率数据"""
    date: str                     # "2026-04-30"
    yield_2y: Optional[float] = None   # 中国国债2年收益率(%)
    yield_5y: Optional[float] = None   # 中国国债5年收益率(%)
    yield_10y: Optional[float] = None  # 中国国债10年收益率(%) — 最核心
    yield_30y: Optional[float] = None  # 中国国债30年收益率(%)
    spread_10y_2y: Optional[float] = None  # 期限利差 10Y-2Y
    us_yield_2y: Optional[float] = None    # 美国国债2年(%)
    us_yield_5y: Optional[float] = None    # 美国国债5年(%)
    us_yield_10y: Optional[float] = None   # 美国国债10年(%)
    us_yield_30y: Optional[float] = None   # 美国国债30年(%)
    us_spread_10y_2y: Optional[float] = None  # 美债期限利差
    source: str = "akshare"


class MacroDAO:
    """宏观数据 DAO（债券收益率等）"""

    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self):
        # 国债收益率表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bond_yield (
                date            TEXT    NOT NULL PRIMARY KEY,
                yield_2y        REAL,
                yield_5y        REAL,
                yield_10y       REAL,
                yield_30y       REAL,
                spread_10y_2y   REAL,
                us_yield_2y     REAL,
                us_yield_5y     REAL,
                us_yield_10y    REAL,
                us_yield_30y    REAL,
                us_spread_10y_2y REAL,
                source          TEXT    DEFAULT 'akshare'
            )
        """)
        self.conn.commit()

    # ── 增删改 ──

    def upsert(self, record: BondYieldRecord) -> bool:
        self.conn.execute("""
            INSERT INTO bond_yield VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                yield_2y=excluded.yield_2y,
                yield_5y=excluded.yield_5y,
                yield_10y=excluded.yield_10y,
                yield_30y=excluded.yield_30y,
                spread_10y_2y=excluded.spread_10y_2y,
                us_yield_2y=excluded.us_yield_2y,
                us_yield_5y=excluded.us_yield_5y,
                us_yield_10y=excluded.us_yield_10y,
                us_yield_30y=excluded.us_yield_30y,
                us_spread_10y_2y=excluded.us_spread_10y_2y,
                source=excluded.source
        """, (
            record.date, record.yield_2y, record.yield_5y, record.yield_10y,
            record.yield_30y, record.spread_10y_2y,
            record.us_yield_2y, record.us_yield_5y, record.us_yield_10y,
            record.us_yield_30y, record.us_spread_10y_2y,
            record.source,
        ))
        self.conn.commit()
        return True

    def upsert_batch(self, records: List[BondYieldRecord]) -> int:
        sql = """
            INSERT INTO bond_yield VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(date) DO UPDATE SET
                yield_2y=excluded.yield_2y,
                yield_5y=excluded.yield_5y,
                yield_10y=excluded.yield_10y,
                yield_30y=excluded.yield_30y,
                spread_10y_2y=excluded.spread_10y_2y,
                us_yield_2y=excluded.us_yield_2y,
                us_yield_5y=excluded.us_yield_5y,
                us_yield_10y=excluded.us_yield_10y,
                us_yield_30y=excluded.us_yield_30y,
                us_spread_10y_2y=excluded.us_spread_10y_2y,
                source=excluded.source
        """
        rows = [
            (r.date, r.yield_2y, r.yield_5y, r.yield_10y,
             r.yield_30y, r.spread_10y_2y,
             r.us_yield_2y, r.us_yield_5y, r.us_yield_10y,
             r.us_yield_30y, r.us_spread_10y_2y,
             r.source)
            for r in records
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    # ── 查询 ──

    def get_series(self, column: str = "yield_10y",
                   start_date: Optional[str] = None,
                   end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取某列收益率的时间序列"""
        sql = f"SELECT date, {column} FROM bond_yield WHERE {column} IS NOT NULL"
        params: list = []
        if start_date:
            sql += " AND date>=?"
            params.append(start_date)
        if end_date:
            sql += " AND date<=?"
            params.append(end_date)
        sql += " ORDER BY date"
        return [dict(row) for row in self.conn.execute(sql, params).fetchall()]

    def get_latest_date(self) -> Optional[str]:
        row = self.conn.execute("SELECT MAX(date) FROM bond_yield").fetchone()
        return row[0] if row and row[0] else None

    def get_record_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM bond_yield").fetchone()
        return row[0] if row else 0

    def close(self):
        self.conn.close()
