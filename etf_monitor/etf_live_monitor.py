"""Unified live monitor for HK ETFs.

Streams IB Gateway top-of-book ticks, IB ETF NAV ticks, optional Level-2
market depth and the official ICE iNAV for a given HK ETF symbol into a
small set of CSV files. Designed to run unattended for hours/days with
automatic reconnect on transient IB Gateway failures.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ib_insync import IB, Stock, Ticker

# ---------------------------------------------------------------------------
# Module-wide logger. Configured by setup_logging() in main().
# ---------------------------------------------------------------------------
log = logging.getLogger("etf_monitor")


DEFAULT_PROBE_SYMBOLS = [
    "2800",
    "2828",
    "2833",
    "2822",
    "2823",
    "3033",
    "3037",
    "3067",
    "3115",
    "3188",
]

IB_TOP_TICK_FIELDS = {
    0: "bid_size",
    1: "bid",
    2: "ask",
    3: "ask_size",
    4: "last",
    5: "last_size",
    9: "close",
    96: "ib_nav_last",
    97: "ib_nav_frozen_last",
    98: "ib_nav_high",
    99: "ib_nav_low",
}

# Generic tick list requested for ETF subscriptions:
#   577 = ETF Average Volume, 614 = ETF NAV Last, 623 = ETF NAV Close.
# These map to the 96/97/98/99 tick types in IB_TOP_TICK_FIELDS.
DEFAULT_GENERIC_TICKS = "577,614,623"

# IB error codes that indicate the market-depth subscription is gone or never
# became available. When a depth error matches one of these and the contract
# matches the primary subscription, depth is marked inactive.
DEPTH_RELATED_ERROR_CODES = frozenset({309, 354, 2152, 10089, 10090, 10091, 10092})

OFFICIAL_INAV_API_URL = "https://inav.ice.com/api/1/csop/application/index/quote"

# A-share market timezone. The official iNAV is referenced to A-share trading
# hours (Shanghai/Shenzhen). Both Beijing and Hong Kong are UTC+8 so the
# offset is sufficient for our coarse open/closed flag.
BEIJING_TZ = timezone(timedelta(hours=8))

MARKET_FIELDS = [
    "event_time",
    "received_at",
    "symbol",
    "tick_type",
    "tick_name",
    "tick_price",
    "tick_size",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "last_size",
    "close",
    "market_data_type",
    "ib_nav_last",
    "ib_nav_frozen_last",
    "ib_nav_high",
    "ib_nav_low",
    "midpoint",
    "ask_ib_nav_discount_bps",
    "bid_ib_nav_discount_bps",
    "last_ib_nav_discount_bps",
]

PROBE_FIELDS = [
    "captured_at",
    "symbol",
    "qualified",
    "bid",
    "ask",
    "last",
    "ib_nav_last",
    "ib_nav_frozen_last",
    "ib_nav_high",
    "ib_nav_low",
    "received_nav",
    "tick_count",
    "error",
]

OFFICIAL_INAV_FIELDS = [
    "captured_at",
    "symbol",
    "error",
    "time_zone",
    "symbols",
    "inav_date",
    "inav_time",
    "inav_rmb",
    "inav_hkd",
    "market_price_date",
    "market_price_time",
    "market_price_rmb",
    "market_price_hkd",
    "premium_discount_date",
    "premium_discount_time",
    "premium_discount_rmb_pct",
    "premium_discount_hkd_pct",
    "rmb_hkd_fx",
    "has_intraday_market_price",
]

SYNC_SNAPSHOT_FIELDS = [
    "captured_at",
    "symbol",
    "official_inav_error",
    "official_inav_hkd",
    "official_market_price_hkd",
    "official_discount_hkd_pct",
    "official_inav_time",
    "official_market_price_time",
    "nav_staleness_seconds",
    "a_share_open",
    "l1_last_update_at",
    "l2_last_update_at",
    "l1_age_ms",
    "l2_age_ms",
    "state_age_ms",
    "depth_active",
    "depth_levels_bid_count",
    "depth_levels_ask_count",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "last_size",
    "close",
    "market_data_type",
    "ib_nav_last",
    "ib_nav_frozen_last",
    "ib_nav_high",
    "ib_nav_low",
    "midpoint",
    "ask_ib_nav_discount_bps",
    "bid_ib_nav_discount_bps",
    "last_ib_nav_discount_bps",
    "best_depth_bid",
    "best_depth_ask",
    "dom_bids",
    "dom_asks",
]

DEPTH_FIELDS = [
    "event_kind",
    "event_time",
    "received_at",
    "symbol",
    "status",
    "message",
    "position",
    "side",
    "operation",
    "market_maker",
    "price",
    "size",
    "best_bid",
    "best_ask",
    "dom_bids",
    "dom_asks",
    "error_code",
]

SENTINEL = object()


class CsvWriteWorker:
    def __init__(
        self,
        file_specs: dict[str, tuple[Path, list[str]]],
        max_queue_size: int,
        batch_size: int,
        flush_interval: float,
    ):
        self.file_specs = file_specs
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue: queue.Queue[Any] = queue.Queue(maxsize=max_queue_size)
        self.thread = threading.Thread(target=self._run, name="csv-writer", daemon=True)
        self.buffers = {key: [] for key in file_specs}
        self.handles: dict[str, Any] = {}
        self.writers: dict[str, csv.DictWriter] = {}
        self._warned_queue_full = False

    def start(self) -> None:
        for path, fields in self.file_specs.values():
            ensure_header(path, fields)
        self.thread.start()

    def submit(self, file_key: str, row: dict[str, Any]) -> None:
        item = (file_key, dict(row))
        while True:
            try:
                self.queue.put(item, timeout=0.25)
                self._warned_queue_full = False
                return
            except queue.Full:
                if not self._warned_queue_full:
                    self._warned_queue_full = True
                    print("Writer queue is full; blocking producer to preserve data integrity.")

    def close(self) -> None:
        self.queue.put(SENTINEL)
        self.thread.join()

    def _open_files(self) -> None:
        for key, (path, fields) in self.file_specs.items():
            handle = path.open("a", newline="", encoding="utf-8-sig")
            self.handles[key] = handle
            self.writers[key] = csv.DictWriter(handle, fieldnames=fields)

    def _flush_key(self, file_key: str) -> None:
        rows = self.buffers[file_key]
        if not rows:
            return
        self.writers[file_key].writerows(rows)
        self.handles[file_key].flush()
        rows.clear()

    def _flush_all(self) -> None:
        for key in self.file_specs:
            self._flush_key(key)

    def _run(self) -> None:
        self._open_files()
        try:
            last_flush = time.monotonic()
            while True:
                timeout = max(0.0, self.flush_interval - (time.monotonic() - last_flush))
                try:
                    item = self.queue.get(timeout=timeout)
                except queue.Empty:
                    self._flush_all()
                    last_flush = time.monotonic()
                    continue

                if item is SENTINEL:
                    self._flush_all()
                    break

                file_key, row = item
                self.buffers[file_key].append(row)
                if len(self.buffers[file_key]) >= self.batch_size:
                    self._flush_key(file_key)
                    last_flush = time.monotonic()

                self.queue.task_done()
        finally:
            self._flush_all()
            for handle in self.handles.values():
                handle.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unified HK ETF monitor for IB top-of-book ticks, IB NAV probing, official ICE iNAV polling, and synchronized snapshots."
    )
    parser.add_argument("symbol", help="Primary HK ETF symbol to monitor, for example 2822")
    parser.add_argument("--host", default="127.0.0.1", help="IB Gateway or TWS host")
    parser.add_argument("--port", type=int, default=4001, help="IB Gateway or TWS API port")
    parser.add_argument("--client-id", type=int, default=41, help="IB API client ID")
    parser.add_argument("--exchange", default="SEHK", help="Exchange, default: SEHK")
    parser.add_argument("--currency", default="HKD", help="Currency, default: HKD")
    parser.add_argument(
        "--ib-wait-seconds",
        type=float,
        default=0.25,
        help="IB wait timeout for processing real-time ticks, default: 0.25",
    )
    parser.add_argument(
        "--discount-threshold-bps",
        type=float,
        default=50.0,
        help="Print an alert when ask/IB_NAV discount is at or below negative this many bps",
    )
    parser.add_argument(
        "--probe-symbols",
        nargs="*",
        default=DEFAULT_PROBE_SYMBOLS,
        help="ETF symbols to probe for IB NAV availability before live monitoring",
    )
    parser.add_argument(
        "--probe-wait-seconds",
        type=float,
        default=12.0,
        help="Seconds to wait per probe symbol for IB NAV ticks, default: 12",
    )
    parser.add_argument(
        "--skip-probe",
        action="store_true",
        help="Skip the 10-ETF IB NAV availability probe",
    )
    parser.add_argument(
        "--official-inav-language",
        default="cn",
        choices=["cn", "en"],
        help="Language for the official ICE iNAV endpoint, default: cn",
    )
    parser.add_argument(
        "--official-inav-interval",
        type=float,
        default=15.0,
        help="Polling interval in seconds for official iNAV, default: 15",
    )
    parser.add_argument(
        "--with-depth",
        action="store_true",
        help="Also subscribe to IB market depth via reqMktDepth",
    )
    parser.add_argument(
        "--depth-rows",
        type=int,
        default=5,
        help="Number of DOM levels to request per side, max 5, default: 5",
    )
    parser.add_argument(
        "--depth-smart",
        action="store_true",
        help="Request smart depth if supported by IB",
    )
    parser.add_argument(
        "--depth-startup-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait before warning that no depth updates have arrived, default: 10",
    )
    parser.add_argument(
        "--writer-queue-size",
        type=int,
        default=20000,
        help="Max number of rows buffered before the writer blocks producers, default: 20000",
    )
    parser.add_argument(
        "--writer-batch-size",
        type=int,
        default=200,
        help="Rows per flush batch per file, default: 200",
    )
    parser.add_argument(
        "--writer-flush-interval",
        type=float,
        default=0.5,
        help="Maximum seconds between writer flushes, default: 0.5",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Directory for CSV outputs, default: ./data",
    )
    parser.add_argument(
        "--official-inav-timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds for the official iNAV request, default: 5",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity, default: INFO",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Path to log file. Defaults to <output-dir>/etf_monitor.log",
    )
    parser.add_argument(
        "--no-reconnect",
        action="store_true",
        help="Exit immediately if the IB session fails instead of reconnecting",
    )
    parser.add_argument(
        "--reconnect-base",
        type=float,
        default=1.0,
        help="Base seconds for the reconnect exponential backoff, default: 1",
    )
    parser.add_argument(
        "--reconnect-cap",
        type=float,
        default=60.0,
        help="Maximum seconds between reconnect attempts, default: 60",
    )
    return parser.parse_args()


def safe_number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or number == -1:
        return None
    return number


def discount_bps(price: float | None, nav_value: float | None) -> float | None:
    """Return premium/discount in basis points or None when inputs are unusable.

    NAV must be strictly positive; otherwise the result is meaningless and
    None is returned.
    """
    if price is None or nav_value is None or nav_value <= 0:
        return None
    return (price - nav_value) / nav_value * 10000


def compute_backoff(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Exponential backoff with cap. attempt=0 returns base; doubles each step."""
    if attempt < 0:
        attempt = 0
    return min(cap, base * (2 ** attempt))


def ensure_header(path: Path, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = ",".join(fieldnames)
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            first_line = handle.readline().strip()
        if first_line == header:
            return
        backup_path = path.with_suffix(path.suffix + ".bak")
        path.replace(backup_path)
        print(f"Existing output header mismatch. Backed up old file to {backup_path.resolve()}")

    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def now_utc() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(tz=timezone.utc)


def iso_now() -> str:
    """Return the current UTC time formatted as an ISO-8601 millisecond string."""
    return now_utc().isoformat(timespec="milliseconds")


def isoformat_timestamp(value: Any) -> str:
    """Format a timestamp (datetime or None) as an ISO-8601 string in UTC.

    Naive datetimes coming from the IB API are interpreted as UTC; aware
    datetimes are converted. When ``value`` is missing the current time is
    returned.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat(timespec="milliseconds")
    return iso_now()


def parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-8601 string. Naive inputs are interpreted as UTC."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def age_ms(reference_time: datetime, update_time_text: str | None) -> int | None:
    update_time = parse_iso_datetime(update_time_text)
    if update_time is None:
        return None
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    return max(0, int((reference_time - update_time).total_seconds() * 1000))


def blank_market_state(symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "bid": None,
        "ask": None,
        "last": None,
        "bid_size": None,
        "ask_size": None,
        "last_size": None,
        "close": None,
        "market_data_type": None,
        "ib_nav_last": None,
        "ib_nav_frozen_last": None,
        "ib_nav_high": None,
        "ib_nav_low": None,
        "l1_last_update_at": None,
    }


def update_market_state(state: dict[str, Any], ticker: Ticker, tick: Any) -> dict[str, Any] | None:
    field_name = IB_TOP_TICK_FIELDS.get(tick.tickType)
    if not field_name:
        return None

    if field_name.endswith("_size"):
        state[field_name] = safe_number(tick.size)
    else:
        state[field_name] = safe_number(tick.price)

    state["market_data_type"] = ticker.marketDataType
    state["l1_last_update_at"] = isoformat_timestamp(getattr(tick, "time", None))

    bid = state["bid"]
    ask = state["ask"]
    midpoint = (bid + ask) / 2 if bid is not None and ask is not None else None
    ib_nav_last = state["ib_nav_last"]

    return {
        "event_time": isoformat_timestamp(getattr(tick, "time", None)),
        "received_at": iso_now(),
        "symbol": state["symbol"],
        "tick_type": tick.tickType,
        "tick_name": field_name,
        "tick_price": safe_number(getattr(tick, "price", None)),
        "tick_size": safe_number(getattr(tick, "size", None)),
        "bid": state["bid"],
        "ask": state["ask"],
        "last": state["last"],
        "bid_size": state["bid_size"],
        "ask_size": state["ask_size"],
        "last_size": state["last_size"],
        "close": state["close"],
        "market_data_type": state["market_data_type"],
        "ib_nav_last": state["ib_nav_last"],
        "ib_nav_frozen_last": state["ib_nav_frozen_last"],
        "ib_nav_high": state["ib_nav_high"],
        "ib_nav_low": state["ib_nav_low"],
        "midpoint": midpoint,
        "ask_ib_nav_discount_bps": discount_bps(state["ask"], ib_nav_last),
        "bid_ib_nav_discount_bps": discount_bps(state["bid"], ib_nav_last),
        "last_ib_nav_discount_bps": discount_bps(state["last"], ib_nav_last),
    }


def latest_nav_ticks(ticker: Ticker) -> dict[str, float | None]:
    values = {
        "ib_nav_last": None,
        "ib_nav_frozen_last": None,
        "ib_nav_high": None,
        "ib_nav_low": None,
    }
    for tick in reversed(ticker.ticks):
        field_name = IB_TOP_TICK_FIELDS.get(tick.tickType)
        if field_name not in values or values[field_name] is not None:
            continue
        values[field_name] = safe_number(getattr(tick, "price", None))
        if all(value is not None for value in values.values()):
            break
    return values


def format_dom_levels(levels: list[Any]) -> str:
    return "|".join(
        f"{index + 1}:{safe_number(level.price)}@{safe_number(level.size)}:{level.marketMaker}"
        for index, level in enumerate(levels)
    )


def best_depth_price(levels: list[Any]) -> float | None:
    return safe_number(levels[0].price) if levels else None


def depth_side_name(side: int) -> str:
    return "bid" if side == 1 else "ask"


def depth_operation_name(operation: int) -> str:
    return {0: "insert", 1: "update", 2: "delete"}.get(operation, str(operation))


def build_depth_row(symbol: str, ticker: Ticker, tick: Any) -> dict[str, Any]:
    return {
        "event_kind": "data",
        "event_time": isoformat_timestamp(getattr(tick, "time", None)),
        "received_at": iso_now(),
        "symbol": symbol,
        "status": "ok",
        "message": "",
        "position": tick.position,
        "side": depth_side_name(tick.side),
        "operation": depth_operation_name(tick.operation),
        "market_maker": tick.marketMaker,
        "price": safe_number(tick.price),
        "size": safe_number(tick.size),
        "best_bid": best_depth_price(ticker.domBids),
        "best_ask": best_depth_price(ticker.domAsks),
        "dom_bids": format_dom_levels(ticker.domBids),
        "dom_asks": format_dom_levels(ticker.domAsks),
        "error_code": "",
    }


def depth_status_row(symbol: str, status: str, message: str, error_code: str = "") -> dict[str, Any]:
    return {
        "event_kind": "status",
        "event_time": iso_now(),
        "received_at": iso_now(),
        "symbol": symbol,
        "status": status,
        "message": message,
        "position": "",
        "side": "",
        "operation": "",
        "market_maker": "",
        "price": "",
        "size": "",
        "best_bid": "",
        "best_ask": "",
        "dom_bids": "",
        "dom_asks": "",
        "error_code": error_code,
    }


def build_sync_snapshot_row(
    symbol: str,
    official_row: dict[str, str],
    market_state: dict[str, Any],
    depth_ticker: Ticker | None,
    depth_active: bool,
    l2_last_update_at: str | None,
) -> dict[str, Any]:
    captured_at = official_row["captured_at"]
    captured_dt = parse_iso_datetime(captured_at) or now_utc()
    bid = market_state["bid"]
    ask = market_state["ask"]
    last = market_state["last"]
    ib_nav_last = market_state["ib_nav_last"]
    midpoint = (bid + ask) / 2 if bid is not None and ask is not None else None

    l1_age = age_ms(captured_dt, market_state.get("l1_last_update_at"))
    l2_age = age_ms(captured_dt, l2_last_update_at)
    age_candidates = [value for value in (l1_age, l2_age) if value is not None]
    state_age = max(age_candidates) if age_candidates else None

    return {
        "captured_at": captured_at,
        "symbol": symbol,
        "official_inav_error": official_row["error"],
        "official_inav_hkd": official_row["inav_hkd"],
        "official_market_price_hkd": official_row["market_price_hkd"],
        "official_discount_hkd_pct": official_row["premium_discount_hkd_pct"],
        "official_inav_time": official_row["inav_time"],
        "official_market_price_time": official_row["market_price_time"],
        "l1_last_update_at": market_state.get("l1_last_update_at") or "",
        "l2_last_update_at": l2_last_update_at or "",
        "l1_age_ms": l1_age,
        "l2_age_ms": l2_age,
        "state_age_ms": state_age,
        "depth_active": depth_active,
        "depth_levels_bid_count": len(depth_ticker.domBids) if depth_ticker else 0,
        "depth_levels_ask_count": len(depth_ticker.domAsks) if depth_ticker else 0,
        "bid": bid,
        "ask": ask,
        "last": last,
        "bid_size": market_state["bid_size"],
        "ask_size": market_state["ask_size"],
        "last_size": market_state["last_size"],
        "close": market_state["close"],
        "market_data_type": market_state["market_data_type"],
        "ib_nav_last": market_state["ib_nav_last"],
        "ib_nav_frozen_last": market_state["ib_nav_frozen_last"],
        "ib_nav_high": market_state["ib_nav_high"],
        "ib_nav_low": market_state["ib_nav_low"],
        "midpoint": midpoint,
        "ask_ib_nav_discount_bps": discount_bps(ask, ib_nav_last),
        "bid_ib_nav_discount_bps": discount_bps(bid, ib_nav_last),
        "last_ib_nav_discount_bps": discount_bps(last, ib_nav_last),
        "best_depth_bid": best_depth_price(depth_ticker.domBids) if depth_ticker else None,
        "best_depth_ask": best_depth_price(depth_ticker.domAsks) if depth_ticker else None,
        "dom_bids": format_dom_levels(depth_ticker.domBids) if depth_ticker else "",
        "dom_asks": format_dom_levels(depth_ticker.domAsks) if depth_ticker else "",
    }


def build_official_inav_url(symbol: str, language: str) -> str:
    query = urlencode({"symbol": symbol, "language": language})
    return f"{OFFICIAL_INAV_API_URL}?{query}"


def fetch_official_inav(symbol: str, language: str, timeout: float = 5.0) -> dict[str, Any]:
    request = Request(
        build_official_inav_url(symbol, language),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def row_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["label"]: row for row in rows}


def row_value(row: dict[str, Any], index: int) -> str:
    values = row.get("values", [])
    if index >= len(values):
        return ""
    return normalize_text(values[index])


def flatten_official_inav(symbol: str, payload: dict[str, Any]) -> dict[str, str]:
    quote = payload["quote"]
    rows = row_map(quote["rows"])
    inav = rows.get("INTRA_DAY_ESTIMATED_NAV_PER_UNIT", {})
    market_price = rows.get("INTRA_DAY_MARKET_PRICE", {})
    premium_discount = rows.get("PREMIUM_DISCOUNT", {})
    ratios = quote.get("ratios", [])

    return {
        "captured_at": now_utc().isoformat(timespec="seconds"),
        "symbol": symbol,
        "error": "",
        "time_zone": quote.get("timeZone", ""),
        "symbols": "|".join(quote.get("symbols", [])),
        "inav_date": normalize_text(inav.get("date", "")),
        "inav_time": normalize_text(inav.get("time", "")),
        "inav_rmb": row_value(inav, 0),
        "inav_hkd": row_value(inav, 1),
        "market_price_date": normalize_text(market_price.get("date", "")),
        "market_price_time": normalize_text(market_price.get("time", "")),
        "market_price_rmb": row_value(market_price, 0),
        "market_price_hkd": row_value(market_price, 1),
        "premium_discount_date": normalize_text(premium_discount.get("date", "")),
        "premium_discount_time": normalize_text(premium_discount.get("time", "")),
        "premium_discount_rmb_pct": row_value(premium_discount, 0),
        "premium_discount_hkd_pct": row_value(premium_discount, 1),
        "rmb_hkd_fx": str(ratios[0]) if ratios else "",
        "has_intraday_market_price": str(bool(quote.get("hasIntraDayMarketPrice", False))),
    }


def official_inav_error_row(symbol: str, error: str) -> dict[str, str]:
    row = {field: "" for field in OFFICIAL_INAV_FIELDS}
    row["captured_at"] = now_utc().isoformat(timespec="seconds")
    row["symbol"] = symbol
    row["error"] = error
    return row


def log_official_snapshot(row: dict[str, str]) -> None:
    if row["error"]:
        log.warning("Official iNAV error: symbol=%s err=%s", row["symbol"], row["error"])
        return
    log.info(
        "Official iNAV: symbol=%s inav_hkd=%s market_price_hkd=%s discount_hkd=%s",
        row["symbol"],
        row["inav_hkd"],
        row["market_price_hkd"],
        row["premium_discount_hkd_pct"],
    )


def qualify_primary_contract(ib: IB, args: argparse.Namespace):
    qualified = ib.qualifyContracts(Stock(args.symbol, args.exchange, args.currency))
    if not qualified:
        raise RuntimeError(
            f"IB could not qualify contract {args.symbol} on {args.exchange} in {args.currency}."
        )
    return qualified[0]


def probe_ib_nav_capabilities(ib: IB, args: argparse.Namespace, writer: CsvWriteWorker) -> None:
    for symbol in args.probe_symbols:
        row = {
            "captured_at": now_utc().isoformat(timespec="seconds"),
            "symbol": symbol,
            "qualified": False,
            "bid": None,
            "ask": None,
            "last": None,
            "ib_nav_last": None,
            "ib_nav_frozen_last": None,
            "ib_nav_high": None,
            "ib_nav_low": None,
            "received_nav": False,
            "tick_count": 0,
            "error": "",
        }

        ticker = None
        try:
            qualified = ib.qualifyContracts(Stock(symbol, args.exchange, args.currency))
            if not qualified:
                row["error"] = "qualify_failed"
                writer.submit("probe", row)
                log.warning("Probe %s: qualify_failed", symbol)
                continue

            row["qualified"] = True
            contract = qualified[0]
            ticker = ib.reqMktData(contract, genericTickList=DEFAULT_GENERIC_TICKS, snapshot=False)

            deadline = time.monotonic() + args.probe_wait_seconds
            while time.monotonic() < deadline:
                ib.waitOnUpdate(timeout=min(0.25, deadline - time.monotonic()))

            row["bid"] = safe_number(ticker.bid)
            row["ask"] = safe_number(ticker.ask)
            row["last"] = safe_number(ticker.last)
            row["tick_count"] = len(ticker.ticks)

            nav_values = latest_nav_ticks(ticker)
            row.update(nav_values)
            row["received_nav"] = any(value is not None for value in nav_values.values())
        except Exception as exc:
            row["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if ticker is not None:
                ib.cancelMktData(ticker.contract)

        writer.submit("probe", row)
        log.info(
            "Probe %s: qualified=%s nav_last=%s nav_high=%s nav_low=%s received_nav=%s error=%s",
            symbol,
            row["qualified"],
            row["ib_nav_last"],
            row["ib_nav_high"],
            row["ib_nav_low"],
            row["received_nav"],
            row["error"],
        )


# ===========================================================================
# Logging configuration
# ===========================================================================
def setup_logging(level_name: str, log_file: Path | None) -> None:
    """Configure root logging with console + optional file handler."""
    level = getattr(logging, level_name.upper(), logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    root = logging.getLogger()
    root.setLevel(level)
    # Drop pre-existing handlers (e.g. when this is called twice).
    for handler in list(root.handlers):
        root.removeHandler(handler)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    # ib_insync logs every tick at INFO; keep it quieter.
    logging.getLogger("ib_insync").setLevel(logging.WARNING)


# ===========================================================================
# Background poller for the official ICE iNAV
# ===========================================================================

class IceInavPoller:
    """Polls the ICE iNAV endpoint at a fixed interval in a background thread.

    Each poll (success or failure) writes one row to the ``official`` CSV via
    the supplied writer and queues the same row for downstream consumption
    (used by the main loop to build sync snapshots together with the latest
    IB market state).
    """

    def __init__(
        self,
        symbol: str,
        language: str,
        interval: float,
        writer: CsvWriteWorker,
        request_timeout: float = 5.0,
    ):
        self.symbol = symbol
        self.language = language
        self.interval = max(1.0, interval)
        self.writer = writer
        self.request_timeout = request_timeout
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="ice-poller", daemon=True
        )
        self._pending: queue.Queue[dict[str, str]] = queue.Queue()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.request_timeout + 1.0))

    def drain(self) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        while True:
            try:
                rows.append(self._pending.get_nowait())
            except queue.Empty:
                break
        return rows

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                payload = fetch_official_inav(
                    self.symbol, self.language, timeout=self.request_timeout
                )
                row = flatten_official_inav(self.symbol, payload)
            except Exception as exc:
                row = official_inav_error_row(
                    self.symbol, f"{type(exc).__name__}: {exc}"
                )
                log.warning("Official iNAV fetch failed: %s", exc)
            self.writer.submit("official", row)
            self._pending.put(row)
            elapsed = time.monotonic() - started
            self._stop.wait(timeout=max(0.0, self.interval - elapsed))


# ===========================================================================
# Subscription state holders
# ===========================================================================

@dataclass
class L1Subscription:
    ticker: Ticker
    market_state: dict[str, Any]
    processed_count: int = 0


@dataclass
class DepthSubscription:
    ticker: Ticker
    contract: Any
    is_smart: bool
    started_at: float
    active: bool = True
    warned_no_data: bool = False
    last_update_at: str | None = None
    processed_count: int = 0
    error_handler: Callable[..., None] | None = None


# ===========================================================================
# Subscription helpers
# ===========================================================================

def subscribe_depth(
    ib: IB,
    primary_contract: Any,
    args: argparse.Namespace,
    writer: CsvWriteWorker,
) -> DepthSubscription | None:
    """Request market depth and return a DepthSubscription, or None on failure.

    The returned subscription owns an IB error-event handler that will mark
    depth inactive (and cancel the subscription) when IB reports a known
    depth-related error code for the same contract.
    """
    rows = max(1, min(args.depth_rows, 5))
    try:
        depth_ticker = ib.reqMktDepth(
            primary_contract,
            numRows=rows,
            isSmartDepth=args.depth_smart,
        )
    except Exception as exc:
        writer.submit(
            "depth",
            depth_status_row(
                args.symbol,
                "error",
                f"Depth subscription failed: {type(exc).__name__}: {exc}",
            ),
        )
        log.warning("Depth disabled: %s", exc)
        return None

    sub = DepthSubscription(
        ticker=depth_ticker,
        contract=primary_contract,
        is_smart=args.depth_smart,
        started_at=time.monotonic(),
    )
    writer.submit(
        "depth",
        depth_status_row(
            args.symbol,
            "subscribed",
            f"Depth subscription started with rows={rows}, smart={args.depth_smart}",
        ),
    )

    def on_ib_error(reqId: int, errorCode: int, errorString: str, contract: Any) -> None:
        if not sub.active:
            return
        if errorCode not in DEPTH_RELATED_ERROR_CODES:
            return
        if contract is None:
            return
        primary_conid = getattr(primary_contract, "conId", 0)
        contract_conid = getattr(contract, "conId", 0)
        if primary_conid and contract_conid:
            same_contract = contract_conid == primary_conid
        else:
            same_contract = (
                getattr(contract, "symbol", None)
                == getattr(primary_contract, "symbol", None)
            )
        if not same_contract:
            return
        sub.active = False
        message = f"IB rejected depth request: {errorString}"
        writer.submit(
            "depth",
            depth_status_row(args.symbol, "error", message, str(errorCode)),
        )
        log.warning("Depth disabled: %s (code=%s)", message, errorCode)
        try:
            ib.cancelMktDepth(primary_contract, isSmartDepth=args.depth_smart)
        except Exception:
            pass

    ib.errorEvent += on_ib_error
    sub.error_handler = on_ib_error
    return sub


def _consume_l1_ticks(
    l1: L1Subscription,
    writer: CsvWriteWorker,
    discount_threshold: float,
) -> None:
    """Forward any new L1 ticks since the last call to the writer."""
    current = len(l1.ticker.ticks)
    if current < l1.processed_count:
        # ib_insync truncated the buffer; reset cursor to avoid skipping data.
        log.debug(
            "L1 ticks truncated (%d -> %d); resetting cursor",
            l1.processed_count, current,
        )
        l1.processed_count = 0
    if current <= l1.processed_count:
        return
    new_ticks = l1.ticker.ticks[l1.processed_count:current]
    for tick in new_ticks:
        row = update_market_state(l1.market_state, l1.ticker, tick)
        if row is None:
            continue
        writer.submit("market", row)
        ask_disc = row["ask_ib_nav_discount_bps"]
        if ask_disc is not None and ask_disc <= discount_threshold:
            log.warning(
                "Discount alert: ask=%s ib_nav_last=%s ask_discount_bps=%.2f",
                row["ask"], row["ib_nav_last"], ask_disc,
            )
    l1.processed_count = current


def _consume_depth_ticks(
    depth: DepthSubscription,
    writer: CsvWriteWorker,
    symbol: str,
    startup_timeout: float,
) -> None:
    """Forward new depth ticks (or a one-shot warning if no data arrives)."""
    if not depth.active:
        return
    current = len(depth.ticker.domTicks)
    if current < depth.processed_count:
        log.debug(
            "Depth domTicks truncated (%d -> %d); resetting cursor",
            depth.processed_count, current,
        )
        depth.processed_count = 0
    if current > depth.processed_count:
        new_ticks = depth.ticker.domTicks[depth.processed_count:current]
        for depth_tick in new_ticks:
            depth.last_update_at = isoformat_timestamp(getattr(depth_tick, "time", None))
            writer.submit("depth", build_depth_row(symbol, depth.ticker, depth_tick))
        depth.processed_count = current
        return
    if (
        not depth.warned_no_data
        and current == 0
        and time.monotonic() - depth.started_at >= startup_timeout
    ):
        depth.warned_no_data = True
        writer.submit(
            "depth",
            depth_status_row(
                symbol,
                "warning",
                "No market depth updates received within startup timeout; "
                "depth may be unavailable, closed, or not entitled.",
            ),
        )
        log.warning("Depth: no updates within startup timeout (%.1fs)", startup_timeout)


def _emit_sync_snapshots(
    poller: IceInavPoller,
    writer: CsvWriteWorker,
    symbol: str,
    l1: L1Subscription,
    depth: DepthSubscription | None,
) -> None:
    """Pair each new official iNAV row with current IB state and write a sync row."""
    for official_row in poller.drain():
        sync_row = build_sync_snapshot_row(
            symbol,
            official_row,
            l1.market_state,
            depth.ticker if depth is not None else None,
            depth.active if depth is not None else False,
            depth.last_update_at if depth is not None else None,
        )
        writer.submit("sync", sync_row)
        log_official_snapshot(official_row)


# ===========================================================================
# Session lifecycle
# ===========================================================================

def run_session(ib: IB, args: argparse.Namespace, writer: CsvWriteWorker) -> None:
    """Subscribe and pump market data until disconnect or KeyboardInterrupt.

    Returns normally on KeyboardInterrupt. Raises ConnectionError if the IB
    connection is lost mid-session so the outer loop can reconnect.
    """
    primary_contract = qualify_primary_contract(ib, args)

    l1_ticker = ib.reqMktData(
        primary_contract,
        genericTickList=DEFAULT_GENERIC_TICKS,
        snapshot=False,
    )
    l1 = L1Subscription(
        ticker=l1_ticker,
        market_state=blank_market_state(args.symbol),
    )

    depth: DepthSubscription | None = None
    if args.with_depth:
        depth = subscribe_depth(ib, primary_contract, args, writer)

    poller = IceInavPoller(
        symbol=args.symbol,
        language=args.official_inav_language,
        interval=args.official_inav_interval,
        writer=writer,
        request_timeout=args.official_inav_timeout,
    )
    poller.start()

    log.info("Subscribed to %s on %s (%s)", args.symbol, args.exchange, args.currency)
    log.info(
        "Recording official ICE iNAV every %.1fs (timeout=%.1fs)",
        args.official_inav_interval, args.official_inav_timeout,
    )
    if args.with_depth:
        log.info(
            "Market depth subscription requested (rows=%d smart=%s)",
            max(1, min(args.depth_rows, 5)), args.depth_smart,
        )
    log.info("Press Ctrl+C to stop.")

    discount_threshold = -abs(args.discount_threshold_bps)

    try:
        while True:
            if not ib.isConnected():
                raise ConnectionError("IB Gateway disconnected")
            ib.waitOnUpdate(timeout=args.ib_wait_seconds)
            _consume_l1_ticks(l1, writer, discount_threshold)
            if depth is not None:
                _consume_depth_ticks(
                    depth, writer, args.symbol, args.depth_startup_timeout
                )
            _emit_sync_snapshots(poller, writer, args.symbol, l1, depth)
    finally:
        poller.stop()
        if depth is not None and depth.error_handler is not None:
            try:
                ib.errorEvent -= depth.error_handler
            except Exception:
                pass
        if depth is not None:
            try:
                ib.cancelMktDepth(depth.contract, isSmartDepth=depth.is_smart)
            except Exception:
                pass
        try:
            ib.cancelMktData(l1_ticker.contract)
        except Exception:
            pass


def run_forever(args: argparse.Namespace, writer: CsvWriteWorker) -> None:
    """Outer loop that handles connection lifecycle and reconnects on failure."""
    attempt = 0
    while True:
        ib = IB()
        try:
            log.info(
                "Connecting to IB at %s:%s (clientId=%d)...",
                args.host, args.port, args.client_id,
            )
            ib.connect(args.host, args.port, clientId=args.client_id)
            attempt = 0  # success resets backoff
            log.info("IB connected.")
            if not args.skip_probe:
                log.info("Running IB NAV probe for the configured ETF list...")
                probe_ib_nav_capabilities(ib, args, writer)
            run_session(ib, args, writer)
            return  # session ended cleanly
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            return
        except Exception as exc:
            if args.no_reconnect:
                log.error("Session failed: %s. --no-reconnect set; exiting.", exc)
                return
            delay = compute_backoff(
                attempt, base=args.reconnect_base, cap=args.reconnect_cap
            )
            attempt += 1
            log.warning(
                "Session failed: %s. Reconnecting in %.1fs (attempt %d).",
                exc, delay, attempt,
            )
            try:
                time.sleep(delay)
            except KeyboardInterrupt:
                log.info("Stopped by user during backoff.")
                return
        finally:
            try:
                if ib.isConnected():
                    ib.disconnect()
            except Exception:
                pass


# ===========================================================================
# Entry point
# ===========================================================================

def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    log_file = args.log_file if args.log_file is not None else args.output_dir / "etf_monitor.log"
    setup_logging(args.log_level, log_file)
    log.info(
        "Starting ETF monitor for symbol=%s output_dir=%s log_file=%s",
        args.symbol, args.output_dir.resolve(), log_file.resolve(),
    )

    market_output_path = args.output_dir / f"{args.symbol}_ib_ticks.csv"
    probe_output_path = args.output_dir / "ib_nav_probe.csv"
    official_inav_output_path = args.output_dir / f"{args.symbol}_official_inav.csv"
    sync_snapshot_output_path = args.output_dir / f"{args.symbol}_sync_snapshot.csv"
    depth_output_path = args.output_dir / f"{args.symbol}_ib_depth.csv"

    file_specs: dict[str, tuple[Path, list[str]]] = {
        "market": (market_output_path, MARKET_FIELDS),
        "official": (official_inav_output_path, OFFICIAL_INAV_FIELDS),
        "sync": (sync_snapshot_output_path, SYNC_SNAPSHOT_FIELDS),
    }
    if not args.skip_probe:
        file_specs["probe"] = (probe_output_path, PROBE_FIELDS)
    if args.with_depth:
        file_specs["depth"] = (depth_output_path, DEPTH_FIELDS)

    writer = CsvWriteWorker(
        file_specs=file_specs,
        max_queue_size=args.writer_queue_size,
        batch_size=args.writer_batch_size,
        flush_interval=args.writer_flush_interval,
    )
    writer.start()
    try:
        run_forever(args, writer)
    finally:
        writer.close()
        log.info("ETF monitor stopped.")


if __name__ == "__main__":
    main()