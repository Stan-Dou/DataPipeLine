from __future__ import annotations

import argparse
import csv
import json
import math
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ib_insync import IB, Stock, Ticker


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

OFFICIAL_INAV_API_URL = "https://inav.ice.com/api/1/csop/application/index/quote"

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
    if price is None or nav_value is None or nav_value == 0:
        return None
    return (price - nav_value) / nav_value * 10000


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


def isoformat_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def age_ms(reference_time: datetime, update_time_text: str | None) -> int | None:
    update_time = parse_iso_datetime(update_time_text)
    if not update_time:
        return None
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
        "received_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
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
        "received_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
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
        "event_time": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        "received_at": datetime.now().astimezone().isoformat(timespec="milliseconds"),
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
    captured_dt = parse_iso_datetime(captured_at) or datetime.now().astimezone()
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


def fetch_official_inav(symbol: str, language: str) -> dict[str, Any]:
    request = Request(
        build_official_inav_url(symbol, language),
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )
    with urlopen(request, timeout=20) as response:
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
        "captured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
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
    row["captured_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
    row["symbol"] = symbol
    row["error"] = error
    return row


def print_official_snapshot(row: dict[str, str]) -> None:
    if row["error"]:
        print(f"{row['captured_at']} | symbol={row['symbol']} | official_inav_error={row['error']}")
        return

    print(
        " | ".join(
            [
                row["captured_at"],
                f"symbol={row['symbol']}",
                f"inav_hkd={row['inav_hkd']}",
                f"market_price_hkd={row['market_price_hkd']}",
                f"discount_hkd={row['premium_discount_hkd_pct']}",
            ]
        )
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
            "captured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
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
                print(f"{symbol}: qualify_failed")
                continue

            row["qualified"] = True
            contract = qualified[0]
            ticker = ib.reqMktData(contract, genericTickList="577,614,623", snapshot=False)

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
        print(
            f"{symbol}: qualified={row['qualified']} nav_last={row['ib_nav_last']} "
            f"nav_high={row['ib_nav_high']} nav_low={row['ib_nav_low']} received_nav={row['received_nav']} error={row['error']}"
        )


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

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

    ib = IB()
    ib.connect(args.host, args.port, clientId=args.client_id)

    l1_ticker: Ticker | None = None
    depth_ticker: Ticker | None = None
    depth_active = False
    depth_error_handler = None
    primary_contract = None
    try:
        if not args.skip_probe:
            print("Running IB NAV probe for the configured ETF list...")
            probe_ib_nav_capabilities(ib, args, writer)
            print(f"Saved probe results to {probe_output_path.resolve()}")

        primary_contract = qualify_primary_contract(ib, args)
        l1_ticker = ib.reqMktData(
            primary_contract,
            genericTickList="577,614,623",
            snapshot=False,
        )

        depth_req_id = None
        if args.with_depth:
            try:
                depth_ticker = ib.reqMktDepth(
                    primary_contract,
                    numRows=max(1, min(args.depth_rows, 5)),
                    isSmartDepth=args.depth_smart,
                )
                depth_req_id = ib.wrapper.ticker2ReqId["mktDepth"].get(depth_ticker)
                depth_active = True
                writer.submit(
                    "depth",
                    depth_status_row(
                        args.symbol,
                        "subscribed",
                        f"Depth subscription started with rows={max(1, min(args.depth_rows, 5))}, smart={args.depth_smart}",
                    ),
                )

                def on_ib_error(reqId: int, errorCode: int, errorString: str, contract: Any) -> None:
                    nonlocal depth_active
                    if not depth_active or reqId != depth_req_id:
                        return
                    depth_active = False
                    message = f"IB rejected depth request: {errorString}"
                    writer.submit("depth", depth_status_row(args.symbol, "error", message, str(errorCode)))
                    print(f"Depth disabled: {message} (code={errorCode})")
                    try:
                        ib.cancelMktDepth(primary_contract, isSmartDepth=args.depth_smart)
                    except Exception:
                        pass

                depth_error_handler = on_ib_error
                ib.errorEvent += depth_error_handler
            except Exception as exc:
                depth_active = False
                writer.submit(
                    "depth",
                    depth_status_row(
                        args.symbol,
                        "error",
                        f"Depth subscription failed: {type(exc).__name__}: {exc}",
                    ),
                )
                print(f"Depth disabled: {type(exc).__name__}: {exc}")

        print(f"Recording IB tick-level top-of-book updates to {market_output_path.resolve()}")
        print(f"Recording official ICE iNAV snapshots to {official_inav_output_path.resolve()}")
        print(f"Recording synchronized snapshots to {sync_snapshot_output_path.resolve()}")
        if args.with_depth:
            print(f"Recording IB market depth updates to {depth_output_path.resolve()}")
        print("Press Ctrl+C to stop.")

        market_state = blank_market_state(args.symbol)
        processed_tick_count = 0
        processed_dom_tick_count = 0
        last_official_inav_poll = 0.0
        discount_threshold = -abs(args.discount_threshold_bps)
        depth_warned_no_data = False
        depth_started_at = time.monotonic()
        l2_last_update_at: str | None = None

        while True:
            ib.waitOnUpdate(timeout=args.ib_wait_seconds)

            if l1_ticker is not None:
                current_tick_count = len(l1_ticker.ticks)
                if current_tick_count > processed_tick_count:
                    new_ticks = l1_ticker.ticks[processed_tick_count:current_tick_count]
                    for tick in new_ticks:
                        row = update_market_state(market_state, l1_ticker, tick)
                        if row is None:
                            continue

                        writer.submit("market", row)
                        ask_discount = row["ask_ib_nav_discount_bps"]
                        if ask_discount is not None and ask_discount <= discount_threshold:
                            print(
                                f"{row['received_at']} ask={row['ask']} ib_nav_last={row['ib_nav_last']} "
                                f"ask_discount_bps={ask_discount:.2f}"
                            )

                    processed_tick_count = current_tick_count

            if args.with_depth and depth_active and depth_ticker is not None:
                current_dom_tick_count = len(depth_ticker.domTicks)
                if current_dom_tick_count > processed_dom_tick_count:
                    new_dom_ticks = depth_ticker.domTicks[processed_dom_tick_count:current_dom_tick_count]
                    for depth_tick in new_dom_ticks:
                        l2_last_update_at = isoformat_timestamp(getattr(depth_tick, "time", None))
                        writer.submit("depth", build_depth_row(args.symbol, depth_ticker, depth_tick))
                    processed_dom_tick_count = current_dom_tick_count
                elif (
                    not depth_warned_no_data
                    and current_dom_tick_count == 0
                    and time.monotonic() - depth_started_at >= args.depth_startup_timeout
                ):
                    depth_warned_no_data = True
                    writer.submit(
                        "depth",
                        depth_status_row(
                            args.symbol,
                            "warning",
                            "No market depth updates received within startup timeout; depth may be unavailable, closed, or not entitled.",
                        ),
                    )
                    print("Depth warning: no market depth updates received within startup timeout.")

            now = time.monotonic()
            if now - last_official_inav_poll >= args.official_inav_interval:
                try:
                    payload = fetch_official_inav(args.symbol, args.official_inav_language)
                    official_row = flatten_official_inav(args.symbol, payload)
                except Exception as exc:
                    official_row = official_inav_error_row(args.symbol, f"{type(exc).__name__}: {exc}")

                writer.submit("official", official_row)
                writer.submit(
                    "sync",
                    build_sync_snapshot_row(
                        args.symbol,
                        official_row,
                        market_state,
                        depth_ticker,
                        depth_active,
                        l2_last_update_at,
                    ),
                )
                print_official_snapshot(official_row)
                last_official_inav_poll = now
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        if depth_error_handler is not None:
            ib.errorEvent -= depth_error_handler
        if args.with_depth and primary_contract is not None:
            try:
                ib.cancelMktDepth(primary_contract, isSmartDepth=args.depth_smart)
            except Exception:
                pass
        if l1_ticker is not None:
            ib.cancelMktData(l1_ticker.contract)
        ib.disconnect()
        writer.close()


if __name__ == "__main__":
    main()