"""Find Japanese PBR < 1 candidates using yfinance (no registration required).

Strategy:
  1. Build a universe list (CSV with column ``code``):
     - --universe-csv <path>   : user-provided
     - else auto-download JPX official listing (Excel)
     - else fall back to a small embedded TOPIX-100-ish list (smoke test)
  2. For each code, query yfinance ``Ticker(f"{code}.T").info`` and cache it.
  3. Filter by ``priceToBook < --max-pbr`` and write a CSV.

Trade-offs vs. the J-Quants version:
  - No registration, no IP issues for the data fetch itself.
  - yfinance hits Yahoo Finance: rate-limited and occasionally flaky.
  - ``priceToBook`` is Yahoo's own calc; usable but not as authoritative as
    JPX. Treat results as a *first-pass screen*.
  - Field coverage is shallower than J-Quants (no 退職給付 / のれん etc).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import pandas as pd
import yfinance as yf

log = logging.getLogger("j_pbr_yf")

# JPX official listed-stock master file (xls). May be IP-restricted in some
# regions; the script silently falls back to an embedded list if this fails.
JPX_LISTING_URL = (
    "https://www.jpx.co.jp/markets/statistics-equities/misc/"
    "tvdivq0000001vg2-att/data_j.xls"
)

# yfinance ``info`` keys we read. yfinance picks these up from Yahoo Finance.
INFO_FIELDS = (
    "priceToBook",
    "marketCap",
    "regularMarketPrice",
    "bookValue",
    "trailingPE",
    "forwardPE",
    "returnOnEquity",
    "trailingEps",
    "totalRevenue",
    "operatingMargins",
    "profitMargins",
    "totalCash",
    "totalDebt",
    "dividendYield",
    "sector",
    "industry",
    "currency",
    "shortName",
    "longName",
)

OUTPUT_FIELDS = [
    "code",
    "ticker",
    "name",
    "name_jp",
    "market",
    "sector33",
    "sector17",
    "yf_sector",
    "yf_industry",
    "currency",
    "price",
    "book_value_per_share",
    "pbr",
    "market_cap",
    "trailing_pe",
    "forward_pe",
    "roe",
    "trailing_eps",
    "total_revenue",
    "operating_margins",
    "profit_margins",
    "total_cash",
    "total_debt",
    "dividend_yield",
    "fetched_at",
]


# ---------------------------------------------------------------------------
# Universe sourcing
# ---------------------------------------------------------------------------

# Last-resort smoke-test universe (TOPIX-100-ish; well-known names).
# A handful of these usually trade with PBR < 1 (banks, trading houses, autos).
EMBEDDED_FALLBACK_CODES = [
    # Banks / financials (often PBR < 1)
    "8306", "8316", "8411", "8053", "8002", "8001", "8031", "8058", "8593",
    "8766", "8725", "8473", "8630", "8697", "7182",
    # Autos
    "7203", "7267", "7269", "7270", "7201", "7272", "7261",
    # Steel / chemicals / materials
    "5401", "5411", "3401", "3402", "4063", "4188", "4005", "5108",
    # Trading / shipping / utilities
    "9101", "9104", "9107", "9501", "9502", "9503", "9531", "9532",
    # Tech / electronics
    "6758", "6502", "6701", "6752", "6724", "6098", "6857", "6981", "6594",
    # Telecoms / services
    "9432", "9433", "9434", "9984", "4661", "4519",
    # Pharma / consumer
    "4502", "4503", "4543", "4452", "4901", "4911", "2502", "2503", "2914",
    # Retail / restaurants / construction
    "9983", "8267", "3382", "3086", "1925", "1928", "1812", "1801",
]


def _read_jpx_excel(content: bytes) -> pd.DataFrame:
    """Read the JPX ``data_j.xls/.xlsx`` file from raw bytes."""
    import io
    bio = io.BytesIO(content)
    last_exc: Exception | None = None
    for engine in ("openpyxl", "xlrd"):
        try:
            return pd.read_excel(bio, engine=engine)
        except Exception as exc:
            last_exc = exc
            bio.seek(0)
    raise RuntimeError(f"Could not read JPX excel with any engine: {last_exc}")


def _normalize_jpx_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize JPX listing columns to a stable English-ish schema."""
    rename: dict[str, str] = {}
    for raw in df.columns:
        c = str(raw).strip()
        if c in {"コード", "Code", "Local Code"}:
            rename[raw] = "code"
        elif c in {"銘柄名", "Name (English)", "Name"}:
            rename[raw] = "name"
        elif "市場" in c or "Market" in c:
            rename[raw] = "market"
        elif "33業種区分" in c or ("Sector33" in c and "Code" not in c):
            rename[raw] = "sector33"
        elif "17業種区分" in c or ("Sector17" in c and "Code" not in c):
            rename[raw] = "sector17"
        elif "規模区分" in c or "Scale" in c:
            rename[raw] = "scale"
    df = df.rename(columns=rename)
    if "code" not in df.columns:
        raise RuntimeError(
            f"JPX listing has unexpected columns; could not find code column. Got: {list(df.columns)}"
        )
    df["code"] = df["code"].astype(str).str.strip().str.zfill(4)
    if "market" in df.columns:
        df = df[
            ~df["market"]
            .astype(str)
            .str.contains(r"ETF|ETN|REIT|PRO|出資証券|優先株", regex=True, na=False)
        ]
    return df.reset_index(drop=True)


def download_jpx_universe(cache_path: Path, force: bool = False) -> pd.DataFrame:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if not force and cache_path.exists():
        log.info("Using cached JPX listing: %s", cache_path)
        return pd.read_csv(cache_path, dtype={"code": str})
    log.info("Downloading JPX listing from %s ...", JPX_LISTING_URL)
    req = Request(JPX_LISTING_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as resp:
        content = resp.read()
    df = _normalize_jpx_columns(_read_jpx_excel(content))
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    log.info("Saved JPX listing -> %s (%d rows)", cache_path, len(df))
    return df


def embedded_universe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "code": EMBEDDED_FALLBACK_CODES,
            "name": ["" for _ in EMBEDDED_FALLBACK_CODES],
            "market": ["" for _ in EMBEDDED_FALLBACK_CODES],
            "sector33": ["" for _ in EMBEDDED_FALLBACK_CODES],
            "sector17": ["" for _ in EMBEDDED_FALLBACK_CODES],
        }
    )


def load_universe(custom_csv: Path | None, cache_path: Path) -> pd.DataFrame:
    if custom_csv is not None:
        log.info("Loading user universe CSV: %s", custom_csv)
        df = pd.read_csv(custom_csv, dtype={"code": str})
        if "code" not in df.columns:
            raise RuntimeError("Custom universe CSV must have a 'code' column.")
        df["code"] = df["code"].astype(str).str.strip().str.zfill(4)
        for col in ("name", "market", "sector33", "sector17"):
            if col not in df.columns:
                df[col] = ""
        return df.reset_index(drop=True)
    try:
        return download_jpx_universe(cache_path)
    except Exception as exc:
        log.warning(
            "JPX listing unavailable (%s). Falling back to embedded TOPIX-100-ish "
            "list (%d names). Provide --universe-csv for the full universe.",
            exc, len(EMBEDDED_FALLBACK_CODES),
        )
        return embedded_universe()


# ---------------------------------------------------------------------------
# yfinance fetching
# ---------------------------------------------------------------------------

def fetch_ticker_info(code: str, max_attempts: int = 2) -> dict[str, Any] | None:
    """Pull the yfinance ``info`` dict for a single Japanese stock code."""
    ticker_str = f"{code}.T"
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            t = yf.Ticker(ticker_str)
            info = t.info or {}
            if info.get("priceToBook") is None and info.get("regularMarketPrice") is None:
                return None  # delisted / unknown / empty
            return {k: info.get(k) for k in INFO_FIELDS}
        except Exception as exc:
            last_exc = exc
            time.sleep(0.5 * (attempt + 1))
    if last_exc is not None:
        log.debug("yfinance failed for %s: %s", ticker_str, last_exc)
    return None


def fetch_all_with_cache(
    codes: list[str],
    cache_dir: Path,
    max_workers: int,
    refresh: bool,
) -> dict[str, dict[str, Any]]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached: dict[str, dict[str, Any]] = {}
    pending: list[str] = []
    for code in codes:
        cache_file = cache_dir / f"{code}.json"
        if not refresh and cache_file.exists():
            try:
                cached[code] = json.loads(cache_file.read_text(encoding="utf-8"))
                continue
            except Exception:
                pass
        pending.append(code)
    log.info("Cache hit: %d, to fetch: %d", len(cached), len(pending))

    if not pending:
        return cached

    completed = 0
    failures = 0
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futs = {exe.submit(fetch_ticker_info, c): c for c in pending}
        for fut in as_completed(futs):
            code = futs[fut]
            completed += 1
            try:
                info = fut.result()
            except Exception as exc:
                log.debug("worker exception for %s: %s", code, exc)
                info = None
            if info is None:
                failures += 1
            else:
                cached[code] = info
                (cache_dir / f"{code}.json").write_text(
                    json.dumps(info, ensure_ascii=False), encoding="utf-8"
                )
            if completed % 50 == 0 or completed == len(pending):
                log.info(
                    "Fetched %d/%d (failures so far: %d)",
                    completed, len(pending), failures,
                )
    log.info("Done. Total cached entries: %d (failed: %d)", len(cached), failures)
    return cached


# ---------------------------------------------------------------------------
# Row assembly + filtering
# ---------------------------------------------------------------------------

def build_row(code: str, uni_row: pd.Series, info: dict[str, Any]) -> dict[str, Any] | None:
    pbr = info.get("priceToBook")
    if pbr is None:
        return None
    try:
        pbr = float(pbr)
    except (TypeError, ValueError):
        return None
    if pbr <= 0:
        return None
    return {
        "code": code,
        "ticker": f"{code}.T",
        "name": info.get("longName") or info.get("shortName") or uni_row.get("name", ""),
        "name_jp": uni_row.get("name", ""),
        "market": uni_row.get("market", ""),
        "sector33": uni_row.get("sector33", ""),
        "sector17": uni_row.get("sector17", ""),
        "yf_sector": info.get("sector"),
        "yf_industry": info.get("industry"),
        "currency": info.get("currency"),
        "price": info.get("regularMarketPrice"),
        "book_value_per_share": info.get("bookValue"),
        "pbr": pbr,
        "market_cap": info.get("marketCap"),
        "trailing_pe": info.get("trailingPE"),
        "forward_pe": info.get("forwardPE"),
        "roe": info.get("returnOnEquity"),
        "trailing_eps": info.get("trailingEps"),
        "total_revenue": info.get("totalRevenue"),
        "operating_margins": info.get("operatingMargins"),
        "profit_margins": info.get("profitMargins"),
        "total_cash": info.get("totalCash"),
        "total_debt": info.get("totalDebt"),
        "dividend_yield": info.get("dividendYield"),
        "fetched_at": date.today().isoformat(),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find Japanese PBR < threshold candidates via yfinance."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "data",
        help="Directory for outputs and caches.",
    )
    parser.add_argument(
        "--max-pbr",
        type=float,
        default=1.0,
        help="Upper bound on PBR (exclusive). Default 1.0",
    )
    parser.add_argument(
        "--min-market-cap",
        type=float,
        default=0.0,
        help="Minimum market cap (JPY). Default 0.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If > 0, only process the first N codes (smoke test).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent yfinance fetchers. Yahoo rate-limits aggressively above ~5.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore on-disk cache and re-fetch.",
    )
    parser.add_argument(
        "--universe-csv",
        type=Path,
        default=None,
        help="Optional user-provided universe CSV (must have a 'code' column).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # yfinance is chatty at INFO; quiet it.
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    universe_cache = output_dir / "universe_jp.csv"
    info_cache_dir = output_dir / "cache_info"

    universe = load_universe(args.universe_csv, universe_cache)
    log.info("Universe size: %d", len(universe))
    if args.limit > 0:
        universe = universe.head(args.limit).reset_index(drop=True)
        log.info("Truncated to first %d codes for smoke test.", len(universe))

    codes: list[str] = universe["code"].tolist()
    info_by_code = fetch_all_with_cache(
        codes, info_cache_dir, args.workers, args.refresh
    )

    rows: list[dict[str, Any]] = []
    skipped_no_info = 0
    skipped_no_pbr = 0
    skipped_above_pbr = 0
    skipped_min_cap = 0
    universe_lookup = {row["code"]: row for _, row in universe.iterrows()}
    for code in codes:
        info = info_by_code.get(code)
        if info is None:
            skipped_no_info += 1
            continue
        uni_row = universe_lookup.get(code, pd.Series(dtype=object))
        row = build_row(code, uni_row, info)
        if row is None:
            skipped_no_pbr += 1
            continue
        if row["pbr"] >= args.max_pbr:
            skipped_above_pbr += 1
            continue
        mc = row["market_cap"]
        if mc is not None and mc < args.min_market_cap:
            skipped_min_cap += 1
            continue
        rows.append(row)

    rows.sort(key=lambda r: r["pbr"])
    log.info(
        "Kept %d rows. Skipped: no_info=%d no_pbr=%d above_pbr=%d below_min_cap=%d",
        len(rows), skipped_no_info, skipped_no_pbr, skipped_above_pbr, skipped_min_cap,
    )

    out_path = output_dir / f"layer0_pbr_under_{args.max_pbr}__{date.today().isoformat()}.csv"
    if rows:
        write_csv(out_path, rows)
        log.info("Wrote %d rows -> %s", len(rows), out_path.resolve())
        log.info("Top 10 by lowest PBR:")
        for r in rows[:10]:
            log.info(
                "  %s %s pbr=%.3f price=%s mc=%s sector=%s",
                r["code"], r["name"][:40], r["pbr"],
                r["price"], r["market_cap"], r["yf_sector"],
            )
    else:
        log.warning("No rows passed the filter; CSV not written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
