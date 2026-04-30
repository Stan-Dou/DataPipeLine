"""Find Japanese listed companies with PBR < 1 using the J-Quants free API.

Pipeline:
  1. Authenticate with J-Quants (refresh_token or mail+password).
  2. Pull /listed/info to get the full universe + sector / market metadata.
  3. Walk back ``--lookback-days`` days, calling /fins/statements?date=YYYYMMDD
     and keeping the *latest* statement per stock code.
  4. Find the latest trading day with /markets/daily_quotes data.
  5. Compute PBR = Close / BookValuePerShare and filter rows.
  6. Write a CSV sorted ascending by PBR.

Free-tier note: J-Quants Free delivers data with ~12 week delay. By default we
use ``today - 90 days`` as the reference date which is safely inside that
window. Pass ``--end-date`` to override.

Credentials are read from environment variables (or a .env file at project root):
    JQUANTS_REFRESH_TOKEN  (preferred; long-lived)
    JQUANTS_MAIL + JQUANTS_PASSWORD (used to obtain refresh_token if missing)
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

JQUANTS_BASE = "https://api.jquants.com/v1"

log = logging.getLogger("j_pbr")


# ---------------------------------------------------------------------------
# Credentials & client
# ---------------------------------------------------------------------------

@dataclass
class JQuantsCreds:
    mail: str | None = None
    password: str | None = None
    refresh_token: str | None = None

    @classmethod
    def from_env(cls) -> "JQuantsCreds":
        return cls(
            mail=os.getenv("JQUANTS_MAIL"),
            password=os.getenv("JQUANTS_PASSWORD"),
            refresh_token=os.getenv("JQUANTS_REFRESH_TOKEN"),
        )


def load_dotenv(path: Path) -> None:
    """Tiny .env loader. Avoids adding python-dotenv as a dependency."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


class JQuantsClient:
    def __init__(self, creds: JQuantsCreds, request_timeout: float = 30.0):
        self.creds = creds
        self.request_timeout = request_timeout
        self.session = requests.Session()
        self._id_token: str | None = None
        self._refresh_token: str | None = creds.refresh_token

    def _ensure_refresh_token(self) -> str:
        if self._refresh_token:
            return self._refresh_token
        if not (self.creds.mail and self.creds.password):
            raise RuntimeError(
                "JQUANTS_REFRESH_TOKEN not set and JQUANTS_MAIL/JQUANTS_PASSWORD "
                "not provided to obtain one."
            )
        log.info("Requesting refresh token via mail/password...")
        r = self.session.post(
            f"{JQUANTS_BASE}/token/auth_user",
            json={
                "mailaddress": self.creds.mail,
                "password": self.creds.password,
            },
            timeout=self.request_timeout,
        )
        r.raise_for_status()
        self._refresh_token = r.json()["refreshToken"]
        return self._refresh_token

    def _ensure_id_token(self) -> str:
        if self._id_token:
            return self._id_token
        refresh = self._ensure_refresh_token()
        log.info("Exchanging refresh token for id token...")
        r = self.session.post(
            f"{JQUANTS_BASE}/token/auth_refresh",
            params={"refreshtoken": refresh},
            timeout=self.request_timeout,
        )
        r.raise_for_status()
        self._id_token = r.json()["idToken"]
        return self._id_token

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{JQUANTS_BASE}{path}"
        for attempt in range(4):
            token = self._ensure_id_token()
            r = self.session.get(
                url,
                params=params,
                headers={"Authorization": f"Bearer {token}"},
                timeout=self.request_timeout,
            )
            if r.status_code == 401 and attempt < 2:
                log.info("idToken rejected; refreshing.")
                self._id_token = None
                continue
            if r.status_code == 429:
                wait = 2 ** attempt
                log.warning("Rate limited (429); sleeping %ds.", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError(f"GET {path} failed after retries.")

    # --- public endpoints ---

    def listed_info(self, as_of: date | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if as_of is not None:
            params["date"] = as_of.strftime("%Y%m%d")
        return self._get("/listed/info", params=params).get("info", [])

    def daily_quotes(self, as_of: date) -> list[dict[str, Any]]:
        return self._get(
            "/markets/daily_quotes",
            params={"date": as_of.strftime("%Y%m%d")},
        ).get("daily_quotes", [])

    def statements_by_date(self, as_of: date) -> list[dict[str, Any]]:
        return self._get(
            "/fins/statements",
            params={"date": as_of.strftime("%Y%m%d")},
        ).get("statements", [])


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def safe_float(value: Any) -> float | None:
    """Convert J-Quants string fields to float, treating '', '-', None as missing."""
    if value is None or value == "" or value == "-":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_pbr(close: float | None, book_value_per_share: float | None) -> float | None:
    if close is None or book_value_per_share is None:
        return None
    if book_value_per_share <= 0:
        return None
    return close / book_value_per_share


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def collect_latest_statements(
    client: JQuantsClient,
    end_date: date,
    lookback_days: int,
) -> dict[str, dict[str, Any]]:
    """Walk back day by day and keep the latest statement per LocalCode."""
    seen: dict[str, dict[str, Any]] = {}
    for offset in range(lookback_days + 1):
        d = end_date - timedelta(days=offset)
        try:
            stmts = client.statements_by_date(d)
        except requests.HTTPError as exc:
            log.debug("Statements unavailable for %s: %s", d, exc)
            continue
        if stmts:
            new_codes = 0
            for stmt in stmts:
                code = stmt.get("LocalCode")
                if not code or code in seen:
                    continue
                seen[code] = stmt
                new_codes += 1
            if new_codes:
                log.debug("%s: +%d new codes (cum %d)", d, new_codes, len(seen))
        if offset and offset % 30 == 0:
            log.info(
                "Walked back %d days; %d unique codes with statements so far.",
                offset, len(seen),
            )
    return seen


def find_latest_trading_day(
    client: JQuantsClient,
    end_date: date,
    lookback: int = 14,
) -> tuple[date, list[dict[str, Any]]]:
    """Walk back from end_date to find the most recent day with quote data."""
    for offset in range(lookback):
        d = end_date - timedelta(days=offset)
        try:
            quotes = client.daily_quotes(d)
        except requests.HTTPError as exc:
            log.debug("Quotes unavailable for %s: %s", d, exc)
            continue
        if quotes:
            return d, quotes
    raise RuntimeError(
        f"No daily_quotes found within {lookback} days back from {end_date}"
    )


# ---------------------------------------------------------------------------
# Row assembly + output
# ---------------------------------------------------------------------------

OUTPUT_FIELDS = [
    "code",
    "name_jp",
    "name_en",
    "market",
    "sector33",
    "sector17",
    "scale",
    "close",
    "book_value_per_share",
    "pbr",
    "earnings_per_share",
    "roe_approx",
    "equity",
    "total_assets",
    "equity_to_asset_ratio",
    "net_sales",
    "operating_profit",
    "ordinary_profit",
    "net_profit",
    "stmt_disclosure_date",
    "stmt_period_end",
    "stmt_type",
    "quote_date",
]


def build_row(
    code: str,
    info_row: dict[str, Any],
    stmt: dict[str, Any],
    quote: dict[str, Any],
    quote_date: date,
) -> dict[str, Any] | None:
    bps = safe_float(stmt.get("BookValuePerShare"))
    close = safe_float(quote.get("Close"))
    pbr = compute_pbr(close, bps)
    if pbr is None:
        return None
    eps = safe_float(stmt.get("EarningsPerShare"))
    roe = (eps / bps) if (eps is not None and bps and bps > 0) else None
    return {
        "code": code,
        "name_jp": info_row.get("CompanyName", ""),
        "name_en": info_row.get("CompanyNameEnglish", ""),
        "market": info_row.get("MarketCodeName", ""),
        "sector33": info_row.get("Sector33CodeName", ""),
        "sector17": info_row.get("Sector17CodeName", ""),
        "scale": info_row.get("ScaleCategory", ""),
        "close": close,
        "book_value_per_share": bps,
        "pbr": pbr,
        "earnings_per_share": eps,
        "roe_approx": roe,
        "equity": safe_float(stmt.get("Equity")),
        "total_assets": safe_float(stmt.get("TotalAssets")),
        "equity_to_asset_ratio": safe_float(stmt.get("EquityToAssetRatio")),
        "net_sales": safe_float(stmt.get("NetSales")),
        "operating_profit": safe_float(stmt.get("OperatingProfit")),
        "ordinary_profit": safe_float(stmt.get("OrdinaryProfit")),
        "net_profit": safe_float(stmt.get("Profit")),
        "stmt_disclosure_date": stmt.get("DisclosedDate", ""),
        "stmt_period_end": stmt.get("CurrentPeriodEndDate", ""),
        "stmt_type": stmt.get("TypeOfDocument", ""),
        "quote_date": quote_date.isoformat(),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# CLI / entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find J-listed companies with PBR < threshold using J-Quants."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "data",
        help="Directory for CSV outputs.",
    )
    parser.add_argument(
        "--max-pbr",
        type=float,
        default=1.0,
        help="Upper bound on PBR (exclusive). Default 1.0",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Reference date YYYY-MM-DD. Default: today - 90 days.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=180,
        help="Days to walk back collecting statements. Default 180.",
    )
    parser.add_argument(
        "--min-equity",
        type=float,
        default=0.0,
        help="Minimum equity (JPY) to keep a row. Default 0 (positive equity only).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).parent / ".env",
        help=".env file with JQUANTS_REFRESH_TOKEN or JQUANTS_MAIL/JQUANTS_PASSWORD",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    load_dotenv(args.env_file)

    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = date.today() - timedelta(days=90)
    log.info("Reference end date: %s", end_date)

    creds = JQuantsCreds.from_env()
    if not (creds.refresh_token or (creds.mail and creds.password)):
        log.error(
            "Missing credentials. Set JQUANTS_REFRESH_TOKEN, or "
            "JQUANTS_MAIL+JQUANTS_PASSWORD, in environment or %s.",
            args.env_file,
        )
        return 2

    client = JQuantsClient(creds)

    # 1) Universe
    log.info("Fetching listed_info as of %s...", end_date)
    info = client.listed_info(as_of=end_date)
    log.info("listed_info: %d rows.", len(info))
    info_by_code: dict[str, dict[str, Any]] = {row["Code"]: row for row in info}

    # 2) Latest statement per code
    log.info("Collecting latest statements (lookback=%dd)...", args.lookback_days)
    latest_stmts = collect_latest_statements(client, end_date, args.lookback_days)
    log.info("Latest statements: %d codes.", len(latest_stmts))

    # 3) Latest trading day with quotes
    quote_date, quotes = find_latest_trading_day(client, end_date)
    log.info("Quote date: %s (%d rows).", quote_date, len(quotes))
    quote_by_code = {q["Code"]: q for q in quotes}

    # 4) Join + compute + filter
    rows: list[dict[str, Any]] = []
    skipped_no_info = 0
    skipped_no_quote = 0
    skipped_bad_pbr = 0
    skipped_min_equity = 0
    for code, stmt in latest_stmts.items():
        info_row = info_by_code.get(code)
        if info_row is None:
            skipped_no_info += 1
            continue
        quote = quote_by_code.get(code)
        if quote is None:
            skipped_no_quote += 1
            continue
        row = build_row(code, info_row, stmt, quote, quote_date)
        if row is None:
            skipped_bad_pbr += 1
            continue
        if row["pbr"] >= args.max_pbr:
            continue
        equity = row["equity"]
        if equity is not None and equity < args.min_equity:
            skipped_min_equity += 1
            continue
        rows.append(row)

    rows.sort(key=lambda r: (r["pbr"] if r["pbr"] is not None else 1e9))

    log.info(
        "Filter result: kept=%d  skipped(no_info=%d, no_quote=%d, bad_pbr=%d, min_equity=%d)",
        len(rows), skipped_no_info, skipped_no_quote, skipped_bad_pbr, skipped_min_equity,
    )

    output_path = args.output_dir / f"pbr_under_{args.max_pbr}_{end_date.isoformat()}.csv"
    if rows:
        write_csv(output_path, rows)
        log.info("Wrote %d rows -> %s", len(rows), output_path.resolve())
        # Quick top-of-list preview to stdout for sanity check.
        log.info("Top 10 by lowest PBR:")
        for r in rows[:10]:
            log.info(
                "  %s %s pbr=%.3f bps=%.1f close=%.1f sector=%s",
                r["code"], r["name_en"] or r["name_jp"],
                r["pbr"] or 0, r["book_value_per_share"] or 0,
                r["close"] or 0, r["sector33"],
            )
    else:
        log.warning("No rows passed the filter; CSV not written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
