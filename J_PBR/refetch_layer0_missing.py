"""Slow / patient retry pass for stocks that yfinance failed on the first run.

Reads the universe and the existing on-disk cache, then re-fetches only
the missing codes one at a time with sleep + exponential backoff. Each
successful fetch is flushed to disk immediately, so Ctrl+C is safe.

Use when Yahoo Finance has rate-limited the IP. After this finishes,
re-run ``fetch_layer0_pbr.py`` to consume the now-complete cache.

Examples:
    # Default: walk the full universe; ~1.5s per call.
    .venv\\Scripts\\python.exe J_PBR\\refetch_layer0_missing.py

    # Slower (e.g. heavy throttling environment):
    .venv\\Scripts\\python.exe J_PBR\\refetch_layer0_missing.py --base-sleep 3

    # Just retry the first 200 missing names (smoke test):
    .venv\\Scripts\\python.exe J_PBR\\refetch_layer0_missing.py --limit 200
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

log = logging.getLogger("j_pbr_refetch")

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

# Heuristics: a "rate-limit-ish" failure should trigger a long pause; a
# "data-not-available" failure should not. We can't fully tell apart in the
# yfinance wrapper, so we use a pessimistic policy: if we see *several*
# consecutive failures, we sleep aggressively.
CONSECUTIVE_FAIL_PAUSES = {
    3: 30,    # 3 fails in a row -> sleep 30s
    6: 120,   # 6 fails -> 2 min
    10: 600,  # 10 fails -> 10 min, then continue
    15: 1800, # 15 fails -> 30 min
}


_stop = False


def _handle_sigint(signum, frame):  # noqa: ARG001
    global _stop
    _stop = True
    log.warning("SIGINT received; will stop after current ticker.")


signal.signal(signal.SIGINT, _handle_sigint)


def fetch_once(code: str) -> dict | None:
    """One yfinance call. Returns None if Yahoo has nothing usable."""
    ticker_str = f"{code}.T"
    t = yf.Ticker(ticker_str)
    info = t.info or {}
    if info.get("priceToBook") is None and info.get("regularMarketPrice") is None:
        return None
    return {k: info.get(k) for k in INFO_FIELDS}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(__file__).parent / "data" / "cache_info",
        help="Directory of <code>.json files (default: J_PBR/data/cache_info).",
    )
    parser.add_argument(
        "--universe-csv",
        type=Path,
        default=Path(__file__).parent / "data" / "universe_jp.csv",
        help="JPX universe CSV; produced by fetch_layer0_pbr.py.",
    )
    parser.add_argument(
        "--base-sleep",
        type=float,
        default=1.5,
        help="Seconds to sleep between requests; jittered. Default 1.5.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Max attempts per code on transient errors. Default 3.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="If > 0, only retry the first N missing codes.",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle missing codes before fetching (avoid hammering one sector).",
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
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    if not args.universe_csv.exists():
        log.error(
            "Universe CSV not found: %s. Run fetch_layer0_pbr.py first.",
            args.universe_csv,
        )
        return 2
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    universe = pd.read_csv(args.universe_csv, dtype={"code": str})
    universe["code"] = universe["code"].astype(str).str.zfill(4)
    all_codes = universe["code"].tolist()

    cached_codes = {p.stem for p in args.cache_dir.glob("*.json")}
    missing = [c for c in all_codes if c not in cached_codes]

    log.info(
        "Universe=%d  cached=%d  missing=%d",
        len(all_codes), len(cached_codes), len(missing),
    )

    if args.shuffle:
        random.shuffle(missing)
    if args.limit > 0:
        missing = missing[: args.limit]
        log.info("Limited to first %d missing codes.", len(missing))

    if not missing:
        log.info("Nothing to do.")
        return 0

    consecutive_fail = 0
    successes = 0
    permanent_misses = 0  # Yahoo confirms no data
    transient_skips = 0   # gave up after retries

    for index, code in enumerate(missing):
        if _stop:
            log.warning("Stopping early as requested.")
            break

        attempt = 0
        info = None
        last_exc: Exception | None = None
        while attempt < args.max_attempts:
            try:
                info = fetch_once(code)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                attempt += 1
                wait = (args.base_sleep + random.uniform(0, 0.5)) * (2 ** attempt)
                log.debug("attempt %d failed for %s: %s; sleep %.1fs", attempt, code, exc, wait)
                time.sleep(wait)

        if info is not None:
            cache_file = args.cache_dir / f"{code}.json"
            cache_file.write_text(json.dumps(info, ensure_ascii=False), encoding="utf-8")
            consecutive_fail = 0
            successes += 1
        elif last_exc is None:
            # Yahoo said "no data". Mark with an empty marker so we don't retry forever.
            (args.cache_dir / f"{code}.json").write_text(
                json.dumps({k: None for k in INFO_FIELDS}, ensure_ascii=False),
                encoding="utf-8",
            )
            permanent_misses += 1
            consecutive_fail = 0
        else:
            transient_skips += 1
            consecutive_fail += 1

        if (index + 1) % 25 == 0 or (index + 1) == len(missing):
            log.info(
                "%d/%d processed  successes=%d permanent_miss=%d transient_skip=%d",
                index + 1, len(missing), successes, permanent_misses, transient_skips,
            )

        # Adaptive throttling on consecutive failures.
        if consecutive_fail in CONSECUTIVE_FAIL_PAUSES:
            extra = CONSECUTIVE_FAIL_PAUSES[consecutive_fail]
            log.warning(
                "%d consecutive failures; sleeping %ds before continuing.",
                consecutive_fail, extra,
            )
            time.sleep(extra)

        # Base jittered sleep between any two calls.
        time.sleep(args.base_sleep + random.uniform(0, 0.5))

    log.info(
        "DONE  processed=%d  successes=%d permanent_miss=%d transient_skip=%d  cache_now=%d",
        index + 1 if missing else 0, successes, permanent_misses, transient_skips,
        len(list(args.cache_dir.glob("*.json"))),
    )
    log.info("Now re-run fetch_layer0_pbr.py to regenerate the filtered CSV.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
