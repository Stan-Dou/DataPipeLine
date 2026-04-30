"""Layer-B history enrichment + multi-year quality filter.

For each code in the Layer-A output, pull yfinance financial statements
(income, balance, cashflow), compute multi-year metrics, cache to JSON,
then produce a filtered CSV.

Multi-year metrics computed:
    - net_income_4y_mean / 4y_cv     (consistency of profit)
    - net_income_4y_min, _max
    - revenue_4y_mean
    - revenue_3y_cagr                (growth, can be negative)
    - roe_3y_mean                    (avg of last 3 years' ROE_t = NI_t / avg(equity_t, t-1))
    - roe_3y_min                     (worst year - rules out cyclicals at peak)
    - fcf_5y_mean                    (Free Cash Flow average)
    - fcf_yield_5y                   (fcf_5y_mean / current market_cap)
    - net_buyback_4y                 (Repurchase Of Capital Stock summed; positive = bought back)
    - shares_change_4y               ((latest_shares / earliest_shares) - 1)

Default Layer-B filter (the "real-quality" cut):
    - net_income > 0 in EVERY one of the last 4 years
    - roe_3y_min >= --min-roe-min     (default 5%)  -- not just peak ROE
    - revenue_3y_cagr >= --min-cagr   (default -0.05) -- shrinking is OK, dying is not
    - fcf_5y_mean > 0                 -- actually generates cash
    - fcf_yield_5y >= --min-fcf-yld   (default 5%)

Examples:
    .venv\\Scripts\\python.exe J_PBR\\screen_layerB_history.py
    .venv\\Scripts\\python.exe J_PBR\\screen_layerB_history.py --input J_PBR\\data\\layerA_quality_default__2026-04-30.csv
    .venv\\Scripts\\python.exe J_PBR\\screen_layerB_history.py --refresh   # ignore cache
    .venv\\Scripts\\python.exe J_PBR\\screen_layerB_history.py --no-fetch  # filter only, use cache
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import random
import shutil
import signal
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

log = logging.getLogger("j_pbr_history")

INC_FIELDS = ("Net Income", "Total Revenue", "Operating Income")
BS_FIELDS = ("Stockholders Equity", "Total Assets", "Ordinary Shares Number",
             "Share Issued", "Goodwill", "Other Intangible Assets")
CF_FIELDS = ("Free Cash Flow", "Operating Cash Flow", "Capital Expenditure",
             "Repurchase Of Capital Stock", "Common Stock Dividend Paid")

_stop = False


def _handle_sigint(signum, frame):  # noqa: ARG001
    global _stop
    _stop = True
    log.warning("SIGINT received; stopping after current ticker.")


signal.signal(signal.SIGINT, _handle_sigint)


# ---------------------------------------------------------------------------
# Fetch + cache
# ---------------------------------------------------------------------------

def _series_to_dict(df: pd.DataFrame, fields: tuple[str, ...]) -> dict[str, dict[str, float | None]]:
    """Convert a (rows=fields, cols=dates) DataFrame slice to {field: {date: value}}."""
    out: dict[str, dict[str, float | None]] = {}
    if df is None or df.empty:
        return {f: {} for f in fields}
    cols = [c.isoformat() if hasattr(c, "isoformat") else str(c) for c in df.columns]
    for f in fields:
        if f in df.index:
            row = df.loc[f]
            out[f] = {
                cols[i]: (float(v) if pd.notna(v) else None) for i, v in enumerate(row)
            }
        else:
            out[f] = {}
    return out


def fetch_history_once(code: str) -> dict | None:
    ticker = yf.Ticker(f"{code}.T")
    try:
        inc = ticker.income_stmt
        bs = ticker.balance_sheet
        cf = ticker.cashflow
    except Exception as exc:
        log.debug("fetch failed for %s: %s", code, exc)
        return None
    if inc is None and bs is None and cf is None:
        return None
    if (inc is None or inc.empty) and (bs is None or bs.empty) and (cf is None or cf.empty):
        return None
    return {
        "income_stmt": _series_to_dict(inc, INC_FIELDS),
        "balance_sheet": _series_to_dict(bs, BS_FIELDS),
        "cashflow": _series_to_dict(cf, CF_FIELDS),
        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }


def fetch_all(
    codes: list[str],
    cache_dir: Path,
    refresh: bool,
    base_sleep: float,
    max_attempts: int,
) -> dict[str, dict]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    out: dict[str, dict] = {}
    pending: list[str] = []
    for code in codes:
        cf = cache_dir / f"{code}.json"
        if not refresh and cf.exists():
            try:
                out[code] = json.loads(cf.read_text(encoding="utf-8"))
                continue
            except Exception:
                pass
        pending.append(code)

    log.info("History cache hit: %d, to fetch: %d", len(out), len(pending))
    if not pending:
        return out

    successes = 0
    failures = 0
    for i, code in enumerate(pending, 1):
        if _stop:
            log.warning("Stopping early.")
            break

        info = None
        for attempt in range(max_attempts):
            try:
                info = fetch_history_once(code)
                break
            except Exception as exc:
                wait = (base_sleep + random.uniform(0, 0.5)) * (2 ** attempt)
                log.debug("attempt %d failed for %s: %s; sleep %.1fs", attempt + 1, code, exc, wait)
                time.sleep(wait)

        if info is not None:
            (cache_dir / f"{code}.json").write_text(
                json.dumps(info, ensure_ascii=False), encoding="utf-8"
            )
            out[code] = info
            successes += 1
        else:
            failures += 1

        if i % 20 == 0 or i == len(pending):
            log.info("Fetched %d/%d  successes=%d failures=%d", i, len(pending), successes, failures)
        time.sleep(base_sleep + random.uniform(0, 0.5))

    return out


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def _sorted_series(d: dict[str, float | None]) -> list[tuple[str, float]]:
    """Return [(date, value)] sorted by date ascending, with None values dropped."""
    items = [(k, v) for k, v in d.items() if v is not None]
    items.sort(key=lambda x: x[0])
    return items


def _mean(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _stdev(xs: list[float]) -> float | None:
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / len(xs)
    return math.sqrt(var)


def compute_metrics(hist: dict, market_cap: float | None) -> dict[str, float | None]:
    inc = hist.get("income_stmt", {})
    bs = hist.get("balance_sheet", {})
    cf = hist.get("cashflow", {})

    ni = _sorted_series(inc.get("Net Income", {}))
    rev = _sorted_series(inc.get("Total Revenue", {}))
    eq = _sorted_series(bs.get("Stockholders Equity", {}))
    fcf = _sorted_series(cf.get("Free Cash Flow", {}))
    repurch = _sorted_series(cf.get("Repurchase Of Capital Stock", {}))
    shares = _sorted_series(bs.get("Ordinary Shares Number", {}))
    if not shares:
        shares = _sorted_series(bs.get("Share Issued", {}))

    out: dict[str, float | None] = {
        "net_income_years": len(ni),
        "net_income_4y_mean": None,
        "net_income_4y_cv": None,
        "net_income_4y_min": None,
        "net_income_all_positive_4y": None,
        "revenue_4y_mean": None,
        "revenue_3y_cagr": None,
        "roe_3y_mean": None,
        "roe_3y_min": None,
        "fcf_5y_mean": None,
        "fcf_yield_5y": None,
        "net_buyback_4y": None,
        "shares_change_4y": None,
    }

    # Net income stats
    ni_vals = [v for _, v in ni]
    if ni_vals:
        m = _mean(ni_vals[-4:])
        sd = _stdev(ni_vals[-4:])
        out["net_income_4y_mean"] = m
        out["net_income_4y_cv"] = (sd / abs(m)) if m and sd is not None else None
        out["net_income_4y_min"] = min(ni_vals[-4:])
        out["net_income_all_positive_4y"] = bool(all(x > 0 for x in ni_vals[-4:]) and len(ni_vals) >= 3)

    # Revenue stats + 3y CAGR
    rev_vals = [v for _, v in rev]
    if rev_vals:
        out["revenue_4y_mean"] = _mean(rev_vals[-4:])
        if len(rev_vals) >= 4 and rev_vals[-4] > 0 and rev_vals[-1] > 0:
            out["revenue_3y_cagr"] = (rev_vals[-1] / rev_vals[-4]) ** (1 / 3) - 1
        elif len(rev_vals) >= 3 and rev_vals[-3] > 0 and rev_vals[-1] > 0:
            out["revenue_3y_cagr"] = (rev_vals[-1] / rev_vals[-3]) ** (1 / 2) - 1

    # ROE per year using avg(equity_t, equity_t-1)
    if ni and eq:
        eq_map = dict(eq)
        ni_map = dict(ni)
        roes: list[float] = []
        eq_dates_sorted = [d for d, _ in eq]
        for i in range(1, len(eq_dates_sorted)):
            d_curr = eq_dates_sorted[i]
            d_prev = eq_dates_sorted[i - 1]
            if d_curr in ni_map and eq_map[d_curr] and eq_map[d_prev]:
                avg_eq = (eq_map[d_curr] + eq_map[d_prev]) / 2
                if avg_eq > 0:
                    roes.append(ni_map[d_curr] / avg_eq)
        if roes:
            recent = roes[-3:]
            out["roe_3y_mean"] = _mean(recent)
            out["roe_3y_min"] = min(recent)

    # FCF stats
    fcf_vals = [v for _, v in fcf]
    if fcf_vals:
        avg_fcf = _mean(fcf_vals[-5:])
        out["fcf_5y_mean"] = avg_fcf
        if avg_fcf is not None and market_cap and market_cap > 0:
            out["fcf_yield_5y"] = avg_fcf / market_cap

    # Buyback (repurchases reported as negative outflows, so negate)
    rep_vals = [v for _, v in repurch]
    if rep_vals:
        out["net_buyback_4y"] = -sum(rep_vals[-4:])  # positive = bought back

    # Share count change
    shr_vals = [v for _, v in shares if v]
    if len(shr_vals) >= 2 and shr_vals[0]:
        out["shares_change_4y"] = (shr_vals[-1] / shr_vals[0]) - 1

    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input CSV (default: latest J_PBR/data/layerA_quality_default__*.csv).",
    )
    parser.add_argument("--cache-dir", type=Path,
                        default=Path(__file__).parent / "data" / "cache_history")
    parser.add_argument("--out-dir", type=Path,
                        default=Path(__file__).parent / "data")
    parser.add_argument("--snap-dir", type=Path,
                        default=Path(__file__).parent / "snapshots")
    parser.add_argument("--refresh", action="store_true",
                        help="Ignore history cache and refetch.")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Do not fetch; use existing cache only (filter step only).")
    parser.add_argument("--base-sleep", type=float, default=1.5)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--min-roe-min", type=float, default=0.05,
                        help="Worst-of-3-years ROE floor. Default 0.05.")
    parser.add_argument("--min-cagr", type=float, default=-0.05,
                        help="Min 3y revenue CAGR. Default -0.05 (modest decline OK).")
    parser.add_argument("--min-fcf-yld", type=float, default=0.05,
                        help="Min 5y average FCF yield. Default 0.05 (=5%%).")
    parser.add_argument("--require-all-positive-4y", action="store_true",
                        default=True,
                        help="Require positive net income in every of last 4 years (default on).")
    parser.add_argument("--allow-loss-year", dest="require_all_positive_4y",
                        action="store_false",
                        help="Allow up to 1 loss-making year in last 4.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def find_default_input(data_dir: Path) -> Path:
    candidates = sorted(data_dir.glob("layerA_quality_default__*.csv"))
    if not candidates:
        raise SystemExit(f"No layerA_quality_default__*.csv in {data_dir}.")
    return candidates[-1]


def write_markdown(
    out_path: Path,
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    funnel: list[tuple[str, int]],
    args: argparse.Namespace,
    input_path: Path,
) -> None:
    lines: list[str] = []
    lines.append(f"# History-enriched watchlist - {dt.date.today().isoformat()}")
    lines.append("")
    lines.append(f"- Input: `{input_path.name}` ({len(df_in)} rows)")
    lines.append(f"- Generated: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(
        f"- Filters: roe_3y_min >= {args.min_roe_min*100:.0f}%, "
        f"revenue_3y_cagr >= {args.min_cagr*100:+.0f}%, "
        f"fcf_yield_5y >= {args.min_fcf_yld*100:.0f}%, "
        f"all_positive_4y={args.require_all_positive_4y}"
    )
    lines.append(f"- **Result: {len(df_out)} survivors**")
    lines.append("")
    lines.append("## Funnel")
    lines.append("")
    lines.append("| Stage | Surviving | Killed |")
    lines.append("|---|---:|---:|")
    prev = funnel[0][1]
    for stage, n in funnel:
        killed = max(prev - n, 0)
        lines.append(f"| {stage} | {n} | {killed} |")
        prev = n
    lines.append("")

    if df_out.empty:
        lines.append("_No rows survived._")
        out_path.write_text("\n".join(lines), encoding="utf-8")
        return

    lines.append("## Sector breakdown")
    lines.append("")
    lines.append("| Sector | Count |")
    lines.append("|---|---:|")
    for s, c in df_out["yf_sector"].value_counts(dropna=False).items():
        label = s if pd.notna(s) else "(unknown)"
        lines.append(f"| {label} | {c} |")
    lines.append("")

    lines.append(f"## Top 50 (sorted by composite_b)")
    lines.append("")
    lines.append(
        "| # | Code | Name | Sector | PBR | ROE3y_min | ROE3y_mean | "
        "Rev_CAGR | FCF_Yld | MCap (bn) |"
    )
    lines.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|")
    top = df_out.sort_values("composite_b", ascending=False).head(50).reset_index(drop=True)
    for i, r in top.iterrows():
        name = r["name"] if pd.notna(r["name"]) else r.get("name_jp", "")
        mc = r["market_cap"] / 1e9 if pd.notna(r["market_cap"]) else float("nan")
        roe_min = (r["roe_3y_min"] or 0) * 100
        roe_avg = (r["roe_3y_mean"] or 0) * 100
        cagr = (r["revenue_3y_cagr"] or 0) * 100
        fy = (r["fcf_yield_5y"] or 0) * 100
        lines.append(
            f"| {i+1} | {r['code']} | {name} | {r.get('yf_sector','')} | "
            f"{r['pbr']:.2f} | {roe_min:.1f}% | {roe_avg:.1f}% | {cagr:+.1f}% | "
            f"{fy:.1f}% | {mc:.1f} |"
        )

    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    input_path = args.input or find_default_input(args.out_dir)
    log.info("Reading %s", input_path)
    df = pd.read_csv(input_path, dtype={"code": str})
    df["code"] = df["code"].astype(str).str.zfill(4)
    log.info("Loaded %d rows", len(df))

    codes = df["code"].tolist()
    if args.no_fetch:
        log.info("--no-fetch: filtering only with existing cache.")
        hist_map: dict[str, dict] = {}
        for code in codes:
            cf = args.cache_dir / f"{code}.json"
            if cf.exists():
                try:
                    hist_map[code] = json.loads(cf.read_text(encoding="utf-8"))
                except Exception:
                    pass
        log.info("Cache hits: %d / %d", len(hist_map), len(codes))
    else:
        hist_map = fetch_all(
            codes,
            cache_dir=args.cache_dir,
            refresh=args.refresh,
            base_sleep=args.base_sleep,
            max_attempts=args.max_attempts,
        )

    # Compute metrics row by row
    metric_rows: list[dict] = []
    for _, r in df.iterrows():
        code = r["code"]
        hist = hist_map.get(code)
        if hist is None:
            metric_rows.append({"code": code})
            continue
        m = compute_metrics(hist, r.get("market_cap"))
        m["code"] = code
        metric_rows.append(m)
    metrics_df = pd.DataFrame(metric_rows)

    enriched = df.merge(metrics_df, on="code", how="left")
    enriched_out = args.out_dir / f"layerB_enriched__{dt.date.today().isoformat()}.csv"
    enriched.to_csv(enriched_out, index=False, encoding="utf-8-sig")
    log.info("Wrote enriched (pre-filter) -> %s", enriched_out)

    # Funnel
    funnel: list[tuple[str, int]] = []
    funnel.append(("Input (Layer-A survivors)", len(enriched)))

    df2 = enriched.copy()
    has_hist = df2["roe_3y_min"].notna() | df2["fcf_yield_5y"].notna()
    df2 = df2[has_hist]
    funnel.append(("has any history data", len(df2)))

    if args.require_all_positive_4y:
        df2 = df2[df2["net_income_all_positive_4y"] == True]  # noqa: E712
        funnel.append(("net_income > 0 in all of last 4y", len(df2)))

    df2 = df2[df2["roe_3y_min"].fillna(-1) >= args.min_roe_min]
    funnel.append((f"roe_3y_min >= {args.min_roe_min*100:.0f}%", len(df2)))

    df2 = df2[df2["revenue_3y_cagr"].fillna(-1) >= args.min_cagr]
    funnel.append((f"revenue_3y_cagr >= {args.min_cagr*100:+.0f}%", len(df2)))

    df2 = df2[df2["fcf_5y_mean"].fillna(-1) > 0]
    funnel.append(("fcf_5y_mean > 0", len(df2)))

    df2 = df2[df2["fcf_yield_5y"].fillna(-1) >= args.min_fcf_yld]
    funnel.append((f"fcf_yield_5y >= {args.min_fcf_yld*100:.0f}%", len(df2)))

    log.info("Funnel:")
    prev = funnel[0][1]
    for stage, n in funnel:
        log.info("  %-50s %5d  (%+d)", stage, n, n - prev)
        prev = n

    if not df2.empty:
        # Composite-B: PBR low (40%) + roe_3y_min high (30%) + fcf_yield high (30%)
        def z(s: pd.Series) -> pd.Series:
            m, sd = s.mean(), s.std(ddof=0)
            if sd == 0 or pd.isna(sd):
                return pd.Series(0.0, index=s.index)
            return (s - m) / sd

        df2 = df2.copy()
        df2["composite_b"] = (
            -0.40 * z(df2["pbr"])
            + 0.30 * z(df2["roe_3y_min"].fillna(df2["roe_3y_min"].min()))
            + 0.30 * z(df2["fcf_yield_5y"].fillna(df2["fcf_yield_5y"].min()))
        ).round(3)
        df2 = df2.sort_values("composite_b", ascending=False).reset_index(drop=True)

    today = dt.date.today().isoformat()
    out_csv = args.out_dir / f"layerB_filtered__{today}.csv"
    snap_csv = args.snap_dir / f"layerB_filtered__{today}.csv"
    snap_md = args.snap_dir / f"layerB_filtered__{today}.md"
    args.snap_dir.mkdir(parents=True, exist_ok=True)
    df2.to_csv(out_csv, index=False, encoding="utf-8-sig")
    shutil.copy(out_csv, snap_csv)
    write_markdown(snap_md, enriched, df2, funnel, args, input_path)
    log.info("Wrote %d filtered rows -> %s", len(df2), out_csv)
    log.info("Snapshot CSV -> %s", snap_csv)
    log.info("Snapshot MD  -> %s", snap_md)

    if not df2.empty:
        log.info("Top 10 by composite_b:")
        for _, r in df2.head(10).iterrows():
            log.info(
                "  %s %-35s pbr=%.2f roe3y_min=%.1f%% rev_cagr=%+.1f%% fcf_yld=%.1f%% mc=%.1fbn",
                r["code"], str(r.get("name", ""))[:35],
                r["pbr"],
                (r["roe_3y_min"] or 0) * 100,
                (r["revenue_3y_cagr"] or 0) * 100,
                (r["fcf_yield_5y"] or 0) * 100,
                (r["market_cap"] or 0) / 1e9,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
