"""Layer-A quality screen on the PBR<1 watchlist.

Reads the PBR<1 candidate CSV and applies a stack of cheap filters that
all use columns yfinance already gave us. No extra network calls.

Default filter (the "value-trap killer"):
    - Required:  PBR < 1 (already true in input)
    - Required:  market_cap >= --min-cap          (default 10 bn JPY, liquidity)
    - Required:  profit_margins > 0               (don't lose money)
    - Quality:   ROE >= --min-roe                 (default 8%)
    - Quality:   operating_margins >= --min-opm   (default 5%)

Optional bonus filters (off by default; turn on with --strict):
    - dividend_yield > 0                          (pays something to shareholders)
    - total_cash > total_debt                     (net-cash balance sheet)
    - 0 < trailing_pe < --max-pe                  (default 20; double-cheap on PE too)

Outputs:
    J_PBR/data/layerA_quality_<suffix>__<date>.csv
    J_PBR/snapshots/layerA_quality_<suffix>__<date>.csv  (immutable copy)
    J_PBR/snapshots/layerA_quality_<suffix>__<date>.md   (human-readable digest)

Examples:
    # Default screen
    .venv\\Scripts\\python.exe J_PBR\\screen_layerA_quality.py

    # Stricter: also require net cash + dividends + PE<15
    .venv\\Scripts\\python.exe J_PBR\\screen_layerA_quality.py --strict --max-pe 15

    # Only mid-cap and above
    .venv\\Scripts\\python.exe J_PBR\\screen_layerA_quality.py --min-cap 30
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import shutil
from pathlib import Path

import pandas as pd

log = logging.getLogger("j_pbr_screen")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Input PBR<1 CSV (default: latest J_PBR/data/layer0_pbr_under_*.csv).",
    )
    parser.add_argument("--min-cap", type=float, default=10.0,
                        help="Minimum market cap in BILLION JPY. Default 10.")
    parser.add_argument("--min-roe", type=float, default=0.08,
                        help="Minimum ROE (decimal, 0.08 = 8%%). Default 0.08.")
    parser.add_argument("--min-opm", type=float, default=0.05,
                        help="Minimum operating margin. Default 0.05.")
    parser.add_argument("--max-pe", type=float, default=20.0,
                        help="Max trailing P/E when --strict. Default 20.")
    parser.add_argument("--strict", action="store_true",
                        help="Also require net cash, positive dividend yield, and 0<PE<max-pe.")
    parser.add_argument("--out-dir", type=Path,
                        default=Path(__file__).parent / "data",
                        help="Output directory for the working CSV.")
    parser.add_argument("--snap-dir", type=Path,
                        default=Path(__file__).parent / "snapshots",
                        help="Snapshot dir (immutable copies).")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def find_default_input(data_dir: Path) -> Path:
    candidates = sorted(data_dir.glob("layer0_pbr_under_*.csv"))
    if not candidates:
        raise SystemExit(
            f"No layer0_pbr_under_*.csv in {data_dir}. "
            "Run fetch_layer0_pbr.py first."
        )
    return candidates[-1]


def apply_filters(
    df: pd.DataFrame,
    min_cap_jpy: float,
    min_roe: float,
    min_opm: float,
    strict: bool,
    max_pe: float,
) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """Return (filtered_df, funnel) where funnel is [(stage_name, count), ...]."""
    funnel: list[tuple[str, int]] = []
    funnel.append(("Input PBR<1", len(df)))

    # Stage 1: market cap floor
    df = df[df["market_cap"].fillna(0) >= min_cap_jpy].copy()
    funnel.append((f"market_cap >= {min_cap_jpy/1e9:.0f} bn JPY", len(df)))

    # Stage 2: profitable
    df = df[df["profit_margins"].fillna(-1) > 0].copy()
    funnel.append(("profit_margins > 0", len(df)))

    # Stage 3: ROE quality
    df = df[df["roe"].fillna(-1) >= min_roe].copy()
    funnel.append((f"roe >= {min_roe*100:.0f}%", len(df)))

    # Stage 4: operating margin quality
    df = df[df["operating_margins"].fillna(-1) >= min_opm].copy()
    funnel.append((f"operating_margins >= {min_opm*100:.0f}%", len(df)))

    if strict:
        df = df[df["dividend_yield"].fillna(0) > 0].copy()
        funnel.append(("dividend_yield > 0", len(df)))

        cash = df["total_cash"].fillna(0)
        debt = df["total_debt"].fillna(0)
        df = df[cash > debt].copy()
        funnel.append(("net cash (cash > debt)", len(df)))

        pe = df["trailing_pe"]
        df = df[(pe > 0) & (pe < max_pe)].copy()
        funnel.append((f"0 < trailing_pe < {max_pe:.0f}", len(df)))

    return df, funnel


def composite_score(df: pd.DataFrame) -> pd.Series:
    """Cheap-and-good rank: lower PBR, higher ROE, higher op margin all good.

    Z-score each, blend, return higher-is-better score.
    """
    def z(s: pd.Series) -> pd.Series:
        m, sd = s.mean(), s.std(ddof=0)
        if sd == 0 or pd.isna(sd):
            return pd.Series([0.0] * len(s), index=s.index)
        return (s - m) / sd

    pbr_z = -z(df["pbr"])                      # lower PBR is better
    roe_z = z(df["roe"].fillna(df["roe"].min()))
    opm_z = z(df["operating_margins"].fillna(df["operating_margins"].min()))
    # weights: PBR & ROE are the twin engines; OPM is the tiebreaker
    return (0.40 * pbr_z) + (0.40 * roe_z) + (0.20 * opm_z)


def write_markdown(
    out_path: Path,
    df: pd.DataFrame,
    funnel: list[tuple[str, int]],
    args: argparse.Namespace,
    input_path: Path,
) -> None:
    lines: list[str] = []
    lines.append(f"# Quality-screened watchlist - {dt.date.today().isoformat()}")
    lines.append("")
    lines.append(f"- Input: `{input_path.name}`")
    lines.append(f"- Generated: {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(
        f"- Filters: min_cap={args.min_cap:g} bn JPY, min_roe={args.min_roe*100:.0f}%, "
        f"min_opm={args.min_opm*100:.0f}%, strict={args.strict}"
        + (f", max_pe={args.max_pe:g}" if args.strict else "")
    )
    lines.append(f"- **Result: {len(df)} candidates**")
    lines.append("")
    lines.append("## Funnel")
    lines.append("")
    lines.append("| Stage | Surviving | Killed |")
    lines.append("|---|---:|---:|")
    prev = funnel[0][1]
    for stage, n in funnel:
        killed = prev - n
        lines.append(f"| {stage} | {n} | {killed if killed >= 0 else 0} |")
        prev = n
    lines.append("")

    if df.empty:
        lines.append("_No rows survived the filter._")
    else:
        lines.append("## Sector breakdown")
        lines.append("")
        lines.append("| Sector | Count |")
        lines.append("|---|---:|")
        for s, c in df["yf_sector"].value_counts(dropna=False).items():
            label = s if pd.notna(s) else "(unknown)"
            lines.append(f"| {label} | {c} |")
        lines.append("")

        lines.append(f"## Top 50 by composite score (lower PBR + higher ROE + higher op margin)")
        lines.append("")
        lines.append(
            "| # | Code | Name | Sector | PBR | ROE | OpMgn | DivYld | "
            "MCap (bn JPY) | PE |"
        )
        lines.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|")
        top = df.sort_values("score", ascending=False).head(50).reset_index(drop=True)
        for i, r in top.iterrows():
            name = r["name"] if pd.notna(r["name"]) else r.get("name_jp", "")
            mc = r["market_cap"] / 1e9 if pd.notna(r["market_cap"]) else float("nan")
            roe = r["roe"] * 100 if pd.notna(r["roe"]) else float("nan")
            om = r["operating_margins"] * 100 if pd.notna(r["operating_margins"]) else float("nan")
            dy = r["dividend_yield"] * 100 if pd.notna(r["dividend_yield"]) else 0.0
            pe = r["trailing_pe"] if pd.notna(r["trailing_pe"]) else float("nan")
            lines.append(
                f"| {i+1} | {r['code']} | {name} | {r.get('yf_sector','')} | "
                f"{r['pbr']:.3f} | {roe:.1f}% | {om:.1f}% | {dy:.1f}% | "
                f"{mc:.1f} | {pe:.1f} |"
            )

    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    input_path = args.input or find_default_input(args.out_dir)
    log.info("Reading %s", input_path)
    df = pd.read_csv(input_path)
    log.info("Loaded %d rows", len(df))

    min_cap_jpy = args.min_cap * 1e9
    filtered, funnel = apply_filters(
        df,
        min_cap_jpy=min_cap_jpy,
        min_roe=args.min_roe,
        min_opm=args.min_opm,
        strict=args.strict,
        max_pe=args.max_pe,
    )

    log.info("Funnel:")
    prev = funnel[0][1]
    for stage, n in funnel:
        log.info("  %-40s %5d  (%+d)", stage, n, n - prev)
        prev = n

    if filtered.empty:
        log.warning("Filter killed everything; consider relaxing thresholds.")
        filtered["score"] = []
    else:
        filtered = filtered.copy()
        filtered["score"] = composite_score(filtered).round(3)
        filtered = filtered.sort_values(
            ["score", "pbr"], ascending=[False, True]
        ).reset_index(drop=True)

    today = dt.date.today().isoformat()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.snap_dir.mkdir(parents=True, exist_ok=True)

    suffix = "strict" if args.strict else "default"
    out_csv = args.out_dir / f"layerA_quality_{suffix}__{today}.csv"
    snap_csv = args.snap_dir / f"layerA_quality_{suffix}__{today}.csv"
    snap_md = args.snap_dir / f"layerA_quality_{suffix}__{today}.md"

    filtered.to_csv(out_csv, index=False, encoding="utf-8-sig")
    shutil.copy(out_csv, snap_csv)
    write_markdown(snap_md, filtered, funnel, args, input_path)

    log.info("Wrote %d rows -> %s", len(filtered), out_csv)
    log.info("Snapshot CSV   -> %s", snap_csv)
    log.info("Snapshot MD    -> %s", snap_md)

    if not filtered.empty:
        log.info("Top 10 by composite score:")
        for _, r in filtered.head(10).iterrows():
            log.info(
                "  %s %-40s pbr=%.2f roe=%.1f%% opm=%.1f%% mc=%.1fbn score=%.2f",
                r["code"],
                str(r.get("name", ""))[:40],
                r["pbr"],
                (r["roe"] or 0) * 100,
                (r["operating_margins"] or 0) * 100,
                (r["market_cap"] or 0) / 1e9,
                r["score"],
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
