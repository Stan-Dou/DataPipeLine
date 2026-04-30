# J_PBR — Japan PBR<1 Value-Investing Pipeline

A funnel that turns the entire **Japanese listed universe (~3,750 names)**
into a hand-reviewable **shortlist of cheap, high-quality businesses**
under 100 names, using free yfinance data only.

## Pipeline

```
universe (~3,750)
    │   fetch_layer0_pbr.py          (yfinance .info per stock)
    ▼
Layer 0: PBR < 1                ~1,378
    │   screen_layerA_quality.py     (single-year ROE, op-margin, mcap)
    ▼
Layer A: cheap & profitable     ~180   (default)  /  ~90 (strict)
    │   screen_layerB_history.py     (4y financials, multi-year metrics)
    ▼
Layer B: cheap & durably profitable  ~50–60
    │   (manual review from here)
```

## Files

```
J_PBR/
├── fetch_layer0_pbr.py          # 1) download JPX universe + filter PBR<1
├── refetch_layer0_missing.py    #    (slow retry helper for rate-limited IPs)
├── screen_layerA_quality.py     # 2) single-year quality filter (ROE / OpM / Cap)
├── screen_layerB_history.py     # 3) multi-year history fetch + filter
├── README.md                    # this file
├── _archive/                    # legacy J-Quants version (unused)
├── data/
│   ├── universe_jp.csv                        # JPX official listing snapshot
│   ├── cache_info/<code>.json                 # yfinance .info cache (one file per code)
│   ├── cache_history/<code>.json              # yfinance financials cache (one file per code)
│   ├── layer0_pbr_under_1__YYYY-MM-DD.csv     # PBR<1 candidates (from step 1)
│   ├── layerA_quality_default__YYYY-MM-DD.csv # Layer A default screen
│   ├── layerA_quality_strict__YYYY-MM-DD.csv  # Layer A strict screen
│   ├── layerB_enriched__YYYY-MM-DD.csv        # Layer A + multi-year columns (pre-filter)
│   └── layerB_filtered__YYYY-MM-DD.csv        # Layer B survivors
└── snapshots/
    └── <same filenames>                        # frozen immutable copies + .md digests
```

Everything dated lives in two places:
- `data/` → working file overwritten on re-runs
- `snapshots/` → immutable copy, never overwritten (so you can compare runs)

## Quick start

```pwsh
# (1) Download universe + PBR<1 list. Slow (~30 min cold; instant if cached).
.venv\Scripts\python.exe J_PBR\fetch_layer0_pbr.py --workers 4

# (1b) Only if Yahoo rate-limited you mid-run:
.venv\Scripts\python.exe J_PBR\refetch_layer0_missing.py --base-sleep 1.5
.venv\Scripts\python.exe J_PBR\fetch_layer0_pbr.py            # re-run; uses cache

# (2) Single-year quality filter (instant; reads CSV + cache only)
.venv\Scripts\python.exe J_PBR\screen_layerA_quality.py
.venv\Scripts\python.exe J_PBR\screen_layerA_quality.py --strict --max-pe 15

# (3) Multi-year history filter (10–15 min cold; instant if cached)
.venv\Scripts\python.exe J_PBR\screen_layerB_history.py
.venv\Scripts\python.exe J_PBR\screen_layerB_history.py --no-fetch  # filter only
```

## Layer-A filter (single year)

Defaults applied to Layer 0:
- `market_cap >= 10 bn JPY`
- `profit_margins > 0`
- `roe >= 8%`
- `operating_margins >= 5%`

`--strict` adds: `dividend_yield > 0`, `total_cash > total_debt`, `0 < trailing_pe < max_pe`.

Output: `layerA_quality_<default|strict>__<date>.csv` with a `score` column
(z-score blend of low PBR + high ROE + high op margin).

## Layer-B filter (multi year)

Pulls 4 years of `income_stmt`/`balance_sheet`/`cashflow` per stock and computes:

| Metric | Why |
|---|---|
| `net_income_all_positive_4y` | rule out single-year flukes |
| `roe_3y_min`, `roe_3y_mean` | worst-of-3-years floor catches cyclicals at peak |
| `revenue_3y_cagr` | shrinking is OK, dying is not |
| `fcf_5y_mean`, `fcf_yield_5y` | actually generates cash; harder than dividend yield |
| `net_buyback_4y` | tracks PBR-improvement actions |
| `shares_change_4y` | dilution check |

Default filter: `all_positive_4y AND roe_3y_min >= 5% AND revenue_3y_cagr >= -5%
AND fcf_5y_mean > 0 AND fcf_yield_5y >= 5%`.

Output: `layerB_filtered__<date>.csv` with a `composite_b` column
(z-score blend: 40% low PBR + 30% high `roe_3y_min` + 30% high `fcf_yield_5y`).

## Caching policy

- All fetches are cached as one JSON per ticker code under `data/cache_info/`
  and `data/cache_history/`.
- Re-runs are O(0) on the network unless `--refresh` is passed.
- Safe to delete a single `<code>.json` to force a re-fetch for that stock.

## Known data caveats

- yfinance's `priceToBook` is sometimes wildly off (e.g. SYLA Holdings PBR = 0.01)
  due to share-count or BVPS issues. Cross-check obvious outliers against the
  company's own kessan-tanshin (決算短信) before trusting them.
- 4-year history can be too short for true cyclicals (steel, shipping,
  semiconductors). For those, manual review of 10-year ROE bands is required.
- All metrics use trailing data; nothing here is forward-looking.

## Roadmap (not yet implemented)

- **Dimension 1: real PBR** — adjust BVPS for goodwill/intangibles/pension
  deficits; uncover hidden assets (real estate, listed-stock holdings) via
  EDINET footnotes.
- **Catalysts**: track each candidate's TDnet disclosures for "資本コスト・
  株価を意識した経営" (capital-cost & equity-value-conscious management) plans.
- **Price-history context**: where is each name's PBR in its own 5-year band?
  Long-term-cheap ≠ recently-cheap.
- **Activist filter**: 5% holder reports (大量保有報告書) for known activists.
