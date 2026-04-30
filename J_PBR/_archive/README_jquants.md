# J_PBR — Find Japanese PBR < 1 candidates

Pure-API, no scraping. Uses J-Quants free tier (12-week delayed data is fine
for value-investing screens).

## 1. Register J-Quants (5 minutes, free)

1. Go to https://jpx-jquants.com/
2. Sign up (mail + password). The Free plan is enough for this script.
3. After login, open **マイページ** -> there is a **Refresh Token** tab.
4. Copy the refresh token.

## 2. Configure credentials

```powershell
# from project root
copy J_PBR\.env.example J_PBR\.env
# then edit J_PBR\.env and paste your refresh token
```

Either set `JQUANTS_REFRESH_TOKEN`, or set `JQUANTS_MAIL` + `JQUANTS_PASSWORD`
and let the script obtain the refresh token on first run.

## 3. Install the one new dependency

```powershell
.venv\Scripts\python.exe -m pip install requests
```

## 4. Run

Basic run (default: PBR < 1.0, end_date = today - 90d, lookback = 180d):

```powershell
.venv\Scripts\python.exe J_PBR\find_pbr_under_1.py
```

Tighter screen (PBR < 0.7), with debug logs:

```powershell
.venv\Scripts\python.exe J_PBR\find_pbr_under_1.py --max-pbr 0.7 --log-level DEBUG
```

Specify a fixed reference date (must be older than 12 weeks for Free tier):

```powershell
.venv\Scripts\python.exe J_PBR\find_pbr_under_1.py --end-date 2026-01-15
```

## 5. Output

CSV at `J_PBR/data/pbr_under_<max>_<date>.csv`, sorted ascending by PBR.

Columns:
- `code`, `name_jp`, `name_en`, `market`, `sector33`, `sector17`, `scale`
- `close`, `book_value_per_share`, `pbr`
- `earnings_per_share`, `roe_approx`
- `equity`, `total_assets`, `equity_to_asset_ratio`
- `net_sales`, `operating_profit`, `ordinary_profit`, `net_profit`
- `stmt_disclosure_date`, `stmt_period_end`, `stmt_type`
- `quote_date`

## 6. What this script does NOT do (yet)

- It does **not** adjust book value for goodwill / intangibles / pension
  liabilities. That is dimension-1 step 2; will be added once the basic
  pipeline is verified.
- It does **not** read EDINET annual report notes (土地含み益, 政策保有株, etc).
- It does **not** track TDnet PBR-improvement disclosures.

These are intentional follow-ups; this script is the smoke test for the
J-Quants link.

## 7. Free tier limits to be aware of

- ~12 weeks delay on all data. Hence the default `today - 90 days`.
- Fewer historical periods of statements than paid tiers.
- Standard rate limits apply; the client retries on HTTP 429 with backoff.
