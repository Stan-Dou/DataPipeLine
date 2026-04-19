"""分析泡泡玛特3-4月份Put OI变化，寻找段永平可能的Sell Put位置"""
import sqlite3

conn = sqlite3.connect("data/options.db")

# 1. 先看每天的收盘价走势
print("=== 泡泡玛特 收盘价走势 ===")
rows = conn.execute("""
    SELECT DISTINCT date, closing_price 
    FROM options_daily 
    WHERE stock_code='09992' 
    ORDER BY date
""").fetchall()
for date, price in rows:
    print(f"  {date}  {price}")

# 2. 找Put OI增长最多的行权价（按到期日分组）
print("\n=== Put OI日均变动 TOP 20（整个3-4月） ===")
print(f"{'到期日':>10} {'行权价':>8} {'OI增长':>10} {'最终OI':>10} {'日均成交':>10}")
print("-" * 55)
rows = conn.execute("""
    SELECT expiry, strike,
           MAX(oi) - MIN(oi) AS oi_growth,
           MAX(oi) AS final_oi,
           AVG(volume) AS avg_volume
    FROM options_daily
    WHERE stock_code='09992' AND type='P'
    GROUP BY expiry, strike
    ORDER BY oi_growth DESC
    LIMIT 20
""").fetchall()
for expiry, strike, growth, final_oi, avg_vol in rows:
    print(f"{expiry:>10} {strike:>8.1f} {growth:>10} {final_oi:>10} {avg_vol:>10.0f}")

# 3. 逐日看Put OI变化最大的行权价
print("\n=== 每日Put OI增长最大的合约 ===")
print(f"{'日期':>12} {'到期日':>10} {'行权价':>8} {'OI变动':>8} {'当日OI':>10} {'成交量':>8} {'收盘价':>8}")
print("-" * 75)
rows = conn.execute("""
    SELECT date, expiry, strike, oi_change, oi, volume, closing_price
    FROM options_daily
    WHERE stock_code='09992' AND type='P' AND oi_change > 500
    ORDER BY date, oi_change DESC
""").fetchall()
for row in rows:
    print(f"{row[0]:>12} {row[1]:>10} {row[2]:>8.1f} {row[3]:>+8} {row[4]:>10} {row[5]:>8} {row[6]:>8}")

# 4. 重点看150 Put的OI逐日变化
print("\n=== 150 Put OI逐日变化（29APR26到期）===")
rows = conn.execute("""
    SELECT date, oi, oi_change, volume, settlement, closing_price
    FROM options_daily
    WHERE stock_code='09992' AND type='P' AND strike=150.0 AND expiry='29APR26'
    ORDER BY date
""").fetchall()
for date, oi, change, vol, settle, close in rows:
    bar = "█" * min(int(oi / 2000), 50)
    print(f"  {date} OI={oi:>7} ({change:>+6}) vol={vol:>5} settle={settle:>5} close={close:>6}  {bar}")

conn.close()
