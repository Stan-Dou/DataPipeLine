"""中证红利类指数历史数据分析 - 寻找低估区间

分析维度：
  1. 价格走势 + 关键均线
  2. 估值分析（PE/PB/股息率历史分位）
  3. 技术面：偏离度、波动率
  4. 低点特征总结

用法：
    python analyze_dividend50.py                              # 默认中证红利
    python analyze_dividend50.py --code H30269                # 中证红利低波（515450追踪）
    python analyze_dividend50.py --code 930955                # 中证红利低波100
    python analyze_dividend50.py --skip-fetch                 # 跳过爬取直接分析
    python analyze_dividend50.py --source csi --code H30269   # CSI源 + 红利低波
"""

import sys
import math
import statistics
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from Scraper.csi_dividend50_scraper import CSIDividend50Scraper, KNOWN_INDICES
from DAO.index_dao import IndexDAO, IndexRecord, IndexValuationRecord

DB_PATH = "data/options.db"
DEFAULT_INDEX_CODE = "000922"


# ══════════════════════════════════════════════════════════
#  数据获取
# ══════════════════════════════════════════════════════════

def fetch_and_store(source: str = "akshare",
                    index_code: str = DEFAULT_INDEX_CODE,
                    start_date: str = "20100101") -> Tuple[int, int]:
    """爬取数据并存入数据库，返回 (行情条数, 估值条数)"""
    dao = IndexDAO(DB_PATH)
    scraper = CSIDividend50Scraper(source=source, index_code=index_code)

    end_date = datetime.now().strftime("%Y%m%d")
    print(f"=== 获取 {scraper.index_name}({index_code}) [{source}源] {start_date} → {end_date} ===")

    # 行情数据
    daily_records = scraper.run(start_date=start_date, end_date=end_date)
    daily_count = 0
    if daily_records:
        # 确保指数表中有该代码
        dao.conn.execute(
            "INSERT OR IGNORE INTO indices VALUES (?,?)",
            (index_code, scraper.index_name)
        )
        dao.conn.commit()
        daily_count = dao.upsert_daily_batch(daily_records)
        print(f"  行情数据: {daily_count} 条")
    else:
        print("  行情数据: 获取失败")

    # 估值数据
    val_count = 0
    try:
        val_records = scraper.fetch_valuation()
        if val_records:
            val_count = dao.upsert_valuation_batch(val_records)
            print(f"  估值数据: {val_count} 条")
        else:
            print("  估值数据: 获取失败或无数据")
    except Exception as e:
        print(f"  估值数据: 获取异常 - {e}")

    dao.close()
    return daily_count, val_count


# ══════════════════════════════════════════════════════════
#  分析工具函数
# ══════════════════════════════════════════════════════════

def calc_ma(prices: List[float], window: int) -> List[Optional[float]]:
    """计算移动平均线"""
    result = [None] * len(prices)
    for i in range(window - 1, len(prices)):
        result[i] = sum(prices[i - window + 1:i + 1]) / window
    return result


def calc_percentile(values: List[float], current: float) -> float:
    """计算当前值在历史中的分位数(0-100)"""
    if not values:
        return 50.0
    below = sum(1 for v in values if v <= current)
    return round(below / len(values) * 100, 2)


def calc_max_drawdown(prices: List[float]) -> Tuple[float, int, int]:
    """计算最大回撤，返回 (回撤比例, 高点索引, 低点索引)"""
    if len(prices) < 2:
        return 0.0, 0, 0
    peak = prices[0]
    peak_idx = 0
    max_dd = 0.0
    dd_peak_idx = 0
    dd_trough_idx = 0
    for i, p in enumerate(prices):
        if p > peak:
            peak = p
            peak_idx = i
        dd = (peak - p) / peak
        if dd > max_dd:
            max_dd = dd
            dd_peak_idx = peak_idx
            dd_trough_idx = i
    return max_dd, dd_peak_idx, dd_trough_idx


def find_local_lows(dates: List[str], prices: List[float],
                    window: int = 60, threshold: float = 0.10) -> List[Dict]:
    """寻找历史局部低点

    条件: 在前后 window 个交易日内是最低的，且从前高点回撤超过 threshold
    """
    lows = []
    n = len(prices)
    if n < window * 2:
        return lows

    for i in range(window, n - window):
        local_min = min(prices[i - window:i + window + 1])
        if prices[i] == local_min:
            # 计算从此前高点的回撤
            recent_high = max(prices[max(0, i - window * 3):i])
            drawdown = (recent_high - prices[i]) / recent_high
            if drawdown >= threshold:
                lows.append({
                    "date": dates[i],
                    "price": prices[i],
                    "drawdown": round(drawdown * 100, 2),
                    "recent_high": recent_high,
                })
    return lows


def calc_volatility(prices: List[float], window: int = 20) -> List[Optional[float]]:
    """计算滚动年化波动率"""
    result = [None] * len(prices)
    for i in range(window, len(prices)):
        returns = [
            math.log(prices[j] / prices[j - 1])
            for j in range(i - window + 1, i + 1)
            if prices[j - 1] > 0
        ]
        if returns:
            std = statistics.stdev(returns) if len(returns) > 1 else 0
            result[i] = round(std * math.sqrt(252) * 100, 2)  # 年化 %
    return result


# ══════════════════════════════════════════════════════════
#  分析主体
# ══════════════════════════════════════════════════════════

def analyze(index_code: str = DEFAULT_INDEX_CODE):
    """执行完整分析"""
    dao = IndexDAO(DB_PATH)
    daily = dao.get_daily_series(index_code)
    valuation = dao.get_valuation_series(index_code)
    dao.close()

    if not daily:
        print(f"❌ 数据库中没有 {index_code} 的行情数据，请先运行爬取。")
        return

    index_name = daily[0].get("index_name") or KNOWN_INDICES.get(index_code, index_code)
    dates = [d["date"] for d in daily]
    closes = [d["close"] for d in daily]
    highs = [d["high"] for d in daily]
    lows_price = [d["low"] for d in daily]

    print(f"\n{'='*70}")
    print(f"  {index_name} ({index_code}) 历史数据分析")
    print(f"  数据范围: {dates[0]} → {dates[-1]}  共 {len(dates)} 个交易日")
    print(f"{'='*70}")

    # ── 1. 价格概览 ──
    print(f"\n【1. 价格概览】")
    current = closes[-1]
    all_high = max(closes)
    all_low = min(closes)
    high_date = dates[closes.index(all_high)]
    low_date = dates[closes.index(all_low)]

    print(f"  当前价格:  {current:.2f}")
    print(f"  历史最高:  {all_high:.2f} ({high_date})")
    print(f"  历史最低:  {all_low:.2f} ({low_date})")
    print(f"  当前/最高: {current/all_high*100:.1f}%")
    print(f"  当前/最低: {current/all_low*100:.1f}%")
    print(f"  当前历史分位: {calc_percentile(closes, current):.1f}%")

    # ── 2. 均线分析 ──
    print(f"\n【2. 均线分析】")
    ma_periods = [20, 60, 120, 250]
    for period in ma_periods:
        ma = calc_ma(closes, period)
        if ma[-1] is not None:
            deviation = (current - ma[-1]) / ma[-1] * 100
            status = "▲ 上方" if deviation > 0 else "▼ 下方"
            print(f"  MA{period:>3}: {ma[-1]:>10.2f}  偏离: {deviation:>+6.2f}%  {status}")

    # ── 3. 最大回撤分析 ──
    print(f"\n【3. 最大回撤分析】")
    max_dd, peak_idx, trough_idx = calc_max_drawdown(closes)
    print(f"  历史最大回撤: {max_dd*100:.2f}%")
    print(f"    高点: {closes[peak_idx]:.2f} ({dates[peak_idx]})")
    print(f"    低点: {closes[trough_idx]:.2f} ({dates[trough_idx]})")

    # 近1年最大回撤
    if len(closes) > 250:
        dd_1y, p1, t1 = calc_max_drawdown(closes[-250:])
        offset = len(closes) - 250
        print(f"  近1年最大回撤: {dd_1y*100:.2f}%")
        print(f"    高点: {closes[offset+p1]:.2f} ({dates[offset+p1]})")
        print(f"    低点: {closes[offset+t1]:.2f} ({dates[offset+t1]})")

    # ── 4. 历史低点特征 ──
    print(f"\n【4. 历史低点识别】（回撤>10%的局部低点）")
    local_lows = find_local_lows(dates, closes, window=60, threshold=0.10)
    if local_lows:
        print(f"  {'日期':>12}  {'低点价格':>10}  {'前高':>10}  {'回撤':>8}")
        print(f"  {'-'*45}")
        for low in local_lows:
            print(f"  {low['date']:>12}  {low['price']:>10.2f}  "
                  f"{low['recent_high']:>10.2f}  {low['drawdown']:>7.2f}%")
    else:
        print("  未找到显著低点（可能数据量不足或近期波动较小）")

    # ── 5. 波动率分析 ──
    print(f"\n【5. 波动率分析】")
    vol = calc_volatility(closes, window=20)
    valid_vol = [v for v in vol if v is not None]
    if valid_vol:
        current_vol = vol[-1]
        avg_vol = statistics.mean(valid_vol)
        print(f"  当前20日年化波动率: {current_vol:.2f}%")
        print(f"  历史平均波动率:     {avg_vol:.2f}%")
        print(f"  波动率分位:         {calc_percentile(valid_vol, current_vol):.1f}%")
        # 高波动率往往对应市场底部附近
        high_vol_threshold = avg_vol + statistics.stdev(valid_vol)
        print(f"  高波动阈值(均值+1σ): {high_vol_threshold:.2f}%")
        if current_vol > high_vol_threshold:
            print(f"  ⚠ 当前波动率偏高，可能处于恐慌/底部区域")

    # ── 6. 估值分析 ──
    if valuation:
        print(f"\n【6. 估值分析】（数据来源: {valuation[0].get('source', 'N/A')}）")
        pe_list = [v["pe"] for v in valuation if v.get("pe")]
        pb_list = [v["pb"] for v in valuation if v.get("pb")]
        dy_list = [v["dividend_yield"] for v in valuation if v.get("dividend_yield")]
        pe_dates = [v["date"] for v in valuation if v.get("pe")]
        dy_dates = [v["date"] for v in valuation if v.get("dividend_yield")]

        if pe_list:
            current_pe = pe_list[-1]
            print(f"\n  [PE 市盈率]")
            print(f"    样本区间: {pe_dates[0]} → {pe_dates[-1]}  共 {len(pe_list)} 条")
            print(f"    当前:   {current_pe:.2f}")
            print(f"    最高:   {max(pe_list):.2f}")
            print(f"    最低:   {min(pe_list):.2f}")
            print(f"    均值:   {statistics.mean(pe_list):.2f}")
            print(f"    中位数: {statistics.median(pe_list):.2f}")
            print(f"    分位数: {calc_percentile(pe_list, current_pe):.1f}%")
            if current_pe <= sorted(pe_list)[int(len(pe_list) * 0.3)]:
                print(f"    ✅ PE处于历史较低水平（<30%分位）")

        if pb_list:
            current_pb = pb_list[-1]
            print(f"\n  [PB 市净率]")
            print(f"    当前:   {current_pb:.2f}")
            print(f"    最高:   {max(pb_list):.2f}")
            print(f"    最低:   {min(pb_list):.2f}")
            print(f"    均值:   {statistics.mean(pb_list):.2f}")
            print(f"    分位数: {calc_percentile(pb_list, current_pb):.1f}%")

        else:
            print(f"\n  [PB 市净率]")
            print(f"    官方当前未提供稳定的长周期 PB 数据")

        if dy_list:
            current_dy = dy_list[-1]
            print(f"\n  [股息率]")
            print(f"    样本区间: {dy_dates[0]} → {dy_dates[-1]}  共 {len(dy_list)} 条")
            print(f"    当前:   {current_dy:.2f}%")
            print(f"    最高:   {max(dy_list):.2f}%")
            print(f"    最低:   {min(dy_list):.2f}%")
            print(f"    均值:   {statistics.mean(dy_list):.2f}%")
            if len(dy_list) >= 120:
                print(f"    分位数: {calc_percentile(dy_list, current_dy):.1f}%")
                # 股息率越高 = 价格越低 = 越有吸引力
                if current_dy >= sorted(dy_list)[int(len(dy_list) * 0.7)]:
                    print(f"    ✅ 股息率处于历史较高水平（>70%分位），具备配置价值")
            else:
                print(f"    提示: 股息率历史样本较短，仅作近期参考，不参与长期低估判断")
    else:
        print(f"\n【6. 估值分析】")
        print(f"  无估值数据，跳过")

    # ── 7. 综合评估 ──
    print(f"\n{'='*70}")
    print(f"【综合评估】")
    print(f"{'='*70}")

    signals = []
    price_pct = calc_percentile(closes, current)
    if price_pct < 30:
        signals.append(f"  🟢 价格处于历史 {price_pct:.0f}% 分位（<30%，偏低）")
    elif price_pct > 70:
        signals.append(f"  🔴 价格处于历史 {price_pct:.0f}% 分位（>70%，偏高）")
    else:
        signals.append(f"  🟡 价格处于历史 {price_pct:.0f}% 分位（中性）")

    # MA250偏离
    ma250 = calc_ma(closes, 250)
    if ma250[-1] is not None:
        dev = (current - ma250[-1]) / ma250[-1] * 100
        if dev < -10:
            signals.append(f"  🟢 低于年线 {abs(dev):.1f}%（深度偏离，历史底部特征）")
        elif dev < 0:
            signals.append(f"  🟡 低于年线 {abs(dev):.1f}%（弱势）")
        else:
            signals.append(f"  🟡 高于年线 {dev:.1f}%")

    # 波动率
    if valid_vol and vol[-1] is not None:
        vol_pct = calc_percentile(valid_vol, vol[-1])
        if vol_pct > 80:
            signals.append(f"  🟢 波动率 {vol[-1]:.1f}% 处于 {vol_pct:.0f}% 分位（恐慌区域，可能见底）")
        elif vol_pct < 20:
            signals.append(f"  🟡 波动率 {vol[-1]:.1f}% 处于 {vol_pct:.0f}% 分位（低波，平静期）")

    # 估值信号
    if valuation:
        pe_vals = [v["pe"] for v in valuation if v.get("pe")]
        dy_vals = [v["dividend_yield"] for v in valuation if v.get("dividend_yield")]
        if pe_vals:
            pe_pct = calc_percentile(pe_vals, pe_vals[-1])
            if pe_pct < 30:
                signals.append(f"  🟢 PE {pe_vals[-1]:.1f} 处于 {pe_pct:.0f}% 分位（低估）")
            elif pe_pct > 70:
                signals.append(f"  🔴 PE {pe_vals[-1]:.1f} 处于 {pe_pct:.0f}% 分位（偏贵）")
        if len(dy_vals) >= 120:
            dy_pct = calc_percentile(dy_vals, dy_vals[-1])
            if dy_pct > 70:
                signals.append(f"  🟢 股息率 {dy_vals[-1]:.2f}% 处于 {dy_pct:.0f}% 分位（高股息，有吸引力）")

    for s in signals:
        print(s)

    green_count = sum(1 for s in signals if "🟢" in s)
    total = len(signals)
    print(f"\n  低估信号: {green_count}/{total}")
    if green_count >= 3:
        print(f"  📊 结论: 多重指标显示当前处于历史低估区域，适合分批配置")
    elif green_count >= 2:
        print(f"  📊 结论: 部分指标显示偏低估，可以开始关注")
    else:
        print(f"  📊 结论: 当前无明显低估信号，建议观望")

    # ── 8. 分批买入建议 ──
    print(f"\n【分批买入参考价位】")
    ma250_val = ma250[-1] if ma250[-1] else current
    levels = [
        ("关注价", ma250_val * 0.95),
        ("首次买入", ma250_val * 0.90),
        ("加仓1", ma250_val * 0.85),
        ("加仓2", ma250_val * 0.80),
        ("重仓", ma250_val * 0.75),
    ]
    print(f"  （基于年线 {ma250_val:.2f} 的偏离度计算）")
    for label, price in levels:
        pct = (price - ma250_val) / ma250_val * 100
        print(f"  {label:>8}: {price:>10.2f}  (年线{pct:>+6.1f}%)")


def main():
    skip_fetch = "--skip-fetch" in sys.argv
    source = "akshare"
    index_code = DEFAULT_INDEX_CODE

    if "--source" in sys.argv:
        idx = sys.argv.index("--source")
        if idx + 1 < len(sys.argv):
            source = sys.argv[idx + 1]

    if "--code" in sys.argv:
        idx = sys.argv.index("--code")
        if idx + 1 < len(sys.argv):
            index_code = sys.argv[idx + 1]

    if not skip_fetch:
        try:
            daily_count, val_count = fetch_and_store(source=source, index_code=index_code)
            if daily_count == 0:
                print("\n⚠ 未获取到行情数据")
                # 尝试另一个源
                alt_source = "csi" if source == "akshare" else "akshare"
                print(f"  尝试切换到 {alt_source} 源...")
                daily_count, val_count = fetch_and_store(source=alt_source, index_code=index_code)
                if daily_count == 0:
                    print("  两个源均失败，尝试分析已有数据...")
        except Exception as e:
            print(f"\n⚠ 数据获取异常: {e}")
            print("  尝试分析已有数据...")

    analyze(index_code=index_code)


if __name__ == "__main__":
    main()
