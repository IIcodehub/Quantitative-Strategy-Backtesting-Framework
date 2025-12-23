import pandas as pd
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from config import Config

class PerformanceAnalyzer:
    @staticmethod
    def plot_performance(profit_df, metrics, output_path):
        """
        绘制:
        1. 累计净值 (Net Strategy)
        2. 基准 (Benchmark)
        3. 累计超额收益 (Cumulative Excess)
        右侧显示: 核心指标 + 年度双列数据(Net/Excess)
        """
        # 设置全局字体风格
        plt.style.use('ggplot')
        
        # 创建画布 (宽:高 = 14:8)
        fig = plt.figure(figsize=(14, 8)) 
        
        # 布局: 左侧 3列画图，右侧 1列写字
        gs = fig.add_gridspec(1, 4)
        
        # === 左侧: 绘图区域 ===
        ax_plot = fig.add_subplot(gs[0, :3])
        
        dates = profit_df['TradingDay']
        
        # 1. 数据准备
        net_cum = profit_df['origin_profit'].fillna(0).cumsum()
        
        # 2. 绘制基准 (Benchmark)
        if 'baseline_return_rate' in profit_df.columns:
            baseline_cum = profit_df['baseline_return_rate'].fillna(0).cumsum()
            ax_plot.plot(dates, baseline_cum, label='Benchmark (Index)', color='gray', linestyle='--', linewidth=1.5, alpha=0.7)
        
        # 3. 绘制累计超额收益 (Cumulative Excess)
        if 'profit' in profit_df.columns:
            excess_cum = profit_df['profit'].fillna(0).cumsum()
            ax_plot.plot(dates, excess_cum, label='Cum. Excess Return', color='#1f77b4', linewidth=1.5, alpha=0.9)
            ax_plot.fill_between(dates, 0, excess_cum, color='#1f77b4', alpha=0.1)

        # 4. 绘制策略净值 (主线)
        ax_plot.plot(dates, net_cum, label=f'Net Strategy ({Config.STOCK_POOL})', color='#d62728', linewidth=2.5)
        
        # 格式设置
        ax_plot.set_title(f"Backtest Report: {Config.SIGN}", fontsize=16, fontweight='bold', pad=20)
        ax_plot.set_ylabel("Cumulative Return", fontsize=12)
        ax_plot.set_xlabel("Date", fontsize=12)
        
        # 图例
        ax_plot.legend(loc='upper left', fontsize=10, frameon=True, facecolor='white', edgecolor='lightgray')
        ax_plot.grid(True, which='major', linestyle='--', linewidth=0.5, color='gray', alpha=0.5)
        
        # 日期轴设置
        ax_plot.xaxis.set_major_locator(mdates.YearLocator())
        ax_plot.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.setp(ax_plot.get_xticklabels(), rotation=45, ha='right')

        # === 右侧: 数据展示区域 ===
        ax_text = fig.add_subplot(gs[0, 3])
        ax_text.axis('off') 
        
        # 1. 准备全局指标文本
        lines = [
            "─── Summary (Net) ───",
            f"Ann. Return:  {metrics.get('RY', 0):>8.2%}",
            f"Max Drawdown: {metrics.get('MDD', 0):>8.2%}",
            f"Avg Turnover: {metrics.get('AvgTurnOver', 0):>8.2%}",
            f"Avg Stock Num:{int(metrics.get('AvgPortN', 0)):>8d}",
            "",
            "─── Alpha Metrics ───",
            f"Excess Ret:   {metrics.get('AnnExcess', 0):>8.2%}",
            f"Info Ratio:   {metrics.get('IR', 0):>8.2f}",
            f"Win Rate:     {metrics.get('WinRate', 0):>8.2%}",
            "",
            "── Annual Performance ──",
            # 表头
            f"{'Year':<6} {'NetRet':>7} {'Excess':>7}"
        ]
        
        # 2. 准备年度 表格 数据
        years = sorted(list(set([k[2:] for k in metrics.keys() if k.startswith('NR') or k.startswith('ER')])))
        
        for y in years:
            net_val = metrics.get(f'NR{y}', 0)
            exc_val = metrics.get(f'ER{y}', 0)
            # 格式: 2016   15.2%   5.1%
            lines.append(f"{y:<6} {net_val:>7.1%} {exc_val:>7.1%}")

        # 3. 逐行绘制
        y_pos = 0.98
        line_height = 0.045
        
        for line in lines:
            font_weight = 'normal'
            font_size = 11
            color = 'black'
            
            # 标题行样式
            if "──" in line:
                font_weight = 'bold'
                font_size = 12
                color = '#333333'
            # 表头行样式
            elif "Year" in line:
                font_weight = 'bold'
                color = '#555555'
     
            # 数据行逻辑
            else:
                if ":" in line:
                    pass 
                elif line.strip() and line.strip()[0].isdigit():
                    pass

            ax_text.text(0.05, y_pos, line, 
                        transform=ax_text.transAxes, 
                        fontsize=font_size, 
                        fontweight=font_weight,
                        fontfamily='monospace',
                        color=color,
                        verticalalignment='top')
            
            y_pos -= line_height

        plt.tight_layout()
        print(f"保存带指标的收益图: {output_path}")
        plt.savefig(str(output_path), format='pdf', bbox_inches='tight')
        plt.close(fig)

    @staticmethod
    def analyze(data):
        """
        计算回测指标并生成图表
        """
        print(">>> [Analysis] 开始计算绩效指标...")
        df = data.copy()
        
        # 确定收益列
        ret_col = 'ret_open5twap' if Config.RET_IDX == 'open5twap' else 'ret_c2c'
        
        df['TradingDay'] = pd.to_datetime(df['TradingDay'])
        
        # 1. 个股收益
        df['stock_return'] = df['weight'] * df[ret_col]
        df['stock_return'] = df['stock_return'].fillna(0)
        
        # 2. 组合毛收益
        profit = df.groupby('TradingDay')['stock_return'].sum().reset_index(name='net_return_rate')
        
        # 3. 换手率与费率 (Pivot方法)
        df = df.sort_values(['SecuCode', 'TradingDay'])
        w_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='weight').fillna(0)
        w_diff = w_pivot.diff().abs().sum(axis=1) / 2 
        
        turnover_df = w_diff.reset_index(name='turn_over')
        if not turnover_df.empty:
            turnover_df.loc[0, 'turn_over'] = 0.5 
            
        profit = profit.merge(turnover_df, on='TradingDay', how='left')
        
        # origin_profit = 策略净收益 (Net Return)
        profit['origin_profit'] = profit['net_return_rate'] - profit['turn_over'] * Config.FEE_RATE
        
        # 4. Baseline
        valid_pool = df[df[ret_col].notna()].copy()
        pool_counts = valid_pool.groupby('TradingDay').size()
        pool_sums = valid_pool.groupby('TradingDay')[ret_col].sum()
        baseline_ret = pool_sums / pool_counts
        baseline_df = baseline_ret.reset_index(name='baseline_return_rate')
        profit = profit.merge(baseline_df, on='TradingDay', how='left')
        
        # profit = 超额收益 (Excess Return)
        profit['profit'] = profit['origin_profit'] - profit['baseline_return_rate']
        
        # 5. 辅助列
        stock_num = df[df['weight'] > 0].groupby('TradingDay').size().reset_index(name='stock_num')
        profit = profit.merge(stock_num, on='TradingDay', how='left').fillna(0)
        
        # 6. 回撤计算 (基于净收益)
        profit['cum_net_val'] = (1 + profit['origin_profit']).cumprod()
        profit['cummax_net'] = profit['cum_net_val'].cummax()
        profit['drawdown_net'] = profit['cummax_net'] - profit['cum_net_val']
        profit['drawdown_pct'] = profit['drawdown_net'] / profit['cummax_net']
        
        # 回撤天数
        high_idx = profit[profit['cummax_net'] != profit['cummax_net'].shift(1)].index
        last_high = pd.Series(index=profit.index, dtype=float)
        last_high.loc[high_idx] = high_idx
        last_high = last_high.ffill()
        profit['drawdown_day'] = profit.index - last_high
        
        # ==========================================
        # 7. 计算汇总指标
        # ==========================================
        days_per_year = 242
        
        # --- A. 绝对收益指标 ---
        mean_net_ret = profit['origin_profit'].mean()
        std_net_ret = profit['origin_profit'].std(ddof=1)
        
        annul_return = mean_net_ret * days_per_year 
        
        # Sharpe (虽然不展示，但保留计算以免其他地方需要)
        sharpe = (annul_return - 0) / (std_net_ret * math.sqrt(days_per_year)) if std_net_ret > 0 else 0
        
        max_dd = profit['drawdown_pct'].max()
        
        # --- B. 相对收益指标 ---
        mean_excess = profit['profit'].mean()
        std_excess = profit['profit'].std(ddof=1)
        
        annul_excess = mean_excess * days_per_year
        ir = annul_excess / (std_excess * math.sqrt(days_per_year)) if std_excess > 0 else 0
        
        win_rate = (profit['profit'] > 0).mean()

        metrics = {
            'RY': annul_return,
            'MDD': max_dd,
            'Sharpe': sharpe, 
            'AvgTurnOver': profit['turn_over'].mean(),
            'MDDayN': profit['drawdown_day'].max(),
            'AvgPortN': profit['stock_num'].mean(),
            'AnnExcess': annul_excess,
            'IR': ir,
            'WinRate': win_rate
        }
        
        # --- C. 分年度统计 ---
        profit['Year'] = profit['TradingDay'].dt.year
        
        # 1. 年度净收益 -> NRxxxx
        annual_net = profit.groupby('Year')['origin_profit'].sum().to_dict()
        for y, r in annual_net.items():
            metrics[f'NR{y}'] = r
            
        # 2. 年度超额收益 -> ERxxxx
        annual_excess = profit.groupby('Year')['profit'].sum().to_dict()
        for y, r in annual_excess.items():
            metrics[f'ER{y}'] = r

        # ==========================================
        # 8. 保存结果
        # ==========================================
        filename_detail = f"Profit_Detail_{Config.STOCK_POOL}_{Config.SIGN}.csv"
        path_detail = Config.DIR_REPORTS / filename_detail
        profit.to_csv(str(path_detail), index=False, encoding='utf_8_sig')

        filename_chart = f"Chart_{Config.STOCK_POOL}_{Config.SIGN}.pdf"
        path_chart = Config.DIR_REPORTS / filename_chart
        
        PerformanceAnalyzer.plot_performance(profit, metrics, path_chart)

        return metrics