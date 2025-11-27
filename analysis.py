import pandas as pd
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from config import Config

class PerformanceAnalyzer:
    @staticmethod
    def plot_performance(profit_df, output_path):
        """
        绘制累积收益曲线并保存为PDF
        """

        plt.style.use('ggplot')
        
        fig, ax = plt.subplots(figsize=(12, 7))
        
        dates = profit_df['TradingDay']
        strategy_cum = profit_df['cum_return']
        
        # 绘制策略曲线
        ax.plot(dates, strategy_cum, label=f'Strategy ({Config.STOCK_POOL})', color='#d62728', linewidth=2)
        
        # 如果有基准收益，计算并绘制基准曲线
        if 'baseline_return_rate' in profit_df.columns:
            baseline_cum = profit_df['baseline_return_rate'].fillna(0).cumsum()
            ax.plot(dates, baseline_cum, label='Benchmark (Equal Weight)', color='#1f77b4', linestyle='--', linewidth=1.5, alpha=0.8)
        
        # 绘制超额收益 (可选，使用副坐标或直接画)
        
        ax.set_title(f"Cumulative Return Analysis - {Config.SIGN}", fontsize=14)
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Cumulative Return", fontsize=12)
        ax.legend(loc='upper left', fontsize=10)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        print(f"保存收益曲线图: {output_path}")
        plt.savefig(str(output_path), format='pdf', bbox_inches='tight')
        plt.close(fig)

    @staticmethod
    def analyze(data):
        """
        计算回测指标并生成图表
        """
        print(">>> [Analysis] 开始计算绩效指标...")
        df = data.copy()
        
        ret_col = 'ret_open5twap' if Config.RET_IDX == 'open5twap' else 'ret_c2c'
        
        df['TradingDay'] = pd.to_datetime(df['TradingDay'])
        
        # 1. 计算个股贡献收益
        df['stock_return'] = df['weight'] * df[ret_col]
        df['stock_return'] = df['stock_return'].fillna(0)
        
        # 2. 计算每日组合毛收益 (net_return_rate)
        profit = df.groupby('TradingDay')['stock_return'].sum().reset_index(name='net_return_rate')
        
        # 3. 计算换手率与手续费
        df = df.sort_values(['SecuCode', 'TradingDay'])
        
        # 使用 pivot table 计算换手率更准确
        w_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='weight').fillna(0)
        # 单边换手率 = sum(|w_t - w_{t-1}|) / 2
        w_diff = w_pivot.diff().abs().sum(axis=1) / 2 
        
        turnover_df = w_diff.reset_index(name='turn_over')
        # 第一天设为 0.5 (假设从0建仓)
        if not turnover_df.empty:
            turnover_df.loc[0, 'turn_over'] = 0.5
            
        profit = profit.merge(turnover_df, on='TradingDay', how='left')
        
        # 扣费后收益 (origin_profit)
        profit['origin_profit'] = profit['net_return_rate'] - profit['turn_over'] * Config.FEE_RATE
        
        # 4. 计算 Baseline (基准) - 股票池内等权
        # 筛选出当日有收益数据的股票作为有效池
        valid_pool = df[df[ret_col].notna()].copy()
        # 每日股票数量
        pool_counts = valid_pool.groupby('TradingDay').size()
        # 每日总收益
        pool_sums = valid_pool.groupby('TradingDay')[ret_col].sum()
        
        baseline_ret = pool_sums / pool_counts
        baseline_df = baseline_ret.reset_index(name='baseline_return_rate')
        
        profit = profit.merge(baseline_df, on='TradingDay', how='left')
        
        # 5. 超额收益 (Active Return)
        profit['profit'] = profit['origin_profit'] - profit['baseline_return_rate']
        
        # 6. 辅助列：每日持仓股票数
        stock_num = df[df['weight'] > 0.005].groupby('TradingDay').size().reset_index(name='stock_num')
        profit = profit.merge(stock_num, on='TradingDay', how='left').fillna(0)
        
        # 7. 计算累计收益与回撤
        profit['cum_return'] = profit['profit'].cumsum()
        profit['cummax'] = profit['cum_return'].cummax()
        profit['drawdown'] = profit['cummax'] - profit['cum_return']
        
        # 8. 计算回撤持续天数
        high_idx = profit[profit['cummax'] != profit['cummax'].shift(1)].index
        last_high = pd.Series(index=profit.index, dtype=float)
        last_high.loc[high_idx] = high_idx
        last_high = last_high.ffill()
        profit['drawdown_day'] = profit.index - last_high
        
        # 9. 保存每日明细 CSV
        filename_detail = f"Profit_Detail_{Config.STOCK_POOL}_{Config.SIGN}.csv"
        path_detail = Config.DIR_REPORTS / filename_detail
        print(f"保存每日净值CSV: {path_detail}")
        profit.to_csv(str(path_detail), index=False, encoding='utf_8_sig')

        # ==========================================
        # 10. 新增：绘制并保存 PDF 图表
        # ==========================================
        filename_chart = f"Chart_{Config.STOCK_POOL}_{Config.SIGN}.pdf"
        path_chart = Config.DIR_REPORTS / filename_chart
        PerformanceAnalyzer.plot_performance(profit, path_chart)
        # ==========================================
        
        # 11. 计算汇总指标
        days_per_year = 242
        mean_ret = profit['profit'].mean()
        std_ret = profit['profit'].std(ddof=1)
        
        annul_return = mean_ret * days_per_year
        if std_ret == 0:
            ir1 = 0
            ir2 = 0
        else:
            ir1 = annul_return / (std_ret * math.sqrt(days_per_year))
            mdd_val = profit['drawdown'].max()
            ir2 = (annul_return - mdd_val / 3) / (std_ret * math.sqrt(days_per_year))
            
        metrics = {
            'RY': annul_return,
            'IR': ir1,
            'IR2': ir2,
            'AvgTurnOver': profit['turn_over'].mean(),
            'MDD': profit['drawdown'].max(),
            'MDDayN': profit['drawdown_day'].max(),
            'AvgPortN': profit['stock_num'].mean()
        }
        
        # 添加分年度收益
        profit['Year'] = profit['TradingDay'].dt.year
        annual_rets = profit.groupby('Year')['profit'].sum().to_dict()
        for y, r in annual_rets.items():
            metrics[f'R{y}'] = r
            
        return metrics