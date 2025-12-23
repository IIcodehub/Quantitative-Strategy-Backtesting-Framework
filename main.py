import pandas as pd
import time
from config import Config
from utils import get_config_identifier, format_secucode
from data_loader import DataLoader
from factor_engine import FactorEngine
from portfolio import PortfolioOptimizer
from analysis import PerformanceAnalyzer

class BacktestRunner:
    def __init__(self):
        Config.initialize_directories()
        self.loader = DataLoader()
        self.identifier = get_config_identifier()
        print(f"\n{'='*40}")
        print(f"回测启动: {Config.SIGN}")
        print(f"配置哈希: {self.identifier}")
        print(f"强制重跑: {Config.FORCE_RERUN}") # 提示当前状态
        print(f"股票池: {Config.STOCK_POOL} | 额外因子: {Config.ADDITIONAL_FACTORS}")
        print(f"{'='*40}\n")

    def run(self):
        t0 = time.time()
        
        try:
            status_df = self.loader.load_stock_status()
            returns_df = self.loader.load_returns()
        except Exception as e:
            print(f"数据加载失败: {e}")
            return
        
        if status_df.empty:
            print("错误: 筛选后的股票池为空。")
            return
        
        years = sorted(status_df['Year'].unique())
        all_scores = []
        
        print(f"即将处理年份: {years}")
        
        for year in years:
            cache_filename = f"score_{year}_{self.identifier}.csv"
            cache_path = Config.DIR_CACHE / cache_filename
            
            year_score = pd.DataFrame()
            
            # [关键修改] 加入 FORCE_RERUN 判断
            if not Config.FORCE_RERUN and cache_path.exists():
                print(f"[{year}] 命中缓存: {cache_filename}")
                year_score = pd.read_csv(str(cache_path))
                year_score['TradingDay'] = pd.to_datetime(year_score['TradingDay'])
                year_score['SecuCode'] = year_score['SecuCode'].apply(format_secucode)
            else:
                if Config.FORCE_RERUN:
                    print(f"[{year}] 强制重算 (忽略缓存)...")
                
                year_status = status_df[status_df['Year'] == year]
                factor_df = self.loader.load_year_factors(year)
                
                if factor_df is None or factor_df.empty:
                    print(f"[{year}] 无因子数据，跳过")
                    continue
                    
                print(f"[{year}] 合并基础数据...")
                combined = pd.merge(year_status, factor_df, on=['TradingDay','SecuCode'], how='left')
                
                # 调用额外因子合并
                combined = self.loader.merge_additional_factors(combined, year)
                
                # 计算得分
                year_score = FactorEngine.run_scoring_for_year(combined, year)
                
                # 写入缓存
                if not year_score.empty:
                    print(f"[{year}] 写入缓存: {cache_filename}")
                    year_score.to_csv(str(cache_path), index=False, encoding='utf_8_sig')
            
            if not year_score.empty:
                all_scores.append(year_score)
            
        if not all_scores:
            print("错误: 未能生成有效数据。")
            return
            
        print("\n>>> 合并全样本数据...")
        full_df = pd.concat(all_scores, ignore_index=True)
        full_df = pd.merge(full_df, returns_df, on=['TradingDay', 'SecuCode'], how='left')
        
        # 组合构建
        port_df = PortfolioOptimizer.construct(full_df)
        
        # 绩效分析
        metrics = PerformanceAnalyzer.analyze(port_df)
        
        summary_file = Config.DIR_REPORTS / f"Summary_{Config.SIGN}.csv"
        pd.DataFrame([metrics]).to_csv(str(summary_file), index=False, encoding='utf_8_sig')
        
        print(f"\n{'='*40}")
        print(f"回测完成! 总耗时: {time.time()-t0:.2f}s")
        print(f"年化收益: {metrics.get('RY', 0):.2%}")
        print(f"指标文件: {summary_file}")

if __name__ == "__main__":
    runner = BacktestRunner()
    runner.run()