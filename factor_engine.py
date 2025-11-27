import pandas as pd
import numpy as np
from config import Config
from utils import format_secucode, mquantiles

class FactorEngine:
    @staticmethod
    def calculate_score(df):
        """
        核心打分逻辑
        df: 当日截面数据
        return: Series (index=df.index, value=score)
        """
        # 1. 安全获取因子数据，防止列不存在报错
        def get_vals(col_name):
            if col_name in df.columns:
                return df[col_name].values
            else:
                # 如果缺少因子，返回全NaN或0，视策略容忍度而定
                return np.full(len(df), np.nan)

        # 提取因子变量 (根据你的策略需要添加)
        Alpha95 = get_vals('Alpha95')
        Alpha100 = get_vals('Alpha100')
        # E_Growth2 = get_vals('E_Growth2') # 示例
        
        # -----------------------------------------------------------
        # [策略逻辑区域] 修改此处公式
        # 下面使用 HighERP LB800 的逻辑作为示例
        # -----------------------------------------------------------
        
        # 注意处理 NaN 值，如果因子有空值，比较运算可能会产生 False 或 Warning
        # 这里假设数据质量尚可，直接运算
        
        # 示例逻辑：
        # stock_pool = (
        #     (1 * (Alpha95 <= mquantiles(Alpha95, 0.7))) +
        #     (1 * (Alpha100 <= mquantiles(Alpha100, 0.7)))
        # ) / 2
        
        score_component_1 = (1 * (Alpha95 <= mquantiles(Alpha95, 0.7)))
        score_component_2 = (1 * (Alpha100 <= mquantiles(Alpha100, 0.7)))
        
        final_score = (score_component_1 + score_component_2) / 2
        
        # -----------------------------------------------------------
        
        return pd.Series(final_score, index=df.index)

    @staticmethod
    def run_scoring_for_year(year_df, year):
        """处理单年数据并计算得分"""
        print(f"正在计算 {year} 年因子得分...")
        results = []
        
        # 按天分组
        for day, group in year_df.groupby('TradingDay'):
            score = FactorEngine.calculate_score(group)
            
            res = pd.DataFrame({
                'TradingDay': day,
                'SecuCode': group['SecuCode'],
                'factor_score': score.values
            })
            
            # 保留必要的元数据列
            meta_cols = ['Industry', 'TradeStatus', 'SwingStatus', 
                         'StopTradeStatus3', 'StopTradeStatus5', 'IpoStatus']
            for col in meta_cols:
                if col in group.columns:
                    res[col] = group[col].values
            
            results.append(res)
            
        if not results:
            return pd.DataFrame()
        
        full_df = pd.concat(results, ignore_index=True)
        # 格式化股票代码
        full_df['SecuCode'] = full_df['SecuCode'].apply(format_secucode)
        return full_df