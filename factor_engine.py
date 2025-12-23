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
        corr_price_turn_1M = get_vals('corr_price_turn_1M')
        corr_price_turn_6M = get_vals('corr_price_turn_6M')
        corr_rety_turn_6M = get_vals('corr_rety_turn_6M')
        liq_turn_std_6M = get_vals('liq_turn_std_6M')
        corr_rety_turn_post_6M = get_vals('corr_rety_turn_post_6M')
        mmt_range_M = get_vals('mmt_range_M')
        mmt_normal_M = get_vals('mmt_normal_M')
        # E_Growth2 = get_vals('E_Growth2') # 示例
        
        # -----------------------------------------------------------
        # [策略逻辑区域] 修改此处公式
        # -----------------------------------------------------------
        
        # 注意处理 NaN 值，如果因子有空值，比较运算可能会产生 False 或 Warning
        
        # 示例逻辑：
        stock_pool = (
            (1 * (Alpha95 <= mquantiles(Alpha95, 0.6))) 
            + (1 * (Alpha100 <= mquantiles(Alpha100, 0.7)))
            - (1 * (corr_price_turn_1M >= mquantiles(corr_price_turn_1M, 0.8)))
            - (1 * (corr_rety_turn_6M >= mquantiles(corr_rety_turn_6M, 0.8)))
            - (1 * (corr_rety_turn_post_6M >= mquantiles(corr_rety_turn_post_6M, 0.8)))
            - (1 * (mmt_normal_M >= mquantiles(mmt_normal_M, 0.9)))
        ) / 2

      
        
        
        final_score = stock_pool
        
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