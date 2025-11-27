import pandas as pd
import numpy as np
from config import Config

class PortfolioOptimizer:
    
    @staticmethod
    def check_industry(data, threshold, turns=3):
        """
        行业中性化约束 (移植自 check_industry.py)
        通过迭代调整权重，使行业暴露控制在 threshold 范围内
        """
        df = data.copy()

        # 总股票数
        stk_num = df['SecuCode'].count()

        # 计算每个行业的股票占比 (基准)
        total = df['Industry'].value_counts().reset_index()
        total.columns = ['Industry', 'total_ratio']
        total['total_ratio'] = total['total_ratio'] / stk_num

        for _ in range(turns):
            # 计算当前选股的行业权重分布
            selected = df[df['selected']==1].groupby('Industry')['weight'].sum().reset_index(name='selected_weight')

            # 合并统计
            industry_port = total.merge(selected, on='Industry', how='left')
            industry_port['selected_weight'] = industry_port['selected_weight'].fillna(0)

            # 检查是否满足收敛条件
            if all(abs(industry_port['selected_weight'] - industry_port['total_ratio']) < threshold + 1e-5):
                break

            # 计算超额/低额比例
            industry_port['over_ratio'] = industry_port['selected_weight'] - industry_port['total_ratio'] - threshold
            industry_port['less_ratio'] = industry_port['selected_weight'] - industry_port['total_ratio'] + threshold
            
            # 标记各类型行业
            over_industry = industry_port[industry_port['over_ratio'] > 1e-5]['Industry']
            over_mask = industry_port['Industry'].isin(over_industry)
            over_sum = industry_port.loc[over_mask, 'over_ratio'].sum()

            less_industry = industry_port[(industry_port['less_ratio'] < -1e-5) & (industry_port['selected_weight'] != 0)]['Industry']
            less_mask = industry_port['Industry'].isin(less_industry)
            less_sum = industry_port.loc[less_mask, 'less_ratio'].sum()

            zero_industry = industry_port[(industry_port['less_ratio'] < -1e-5) & (industry_port['selected_weight'] == 0)]['Industry']
            zero_mask = industry_port['Industry'].isin(zero_industry)
            zero_sum = industry_port.loc[zero_mask, 'less_ratio'].sum()

            other_sum = industry_port.loc[(~over_mask) & (~less_mask) & (~zero_mask), 'selected_weight'].sum()

            # --- 调整权重 ---
            df = df.merge(industry_port[['Industry', 'selected_weight', 'total_ratio']], on='Industry', how='left')

            selected_mask = df['selected'] == 1
            idx_over_mask = df['Industry'].isin(over_industry)
            idx_less_mask = df['Industry'].isin(less_industry)
            idx_zero_mask = df['Industry'].isin(zero_industry)

            # 1. 调低超配行业
            mask1 = idx_over_mask & selected_mask
            df.loc[mask1, 'weight'] = (df.loc[mask1, 'weight'] * (df.loc[mask1, 'total_ratio'] + threshold)) / df.loc[mask1, 'selected_weight']
            
            # 2. 调高低配行业
            mask2 = idx_less_mask & selected_mask
            df.loc[mask2, 'weight'] = (df.loc[mask2, 'weight'] * (df.loc[mask2, 'total_ratio'] - threshold)) / df.loc[mask2, 'selected_weight']
            
            # 3. 填补零配行业 (如果有 NextIndexTrade 的话)
            mask3 = idx_zero_mask & (df['NextIndexTrade'] == 1)
            zero_num = df.loc[mask3, 'weight'].count()
            if zero_num > 0:
                df.loc[mask3, 'weight'] = (df.loc[mask3, 'total_ratio'] - threshold) / zero_num
            
            # 4. 调整其他行业 (归一化剩余权重)
            mask4 = (~idx_over_mask) & (~idx_less_mask) & (~idx_zero_mask) & selected_mask
            if other_sum != 0:
                factor = (over_sum + less_sum + zero_sum + other_sum) / other_sum
                df.loc[mask4, 'weight'] = df.loc[mask4, 'weight'] * factor
            
            # 清理临时列
            df = df.drop(columns=['selected_weight', 'total_ratio'])
        
        return df['weight']

    @staticmethod
    def adjust_untradable(df):
        """
        处理停牌/无法交易的股票 (移植自 adjust_untradable.py)
        不可交易股票继承上一日权重，其他股票按比例缩放
        """
        trading_days = sorted(df['TradingDay'].unique())
        df['last_weight'] = 0.0
        
        # 注意：这里需要在内存中按顺序处理，无法简单的 groupby apply
        # 为了保持逻辑一致性，这里采用按日循环 (效率稍低但逻辑最安全)
        
        # 我们需要先构建一个完整的 DataFrame 包含所有 days 以便 shift，或者按列表循环
        # 鉴于数据量，按列表循环是可行的
        
        # 预先将数据按 (TradingDay, SecuCode) 索引，方便快速查找
        # 但 df 已经是长表，直接操作可能较慢。优化方案：
        # 使用 pivot 转换权重表 -> 处理 -> stack 回来
        
        print(">>> [Portfolio] 开始计算权重继承 (Pivot方法)...")
        
        # 1. Pivot 展开权重和状态
        weight_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='weight').fillna(0)
        swing_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='SwingStatus').fillna(0)
        selected_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='selected').fillna(0)
        
        # 确保 trading_days 顺序
        weight_pivot = weight_pivot.sort_index()
        swing_pivot = swing_pivot.sort_index()
        selected_pivot = selected_pivot.sort_index()
        
        # 2. 循环处理
        # 这里的逻辑是：如果 SwingStatus==0 (不可交易)，则 weight = last_weight
        # 否则 weight = weight * (1 - sum(untradable_weights))
        
        # 将结果存储在 numpy array 中加速
        w_vals = weight_pivot.values
        s_vals = swing_pivot.values # 1=可交易, 0=不可交易
        sel_vals = selected_pivot.values
        
        # 获取列名对应的索引
        # n_days, n_stocks = w_vals.shape
        
        for i in range(1, len(trading_days)):
            # 上一日权重
            last_w = w_vals[i-1, :]
            
            # 当日状态
            curr_swing = s_vals[i, :] # 0为不可交易
            curr_sel = sel_vals[i, :]
            
            # 找出不可交易的股票索引
            untradable_idx = (curr_swing == 0)
            
            # 1. 继承权重
            # 将不可交易股票的权重强制设为上一日权重
            # 注意：原逻辑中，只有 SwingStatus==0 的才继承
            w_vals[i, untradable_idx] = last_w[untradable_idx]
            
            # 2. 计算不可交易占用的总权重
            total_untradable_w = np.sum(w_vals[i, untradable_idx])
            
            # 3. 调整可交易且被选中的股票权重
            # 原逻辑：df.loc[mask & (df['selected']==1), 'weight'] *= (1-total_untradable_weight)
            # 这里要注意，如果 w_vals[i] 还是初始权重 (sum=1)，则直接缩放
            # 如果 w_vals[i] 已经被 industry_constraint 处理过 (sum=1)，也直接缩放
            
            # 仅对 selected=1 且可交易的股票进行缩放
            adjustable_idx = (curr_sel == 1) & (curr_swing == 1)
            
            # 为了防止除以0或逻辑错误，确保 sum <= 1
            if total_untradable_w > 1.0: total_untradable_w = 1.0
            
            scaling_factor = 1.0 - total_untradable_w
            w_vals[i, adjustable_idx] *= scaling_factor
            
        # 3. 还原回长表
        new_weight_df = pd.DataFrame(w_vals, index=weight_pivot.index, columns=weight_pivot.columns)
        new_weight_stack = new_weight_df.stack().reset_index(name='adjusted_weight')
        
        # 合并回原 df
        df = pd.merge(df, new_weight_stack, on=['TradingDay', 'SecuCode'], how='left')
        df['weight'] = df['adjusted_weight'].fillna(0)
        df = df.drop(columns=['adjusted_weight', 'last_weight'], errors='ignore')
        
        return df

    @staticmethod
    def construct(scored_df):
        """组合构建主流程"""
        print(">>> [Portfolio] 开始构建组合...")
        df = scored_df.copy()
        
        # 1. 基础筛选: 计算 NextIndexTrade
        # 条件：TradeStatus=1 & SwingStatus=1 & StopStatus3=1 & StopStatus5=0 & IpoStatus=1
        df['NextIndexTrade'] = 0
        condition = (
            (df['TradeStatus'] == 1) &
            (df['SwingStatus'] == 1) &
            (df['StopTradeStatus3'] == 1) &
            (df['StopTradeStatus5'] == 0) &
            (df['IpoStatus'] == 1)
        )
        df.loc[condition, 'NextIndexTrade'] = 1
        
        # 2. 选中股票
        # 条件：FactorScore >= 1 且 NextIndexTrade == 1
        df['selected'] = 0
        sel_mask = (df['factor_score'] >= 1) & (df['NextIndexTrade'] == 1)
        df.loc[sel_mask, 'selected'] = 1
        
        # 3. 初始等权
        print(">>> [Portfolio] 计算初始等权...")
        def _init_equal_weight(group):
            # 仅在被选中的股票中分配
            selected_mask = group['selected'] == 1
            count = selected_mask.sum()
            w = pd.Series(0.0, index=group.index)
            if count > 0:
                w[selected_mask] = 1.0 / count
            return w

        df['weight'] = df.groupby('TradingDay', group_keys=False).apply(_init_equal_weight)
        
        # 4. 行业中性化
        print(f">>> [Portfolio] 执行行业中性化约束 (Tol={Config.INDUSTRY_TOL})...")
        weights = df.groupby('TradingDay', group_keys=False).apply(
            lambda x: PortfolioOptimizer.check_industry(x, Config.INDUSTRY_TOL)
        )
        # 注意：apply 返回的是 Series，索引对齐即可
        df['weight'] = weights
        
        # 5. 不可交易调整 (全时间序列处理)
        print(">>> [Portfolio] 调整不可交易股票仓位...")
        df = PortfolioOptimizer.adjust_untradable(df)
        
        # 6. 保存持仓结果
        filename = f"Portfolio_{Config.STOCK_POOL}_{Config.SIGN}.csv"
        save_path = Config.DIR_PORTFOLIO / filename
        print(f"保存每日持仓: {save_path}")
        df.to_csv(str(save_path), index=False, encoding='utf_8_sig')
        
        return df