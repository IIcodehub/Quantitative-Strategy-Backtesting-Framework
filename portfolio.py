
import pandas as pd
import numpy as np
from config import Config

class PortfolioOptimizer:
    
    @staticmethod
    def check_industry(data, threshold, turns=3):
        """
        行业中性化约束 (修复版：使用 map 代替 merge 以防止索引丢失)
        """
        df = data.copy()

        # 总股票数
        stk_num = df['SecuCode'].count()

        # 计算每个行业的股票占比 (基准)
        # 注意：如果某天stk_num为0，这里会报错，加个判断
        if stk_num == 0:
            return df['weight']

        total = df['Industry'].value_counts().reset_index()
        total.columns = ['Industry', 'total_ratio']
        total['total_ratio'] = total['total_ratio'] / stk_num

        # 将 total 转为字典映射，方便后续 map
        total_map = total.set_index('Industry')['total_ratio'].to_dict()

        for _ in range(turns):
            # 计算当前选股的行业权重分布
            selected = df[df['selected']==1].groupby('Industry')['weight'].sum().reset_index(name='selected_weight')
            
            # 1. 准备数据表
            industry_port = total.merge(selected, on='Industry', how='left')
            industry_port['selected_weight'] = industry_port['selected_weight'].fillna(0)
            
            # 2. 计算超额/低额状态 (在行业层面上计算)
            industry_port['over_ratio'] = industry_port['selected_weight'] - industry_port['total_ratio'] - threshold
            industry_port['less_ratio'] = industry_port['selected_weight'] - industry_port['total_ratio'] + threshold
            
            # 3. 筛选出需要调整的行业列表
            over_inds = industry_port[industry_port['over_ratio'] > 1e-5]['Industry'].values
            less_inds = industry_port[(industry_port['less_ratio'] < -1e-5) & (industry_port['selected_weight'] != 0)]['Industry'].values
            zero_inds = industry_port[(industry_port['less_ratio'] < -1e-5) & (industry_port['selected_weight'] == 0)]['Industry'].values
            
            # 4. 计算调整系数所需的 Sum 值
            over_mask_ind = industry_port['Industry'].isin(over_inds)
            less_mask_ind = industry_port['Industry'].isin(less_inds)
            zero_mask_ind = industry_port['Industry'].isin(zero_inds)
            
            over_sum = industry_port.loc[over_mask_ind, 'over_ratio'].sum()
            less_sum = industry_port.loc[less_mask_ind, 'less_ratio'].sum()
            zero_sum = industry_port.loc[zero_mask_ind, 'less_ratio'].sum()
            other_sum = industry_port.loc[(~over_mask_ind) & (~less_mask_ind) & (~zero_mask_ind), 'selected_weight'].sum()


            weight_map = industry_port.set_index('Industry')['selected_weight'].to_dict()
            
            df['temp_total_ratio'] = df['Industry'].map(total_map)
            df['temp_selected_weight'] = df['Industry'].map(weight_map)
            
            # 收敛判断 (直接用 industry_port 判断即可)
            if all(abs(industry_port['selected_weight'] - industry_port['total_ratio']) < threshold + 1e-5):
                break

            # 生成个股层面的 Mask
            selected_mask = df['selected'] == 1
            idx_over_mask = df['Industry'].isin(over_inds)
            idx_less_mask = df['Industry'].isin(less_inds)
            idx_zero_mask = df['Industry'].isin(zero_inds)

            # 1. 调低超配行业
            mask1 = idx_over_mask & selected_mask
            # 公式: w * (total + th) / selected
            df.loc[mask1, 'weight'] = (
                df.loc[mask1, 'weight'] * (df.loc[mask1, 'temp_total_ratio'] + threshold)
            ) / df.loc[mask1, 'temp_selected_weight']
            
            # 2. 调高低配行业
            mask2 = idx_less_mask & selected_mask
            # 公式: w * (total - th) / selected
            df.loc[mask2, 'weight'] = (
                df.loc[mask2, 'weight'] * (df.loc[mask2, 'temp_total_ratio'] - threshold)
            ) / df.loc[mask2, 'temp_selected_weight']
            
            # 3. 填补零配行业
            mask3 = idx_zero_mask & (df['NextIndexTrade'] == 1)
            # 这里需要知道每个行业有多少只 NextIndexTrade==1 的股票
            # 这种分组统计还是得用 transform 或者 map
            # 简便起见，这里做一个临时 group count
            zero_counts = df[mask3].groupby('Industry')['SecuCode'].count()
            # 映射回 mask3 的行
            df.loc[mask3, 'weight'] = (
                (df.loc[mask3, 'temp_total_ratio'] - threshold) / 
                df.loc[mask3, 'Industry'].map(zero_counts)
            )
            
            # 4. 调整其他行业
            mask4 = (~idx_over_mask) & (~idx_less_mask) & (~idx_zero_mask) & selected_mask
            if other_sum != 0:
                factor = (over_sum + less_sum + zero_sum + other_sum) / other_sum
                df.loc[mask4, 'weight'] = df.loc[mask4, 'weight'] * factor
            
            # --- [修复点结束] ---
            
            # 清理临时列，防止污染下一次循环或输出
            df = df.drop(columns=['temp_total_ratio', 'temp_selected_weight'], errors='ignore')

        return df['weight']

    @staticmethod
    def adjust_untradable(df):
        """
        处理停牌/无法交易的股票 (使用 Pivot 向量化方法)
        """
        print(">>> [Portfolio] 开始计算权重继承 (Pivot方法)...")
        trading_days = sorted(df['TradingDay'].unique())
        
        # 1. Pivot 展开
        weight_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='weight').fillna(0)
        swing_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='SwingStatus').fillna(0)
        selected_pivot = df.pivot(index='TradingDay', columns='SecuCode', values='selected').fillna(0)
        
        # 排序
        weight_pivot = weight_pivot.sort_index()
        swing_pivot = swing_pivot.sort_index()
        selected_pivot = selected_pivot.sort_index()
        
        w_vals = weight_pivot.values
        s_vals = swing_pivot.values 
        sel_vals = selected_pivot.values
        
        # 2. 循环处理
        for i in range(1, len(trading_days)):
            last_w = w_vals[i-1, :]
            curr_swing = s_vals[i, :] 
            curr_sel = sel_vals[i, :]
            
            # 找出不可交易的股票 (SwingStatus=0)
            # 如果昨日持有权重 > 0 且今日不可交易，则强制继承权重
            untradable_idx = (curr_swing == 0) & (last_w > 0)
            
            w_vals[i, untradable_idx] = last_w[untradable_idx]
            
            total_untradable_w = np.sum(w_vals[i, untradable_idx])
            if total_untradable_w > 1.0: total_untradable_w = 1.0
            
            # 调整可交易且被选中的股票权重
            adjustable_idx = (curr_sel == 1) & (curr_swing == 1)
            
            scaling_factor = 1.0 - total_untradable_w
            # 只有当 scaling_factor 有意义且有股票可调时才计算
            if scaling_factor >= 0:
                # 归一化当前的选中股票权重 (因为经过行业中性化后 sum 可能不完全是1)
                current_sel_sum = np.sum(w_vals[i, adjustable_idx])
                if current_sel_sum > 0:
                    w_vals[i, adjustable_idx] = (w_vals[i, adjustable_idx] / current_sel_sum) * scaling_factor
                else:
                    # 极端情况：全是停牌股，或者没有选出票
                    pass

        # 3. 还原回长表
        new_weight_df = pd.DataFrame(w_vals, index=weight_pivot.index, columns=weight_pivot.columns)
        new_weight_stack = new_weight_df.stack().reset_index(name='adjusted_weight')
        
        # 合并回原 df
        # 注意：这里也可能存在 duplicate labels 问题，如果原始 df 有重复行
        # 我们先做一次去重检查
        df = pd.merge(df, new_weight_stack, on=['TradingDay', 'SecuCode'], how='left')
        df['weight'] = df['adjusted_weight'].fillna(0)
        df = df.drop(columns=['adjusted_weight'], errors='ignore')
        
        return df

    @staticmethod
    def construct(scored_df):
        """组合构建主流程"""
        print(">>> [Portfolio] 开始构建组合...")
        
        # [防卫性编程]：确保没有重复索引和重复数据
        # 必须确保 TradingDay + SecuCode 是唯一的，否则后续 pivot 会报错
        df = scored_df.drop_duplicates(subset=['TradingDay', 'SecuCode']).copy()
        
        # 强制排序，确保 groupby 的顺序和 df 的顺序在逻辑上是一致的
        df = df.sort_values(by=['TradingDay', 'SecuCode'])
        df = df.reset_index(drop=True)

        # 1. 基础筛选
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
        df['selected'] = 0
        sel_mask = (df['factor_score'] >= 1) & (df['NextIndexTrade'] == 1)
        df.loc[sel_mask, 'selected'] = 1
        
        # 3. 初始等权
        print(">>> [Portfolio] 计算初始等权...")
        def _init_equal_weight(group):
            selected_mask = group['selected'] == 1
            count = selected_mask.sum()
            w = pd.Series(0.0, index=group.index)
            if count > 0:
                w[selected_mask] = 1.0 / count
            return w

        df['weight'] = df.groupby('TradingDay', group_keys=False).apply(_init_equal_weight)
        
        # 4. 行业中性化
        print(f">>> [Portfolio] 执行行业中性化约束 (Tol={Config.INDUSTRY_TOL})...")
        # 这里的 weights 索引将和 df 严格对齐
        weights = df.groupby('TradingDay', group_keys=False).apply(
            lambda x: PortfolioOptimizer.check_industry(x, Config.INDUSTRY_TOL)
        )
        
        # --- [关键修复] ---
        # 如果 weights 出现重复索引（极罕见情况），去重后再赋值
        if weights.index.duplicated().any():
            print(">>> [Warning] weights index has duplicates! Keeping first occurrence.")
            weights = weights[~weights.index.duplicated()]
            
        df['weight'] = weights
        # -----------------
        
        # 5. 不可交易调整
        print(">>> [Portfolio] 调整不可交易股票仓位...")
        df = PortfolioOptimizer.adjust_untradable(df)
        
        # 6. 保存结果
        filename = f"Portfolio_{Config.STOCK_POOL}_{Config.SIGN}.csv"
        save_path = Config.DIR_PORTFOLIO / filename
        print(f"保存每日持仓: {save_path}")
        df.to_csv(str(save_path), index=False, encoding='utf_8_sig')
        
        return df