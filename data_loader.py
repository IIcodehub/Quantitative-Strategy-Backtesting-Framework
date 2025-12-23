import pandas as pd
import os
from config import Config

class StockPoolSelector:
    @staticmethod
    def filter(df):
        pool_name = str(Config.STOCK_POOL)
        input_lower = pool_name.lower()
        if input_lower == 'all':
            return df
        mapping = Config.POOL_MAPPING
        if pool_name in mapping:
            cols = mapping[pool_name]
            mask = pd.Series(False, index=df.index)
            valid_cols = [c for c in cols if c in df.columns]
            if not valid_cols:
                raise ValueError(f"股票池定义的列 {cols} 均不存在")
            for col in valid_cols:
                mask = mask | (df[col] == 1)
            return df[mask].copy()
        if pool_name in df.columns:
            mask = df[pool_name] == 1
            if 'Index1800' in df.columns and ('Beta' in pool_name):
                mask = mask & (df['Index1800'] == 1)
            return df[mask].copy()
        raise ValueError(f"未知的股票池标识: {pool_name}")

class DataLoader:
    def __init__(self):
        self.start_dt = pd.to_datetime(Config.START_DATE)
        self.end_dt = pd.to_datetime(Config.END_DATE)

    def load_stock_status(self):
        print(f"读取状态文件: {Config.STOCK_STATUS_FILE}")
        if not Config.STOCK_STATUS_FILE.exists():
            raise FileNotFoundError(f"找不到状态文件: {Config.STOCK_STATUS_FILE}")
        df = pd.read_parquet(str(Config.STOCK_STATUS_FILE))
        df['TradingDay'] = pd.to_datetime(df['TradingDay'])
        df = df[(df['TradingDay'] >= self.start_dt) & (df['TradingDay'] <= self.end_dt)]
        df = StockPoolSelector.filter(df)
        df['Year'] = df['TradingDay'].dt.year.astype(str)
        return df

    def load_returns(self):
        print(f"读取收益文件: {Config.RETURNS_FILE}")
        if not Config.RETURNS_FILE.exists():
             raise FileNotFoundError(f"找不到收益文件: {Config.RETURNS_FILE}")
        df = pd.read_parquet(str(Config.RETURNS_FILE))
        df['TradingDay'] = pd.to_datetime(df['TradingDay'])
        ret_map = {'open5twap': 'ret_open5twap', 'c2c': 'ret_c2c'}
        col = ret_map.get(Config.RET_IDX)
        if not col or col not in df.columns:
            raise ValueError(f"收益列 {col} 无效或缺失")
        return df[['TradingDay', 'SecuCode', col]]

    def load_year_factors(self, year):
        file_path = Config.DATA_DIR / str(year) / "Factors_ALL_all.parquet"
        if not file_path.exists():
            print(f"警告: 年份 {year} 的基础因子文件不存在")
            return None
        df = pd.read_parquet(str(file_path))
        df['TradingDay'] = pd.to_datetime(df['TradingDay'])
        return df

    # ===============================================
    # [新增] 处理额外因子文件的逻辑
    # ===============================================
    def merge_additional_factors(self, combined_df, year):
        """
        读取 Config.ADDITIONAL_FACTORS 中的文件并合并
        """
        if not Config.ADDITIONAL_FACTORS:
            return combined_df

        year_dir = Config.DATA_DIR / str(year)
        
        for factor_name in Config.ADDITIONAL_FACTORS:
            # 跳过基础文件，防止重复
            if factor_name == 'Factors_ALL_all':
                continue
                
            file_path = year_dir / f"{factor_name}.parquet"
            
            if not file_path.exists():
                print(f"警告: 额外因子文件不存在: {file_path}")
                continue
                
            print(f"   + 合并额外因子: {factor_name}")
            try:
                add_df = pd.read_parquet(str(file_path))
                if 'TradingDay' in add_df.columns:
                    add_df['TradingDay'] = pd.to_datetime(add_df['TradingDay'])

             
                if add_df.duplicated(subset=['TradingDay', 'SecuCode']).any():
                    dup_count = add_df.duplicated(subset=['TradingDay', 'SecuCode']).sum()
                    print(f"     [警告] 发现 {dup_count} 条重复数据，正在去重...")
                    add_df = add_df.drop_duplicates(subset=['TradingDay', 'SecuCode'], keep='first')
                
                # 左连接合并
                combined_df = pd.merge(combined_df, add_df, on=['TradingDay', 'SecuCode'], how='left')
                
            except Exception as e:
                print(f"错误: 合并因子文件 {factor_name} 失败: {e}")
                
        return combined_df
                
    