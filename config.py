import os
from pathlib import Path

class Config:
    # ==========================
    # 1. 运行参数设置
    # ==========================
    START_DATE = '20160108'
    END_DATE   = '20250218'
    STOCK_POOL = 'all'  
    RET_IDX    = 'open5twap'
    SIGN       = 'Full_Test_Run_V1'
    
    # [新增] 额外因子文件列表
    # 如果有其他因子文件需要合并 (如 'Alpha191', 'Style_Factors' 等)，在此添加文件名(不含后缀)
    # 如果为空，则只读取默认的 Factors_ALL_all.parquet
    ADDITIONAL_FACTORS = [] 
    # 示例: ADDITIONAL_FACTORS = ['StyleFactors_2023', 'HighFreqFactors']

    # 策略常量
    FEE_RATE     = 0.001
    INDUSTRY_TOL = 0.1

    # ==========================
    # 2. 路径配置
    # ==========================
    BASE_DIR = Path(__file__).resolve().parent 
    DATA_DIR = BASE_DIR / 'data'
    
    STOCK_STATUS_FILE = DATA_DIR / 'BetaPool_TradeStatus_ind_index1800_shifted_index_forward.parquet'
    RETURNS_FILE      = DATA_DIR / 'ret_df.parquet'
    
    RESULTS_DIR = BASE_DIR / 'results'
    DIR_CACHE     = RESULTS_DIR / 'cache'
    DIR_PORTFOLIO = RESULTS_DIR / 'portfolio'
    DIR_REPORTS   = RESULTS_DIR / 'reports'
    
    # ==========================
    # 3. 映射逻辑
    # ==========================
    POOL_MAPPING = {
        '800':  ['HighBeta800', 'LowBeta800'],     
        '1000': ['HighBeta1000', 'LowBeta1000'],
        '1800': ['Index1800'],
        'all':  ['All']
    }

    @classmethod
    def initialize_directories(cls):
        for path in [cls.DIR_CACHE, cls.DIR_PORTFOLIO, cls.DIR_REPORTS]:
            path.mkdir(parents=True, exist_ok=True)
        print(f"工作目录已初始化: {cls.RESULTS_DIR}")