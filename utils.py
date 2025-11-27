import hashlib
import pandas as pd
import numpy as np
from config import Config

def get_config_identifier():
    """生成包含额外因子的唯一标识符"""
    components = [
        f"sd:{Config.START_DATE}",
        f"ed:{Config.END_DATE}",
        f"pool:{Config.STOCK_POOL}",
        f"ret:{Config.RET_IDX}"
    ]
    
    # [新增] 将额外因子列表加入哈希
    if Config.ADDITIONAL_FACTORS:
        # 排序确保列表顺序不影响哈希 ('A','B' 和 'B','A' 应视为相同配置)
        factors_str = ",".join(sorted(Config.ADDITIONAL_FACTORS))
        components.append(f"add_factors:{factors_str}")
    
    combined_str = ";".join(components)
    hasher = hashlib.md5()
    hasher.update(combined_str.encode('utf-8'))
    return hasher.hexdigest()[:8]

def format_secucode(code):
    if pd.isna(code): return None
    try:
        return str(int(code)).zfill(6)
    except:
        return str(code).zfill(6)

def mquantiles(data, q):
    data = np.asarray(data).flatten()
    data = data[~np.isnan(data)]
    n = len(data)
    if n == 0: return np.nan
    r = q * n - 1
    k = np.floor(r + 0.5).astype(int)
    kp1 = k + 1
    r = r - k
    k = np.clip(k, 0, n - 1)
    kp1 = np.clip(kp1, 0, n - 1)
    sorted_data = np.sort(data)
    return (0.5 + r) * sorted_data[kp1] + (0.5 - r) * sorted_data[k]