简体中文 | [English](./readme.md)
# 📈 Quantitative Strategy Backtesting Framework V2.0

量化多因子策略回测框架 V2.0

# 📖 项目简介 (Introduction)

这是一个面向工程化、轻量级但功能完备的量化策略回测框架。

本项目旨在解决传统脚本式回测代码（"面条代码"）中常见的痛点：

❌ 逻辑耦合：数据读取、策略计算、绩效分析混杂在一起，难以维护。

❌ 计算效率低：每次调参都需要重新读取庞大的历史数据和重复计算因子。

❌ 数据污染：回测产生的中间文件与原始数据混放。

❌ 成本估算不准：忽略了股票退市、剔除出股票池时的交易成本，导致净值虚高。

V2.0 版本通过模块化设计、智能缓存机制和向量化运算，提供了一个高效、严谨的回测解决方案。非常适合量化初学者理解从因子处理到组合构建再到绩效归因的完整闭环。

# ✨ 核心特性 (Features)

🚀 极速回测 (Smart Caching)：内置基于配置指纹 (Config Hash) 的智能缓存机制。当你只修改组合权重逻辑而未修改因子公式时，系统会自动复用上一次的打分结果，实现秒级加载。

🧩 模块完全解耦：IO 操作、策略逻辑、组合构建、绩效分析完全分离。

⚖️ 严谨的组合构建：

实现迭代式行业中性化 (Iterative Industry Neutralization) 算法。

包含真实的停牌/不可交易 (Untradable) 权重继承逻辑（向量化全矩阵运算）。

💰 精确的成本计算：修正了传统回测中漏算退市/剔除股票交易成本的 Bug，基于全矩阵差分计算真实换手率。

📊 灵活的股票池：支持全市场 (All)、宽基指数 (800/1000) 及自定义策略池 (HighBeta/LowBeta) 的无缝切换。

# 🏗️ 目录结构 (Directory Structure)

```text
strategy_framework/
├── config.py           # [控制中心] 全局参数、路径管理、逻辑映射 (唯一修改入口)
├── main.py             # [启动程序] 调度各个模块，控制回测生命周期
├── data_loader.py      # [数据层] 读取 Parquet，清洗并筛选股票池
├── factor_engine.py    # [策略层] 编写因子打分公式 (Alpha Model)
├── portfolio.py        # [组合层] 核心回测逻辑：行业约束、停牌处理
├── analysis.py         # [分析层] 计算每日收益、扣费、最大回撤及绘图
├── utils.py            # [工具箱] 通用函数 (哈希、分位数计算)
│
├── data/               # [数据源] (只读，需自行准备)
│   ├── 2016/ ... 2025/ # 分年份的因子文件 (Parquet)
│   ├── ret_df.parquet  # 收益率数据
│   └── BetaPool...     # 股票状态与行业数据
│
└── results/            # [输出结果] (自动生成，无需手动创建)
    ├── cache/          # 中间打分缓存 (加速下次运行)
    ├── portfolio/      # 每日持仓权重明细 (CSV)
    └── reports/        # 绩效报表、净值曲线图 (PDF/CSV)
```

# 🧠 框架核心逻辑详解 (Deep Dive)

本框架遵循经典的 Pipeline 设计模式，数据流向清晰明确：

1. 数据加载与筛选 (data_loader.py)

框架首先读取全量状态文件，根据 config.py 中的 STOCK_POOL 进行筛选。

支持 逻辑并集：例如配置 '800'，会自动选取 HighBeta800 OR LowBeta800 为 1 的股票。

2. 因子打分与缓存 (factor_engine.py)

原理：将原始因子值转化为标准化的分数 (Score)。

缓存机制：程序启动时，会根据 开始日期+结束日期+股票池+因子公式+额外因子列表 生成唯一的 MD5 哈希值。

如果该哈希对应的 CSV 已存在于 results/cache/，直接读取（耗时 < 1秒）。

如果不存在，读取原始 Parquet 数据进行计算，并写入缓存。

3. 组合构建 (portfolio.py) —— 核心难点

这是将“因子分数”转化为“真实持仓权重”的关键步骤。

步骤 A: 基础筛选

剔除当日不可交易状态的股票，选股条件：
```text

正常交易 (TradeStatus=1, SwingStatus=1)

非涨跌停 (StopTradeStatus=1)

非新股 (IpoStatus=1)

因子得分达标 (Score >= 1)
```
步骤 B: 迭代式行业中性化 (Industry Neutralization)

为了防止策略在某个行业上过度暴露（赌行业），框架使用迭代法调整权重，使持仓的行业分布逼近全市场基准。

算法逻辑：
```text
计算全市场各行业权重占比 (Target)。

计算当前持仓各行业权重占比 (Current)。

若偏差 < 阈值，停止迭代。

超配行业：统一降低该行业内所有股票权重。

低配行业：统一提高该行业内所有股票权重。

归一化剩余权重，重复上述步骤直至收敛。
```
步骤 C: 不可交易(停牌)处理 (Untradable Adjustment)

模拟真实交易中无法卖出停牌股的场景：
```text
强制继承：若某股票当日停牌，强制其权重等于昨日权重（锁仓）。

挤压效应：停牌股票占用了仓位，剩余可用的资金 (1 - 停牌权重) 按比例分配给其他可交易的股票。

向量化实现：使用 Pandas Pivot 表进行全矩阵运算，避免了低效的循环。
```
4. 绩效分析 (analysis.py)

精确换手率：传统回测常使用 groupby.diff() 计算换手，这会导致漏算股票被剔除出池子时的卖出成本。本框架使用全矩阵差分 abs(Weight_t - Weight_t-1)，精确捕获每一笔进出交易。

指标输出：
```text
IR (信息比率)：超额收益 / 跟踪误差。

IR2 (修正信息比率)：(超额收益 - 最大回撤/3) / 跟踪误差。引入回撤惩罚，惩罚那些历史上出现过“深坑”的策略。
```
# 🚀 快速开始 (Quick Start)

1. 环境准备
```text
pip install pandas numpy matplotlib pyarrow
```

2. 数据准备

请确保项目根目录下有 data/ 文件夹，并按年份存放 Parquet 格式的因子数据。
(注：本项目不包含示例数据，需用户自行接入)

3. 配置策略

打开 config.py，这是你唯一需要频繁修改的文件：
```text
class Config:
    START_DATE = '20160108'
    END_DATE   = '20250218'
    STOCK_POOL = 'all'        # 可选: '800', '1000', 'LowBeta800'
    RET_IDX    = 'open5twap'  # 收益模式
    SIGN       = 'Test_Run_v1' # 本次实验的标签
    
    # 定义需要合并的额外因子文件
    ADDITIONAL_FACTORS = [] 
```

4. 编写因子公式

打开 factor_engine.py，在 calculate_score 函数中修改打分逻辑：
```text
# 示例：选中 Alpha95 因子排名靠前的 30% 股票
score = (1 * (Alpha95 <= mquantiles(Alpha95, 0.3)))
```

5. 运行回测
```text
python main.py
```

6. 查看报告

运行完成后，进入 results/reports/ 目录：

📈 打开 Chart_all_Test_Run_v1.pdf 查看净值走势图。

📄 打开 Summary_Test_Run_v1.csv 查看年化收益、最大回撤等指标。

# ❓ 常见问题 (FAQ)

Q: 我修改了因子公式，为什么运行结果没变？
A: 请检查 config.py 中的参数。如果日期、股票池、收益模式都没变，程序可能会直接命中缓存。
解决方法：
```text
修改 config.py 中的 SIGN 字段（推荐）。

或者手动删除 results/cache/ 下的文件。
```
Q: 如何添加新的年份数据？
A: 无需修改代码。只需将新的年份文件夹（如 2026）放入 data/ 目录，并确保里面有 parquet 文件即可。

# 🧬 算法流程
![算法流程预览](./strategy.png)

# 🤝 贡献 (Contributing)

欢迎提交 Issue 或 Pull Request 来改进此框架！

