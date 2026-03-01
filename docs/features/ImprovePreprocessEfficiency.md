整合决策摘要
方案来源	内容	决策
用户优化1	存储层Upsert向量化	✅ 采纳 - 收益最大(5-10x)
用户优化4	智能增量预处理(adj_factor检测)	✅ 采纳 - 核心优化(减少85-95%工作量)
用户优化2	批次间asyncio并发	✅ 采纳 - 加速I/O
用户优化3	估值分位raw=True	✅ 采纳 - 改动小收益大
用户优化5	ProcessPoolExecutor多进程	✅ 采纳 - 替代原multiprocessing方案
用户优化6	数据库查询并行	✅ 采纳 - 与原有方案合并
原方案	向量化指标计算	✅ 保留为Phase 3
原方案	F-Score TTM向量化	✅ 保留为Phase 3
关于方案六的重要修正
用户您指出的问题非常关键——前复权确实不能简单增量处理。整合方案B采用了您的智能检测策略：


开始预处理
    ↓
检测每只股票最新adj_factor是否变化
    ↓
├─ 变化(约5%股票) → 全量重算该股票
└─ 未变(约95%股票) → 仅增量计算最近N天
关键优化: 日常更新时，95%的股票只需处理最近80天数据（而非5年历史），大幅减少计算量。

推荐实施组合
Phase 1立即实施（预期减少90%日常处理时间）:

整合方案A + 整合方案B: 存储层向量化 + 智能增量检测
整合方案F: 数据库查询并行
预期效果:

Technical日常更新: ~25分钟 → ~3-5分钟
无需改变现有调度习惯，自动检测变化
完整方案已更新到计划文件，包含详细实现代码、收益预估和风险缓解措施。

Claude’s Plan
预处理模块性能优化整合方案
背景与问题分析
当前性能瓶颈
瓶颈点	位置	影响	严重程度
存储层逐行转换	storage.py upsert方法	iterrows()逐行处理，影响所有3类预处理	🔴 高
串行批处理	preprocess.py 第214-278行	股票批次顺序处理，CPU利用率低	🔴 高
指标计算循环	technical/momentum.py 第88-116行	每只股票循环计算MACD/RSI	🔴 高
估值分位滚动计算	fundamental/valuation.py	rolling().apply(raw=False)开销大	🟡 中
多表串行查询	preprocess.py 第464-527行	每批次执行4次串行SQL查询	🟡 中
TTM计算循环	fundamental/quality.py 第419-476行	F-Score的TTM转换使用Python循环	🟡 中
全量更新策略	schedules.yml force=true	无论adj_factor是否变化都全量重算	🔴 高
数据规模估算
股票数量: 约5000+只A股股票
日线数据: 5000只 × 250交易日/年 × 5年 ≈ 625万条记录
批次配置: 默认batch_size=100，需处理50+批次
adj_factor变化: 每天约5%股票发生除权除息（~250只）
当前策略: 每天全量处理5000只，95%的计算是重复的
优化目标
总体性能提升: 减少80-95%的处理时间
CPU利用率: 充分利用多核CPU（8-16核）
日常更新: 从25分钟降至2-3分钟
内存效率: 控制峰值内存占用，避免OOM
优化方案整合
方案对比与整合决策
原方案	用户方案	整合决策	理由
多进程并行	优化5: CPU多进程加速	采纳用户方案5	使用ProcessPoolExecutor，与asyncio兼容更好
向量化指标计算	-	保留原方案	补充优化，不冲突
异步SQL并行	优化6: 数据库查询优化	合并	内容一致，共同采纳
F-Score TTM向量化	-	保留原方案	Phase 2实施
内存批处理优化	-	保留原方案	Phase 1实施
滑动窗口增量	优化4: 智能增量预处理	采纳用户方案4	adj_factor检测策略更精准
-	优化1: 存储层向量化	采纳	收益最大(5-10x)，影响面广
-	优化2: 批次间并发	采纳	asyncio并发，与多进程互补
-	优化3: 估值分位优化	采纳	改动小收益大(3-5x)
最终整合方案
Phase 1: 高优先级（立即实施）
整合方案A: 存储层Upsert向量化（优化1）

收益: 5-10x - 影响所有3类预处理
改动: 中等，修改storage.py三个Storage类
整合方案B: 智能增量预处理（优化4）

收益: **85-95%**工作量减少 - 技术预处理核心优化
改动: 较大，需新增last_adj_factor列和检测逻辑
关键洞察: 仅当adj_factor变化时才需要全量重算
整合方案C: 异步批次并发（优化2）

收益: 2-4x - 加速I/O部分
改动: 中等，使用asyncio.Semaphore控制并发
Phase 2: 中优先级（短期实施）
整合方案D: 估值分位计算优化（优化3）

收益: 3-5x - fundamental预处理
改动: 小，raw=True参数修改
整合方案E: 多进程CPU加速（优化5）

收益: 2-4x - CPU密集型计算
改动: 中等，ProcessPoolExecutor集成
整合方案F: 数据库查询并行（优化6）

收益: 1.5-2x - F-Score预处理
改动: 小，asyncio.gather并行查询
Phase 3: 低优先级（长期优化）
整合方案G: 向量化指标计算（原方案）

收益: 30-50% - 指标计算阶段
改动: 中等，新增vectorized.py模块
整合方案H: F-Score TTM向量化（原方案）

收益: 40-60% - quarterly_fundamental预处理
改动: 中等，quality.py重构
详细实施方案
整合方案A: 存储层Upsert向量化
问题: 当前使用iterrows()逐行转换数据类型，是pandas中最慢的迭代操作

实现: storage.py


def _prepare_values_list(df: pd.DataFrame, columns: list) -> list:
    """向量化转换 DataFrame 为 asyncpg 可接受的参数列表"""
    prepared = df[columns].copy()
    for col in columns:
        dtype = prepared[col].dtype
        if pd.api.types.is_datetime64_any_dtype(dtype):
            prepared[col] = prepared[col].dt.to_pydatetime()
        elif pd.api.types.is_float_dtype(dtype):
            prepared[col] = prepared[col].where(prepared[col].notna(), None)
        elif pd.api.types.is_integer_dtype(dtype):
            prepared[col] = prepared[col].where(prepared[col].notna(), None)
    # 用 numpy 快速转换为元组列表
    records = prepared.to_numpy()
    return [tuple(None if pd.isna(v) else (v.item() if hasattr(v, 'item') else v)
                  for v in row) for row in records]
收益: 5-10x提升，所有预处理类别受益

整合方案B: 智能增量预处理（核心技术优化）
问题: technical预处理每天全量执行，但95%的股票adj_factor未变化，不需要重算

核心逻辑:


开始预处理
    ↓
查询每只股票最新 adj_factor
    ↓
与上次预处理时的 adj_factor 对比
    ↓
是否变化?
    ├─ 是 → 全量重算该股票
    └─ 否 → 增量处理（仅计算最近N天）
数据库迁移:


-- 新增 last_adj_factor 列到预处理表
ALTER TABLE processed_daily_qfq ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);
ALTER TABLE processed_weekly_qfq ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);
ALTER TABLE processed_monthly_qfq ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);
分类逻辑: preprocess.py


INDICATOR_MAX_WINDOW = 60   # 最大指标窗口(MA_50需要50天)
LOOKBACK_BUFFER = 20        # 额外缓冲

async def _classify_stocks(db_manager, symbols):
    """将股票分为全量组和增量组"""
    sql = """
        WITH current_adj AS (
            SELECT DISTINCT ON (symbol) symbol, adj_factor as current_adj
            FROM adj_factor ORDER BY symbol, time DESC
        ),
        processed_adj AS (
            SELECT DISTINCT ON (symbol) symbol, last_adj_factor
            FROM processed_daily_qfq ORDER BY symbol, time DESC
        )
        SELECT c.symbol, c.current_adj,
               CASE WHEN p.last_adj_factor IS NULL
                    OR c.current_adj != p.last_adj_factor
                    THEN true ELSE false END AS needs_full
        FROM current_adj c
        LEFT JOIN processed_adj p ON c.symbol = p.symbol
        WHERE c.symbol = ANY(:symbols)
    """
    result = await db_manager.execute_raw_sql(sql, {"symbols": symbols})

    full_symbols = []      # adj_factor 变化 → 全量重算
    incr_symbols = []      # adj_factor 未变 → 增量处理
    adj_factor_map = {}    # symbol → latest adj_factor

    for row in result.fetchall():
        adj_factor_map[row[0]] = row[1]
        if row[2]:  # needs_full
            full_symbols.append(row[0])
        else:
            incr_symbols.append(row[0])

    return full_symbols, incr_symbols, adj_factor_map
处理流程:


async def _run_technical_preprocess(...):
    if force:
        full_symbols, incr_symbols = symbols, []
    else:
        full_symbols, incr_symbols, adj_map = await _classify_stocks(db_manager, symbols)
        console.print(f"  全量重算: {len(full_symbols)} 只 (adj_factor变化)")
        console.print(f"  增量处理: {len(incr_symbols)} 只")

    # 1. 全量组：获取全部历史数据
    if full_symbols:
        await _process_batch(db_manager, full_symbols, start_date=None, ...)

    # 2. 增量组：只获取最近 lookback_days 天
    if incr_symbols:
        lookback = INDICATOR_MAX_WINDOW + LOOKBACK_BUFFER
        incr_start = (datetime.now() - timedelta(days=lookback * 1.5)).strftime('%Y-%m-%d')
        await _process_batch(db_manager, incr_symbols, start_date=incr_start, ...)
schedules.yml调整:


technical_preprocess:
    # 工作日：智能增量
    params:
      all: true
      freq: "daily,weekly,monthly"
      adjust: qfq
      force: false  # 改为false，启用智能检测

# 周末全量校验（兜底）
technical_preprocess_weekly_full:
    enabled: true
    type: preprocess
    category: technical
    schedule:
      type: cron
      day_of_week: "sat"
      hour: 2
      minute: 0
    params:
      all: true
      freq: "daily,weekly,monthly"
      adjust: qfq
      force: true
收益: 日常更新减少85-95%工作量，从25分钟降至2-3分钟

整合方案C: 异步批次并发
问题: 批次间串行执行，CPU等待I/O时闲置

实现: preprocess.py


async def _run_technical_preprocess(..., max_concurrent: int = 4):
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_batch(batch_symbols):
        async with semaphore:
            df = await _get_stock_data(db_manager, batch_symbols, ...)
            if df.empty:
                return 0
            # 复权 + 指标计算 + 存储
            ...
            return count

    tasks = []
    for i in range(0, len(symbols), batch_size):
        batch_symbols = symbols[i:i+batch_size]
        tasks.append(process_batch(batch_symbols))

    results = await asyncio.gather(*tasks)
注意: asyncio并发主要加速I/O部分。对于CPU密集的技术指标计算，需结合方案E（多进程）

收益: I/O部分提升2-4x

整合方案D: 估值分位计算优化
问题: rolling().apply(calc_percentile, raw=False) 每次传入Series，开销巨大

实现: fundamental/valuation.py


def _rolling_percentile(self, series, window):
    def calc_percentile(x):
        # raw=True 时 x 是 numpy array，性能大幅提升
        valid = x[(~np.isnan(x)) & (x > 0)]
        if len(valid) < 2:
            return np.nan
        current = x[-1]  # 直接索引 numpy 数组
        if np.isnan(current) or current <= 0:
            return np.nan
        rank = (valid < current).sum()
        return rank / len(valid) * 100

    return series.rolling(window=window, min_periods=min(20, window)).apply(
        calc_percentile, raw=True  # 关键改动：raw=True
    )
收益: fundamental预处理提升3-5x

整合方案E: 多进程CPU加速
问题: TA-Lib计算是CPU密集型，Python GIL限制多线程并行

实现: preprocess.py


import concurrent.futures
import os
import pickle

def _compute_indicators_for_batch(df_bytes, indicators, adjust_type):
    """在子进程中执行指标计算（CPU密集部分）"""
    import pandas as pd
    from ..preprocessing import AdjustProcessor
    from ..preprocessing.technical.base import create_indicator

    df = pickle.loads(df_bytes)

    # 复权处理
    processor = AdjustProcessor()
    if adjust_type == "qfq":
        df = processor.adjust_qfq(df)
    elif adjust_type == "hfq":
        df = processor.adjust_hfq(df)

    # 计算指标（CPU密集）
    for ind_name in indicators:
        try:
            indicator = create_indicator(ind_name)
            df = indicator.calculate(df)
        except:
            pass

    return pickle.dumps(df)

async def _run_technical_preprocess(..., num_workers: int = None):
    num_workers = num_workers or min(os.cpu_count() - 1, 4)

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        loop = asyncio.get_event_loop()
        futures = []
        for batch_df in batch_dataframes:
            future = loop.run_in_executor(
                executor,
                _compute_indicators_for_batch,
                pickle.dumps(batch_df), indicators, adjust_type
            )
            futures.append(future)

        results = await asyncio.gather(*futures)
注意: 使用pickle序列化/反序列化DataFrame有开销，建议batch_size >= 100

收益: CPU密集型计算提升2-4x

整合方案F: 数据库查询并行
问题: quarterly_fundamental每批次串行查询4张表

实现: preprocess.py


# 优化：并行查询4张表
fina_result, bs_result, cf_result, inc_result = await asyncio.gather(
    db_manager.execute_raw_sql(fina_sql, params),
    db_manager.execute_raw_sql(bs_sql, params),
    db_manager.execute_raw_sql(cf_sql, params),
    db_manager.execute_raw_sql(inc_sql, params),
)
连接池调整: 需增大预处理专用连接池


PREPROCESS_POOL_SIZE = 20
PREPROCESS_MAX_OVERFLOW = 10
收益: quarterly_fundamental提升1.5-2x

实施优先级与路线图

Phase 1 (立即实施，预期减少90%日常处理时间)
├── 整合方案A: 存储层Upsert向量化
│   └── 收益: 5-10x，所有类别受益
├── 整合方案B: 智能增量预处理
│   └── 收益: 85-95%工作量减少，technical核心优化
└── 整合方案F: 数据库查询并行
    └── 收益: 1.5-2x，改动小

Phase 2 (短期实施，进一步提升性能)
├── 整合方案D: 估值分位计算优化
│   └── 收益: 3-5x，fundamental受益
├── 整合方案C: 异步批次并发
│   └── 收益: 2-4x，I/O加速
└── 整合方案E: 多进程CPU加速
    └── 收益: 2-4x，CPU密集部分

Phase 3 (长期优化，细节完善)
├── 整合方案G: 向量化指标计算
└── 整合方案H: F-Score TTM向量化
预期最终效果
指标	当前	Phase 1后	Phase 2后	提升
Technical日常更新	~25分钟	~3-5分钟	~2-3分钟	90%
Technical全量处理	~30分钟	~25分钟	~8-10分钟	70%
Fundamental全量	~20分钟	~18分钟	~6-8分钟	65%
Quarterly全量	~15分钟	~10分钟	~5-6分钟	65%
峰值内存	8GB	6GB	4-5GB	40%
CPU利用率	15%	40%	85%	5.6x
风险评估与缓解措施
风险	影响	缓解措施
adj_factor检测遗漏	数据不一致	周末force全量校验兜底
多进程内存爆炸	OOM崩溃	限制workers数量、监控内存使用
数据库连接耗尽	连接拒绝	增大max_connections、使用连接池
pickle序列化开销	性能下降	batch_size>=100、评估使用共享内存
并发写入冲突	数据错误	UPSERT语义、保持幂等性
文件修改清单
新增文件
sql/migrations/015_add_last_adj_factor.sql - 新增last_adj_factor列
修改文件
finance_data_hub/preprocessing/storage.py - 向量化upsert，添加last_adj_factor支持
finance_data_hub/cli/preprocess.py - 智能增量策略，批次并发，多进程支持
finance_data_hub/preprocessing/fundamental/valuation.py - raw=True优化
finance_data_hub/database/manager.py - 预处理专用连接池配置
schedules.yml - 调整调度策略（force: false + 周末兜底）
建议实施顺序
强烈推荐首先实施整合方案B（智能增量预处理）+ 方案A（存储层向量化）:

收益最大组合: 日常更新减少90%时间
互补性强: 方案B减少计算量，方案A加速剩余计算
风险可控:
可通过--force回退到全量模式
周末全量校验兜底
立即见效: 每天预处理从25分钟降至3-5分钟
实施完成Phase 1后，再逐步推进Phase 2的其他优化。