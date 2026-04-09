"""
预处理 CLI 命令

提供 fdh-cli preprocess 子命令：
- run: 执行预处理（默认命令）
- status: 查看预处理状态
- info: 显示预处理表信息
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, date
import asyncio
import concurrent.futures
import os
import pandas as pd
import pickle
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeElapsedColumn,
)
from loguru import logger

from ..config import get_settings
from ..database.manager import DatabaseManager
from ..database.operations import DataOperations
from ..preprocessing import (
    AdjustType,
    AdjustProcessor,
    MacroCycleCalculator,
    PreprocessPipeline,
    ProcessedDataStorage,
)
from ..preprocessing.storage import (
    FundamentalDataStorage,
    MacroCycleIndustryStorage,
    MacroCyclePhaseStorage,
)
from ..preprocessing.technical.base import create_indicator

console = Console(legacy_windows=False)

# 创建子应用
preprocess_app = typer.Typer(
    name="preprocess",
    help="数据预处理命令",
    rich_markup_mode="rich"
)

# 默认技术指标
DEFAULT_INDICATORS = [
    "ma_20", "ma_50",
    "macd", "rsi_14", "atr_14", "nda"
]

# 默认频率
DEFAULT_FREQS = ["daily", "weekly", "monthly"]

# 增量预处理配置
# 指标计算所需的最小记录数（按频率）
# 使用记录数而非天数，更精确地控制历史数据需求
RESAMPLE_MIN_RECORDS = {
    "daily": 80,      # MA50需要50条，加buffer
    "weekly": 70,     # MA50需要50条，加buffer（约需350天日线数据）
    "monthly": 30,    # MA20需要20条，加buffer（约需600天日线数据）
}

# 技术指标最小预热窗口（按目标频率的K线根数）
# 这里按 TA-Lib / 行业常见习惯保守估计，避免分段重算时出现长串空值。
INDICATOR_WARMUP_BARS = {
    "ma_20": 20,
    "ma_50": 50,
    "macd": 35,
    "rsi_14": 14,
    "atr_14": 14,
    "nda": 20,
}

# 每种目标频率对应的大致交易日/自然日换算，用于估算回看窗口。
TRADING_DAYS_PER_FREQ = {
    "daily": 1,
    "weekly": 5,
    "monthly": 22,
}

CALENDAR_DAYS_PER_FREQ = {
    "daily": 2,
    "weekly": 7,
    "monthly": 31,
}

# Phase 2: 并发控制配置
DEFAULT_MAX_CONCURRENT_BATCHES = 4  # 默认并发批次数（I/O并发）
DEFAULT_NUM_WORKERS = None  # 默认工作进程数（None表示自动：min(CPU核心数-1, 4)）


def _get_required_bars_for_freq(freq: str, indicators: Optional[List[str]] = None) -> int:
    """
    估算目标频率下技术指标所需的最小K线数量。

    优先使用按指标推导出的窗口，再与经验安全值取最大，避免分段回填时
    因 warm-up 不足导致 MA / MACD / RSI / ATR 在起始阶段被重算为空。
    """
    indicator_names = indicators or DEFAULT_INDICATORS
    indicator_bars = max(
        (INDICATOR_WARMUP_BARS.get(name, 1) for name in indicator_names),
        default=1,
    )
    return max(indicator_bars, RESAMPLE_MIN_RECORDS.get(freq.lower(), indicator_bars))


def _estimate_records_per_symbol(freqs: Optional[List[str]] = None, indicators: Optional[List[str]] = None) -> int:
    """
    估算增量模式下每只股票应抓取的日线记录数。

    不同目标频率需要的 warm-up 长度不同：
    - 日线：直接使用指标窗口
    - 周线：按约 5 个交易日折算
    - 月线：按约 22 个交易日折算
    """
    target_freqs = freqs or ["daily"]
    return max(
        _get_required_bars_for_freq(freq, indicators) * TRADING_DAYS_PER_FREQ.get(freq.lower(), 22)
        for freq in target_freqs
    )


def _estimate_fetch_start_date(
    start_date: Optional[str],
    freqs: Optional[List[str]] = None,
    indicators: Optional[List[str]] = None,
) -> Optional[str]:
    """
    为带 start_date 的技术预处理估算一个更早的抓取起点。

    计算指标时会用到 start_date 之前的历史样本，但最终 upsert 仍只写回用户
    请求的日期范围，从而兼顾正确性与边界控制。
    """
    if not start_date:
        return None

    target_freqs = freqs or ["daily"]
    requested_start = pd.to_datetime(start_date)
    warmup_days = max(
        _get_required_bars_for_freq(freq, indicators) * CALENDAR_DAYS_PER_FREQ.get(freq.lower(), 31)
        for freq in target_freqs
    )
    return (requested_start - timedelta(days=warmup_days)).strftime("%Y-%m-%d")


def _to_local_timestamp(value: Any) -> pd.Timestamp:
    """将时间统一转换为 Asia/Shanghai 时区的 pandas Timestamp。"""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("Asia/Shanghai")
    return ts.tz_convert("Asia/Shanghai")


def _to_local_date(value: Any) -> date:
    """将时间统一转换为 Asia/Shanghai 交易日日期。"""
    return _to_local_timestamp(value).date()


def _get_period_end_date(value: Any, freq: str) -> date:
    """
    获取指定时间所属周期的结束日期（按交易日所在本地日期计算）。
    """
    local_ts = _to_local_timestamp(value)
    freq_key = freq.lower()

    if freq_key == "daily":
        return local_ts.date()

    if freq_key == "weekly":
        days_to_friday = (4 - local_ts.weekday()) % 7
        return (local_ts + pd.Timedelta(days=days_to_friday)).date()

    if freq_key == "monthly":
        return (local_ts + pd.offsets.MonthEnd(0)).date()

    raise ValueError(f"Unsupported frequency: {freq}")


def _build_incremental_upsert_rule(
    freq: str,
    source_latest_time: Any,
    latest_processed_time: Any,
    requested_start_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    为单个频率生成增量回写规则。

    返回：
    - None: 当前频率无新受影响数据，可跳过
    - {"start_date": date, "inclusive": bool}
    """
    if requested_start_date:
        return {
            "start_date": pd.to_datetime(requested_start_date).date(),
            "inclusive": True,
        }

    if source_latest_time is None:
        return None

    if latest_processed_time is None:
        return {"start_date": None, "inclusive": True}

    freq_key = freq.lower()
    source_latest_date = _to_local_date(source_latest_time)
    latest_processed_date = _to_local_date(latest_processed_time)

    if freq_key == "daily":
        if source_latest_date <= latest_processed_date:
            return None
        return {"start_date": latest_processed_date, "inclusive": False}

    source_period_end = _get_period_end_date(source_latest_time, freq_key)

    if latest_processed_date < source_period_end:
        return {"start_date": latest_processed_date, "inclusive": False}

    if latest_processed_date == source_period_end:
        return {"start_date": latest_processed_date, "inclusive": True}

    return None


async def _get_latest_source_times(
    db_manager: DatabaseManager,
    symbols: List[str],
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """查询 symbol_daily 中每只股票的最新时间。"""
    if not symbols:
        return {}

    params: Dict[str, Any] = {"symbols": symbols}
    date_condition = ""
    if end_date:
        date_condition = "AND time <= :end_date"
        params["end_date"] = pd.to_datetime(end_date).to_pydatetime()

    sql = f"""
        SELECT symbol, MAX(time) AS latest_time
        FROM symbol_daily
        WHERE symbol = ANY(:symbols)
        {date_condition}
        GROUP BY symbol
    """

    result = await db_manager.execute_raw_sql(sql, params)
    return {row[0]: row[1] for row in result.fetchall()}


async def _get_latest_processed_times(
    db_manager: DatabaseManager,
    symbols: List[str],
    freqs: List[str],
    adjust_type: str,
) -> Dict[str, Dict[str, Any]]:
    """查询各预处理表中每只股票的最新时间。"""
    if not symbols:
        return {}

    storage = ProcessedDataStorage()
    latest_map: Dict[str, Dict[str, Any]] = {}

    for freq in freqs:
        table_name = storage.TABLE_MAP.get((freq.lower(), adjust_type.lower()))
        if not table_name:
            continue

        sql = f"""
            SELECT symbol, MAX(time) AS latest_time
            FROM {table_name}
            WHERE symbol = ANY(:symbols)
            GROUP BY symbol
        """

        result = await db_manager.execute_raw_sql(sql, {"symbols": symbols})
        latest_map[freq] = {row[0]: row[1] for row in result.fetchall()}

    return latest_map


def _filter_by_upsert_rules(
    df: pd.DataFrame,
    freq: str,
    upsert_rules: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    """
    按 symbol 对结果集应用增量回写规则，只保留受影响区间。
    """
    if df.empty:
        return df

    if not upsert_rules:
        return pd.DataFrame(columns=df.columns)

    filtered_parts = []
    for symbol, group in df.groupby("symbol", sort=False):
        symbol_rules = upsert_rules.get(symbol, {})
        rule = symbol_rules.get(freq)
        if rule is None:
            continue

        if rule.get("start_date") is None:
            filtered_parts.append(group)
            continue

        local_time = pd.to_datetime(group["time"])
        if pd.api.types.is_datetime64tz_dtype(local_time):
            local_dates = local_time.dt.tz_convert("Asia/Shanghai").dt.date
        else:
            local_dates = local_time.dt.tz_localize("Asia/Shanghai").dt.date

        if rule.get("inclusive", True):
            filtered_group = group[local_dates >= rule["start_date"]]
        else:
            filtered_group = group[local_dates > rule["start_date"]]

        if not filtered_group.empty:
            filtered_parts.append(filtered_group)

    if not filtered_parts:
        return pd.DataFrame(columns=df.columns)

    return pd.concat(filtered_parts, ignore_index=True)


def _compute_indicators_in_process(df_bytes: bytes, indicators: List[str], adjust_type: str) -> bytes:
    """
    在子进程中执行指标计算（CPU密集型任务）

    这是多进程优化的核心函数，在独立进程中执行：
    1. 数据反序列化
    2. 复权处理
    3. 技术指标计算（CPU密集型，使用TA-Lib）
    4. 结果序列化返回

    Args:
        df_bytes: pickle序列化的DataFrame
        indicators: 指标名称列表
        adjust_type: 复权类型 (qfq/hfq/none)

    Returns:
        pickle序列化后的结果DataFrame
    """
    import pandas as pd
    from ..preprocessing import AdjustProcessor
    from ..preprocessing.technical.base import create_indicator

    # 反序列化DataFrame
    df = pickle.loads(df_bytes)

    if df.empty:
        return pickle.dumps(df)

    # 复权处理
    adjust_processor = AdjustProcessor()
    if adjust_type == "qfq":
        df = adjust_processor.adjust_qfq(df)
    elif adjust_type == "hfq":
        df = adjust_processor.adjust_hfq(df)

    # 计算技术指标（CPU密集型）
    for ind_name in indicators:
        try:
            indicator = create_indicator(ind_name)
            df = indicator.calculate(df)
        except Exception:
            # 跳过失败的指标
            pass

    return pickle.dumps(df)


def _compute_with_resample(
    df_bytes: bytes, indicators: List[str], adjust_type: str, freqs: List[str]
) -> bytes:
    """
    在子进程中执行: 复权 → 指标计算 → 重采样 → 重采样指标计算
    
    将原来每个频率一次 pickle 序列化/反序列化合并为一次，减少 3x 开销。
    
    Args:
        df_bytes: pickle序列化的原始DataFrame
        indicators: 指标名称列表
        adjust_type: 复权类型
        freqs: 频率列表 e.g. ["daily", "weekly", "monthly"]
    
    Returns:
        pickle序列化的字典: {freq: DataFrame}
    """
    import pandas as pd
    from ..preprocessing import AdjustProcessor
    from ..preprocessing.technical.base import create_indicator
    from ..preprocessing.resample import ResampleProcessor, ResampleFreq

    df = pickle.loads(df_bytes)
    result = {}
    
    if df.empty:
        for freq in freqs:
            result[freq] = pd.DataFrame()
        return pickle.dumps(result)

    # 1. 复权处理
    adjust_processor = AdjustProcessor()
    if adjust_type == "qfq":
        df = adjust_processor.adjust_qfq(df)
    elif adjust_type == "hfq":
        df = adjust_processor.adjust_hfq(df)

    # 2. 日线指标计算
    for ind_name in indicators:
        try:
            indicator = create_indicator(ind_name)
            df = indicator.calculate(df)
        except Exception:
            pass

    resample_processor = ResampleProcessor()
    freq_map = {"weekly": ResampleFreq.WEEKLY, "monthly": ResampleFreq.MONTHLY}

    for freq in freqs:
        if freq.lower() == "daily":
            result[freq] = df
        elif freq.lower() in freq_map:
            # 3. 重采样
            data = resample_processor.resample(df, freq_map[freq.lower()])
            if not data.empty:
                # 4. 重采样后重新计算指标
                for ind_name in indicators:
                    try:
                        indicator = create_indicator(ind_name)
                        data = indicator.calculate(data)
                    except Exception:
                        pass
            result[freq] = data

    return pickle.dumps(result)

def _compute_fundamental_in_process(
    df_bytes: bytes, fina_bytes: bytes
) -> bytes:
    """
    在子进程中执行基本面指标计算（CPU密集型任务）

    计算内容：
    1. PEG 计算（fina_indicator 数据）
    2. 估值分位计算（rolling percentile，CPU密集型）

    Args:
        df_bytes: pickle序列化的 daily_basic DataFrame
        fina_bytes: pickle序列化的 fina_indicator DataFrame（可能为空 bytes）

    Returns:
        pickle序列化后的结果DataFrame
    """
    import pandas as pd
    from ..preprocessing.fundamental.valuation import ValuationPercentile, PEGCalculator

    df = pickle.loads(df_bytes)
    if df.empty:
        return pickle.dumps(df)

    # PEG 计算
    if fina_bytes:
        fina_df = pickle.loads(fina_bytes)
        if not fina_df.empty:
            peg_calculator = PEGCalculator()
            df = peg_calculator.calculate_batch(df, fina_df)

    # 估值分位计算（CPU密集型：rolling percentile）
    valuation = ValuationPercentile()
    df = valuation.calculate(df)

    return pickle.dumps(df)


async def _get_all_stock_symbols(db_manager: DatabaseManager) -> List[str]:
    """获取所有股票代码"""
    await db_manager.initialize()

    sql = """
        SELECT DISTINCT symbol
        FROM symbol_daily
        ORDER BY symbol
    """

    result = await db_manager.execute_raw_sql(sql)
    rows = result.fetchall()
    return [row[0] for row in rows]


async def _classify_stocks_by_adj_factor(
    db_manager: DatabaseManager,
    symbols: List[str],
    verbose: bool = False
) -> tuple[List[str], List[str], dict]:
    """
    根据 adj_factor 变化情况将股票分类为全量组和增量组

    核心逻辑：
    - 前复权公式：复权后价格 = 原始价格 × (当日adj_factor / 最新adj_factor)
    - 当最新 adj_factor 变化时，所有历史前复权价格都会改变
    - 但每天只有约5%的股票发生除权除息（adj_factor变化）

    Args:
        db_manager: 数据库管理器
        symbols: 股票代码列表
        verbose: 是否显示详细信息

    Returns:
        (full_symbols, incr_symbols, adj_factor_map)
        - full_symbols: 需要全量重算的股票（adj_factor变化或从未处理过）
        - incr_symbols: 可以增量处理的股票（adj_factor未变）
        - adj_factor_map: symbol -> 最新adj_factor的字典
    """
    if not symbols:
        return [], [], {}

    sql = """
        WITH current_adj AS (
            SELECT DISTINCT ON (symbol) symbol, adj_factor as current_adj
            FROM adj_factor
            WHERE symbol = ANY(:symbols)
            ORDER BY symbol, time DESC
        ),
        processed_adj AS (
            SELECT DISTINCT ON (symbol) symbol, last_adj_factor
            FROM processed_daily_qfq
            WHERE symbol = ANY(:symbols)
            ORDER BY symbol, time DESC
        )
        SELECT
            c.symbol,
            c.current_adj,
            CASE
                WHEN p.last_adj_factor IS NULL THEN true
                WHEN ABS(c.current_adj - p.last_adj_factor) > 1e-10 THEN true
                ELSE false
            END AS needs_full
        FROM current_adj c
        LEFT JOIN processed_adj p ON c.symbol = p.symbol
    """

    try:
        result = await db_manager.execute_raw_sql(sql, {"symbols": symbols})

        full_symbols = []
        incr_symbols = []
        adj_factor_map = {}

        for row in result.fetchall():
            symbol = row[0]
            current_adj = row[1]
            needs_full = row[2]

            adj_factor_map[symbol] = current_adj
            if needs_full:
                full_symbols.append(symbol)
            else:
                incr_symbols.append(symbol)

        # 检查是否有股票在adj_factor表中不存在（新股或数据缺失）
        processed_symbols = set(full_symbols + incr_symbols)
        missing_symbols = set(symbols) - processed_symbols
        if missing_symbols:
            # 缺失的股票视为需要全量处理
            full_symbols.extend(list(missing_symbols))
            for sym in missing_symbols:
                adj_factor_map[sym] = 1.0  # 默认复权因子

        if verbose:
            console.print(f"  [dim]adj_factor检测: {len(full_symbols)}只全量, {len(incr_symbols)}只增量[/dim]")

        return full_symbols, incr_symbols, adj_factor_map

    except Exception as e:
        logger.warning(f"adj_factor分类失败，全部按全量处理: {e}")
        # 失败时全部按全量处理
        adj_map = {s: 1.0 for s in symbols}
        return symbols, [], adj_map


async def _get_all_stock_symbols_from_daily_basic(db_manager: DatabaseManager) -> List[str]:
    """从 daily_basic 表获取有基本面数据的股票代码"""
    await db_manager.initialize()

    sql = """
        SELECT DISTINCT symbol
        FROM daily_basic
        ORDER BY symbol
    """

    result = await db_manager.execute_raw_sql(sql)
    rows = result.fetchall()
    return [row[0] for row in rows]


async def _get_all_stock_symbols_from_fina_indicator(db_manager: DatabaseManager) -> List[str]:
    """从 fina_indicator 表获取有财务数据的股票代码"""
    await db_manager.initialize()

    sql = """
        SELECT DISTINCT ts_code
        FROM fina_indicator
        ORDER BY ts_code
    """

    result = await db_manager.execute_raw_sql(sql)
    rows = result.fetchall()
    return [row[0] for row in rows]


async def _get_stock_data(
    db_manager: DatabaseManager,
    symbols: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_adj_factor: bool = True
):
    """获取股票日线数据"""
    import pandas as pd
    
    # 构建 WHERE 子句 — 使用 ANY(:symbols) 替代 IN (:sym_0, :sym_1, ...)
    conditions = []
    params = {}
    
    if symbols:
        conditions.append("d.symbol = ANY(:symbols)" if include_adj_factor else "symbol = ANY(:symbols)")
        params["symbols"] = symbols
    
    if start_date:
        col_prefix = "d." if include_adj_factor else ""
        conditions.append(f"{col_prefix}time >= :start_date")
        params["start_date"] = pd.to_datetime(start_date).to_pydatetime()
    
    if end_date:
        col_prefix = "d." if include_adj_factor else ""
        conditions.append(f"{col_prefix}time <= :end_date")
        params["end_date"] = pd.to_datetime(end_date).to_pydatetime()
    
    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
    
    # 构建 SQL，关联复权因子表
    if include_adj_factor:
        sql = f"""
            SELECT 
                d.time, d.symbol, d.open, d.high, d.low, d.close, 
                d.volume, d.amount,
                COALESCE(a.adj_factor, 1.0) as adj_factor
            FROM symbol_daily d
            LEFT JOIN adj_factor a ON d.symbol = a.symbol AND d.time = a.time
            {where_clause}
            ORDER BY d.symbol, d.time
        """
    else:
        sql = f"""
            SELECT 
                time, symbol, open, high, low, close, volume, amount
            FROM symbol_daily
            {where_clause}
            ORDER BY symbol, time
        """
    
    result = await db_manager.execute_raw_sql(sql, params)
    rows = result.fetchall()
    
    if not rows:
        return pd.DataFrame()
    
    if include_adj_factor:
        columns = ["time", "symbol", "open", "high", "low", "close", "volume", "amount", "adj_factor"]
    else:
        columns = ["time", "symbol", "open", "high", "low", "close", "volume", "amount"]
    
    return pd.DataFrame(rows, columns=columns)


async def _get_stock_data_by_records(
    db_manager: DatabaseManager,
    symbols: List[str],
    records_per_symbol: int,
    end_date: Optional[str] = None,
):
    """
    按记录数获取股票日线数据

    对每个股票获取最近的 N 条记录，比按日期范围更精确。
    使用窗口函数 ROW_NUMBER() 实现。

    Args:
        db_manager: 数据库管理器
        symbols: 股票代码列表
        records_per_symbol: 每个股票获取的记录数
        end_date: 结束日期（可选，用于增量处理时限制最新日期）

    Returns:
        DataFrame with columns: time, symbol, open, high, low, close, volume, amount, adj_factor
    """
    import pandas as pd

    if not symbols:
        return pd.DataFrame()

    # 构建日期条件
    date_condition = ""
    params = {"symbols": symbols, "limit": records_per_symbol}

    if end_date:
        date_condition = "AND d.time <= :end_date"
        params["end_date"] = pd.to_datetime(end_date).to_pydatetime()

    # 使用窗口函数获取每个symbol最近的N条记录
    sql = f"""
        SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
        FROM (
            SELECT
                d.time,
                d.symbol,
                d.open,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.amount,
                COALESCE(a.adj_factor, 1.0) as adj_factor,
                ROW_NUMBER() OVER (PARTITION BY d.symbol ORDER BY d.time DESC) as rn
            FROM symbol_daily d
            LEFT JOIN adj_factor a ON d.symbol = a.symbol AND d.time = a.time
            WHERE d.symbol = ANY(:symbols)
            {date_condition}
        ) t
        WHERE rn <= :limit
        ORDER BY symbol, time
    """

    result = await db_manager.execute_raw_sql(sql, params)
    rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    columns = ["time", "symbol", "open", "high", "low", "close", "volume", "amount", "adj_factor"]
    return pd.DataFrame(rows, columns=columns)


async def _run_technical_preprocess(
    db_manager: DatabaseManager,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    indicators: Optional[List[str]] = None,
    freqs: Optional[List[str]] = None,
    adjust_type: str = "qfq",
    batch_size: int = 100,
    verbose: bool = False,
    force: bool = False,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_BATCHES,
    num_workers: Optional[int] = DEFAULT_NUM_WORKERS,
) -> dict:
    """
    执行技术指标预处理（支持智能增量更新 + 并发优化）

    Phase 2 优化：
    1. 异步批次并发：使用 asyncio.Semaphore 控制 I/O 并发度
    2. 多进程 CPU 加速：使用 ProcessPoolExecutor 并行计算指标

    Args:
        max_concurrent: 最大并发批次数（默认4），控制同时处理的批次数量
        num_workers: 进程池工作进程数（默认None=自动），用于CPU密集型计算
    """
    import pandas as pd

    indicators = indicators or DEFAULT_INDICATORS
    freqs = freqs or ["daily"]
    requested_start_date = start_date
    fetch_start_date = _estimate_fetch_start_date(start_date, freqs=freqs, indicators=indicators)

    # 自动确定工作进程数
    if num_workers is None:
        num_workers = min(os.cpu_count() or 4 - 1, 4)
        num_workers = max(1, num_workers)  # 至少1个

    # 获取股票列表
    if not symbols:
        console.print("[cyan]获取股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols(db_manager)
        console.print(f"共 {len(symbols)} 只股票")

    # 智能增量策略：根据 adj_factor 变化分类股票
    if force:
        # 强制全量模式
        full_symbols = symbols
        incr_symbols = []
        adj_factor_map = {}
        console.print("[yellow]强制全量模式: 所有股票将全量重算[/yellow]")
    else:
        # 智能增量模式
        console.print("[cyan]检测 adj_factor 变化...[/cyan]")
        full_symbols, incr_symbols, adj_factor_map = await _classify_stocks_by_adj_factor(
            db_manager, symbols, verbose=verbose
        )
        console.print(f"  初始全量重算: {len(full_symbols)} 只 (adj_factor变化或首次处理)")
        console.print(f"  初始增量处理: {len(incr_symbols)} 只 (adj_factor未变)")

    incremental_plan: Dict[str, Dict[str, Any]] = {}
    if incr_symbols:
        source_latest_map = await _get_latest_source_times(
            db_manager,
            incr_symbols,
            end_date=end_date,
        )
        processed_latest_map = await _get_latest_processed_times(
            db_manager,
            incr_symbols,
            freqs,
            adjust_type,
        )

        promoted_to_full = []
        planned_incr_symbols = []

        for symbol in incr_symbols:
            source_latest_time = source_latest_map.get(symbol)
            if source_latest_time is None:
                continue

            symbol_rules: Dict[str, Dict[str, Any]] = {}
            needs_full = False

            for freq in freqs:
                latest_processed_time = processed_latest_map.get(freq, {}).get(symbol)

                if requested_start_date is None and latest_processed_time is None:
                    needs_full = True
                    break

                rule = _build_incremental_upsert_rule(
                    freq,
                    source_latest_time,
                    latest_processed_time,
                    requested_start_date=requested_start_date,
                )
                if rule is not None:
                    symbol_rules[freq] = rule

            if needs_full:
                promoted_to_full.append(symbol)
                continue

            if not symbol_rules:
                continue

            rule_dates = [
                rule["start_date"]
                for rule in symbol_rules.values()
                if rule.get("start_date") is not None
            ]
            anchor_start = requested_start_date or (min(rule_dates).isoformat() if rule_dates else None)
            fetch_start = (
                _estimate_fetch_start_date(
                    anchor_start,
                    freqs=list(symbol_rules.keys()),
                    indicators=indicators,
                )
                if anchor_start
                else None
            )

            incremental_plan[symbol] = {
                "fetch_start_date": fetch_start,
                "upsert_rules": symbol_rules,
            }
            planned_incr_symbols.append(symbol)

        if promoted_to_full:
            full_symbols.extend(promoted_to_full)

        incr_symbols = planned_incr_symbols

        if verbose:
            console.print(f"  [dim]增量计划生成: {len(incr_symbols)}只待增量, {len(promoted_to_full)}只补升为全量[/dim]")

    console.print(f"  最终全量重算: {len(full_symbols)} 只")
    console.print(f"  最终增量处理: {len(incr_symbols)} 只")

    # 初始化存储（resample 已移入子进程）
    storage = ProcessedDataStorage(db_manager)

    total_symbols = 0
    total_records = 0
    total_full = 0
    total_incr = 0

    # 创建信号量控制并发
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_batch(
        batch_symbols: List[str],
        is_full: bool,
        batch_fetch_start_date: Optional[str] = None,
        task=None,
        upsert_start_date: Optional[str] = None,
        symbol_upsert_rules: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> tuple[int, int]:
        """
        处理单个批次（支持并发控制）

        Args:
            batch_symbols: 股票代码列表
            is_full: 是否全量处理
            batch_fetch_start_date: 抓取开始日期（全量模式使用，可能早于用户请求的start_date）
            task: 进度任务
            upsert_start_date: 最终写回的开始日期（用于裁掉 warm-up 数据）
            symbol_upsert_rules: 每只股票、每个频率的精确回写规则（增量模式使用）

        Returns:
            (processed_symbols, processed_records)
        """
        async with semaphore:
            # 获取数据
            df = await _get_stock_data(
                db_manager,
                batch_symbols,
                batch_fetch_start_date,
                end_date,
                include_adj_factor=True
            )

            if df.empty:
                if task is not None:
                    progress.advance(task, len(batch_symbols))
                return len(batch_symbols), 0

            # 使用进程池执行CPU密集型计算（复权+指标+重采样，一次 pickle）
            loop = asyncio.get_event_loop()
            df_bytes = pickle.dumps(df)
            result_bytes = await loop.run_in_executor(
                executor,
                _compute_with_resample,
                df_bytes,
                indicators,
                adjust_type,
                freqs
            )
            freq_results = pickle.loads(result_bytes)

            # 添加 last_adj_factor 列（向量化 .map()，替代逐股票循环）
            batch_records = 0

            for freq in freqs:
                data = freq_results.get(freq)
                if data is None or data.empty:
                    continue

                if symbol_upsert_rules is not None:
                    data = _filter_by_upsert_rules(data, freq, symbol_upsert_rules)
                    if data.empty:
                        continue
                elif upsert_start_date:
                    local_time = pd.to_datetime(data["time"])
                    if pd.api.types.is_datetime64tz_dtype(local_time):
                        local_dates = local_time.dt.tz_convert("Asia/Shanghai").dt.date
                    else:
                        local_dates = local_time.dt.tz_localize("Asia/Shanghai").dt.date
                    data = data[local_dates >= pd.to_datetime(upsert_start_date).date()].copy()
                    if data.empty:
                        continue

                # 向量化设置 last_adj_factor
                if adj_factor_map:
                    data['last_adj_factor'] = data['symbol'].map(adj_factor_map)

                # 存储数据
                count = await storage.upsert(data, freq=freq, adjust_type=adjust_type)
                batch_records += count

            if task is not None:
                progress.advance(task, len(batch_symbols))

            return len(batch_symbols), batch_records

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress, concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        # 处理全量组（并发执行）
        if full_symbols:
            task_full = progress.add_task(
                f"[cyan]全量处理 (并发{max_concurrent}, 进程{num_workers})...",
                total=len(full_symbols)
            )

            # 创建所有批次的任务
            batch_tasks = []
            for i in range(0, len(full_symbols), batch_size):
                batch_symbols = full_symbols[i:i+batch_size]
                if verbose:
                    progress.console.print(f"  [全量]批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
                batch_tasks.append(
                    process_batch(
                        batch_symbols,
                        True,
                        fetch_start_date,
                        task_full,
                        upsert_start_date=requested_start_date,
                    )
                )

            # 并发执行所有批次
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # 汇总结果
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"批次处理失败: {result}")
                    progress.console.print(f"  [red]批次失败: {result}[/red]")
                    continue
                batch_symbols_count, batch_records = result
                total_symbols += batch_symbols_count
                total_full += batch_symbols_count
                total_records += batch_records

        # 处理增量组（并发执行）
        if incr_symbols:
            task_incr = progress.add_task(
                f"[cyan]增量处理 (并发{max_concurrent}, 进程{num_workers})...",
                total=len(incr_symbols)
            )

            # 创建所有批次的任务
            batch_tasks = []
            for i in range(0, len(incr_symbols), batch_size):
                batch_symbols = incr_symbols[i:i+batch_size]
                batch_plan = {sym: incremental_plan[sym]["upsert_rules"] for sym in batch_symbols}
                batch_fetch_starts = [
                    incremental_plan[sym]["fetch_start_date"]
                    for sym in batch_symbols
                    if incremental_plan[sym].get("fetch_start_date")
                ]
                batch_fetch_start = min(batch_fetch_starts) if batch_fetch_starts else None
                if verbose:
                    progress.console.print(f"  [增量]批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
                    if batch_fetch_start:
                        progress.console.print(f"    [dim]抓取窗口起点: {batch_fetch_start}[/dim]")
                batch_tasks.append(
                    process_batch(
                        batch_symbols,
                        False,
                        batch_fetch_start,
                        task_incr,
                        symbol_upsert_rules=batch_plan,
                    )
                )

            # 并发执行所有批次
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # 汇总结果
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"批次处理失败: {result}")
                    progress.console.print(f"  [red]批次失败: {result}[/red]")
                    continue
                batch_symbols_count, batch_records = result
                total_symbols += batch_symbols_count
                total_incr += batch_symbols_count
                total_records += batch_records

    if full_symbols:
        console.print(f"  [dim]全量处理完成: {total_full} 只股票[/dim]")
    if incr_symbols:
        console.print(f"  [dim]增量处理完成: {total_incr} 只股票[/dim]")

    return {
        "symbols_processed": total_symbols,
        "records_processed": total_records,
        "full_processed": total_full,
        "incremental_processed": total_incr,
    }


async def _run_fundamental_preprocess(
    db_manager: DatabaseManager,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    verbose: bool = False,
    force: bool = False,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_BATCHES,
    num_workers: Optional[int] = None,
) -> dict:
    """执行基本面指标预处理（支持智能增量 + 多进程 + 异步并发）
    
    优化架构：
    1. 智能增量：仅处理新数据
    2. 多进程：CPU密集型的 rolling percentile 计算在子进程执行
    3. 异步并发：多批次 I/O（DB查询 + upsert）并行执行
    """
    import pandas as pd
    from datetime import timedelta

    # 自动确定工作进程数
    if num_workers is None:
        num_workers = min(os.cpu_count() - 1, 4) if os.cpu_count() and os.cpu_count() > 1 else 1

    # 获取股票列表（从 daily_basic 表获取，确保有基本面数据）
    if not symbols:
        console.print("[cyan]获取有基本面数据的股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols_from_daily_basic(db_manager)
        console.print(f"共 {len(symbols)} 只股票有基本面数据")

    # === 智能增量：确定时间范围 ===
    WINDOW_BUFFER_DAYS = 1900
    data_start_date = start_date
    upsert_cutoff = None
    incremental_mode = False

    if not force and not start_date:
        try:
            result = await db_manager.execute_raw_sql(
                "SELECT MAX(time) FROM processed_valuation_pct"
            )
            row = result.fetchone()
            if row and row[0] is not None:
                latest_time = pd.to_datetime(row[0])
                data_start_date = (latest_time - timedelta(days=WINDOW_BUFFER_DAYS)).strftime("%Y-%m-%d")
                upsert_cutoff = (latest_time - timedelta(days=5)).to_pydatetime()
                incremental_mode = True
                console.print(f"[green]智能增量模式[/green]: 已处理至 {latest_time.strftime('%Y-%m-%d')}")
                console.print(f"  数据窗口: {data_start_date} → 今天")
                console.print(f"  写入范围: {(latest_time - timedelta(days=5)).strftime('%Y-%m-%d')} → 今天")
        except Exception as e:
            if verbose:
                console.print(f"[yellow]无法获取最新时间，使用全量模式: {e}[/yellow]")

    if not incremental_mode and not start_date:
        console.print("[yellow]全量处理模式[/yellow]")

    storage = FundamentalDataStorage(db_manager)
    total_symbols = 0
    total_records = 0
    semaphore = asyncio.Semaphore(max_concurrent)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress, concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        mode_label = "增量" if incremental_mode else "全量"
        task_id = progress.add_task(
            f"[cyan]{mode_label}预处理基本面 (并发{max_concurrent}, 进程{num_workers})...",
            total=len(symbols)
        )

        async def process_batch(batch_syms: List[str]) -> tuple:
            """处理单个批次（I/O + CPU 并发）"""
            async with semaphore:
                import pandas as pd
                placeholders = ", ".join([f":sym_{j}" for j in range(len(batch_syms))])
                params = {f"sym_{j}": sym for j, sym in enumerate(batch_syms)}

                conditions = [f"symbol IN ({placeholders})"]
                if data_start_date:
                    conditions.append("time >= :start_date")
                    params["start_date"] = pd.to_datetime(data_start_date).to_pydatetime()
                if end_date:
                    conditions.append("time <= :end_date")
                    params["end_date"] = pd.to_datetime(end_date).to_pydatetime()
                where_clause = " AND ".join(conditions)

                sql = f"""
                    SELECT time, symbol, pe_ttm, pb, ps_ttm, dv_ttm
                    FROM daily_basic
                    WHERE {where_clause}
                    ORDER BY symbol, time
                """
                fina_sql = f"""
                    SELECT ts_code, ann_date_time, end_date_time, netprofit_yoy
                    FROM fina_indicator
                    WHERE ts_code IN ({placeholders})
                      AND ann_date_time IS NOT NULL
                    ORDER BY ts_code, ann_date_time
                """

                # 并行查询两张表
                db_result, fina_result = await asyncio.gather(
                    db_manager.execute_raw_sql(sql, params),
                    db_manager.execute_raw_sql(fina_sql, params),
                )

                rows = db_result.fetchall()
                if not rows:
                    progress.advance(task_id, len(batch_syms))
                    return 0, 0

                df = pd.DataFrame(rows, columns=["time", "symbol", "pe_ttm", "pb", "ps_ttm", "dv_ttm"])
                fina_rows = fina_result.fetchall()
                fina_df = pd.DataFrame(
                    fina_rows,
                    columns=["ts_code", "ann_date_time", "end_date_time", "netprofit_yoy"]
                ) if fina_rows else pd.DataFrame()

                # 在子进程中执行 CPU 密集型计算
                loop = asyncio.get_event_loop()
                df_bytes = pickle.dumps(df)
                fina_bytes = pickle.dumps(fina_df) if not fina_df.empty else b""
                result_bytes = await loop.run_in_executor(
                    executor,
                    _compute_fundamental_in_process,
                    df_bytes,
                    fina_bytes,
                )
                df = pickle.loads(result_bytes)

                # 增量模式：仅 upsert 新数据
                if upsert_cutoff is not None:
                    df = df[df["time"] >= upsert_cutoff]

                batch_records = 0
                if not df.empty:
                    batch_records = await storage.upsert(df)

                progress.advance(task_id, len(batch_syms))
                return len(batch_syms), batch_records

        # 创建所有批次任务
        batch_tasks = []
        for i in range(0, len(symbols), batch_size):
            batch_syms = symbols[i:i+batch_size]
            if verbose:
                progress.console.print(f"  批次 {i//batch_size + 1}: {len(batch_syms)} 只股票")
            batch_tasks.append(process_batch(batch_syms))

        # 并发执行所有批次
        results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"批次处理失败: {result}")
                if verbose:
                    progress.console.print(f"  [red]批次失败: {result}[/red]")
                continue
            batch_count, batch_records = result
            total_symbols += batch_count
            total_records += batch_records

    return {
        "symbols_processed": total_symbols,
        "records_processed": total_records,
    }




async def _run_quarterly_fundamental_preprocess(
    db_manager: DatabaseManager,
    symbols: Optional[List[str]] = None,
    batch_size: int = 50,
    verbose: bool = False,
    force: bool = False,
) -> dict:
    """执行季度基本面指标预处理（F-Score等，支持智能增量）
    
    智能增量逻辑：
    1. 查询 processed_fundamental_quality 中的最新 end_date_time
    2. 数据始终全量拉取（季度数据量小，且 rolling(20) 需要 5 年窗口）
    3. F-Score 全量计算（确保累计→TTM 转换准确）
    4. 仅 upsert end_date_time > latest - 1 季度的新记录
    5. force=True 时全量写入
    """
    import pandas as pd
    from datetime import timedelta
    import json
    from ..preprocessing.fundamental.quality import FScoreCalculator
    from ..preprocessing.storage import QuarterlyFundamentalDataStorage

    # 获取股票列表（从 fina_indicator 表获取，确保有财务数据）
    if not symbols:
        console.print("[cyan]获取有财务数据的股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols_from_fina_indicator(db_manager)
        console.print(f"共 {len(symbols)} 只股票有财务数据")
    
    # === 智能增量：确定 upsert 范围 ===
    upsert_cutoff = None  # None = upsert 全部
    incremental_mode = False

    if not force:
        try:
            result = await db_manager.execute_raw_sql(
                "SELECT MAX(end_date_time) FROM processed_fundamental_quality"
            )
            row = result.fetchone()
            if row and row[0] is not None:
                latest_end_date = pd.to_datetime(row[0])
                # 回退 1 个季度（约 100 天）作为安全边际
                upsert_cutoff = (latest_end_date - timedelta(days=100)).to_pydatetime()
                incremental_mode = True
                console.print(f"[green]智能增量模式[/green]: 已处理至 {latest_end_date.strftime('%Y-%m-%d')}")
                console.print(f"  写入范围: {(latest_end_date - timedelta(days=100)).strftime('%Y-%m-%d')} 之后的季度")
        except Exception as e:
            if verbose:
                console.print(f"[yellow]无法获取最新时间，使用全量模式: {e}[/yellow]")

    if not incremental_mode:
        console.print("[yellow]全量处理模式[/yellow]")

    # 加载行业配置
    import os
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "industry_config.json"
    )
    if not os.path.exists(config_path):
        config_path = None
    
    # 初始化计算器和存储
    calculator = FScoreCalculator(industry_config_path=config_path)
    storage = QuarterlyFundamentalDataStorage(db_manager)

    # 获取股票-行业映射（用于获取豁免规则）
    # 从 sw_industry_member 表获取最新的行业分类
    industry_sql = """
        SELECT ts_code, l3_name
        FROM sw_industry_member
        WHERE is_new = 'Y'
    """
    try:
        industry_result = await db_manager.execute_raw_sql(industry_sql)
        industry_rows = industry_result.fetchall()
        # 构建 ts_code -> l3_name 映射
        stock_industry_map = {row[0]: row[1] for row in industry_rows if row[1]}
        if verbose:
            console.print(f"  已获取 {len(stock_industry_map)} 只股票的行业分类")
    except Exception as e:
        if verbose:
            console.print(f"[yellow]获取行业分类失败: {e}，将不使用行业豁免规则[/yellow]")
        stock_industry_map = {}

    total_symbols = 0
    total_records = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        mode_label = "增量" if incremental_mode else "全量"
        task = progress.add_task(
            f"[cyan]{mode_label}预处理季度F-Score...",
            total=len(symbols)
        )
        
        # 分批处理
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            
            if verbose:
                progress.console.print(f"  批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
            
            placeholders = ", ".join([f":sym_{j}" for j in range(len(batch_symbols))])
            params = {f"sym_{j}": sym for j, sym in enumerate(batch_symbols)}
            
            try:
                # 并行查询4张表（性能优化）
                fina_sql = f"""
                    SELECT ts_code, end_date_time as end_date, ann_date_time as ann_date,
                           roe, roe_yearly, roe_dt, roa, grossprofit_margin, assets_turn,
                           netprofit_margin, current_ratio, debt_to_assets, netprofit_yoy,
                           q_gsprofit_margin, q_netprofit_margin, q_roe
                    FROM fina_indicator
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date_time
                """
                bs_sql = f"""
                    SELECT ts_code, end_date_time as end_date, f_ann_date_time as f_ann_date,
                           total_assets, total_liab, total_ncl, total_cur_assets, total_cur_liab, total_share
                    FROM balancesheet
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date_time
                """
                cf_sql = f"""
                    SELECT ts_code, end_date_time as end_date, f_ann_date_time as f_ann_date, n_cashflow_act
                    FROM cashflow
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date_time
                """
                inc_sql = f"""
                    SELECT ts_code, end_date_time as end_date, f_ann_date_time as f_ann_date, n_income
                    FROM income
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date_time
                """

                # 使用 asyncio.gather 并行执行4个查询
                fina_result, bs_result, cf_result, inc_result = await asyncio.gather(
                    db_manager.execute_raw_sql(fina_sql, params),
                    db_manager.execute_raw_sql(bs_sql, params),
                    db_manager.execute_raw_sql(cf_sql, params),
                    db_manager.execute_raw_sql(inc_sql, params),
                )

                fina_rows = fina_result.fetchall()
                if not fina_rows:
                    progress.advance(task, len(batch_symbols))
                    continue

                fina_df = pd.DataFrame(fina_rows, columns=[
                    "ts_code", "end_date", "ann_date",
                    "roe", "roe_yearly", "roe_dt", "roa", "grossprofit_margin", "assets_turn",
                    "netprofit_margin", "current_ratio", "debt_to_assets", "netprofit_yoy",
                    "q_gsprofit_margin", "q_netprofit_margin", "q_roe"
                ])

                bs_rows = bs_result.fetchall()
                bs_df = pd.DataFrame(bs_rows, columns=[
                    "ts_code", "end_date", "f_ann_date",
                    "total_assets", "total_liab", "total_ncl", "total_cur_assets", "total_cur_liab", "total_share"
                ]) if bs_rows else pd.DataFrame()

                cf_rows = cf_result.fetchall()
                cf_df = pd.DataFrame(cf_rows, columns=[
                    "ts_code", "end_date", "f_ann_date", "n_cashflow_act"
                ]) if cf_rows else pd.DataFrame()

                inc_rows = inc_result.fetchall()
                inc_df = pd.DataFrame(inc_rows, columns=[
                    "ts_code", "end_date", "f_ann_date", "n_income"
                ]) if inc_rows else pd.DataFrame()

                # 构建豁免规则映射（根据股票所属行业）
                exemptions_map = {}
                for sym in batch_symbols:
                    l3_name = stock_industry_map.get(sym)
                    if l3_name:
                        exemptions_map[sym] = calculator.get_exemptions_for_industry(l3_name)

                # 计算 F-Score（始终全量计算，确保 TTM 转换准确）
                fscore_df = calculator.calculate(
                    fina_indicator=fina_df,
                    balancesheet=bs_df,
                    cashflow=cf_df,
                    income=inc_df,
                    exemptions=None,  # 不使用全局豁免
                    exemptions_map=exemptions_map  # 使用按股票的豁免规则
                )
                
                if fscore_df.empty:
                    progress.advance(task, len(batch_symbols))
                    continue
                
                # 转换列名以匹配数据库表
                # quality.py 返回的是 f_ann_date_final，需要映射为 f_ann_date_time
                fscore_df = fscore_df.rename(columns={
                    "end_date": "end_date_time",
                    "ann_date": "ann_date_time",
                    "f_ann_date_final": "f_ann_date_time"
                })
                
                # 转换日期列
                for col in ["end_date_time", "ann_date_time", "f_ann_date_time"]:
                    if col in fscore_df.columns:
                        fscore_df[col] = pd.to_datetime(fscore_df[col])
                
                # 增量模式：仅 upsert 新季度数据
                if upsert_cutoff is not None and "end_date_time" in fscore_df.columns:
                    fscore_df = fscore_df[fscore_df["end_date_time"] >= upsert_cutoff]
                
                if fscore_df.empty:
                    progress.advance(task, len(batch_symbols))
                    continue

                # 存储数据
                count = await storage.upsert(fscore_df)
                total_records += count
                total_symbols += len(batch_symbols)
                
            except Exception as e:
                if verbose:
                    progress.console.print(f"  [yellow]批次处理失败: {e}[/yellow]")
                    import traceback
                    progress.console.print(traceback.format_exc())
            
            progress.advance(task, len(batch_symbols))
    
    return {
        "symbols_processed": total_symbols,
        "records_processed": total_records,
    }


async def _get_table_stats(db_manager: DatabaseManager, table_name: str) -> dict:
    """获取表统计信息"""
    try:
        distinct_expr = "COUNT(DISTINCT symbol)"
        if table_name == MacroCyclePhaseStorage.TABLE_NAME:
            distinct_expr = "0"
        elif table_name == MacroCycleIndustryStorage.TABLE_NAME:
            distinct_expr = "COUNT(DISTINCT l3_name)"

        sql = f"""
            SELECT 
                COUNT(*) as record_count,
                {distinct_expr} as symbol_count,
                MIN(time) as min_time,
                MAX(time) as max_time,
                MAX(processed_at) as last_update
            FROM {table_name}
        """
        result = await db_manager.execute_raw_sql(sql)
        row = result.fetchone()
        
        if row:
            return {
                "record_count": row[0] or 0,
                "symbol_count": row[1] or 0,
                "min_time": row[2],
                "max_time": row[3],
                "last_update": row[4],
            }
    except Exception:
        pass
    
    return {
        "record_count": 0,
        "symbol_count": 0,
        "min_time": None,
        "max_time": None,
        "last_update": None,
    }


async def _run_macro_cycle_preprocess(
    db_manager: DatabaseManager,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """执行中国宏观周期预处理（全量重建）。"""
    from ..preprocessing.fundamental.industry_config import get_industry_config_loader

    if start_date or end_date:
        console.print("[yellow]macro_cycle 预处理固定执行全量重建，忽略 --start-date/--end-date[/yellow]")

    console.print("[cyan]读取宏观原始数据...[/cyan]")
    ops = DataOperations(db_manager)

    m_df = await ops.get_cn_m()
    ppi_df = await ops.get_cn_ppi()
    pmi_df = await ops.get_cn_pmi()
    gdp_df = await ops.get_cn_gdp()

    console.print("[cyan]读取行业映射数据...[/cyan]")
    result = await db_manager.execute_raw_sql(
        """
        SELECT l1_code, l1_name, l2_code, l2_name, l3_code, l3_name, is_new
        FROM sw_industry_member
        """
    )
    industry_rows = result.fetchall()
    industry_df = pd.DataFrame(
        industry_rows,
        columns=["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name", "is_new"],
    )

    config_loader = get_industry_config_loader()
    current_l3 = set()
    if not industry_df.empty:
        current_l3 = set(
            industry_df.loc[industry_df["is_new"] == "Y", "l3_name"].dropna().unique()
        )
    config_l3 = set(config_loader.get_all_industries())
    missing_in_config = sorted(current_l3 - config_l3)
    if missing_in_config:
        preview = ", ".join(missing_in_config[:20])
        raise ValueError(f"industry_config.json 缺少 {len(missing_in_config)} 个三级行业配置: {preview}")

    calculator = MacroCycleCalculator()
    phase_df = calculator.calculate(m_df, ppi_df, pmi_df, gdp_df)
    industry_snapshot_df = calculator.build_industry_snapshot(phase_df, industry_df)

    phase_storage = MacroCyclePhaseStorage(db_manager)
    industry_storage = MacroCycleIndustryStorage(db_manager)

    console.print("[cyan]写入宏观周期主表...[/cyan]")
    phase_count = await phase_storage.replace_all(phase_df)

    console.print("[cyan]写入宏观周期行业快照表...[/cyan]")
    industry_count = await industry_storage.replace_all(industry_snapshot_df)

    if verbose and not phase_df.empty:
        console.print(
            f"[dim]宏观周期范围: {phase_df['observation_time'].min().strftime('%Y-%m')} "
            f"~ {phase_df['observation_time'].max().strftime('%Y-%m')}[/dim]"
        )

    return {
        "symbols_processed": 0,
        "records_processed": phase_count + industry_count,
        "phase_records": phase_count,
        "industry_records": industry_count,
    }


async def _run_industry_valuation_preprocess(
    db_manager: DatabaseManager,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    verbose: bool = False,
    force: bool = False,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT_BATCHES,
) -> dict:
    """执行行业差异化估值预处理（支持智能增量 + 并发优化）

    根据行业配置自动选择核心估值指标（PE/PB/PS/PEG），
    计算自身历史分位和行业内相对分位。

    智能增量：
    - 检查 processed_industry_valuation 的最新时间
    - 只处理 processed_valuation_pct 中新增的数据

    并发优化：
    1. I/O 并发：使用 asyncio.Semaphore 控制批次并发度
    2. 批量查询：一次查询多只股票的数据，减少数据库往返

    数据来源:
    - processed_valuation_pct: 估值数据
    - sw_industry_member: 行业分类
    - industry_config.json: 行业配置
    """
    import pandas as pd
    from datetime import timedelta
    from ..preprocessing.fundamental.industry_valuation import IndustryValuationCalculator
    from ..preprocessing.storage import IndustryValuationStorage, FundamentalDataStorage

    # 1. 获取股票列表
    if not symbols:
        console.print("[cyan]获取股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols_from_daily_basic(db_manager)
        console.print(f"共 {len(symbols)} 只股票")

    # 2. 智能增量：确定时间范围
    incremental_mode = False
    data_start_date = start_date
    upsert_cutoff = None

    # 获取源数据（processed_valuation_pct）的最新时间
    result = await db_manager.execute_raw_sql(
        "SELECT MAX(time) FROM processed_valuation_pct"
    )
    row = result.fetchone()
    source_latest_time = pd.to_datetime(row[0]) if row and row[0] else None

    if not force and not start_date and source_latest_time:
        # 检查已处理数据的最新时间
        result = await db_manager.execute_raw_sql(
            "SELECT MAX(time) FROM processed_industry_valuation"
        )
        row = result.fetchone()
        processed_latest_time = pd.to_datetime(row[0]) if row and row[0] else None

        if processed_latest_time:
            # 有已处理数据，检查是否需要增量更新
            if processed_latest_time >= source_latest_time:
                console.print(f"[green]已处理至 {processed_latest_time.strftime('%Y-%m-%d')}，无需更新[/green]")
                return {
                    "symbols_processed": 0,
                    "records_processed": 0,
                    "message": "已处理至最新，无需更新"
                }

            # 需要增量更新
            incremental_mode = True
            # 从已处理时间的下一天开始
            data_start_date = (processed_latest_time + timedelta(days=1)).strftime("%Y-%m-%d")
            upsert_cutoff = processed_latest_time.to_pydatetime()
            console.print(f"[green]智能增量模式[/green]: 已处理至 {processed_latest_time.strftime('%Y-%m-%d')}")
            console.print(f"  新数据范围: {data_start_date} → {source_latest_time.strftime('%Y-%m-%d')}")
        else:
            # 无已处理数据，使用源数据的时间范围
            if not start_date:
                result = await db_manager.execute_raw_sql(
                    "SELECT MIN(time) FROM processed_valuation_pct"
                )
                row = result.fetchone()
                if row and row[0]:
                    data_start_date = row[0].strftime("%Y-%m-%d")
                else:
                    data_start_date = (pd.Timestamp.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
            console.print("[yellow]全量处理模式[/yellow]")

    # 确定结束日期
    if not end_date:
        if source_latest_time:
            end_date = source_latest_time.strftime("%Y-%m-%d")
        else:
            end_date = pd.Timestamp.now().strftime("%Y-%m-%d")

    console.print(f"日期范围: {data_start_date or start_date} ~ {end_date}")

    # 3. 预查询行业分类数据（一次性查询所有股票的行业分类，减少数据库往返）
    console.print("[cyan]预查询行业分类数据...[/cyan]")
    all_industry_sql = """
        SELECT ts_code, l1_code, l1_name, l2_code, l2_name, l3_code, l3_name
        FROM sw_industry_member
        WHERE is_new = 'Y'
    """
    result = await db_manager.execute_raw_sql(all_industry_sql)
    industry_rows = result.fetchall()
    all_industry_df = pd.DataFrame(industry_rows, columns=[
        "ts_code", "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"
    ])
    console.print(f"  行业分类: {len(all_industry_df)} 条记录")

    # 4. 初始化
    calculator = IndustryValuationCalculator()
    storage = IndustryValuationStorage(db_manager)
    valuation_storage = FundamentalDataStorage(db_manager)

    total_symbols = 0
    total_records = 0

    # 创建信号量控制并发
    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_batch(batch_symbols: List[str]) -> tuple[int, int]:
        """处理单个批次"""
        async with semaphore:
            try:
                # 查询估值数据（使用增量时间范围）
                valuation_df = await valuation_storage.query(
                    symbols=batch_symbols,
                    start_date=data_start_date or start_date,
                    end_date=end_date
                )

                if valuation_df.empty:
                    return 0, 0

                # 从预查询的行业数据中筛选
                industry_df = all_industry_df[all_industry_df["ts_code"].isin(batch_symbols)]

                if industry_df.empty:
                    return 0, 0

                # 计算行业差异化估值
                calc_result = calculator.calculate(
                    valuation_df=valuation_df,
                    industry_members_df=industry_df
                )

                if calc_result.empty:
                    return 0, 0

                # 写入数据库
                count = await storage.upsert(calc_result, batch_size=1000)
                return calc_result["symbol"].nunique(), count

            except Exception as e:
                if verbose:
                    console.print(f"[red]批次处理失败: {e}[/red]")
                return 0, 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[cyan]预处理行业差异化估值 (并发{max_concurrent})...",
            total=len(symbols)
        )

        # 创建所有批次的任务
        batch_tasks = []
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            batch_tasks.append(process_batch(batch_symbols))

        # 使用 as_completed 实现实时进度更新
        for coro in asyncio.as_completed(batch_tasks):
            try:
                result = await coro
                symbols_count, records_count = result
                total_symbols += symbols_count
                total_records += records_count
                progress.advance(task, batch_size)
            except Exception as e:
                if verbose:
                    console.print(f"[red]批次异常: {e}[/red]")
                progress.advance(task, batch_size)

    return {
        "symbols_processed": total_symbols,
        "records_processed": total_records,
    }


@preprocess_app.command("run")
def run_preprocess(
    all_data: bool = typer.Option(
        False,
        "--all",
        "-a",
        help="处理全部股票"
    ),
    category: Optional[str] = typer.Option(
        None,
        "--category",
        "-c",
        help="预处理类别 (technical, fundamental, quarterly_fundamental, industry_valuation, macro_cycle, all)"
    ),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="股票代码列表（逗号分隔）"
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="开始日期 (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="结束日期 (YYYY-MM-DD)"
    ),
    freq: Optional[str] = typer.Option(
        None,
        "--freq",
        "-f",
        help="频率列表（逗号分隔: daily,weekly,monthly）"
    ),
    adjust: str = typer.Option(
        "qfq",
        "--adjust",
        help="复权类型 (qfq, hfq, none)"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="强制全量重新计算（忽略增量更新）"
    ),
    batch_size: int = typer.Option(
        100,
        "--batch-size",
        "-b",
        help="批处理大小"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细日志"
    ),
    max_concurrent: int = typer.Option(
        DEFAULT_MAX_CONCURRENT_BATCHES,
        "--max-concurrent",
        "-C",
        help="最大并发批次数（I/O并发，默认4）"
    ),
    num_workers: int = typer.Option(
        0,
        "--num-workers",
        "-w",
        help="进程池工作进程数（CPU并发，0=自动）"
    ),
):
    """
    执行数据预处理

    计算技术指标和基本面指标，并存储到预处理表。

    示例:
        # 全量预处理所有类别
        fdh-cli preprocess run --all --category all --force

        # 只处理技术指标
        fdh-cli preprocess run --all --category technical

        # 处理指定股票
        fdh-cli preprocess run --symbols 600519.SH,000858.SZ --category technical
    """
    console.print("[bold blue]数据预处理[/bold blue]\n")

    # 解析参数
    symbol_list = None
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",")]
    
    freq_list = None
    if freq:
        freq_list = [f.strip().lower() for f in freq.split(",")]
    else:
        freq_list = ["daily"]  # 默认只处理日线

    category = category or "technical"

    requires_symbol_scope = category not in ["macro_cycle"]

    # 参数校验
    if requires_symbol_scope and not all_data and not symbols:
        console.print("[yellow]提示: 使用 --all 处理全部股票，或使用 --symbols 指定股票代码[/yellow]")
        console.print("示例: fdh-cli preprocess run --all --category technical")
        raise typer.Exit(1)

    if category == "macro_cycle" and symbol_list:
        console.print("[yellow]macro_cycle 预处理不使用 --symbols 参数，将忽略该参数[/yellow]")
        symbol_list = None

    # 如果使用 --force，清除日期限制进行全量处理
    if force:
        console.print("[yellow]强制模式: 将进行全量重新计算[/yellow]")
        start_date = None
    
    console.print(f"类别: {category}")
    console.print(f"复权类型: {adjust}")
    console.print(f"频率: {', '.join(freq_list)}")
    if symbol_list:
        console.print(f"股票数量: {len(symbol_list)}")
    else:
        console.print("股票范围: 全部")
    if start_date:
        console.print(f"开始日期: {start_date}")
    if end_date:
        console.print(f"结束日期: {end_date}")
    console.print()
    
    async def _run():
        settings = get_settings()
        db_manager = DatabaseManager(settings)
        
        try:
            await db_manager.initialize()
            
            results = {}
            
            # 转换num_workers（0表示自动）
            workers = None if num_workers == 0 else num_workers

            if category in ["technical", "all"]:
                console.print("[bold cyan]== 技术指标预处理 ==[/bold cyan]\n")
                result = await _run_technical_preprocess(
                    db_manager,
                    symbols=symbol_list,
                    start_date=start_date,
                    end_date=end_date,
                    freqs=freq_list,
                    adjust_type=adjust,
                    batch_size=batch_size,
                    verbose=verbose,
                    force=force,
                    max_concurrent=max_concurrent,
                    num_workers=workers,
                )
                results["technical"] = result
                console.print(f"\n[green]技术指标处理完成[/green]")
                console.print(f"  处理股票: {result['symbols_processed']}")
                console.print(f"  处理记录: {result['records_processed']}\n")
            
            if category in ["fundamental", "all"]:
                console.print("[bold cyan]== 基本面指标预处理 ==[/bold cyan]\n")
                result = await _run_fundamental_preprocess(
                    db_manager,
                    symbols=symbol_list,
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=batch_size,
                    verbose=verbose,
                    force=force,
                    max_concurrent=max_concurrent,
                    num_workers=workers,
                )
                results["fundamental"] = result
                console.print(f"\n[green]基本面指标处理完成[/green]")
                console.print(f"  处理股票: {result['symbols_processed']}")
                console.print(f"  处理记录: {result['records_processed']}\n")
            
            if category in ["quarterly_fundamental", "quarterly", "all"]:
                console.print("[bold cyan]== 季度F-Score预处理 ==[/bold cyan]\n")
                result = await _run_quarterly_fundamental_preprocess(
                    db_manager,
                    symbols=symbol_list,
                    batch_size=batch_size,
                    verbose=verbose,
                    force=force,
                )
                results["quarterly_fundamental"] = result
                console.print(f"\n[green]季度F-Score处理完成[/green]")
                console.print(f"  处理股票: {result['symbols_processed']}")
                console.print(f"  处理记录: {result['records_processed']}\n")

            if category in ["industry_valuation", "all"]:
                console.print("[bold cyan]== 行业差异化估值预处理 ==[/bold cyan]\n")
                result = await _run_industry_valuation_preprocess(
                    db_manager,
                    symbols=symbol_list,
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=batch_size,
                    verbose=verbose,
                    force=force,
                    max_concurrent=max_concurrent,
                )
                results["industry_valuation"] = result
                console.print(f"\n[green]行业差异化估值处理完成[/green]")
                console.print(f"  处理股票: {result['symbols_processed']}")
                console.print(f"  处理记录: {result['records_processed']}\n")

            if category in ["macro_cycle", "all"]:
                console.print("[bold cyan]== 中国宏观周期预处理 ==[/bold cyan]\n")
                result = await _run_macro_cycle_preprocess(
                    db_manager,
                    start_date=start_date,
                    end_date=end_date,
                    verbose=verbose,
                )
                results["macro_cycle"] = result
                console.print(f"\n[green]中国宏观周期处理完成[/green]")
                console.print(f"  主表记录: {result['phase_records']}")
                console.print(f"  行业快照记录: {result['industry_records']}")
                console.print(f"  总处理记录: {result['records_processed']}\n")

            # 汇总
            total_records = sum(r.get("records_processed", 0) for r in results.values())
            console.print(Panel(
                f"[bold green]预处理完成！[/bold green]\n\n"
                f"总处理记录: {total_records:,}",
                title="完成"
            ))
            
        finally:
            await db_manager.close()
    
    try:
        asyncio.run(_run())
    except Exception as e:
        console.print(f"[bold red]预处理失败:[/bold red] {e}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@preprocess_app.command("status")
def show_status(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
):
    """
    显示预处理状态

    显示各预处理表的数据统计信息。
    """
    console.print("[bold cyan]预处理数据状态[/bold cyan]\n")
    
    async def _run():
        settings = get_settings()
        db_manager = DatabaseManager(settings)
        
        try:
            await db_manager.initialize()
            
            # 技术指标表
            console.print("[bold]技术指标表:[/bold]")
            
            table = Table()
            table.add_column("表名", style="cyan")
            table.add_column("记录数", justify="right")
            table.add_column("股票数", justify="right")
            table.add_column("数据范围")
            table.add_column("最后更新")
            
            for (freq, adj), table_name in ProcessedDataStorage.TABLE_MAP.items():
                stats = await _get_table_stats(db_manager, table_name)
                
                date_range = "-"
                if stats["min_time"] and stats["max_time"]:
                    date_range = f"{stats['min_time'].strftime('%Y-%m-%d')} ~ {stats['max_time'].strftime('%Y-%m-%d')}"
                
                last_update = "-"
                if stats["last_update"]:
                    last_update = stats["last_update"].strftime("%Y-%m-%d %H:%M")
                
                table.add_row(
                    table_name,
                    f"{stats['record_count']:,}",
                    str(stats['symbol_count']),
                    date_range,
                    last_update,
                )
            
            console.print(table)
            console.print()
            
            # 基本面指标表
            console.print("[bold]基本面指标表:[/bold]")
            
            stats = await _get_table_stats(db_manager, FundamentalDataStorage.TABLE_NAME)
            
            table2 = Table()
            table2.add_column("表名", style="cyan")
            table2.add_column("记录数", justify="right")
            table2.add_column("股票数", justify="right")
            table2.add_column("数据范围")
            table2.add_column("最后更新")
            
            date_range = "-"
            if stats["min_time"] and stats["max_time"]:
                date_range = f"{stats['min_time'].strftime('%Y-%m-%d')} ~ {stats['max_time'].strftime('%Y-%m-%d')}"
            
            last_update = "-"
            if stats["last_update"]:
                last_update = stats["last_update"].strftime("%Y-%m-%d %H:%M")
            
            table2.add_row(
                FundamentalDataStorage.TABLE_NAME,
                f"{stats['record_count']:,}",
                str(stats['symbol_count']),
                date_range,
                last_update,
            )
            
            console.print(table2)
            console.print()

            # 宏观周期预处理表
            console.print("[bold]宏观周期预处理表:[/bold]")

            table3 = Table()
            table3.add_column("表名", style="cyan")
            table3.add_column("记录数", justify="right")
            table3.add_column("实体数", justify="right")
            table3.add_column("数据范围")
            table3.add_column("最后更新")

            for table_name in [MacroCyclePhaseStorage.TABLE_NAME, MacroCycleIndustryStorage.TABLE_NAME]:
                stats = await _get_table_stats(db_manager, table_name)

                date_range = "-"
                if stats["min_time"] and stats["max_time"]:
                    date_range = f"{stats['min_time'].strftime('%Y-%m-%d')} ~ {stats['max_time'].strftime('%Y-%m-%d')}"

                last_update = "-"
                if stats["last_update"]:
                    last_update = stats["last_update"].strftime("%Y-%m-%d %H:%M")

                table3.add_row(
                    table_name,
                    f"{stats['record_count']:,}",
                    str(stats["symbol_count"]),
                    date_range,
                    last_update,
                )

            console.print(table3)
            
        except Exception as e:
            console.print(f"[red]获取状态失败: {e}[/red]")
            if verbose:
                import traceback
                console.print(traceback.format_exc())
        finally:
            await db_manager.close()
    
    try:
        asyncio.run(_run())
    except Exception as e:
        console.print(f"[bold red]查询失败:[/bold red] {e}")
        raise typer.Exit(1)


@preprocess_app.command("info")
def show_info():
    """
    显示预处理模块信息

    显示支持的指标、频率和复权类型。
    """
    console.print("[bold cyan]预处理模块信息[/bold cyan]\n")
    
    # 技术指标
    console.print("[bold]支持的技术指标:[/bold]")
    indicators = [
        ("ma_20, ma_50", "简单移动平均线"),
        ("macd (macd_dif, macd_dea, macd_hist)", "MACD 指标"),
        ("rsi_14", "相对强弱指标"),
        ("atr_14", "平均真实波幅"),
    ]
    
    for ind, desc in indicators:
        console.print(f"  • {ind}: {desc}")
    console.print()
    
    # 基本面指标
    console.print("[bold]支持的基本面指标:[/bold]")
    console.print("  • 估值分位: PE/PB/PS 的 5年/10年 历史分位")
    console.print("  • F-Score: Piotroski 财务质量评分 (0-9)")
    console.print("  • 中国宏观周期: raw_phase/stable_phase + 月度行业快照")
    console.print()
    
    # 频率
    console.print("[bold]支持的频率:[/bold]")
    console.print("  • daily: 日线")
    console.print("  • weekly: 周线")
    console.print("  • monthly: 月线")
    console.print()
    
    # 复权类型
    console.print("[bold]支持的复权类型:[/bold]")
    console.print("  • qfq: 前复权（存储）")
    console.print("  • hfq: 后复权（实时计算）")
    console.print("  • none: 不复权")
    console.print()
    
    # 数据表
    console.print("[bold]预处理数据表:[/bold]")
    for (freq, adj), table_name in ProcessedDataStorage.TABLE_MAP.items():
        console.print(f"  • {table_name}: {freq} + {adj}")
    console.print(f"  • {FundamentalDataStorage.TABLE_NAME}: 基本面指标")
    console.print(f"  • {MacroCyclePhaseStorage.TABLE_NAME}: 中国宏观周期主表")
    console.print(f"  • {MacroCycleIndustryStorage.TABLE_NAME}: 中国宏观周期行业快照")
    console.print()
    
    # 使用示例
    console.print("[bold]使用示例:[/bold]")
    console.print("  # 全量预处理（首次运行）")
    console.print("  fdh-cli preprocess run --all --category all --force")
    console.print()
    console.print("  # 只处理技术指标")
    console.print("  fdh-cli preprocess run --all --category technical")
    console.print()
    console.print("  # 处理指定股票")
    console.print("  fdh-cli preprocess run --symbols 600519.SH,000858.SZ")
    console.print()
    console.print("  # 重建中国宏观周期预处理")
    console.print("  fdh-cli preprocess run --category macro_cycle")
    console.print()
    console.print("  # 查看预处理状态")
    console.print("  fdh-cli preprocess status")
