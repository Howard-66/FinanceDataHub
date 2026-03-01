"""
预处理 CLI 命令

提供 fdh-cli preprocess 子命令：
- run: 执行预处理（默认命令）
- status: 查看预处理状态
- info: 显示预处理表信息
"""

from typing import Optional, List
from datetime import datetime, timedelta
import asyncio
import concurrent.futures
import os
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
from ..preprocessing import (
    AdjustType,
    AdjustProcessor,
    PreprocessPipeline,
    ProcessedDataStorage,
)
from ..preprocessing.storage import FundamentalDataStorage
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
    "macd", "rsi_14", "atr_14"
]

# 默认频率
DEFAULT_FREQS = ["daily", "weekly", "monthly"]

# 增量预处理配置
INDICATOR_MAX_WINDOW = 60  # 最大指标窗口(MA_50需要50天)
LOOKBACK_BUFFER = 20  # 额外缓冲天数

# Phase 2: 并发控制配置
DEFAULT_MAX_CONCURRENT_BATCHES = 6  # 默认并发批次数（I/O并发）
DEFAULT_NUM_WORKERS = None  # 默认工作进程数（None表示自动：min(CPU核心数-1, 4)）


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

    # 自动确定工作进程数
    if num_workers is None:
        num_workers = min(os.cpu_count() or 4 - 1, 6)
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
        console.print(f"  全量重算: {len(full_symbols)} 只 (adj_factor变化或首次处理)")
        console.print(f"  增量处理: {len(incr_symbols)} 只 (adj_factor未变)")

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
        batch_start_date: Optional[str] = None,
        task=None
    ) -> tuple[int, int]:
        """
        处理单个批次（支持并发控制）

        Returns:
            (processed_symbols, processed_records)
        """
        async with semaphore:
            # 获取数据
            df = await _get_stock_data(
                db_manager,
                batch_symbols,
                batch_start_date,
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
                batch_tasks.append(process_batch(batch_symbols, True, start_date, task_full))

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
            # 计算增量所需的回溯天数
            lookback_days = INDICATOR_MAX_WINDOW + LOOKBACK_BUFFER

            # 计算增量数据的起始日期
            if start_date:
                incr_start = min(
                    pd.to_datetime(start_date),
                    pd.Timestamp.now() - pd.Timedelta(days=lookback_days * 1.5)
                ).strftime('%Y-%m-%d')
            else:
                incr_start = (pd.Timestamp.now() - pd.Timedelta(days=lookback_days * 1.5)).strftime('%Y-%m-%d')

            task_incr = progress.add_task(
                f"[cyan]增量处理 (并发{max_concurrent}, 进程{num_workers})...",
                total=len(incr_symbols)
            )

            # 创建所有批次的任务
            batch_tasks = []
            for i in range(0, len(incr_symbols), batch_size):
                batch_symbols = incr_symbols[i:i+batch_size]
                if verbose:
                    progress.console.print(f"  [增量]批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
                batch_tasks.append(process_batch(batch_symbols, False, incr_start, task_incr))

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
) -> dict:
    """执行基本面指标预处理（支持智能增量）
    
    智能增量逻辑：
    1. 查询 processed_valuation_pct 中的最新时间
    2. 数据拉取：从 最新时间 - 1900天 开始（确保 rolling(1250) 窗口足够）
    3. 计算指标后，仅 upsert 最新时间之后的新记录
    4. force=True 时全量重算
    """
    import pandas as pd
    from datetime import timedelta
    from ..preprocessing.fundamental.valuation import ValuationPercentile, PEGCalculator

    # 获取股票列表（从 daily_basic 表获取，确保有基本面数据）
    if not symbols:
        console.print("[cyan]获取有基本面数据的股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols_from_daily_basic(db_manager)
        console.print(f"共 {len(symbols)} 只股票有基本面数据")

    # === 智能增量：确定时间范围 ===
    # rolling(1250) 交易日 ≈ 1800 日历日，留余量用 1900
    WINDOW_BUFFER_DAYS = 1900
    data_start_date = start_date  # 用户显式指定的 start_date 优先
    upsert_cutoff = None  # None = upsert 全部
    incremental_mode = False

    if not force and not start_date:
        # 查询已处理数据的最新时间
        try:
            result = await db_manager.execute_raw_sql(
                "SELECT MAX(time) FROM processed_valuation_pct"
            )
            row = result.fetchone()
            if row and row[0] is not None:
                latest_time = pd.to_datetime(row[0])
                # 数据窗口：从 latest_time - 1900天 开始拉取
                data_start_date = (latest_time - timedelta(days=WINDOW_BUFFER_DAYS)).strftime("%Y-%m-%d")
                # 仅 upsert latest_time 之后的数据（回退5天作为安全边际）
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

    # 初始化处理器和存储
    valuation = ValuationPercentile()
    peg_calculator = PEGCalculator()
    storage = FundamentalDataStorage(db_manager)
    
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
            f"[cyan]{mode_label}预处理基本面指标...",
            total=len(symbols)
        )
        
        # 分批处理
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            
            if verbose:
                progress.console.print(f"  批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
            
            # 获取 daily_basic 数据（包含 PE, PB, PS）
            placeholders = ", ".join([f":sym_{j}" for j in range(len(batch_symbols))])
            params = {f"sym_{j}": sym for j, sym in enumerate(batch_symbols)}
            
            conditions = [f"symbol IN ({placeholders})"]
            
            if data_start_date:
                conditions.append("time >= :start_date")
                params["start_date"] = pd.to_datetime(data_start_date).to_pydatetime()
            
            if end_date:
                conditions.append("time <= :end_date")
                params["end_date"] = pd.to_datetime(end_date).to_pydatetime()
            
            where_clause = " AND ".join(conditions)

            # 获取 daily_basic 数据（包含 PE, PB, PS, DV_TTM）
            sql = f"""
                SELECT time, symbol, pe_ttm, pb, ps_ttm, dv_ttm
                FROM daily_basic
                WHERE {where_clause}
                ORDER BY symbol, time
            """

            # 获取财务数据用于计算 PEG（直接使用 ann_date_time）
            fina_sql = f"""
                SELECT ts_code, ann_date_time, end_date_time, netprofit_yoy
                FROM fina_indicator
                WHERE ts_code IN ({placeholders})
                  AND ann_date_time IS NOT NULL
                ORDER BY ts_code, ann_date_time
            """

            try:
                result = await db_manager.execute_raw_sql(sql, params)
                rows = result.fetchall()

                if not rows:
                    progress.advance(task, len(batch_symbols))
                    continue

                df = pd.DataFrame(rows, columns=["time", "symbol", "pe_ttm", "pb", "ps_ttm", "dv_ttm"])

                # 获取财务数据用于计算 PEG
                fina_result = await db_manager.execute_raw_sql(fina_sql, params)
                fina_rows = fina_result.fetchall()

                if fina_rows:
                    fina_df = pd.DataFrame(fina_rows, columns=["ts_code", "ann_date_time", "end_date_time", "netprofit_yoy"])
                    # 计算 PEG
                    df = peg_calculator.calculate_batch(df, fina_df)

                # 计算估值分位
                df = valuation.calculate(df)

                # 增量模式：仅 upsert 新数据
                if upsert_cutoff is not None:
                    df = df[df["time"] >= upsert_cutoff]

                # 存储数据
                if not df.empty:
                    count = await storage.upsert(df)
                    total_records += count
                
                total_symbols += len(batch_symbols)
                
            except Exception as e:
                if verbose:
                    progress.console.print(f"  [yellow]批次处理失败: {e}[/yellow]")
            
            progress.advance(task, len(batch_symbols))
    
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
                    SELECT ts_code, end_date, ann_date,
                           roe, roe_yearly, roe_dt, roa, grossprofit_margin, assets_turn,
                           current_ratio, debt_to_assets, netprofit_yoy,
                           q_gsprofit_margin, q_roe
                    FROM fina_indicator
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                bs_sql = f"""
                    SELECT ts_code, end_date, f_ann_date,
                           total_assets, total_liab, total_ncl, total_cur_assets, total_cur_liab, total_share
                    FROM balancesheet
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                cf_sql = f"""
                    SELECT ts_code, end_date, n_cashflow_act
                    FROM cashflow
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                inc_sql = f"""
                    SELECT ts_code, end_date, n_income
                    FROM income
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
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
                    "current_ratio", "debt_to_assets", "netprofit_yoy",
                    "q_gsprofit_margin", "q_roe"
                ])

                bs_rows = bs_result.fetchall()
                bs_df = pd.DataFrame(bs_rows, columns=[
                    "ts_code", "end_date", "f_ann_date",
                    "total_assets", "total_liab", "total_ncl", "total_cur_assets", "total_cur_liab", "total_share"
                ]) if bs_rows else pd.DataFrame()

                cf_rows = cf_result.fetchall()
                cf_df = pd.DataFrame(cf_rows, columns=[
                    "ts_code", "end_date", "n_cashflow_act"
                ]) if cf_rows else pd.DataFrame()

                inc_rows = inc_result.fetchall()
                inc_df = pd.DataFrame(inc_rows, columns=[
                    "ts_code", "end_date", "n_income"
                ]) if inc_rows else pd.DataFrame()
                
                # 计算 F-Score（始终全量计算，确保 TTM 转换准确）
                fscore_df = calculator.calculate(
                    fina_indicator=fina_df,
                    balancesheet=bs_df,
                    cashflow=cf_df,
                    income=inc_df,
                    exemptions=None  # 暂不使用豁免规则
                )
                
                if fscore_df.empty:
                    progress.advance(task, len(batch_symbols))
                    continue
                
                # 转换列名以匹配数据库表
                fscore_df = fscore_df.rename(columns={
                    "end_date": "end_date_time",
                    "ann_date": "ann_date_time",
                    "f_ann_date": "f_ann_date_time"
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
        sql = f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as symbol_count,
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
        help="预处理类别 (technical, fundamental, quarterly_fundamental, all)"
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
    
    # 参数校验
    if not all_data and not symbols:
        console.print("[yellow]提示: 使用 --all 处理全部股票，或使用 --symbols 指定股票代码[/yellow]")
        console.print("示例: fdh-cli preprocess run --all --category technical")
        raise typer.Exit(1)
    
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
            
            if category in ["technical", "all"]:
                console.print("[bold cyan]== 技术指标预处理 ==[/bold cyan]\n")
                # 转换num_workers（0表示自动）
                workers = None if num_workers == 0 else num_workers
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
    console.print("  # 查看预处理状态")
    console.print("  fdh-cli preprocess status")
