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


async def _get_stock_data(
    db_manager: DatabaseManager,
    symbols: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_adj_factor: bool = True
):
    """获取股票日线数据"""
    import pandas as pd
    
    # 构建 WHERE 子句
    conditions = []
    params = {}
    
    if symbols:
        placeholders = ", ".join([f":sym_{i}" for i in range(len(symbols))])
        conditions.append(f"d.symbol IN ({placeholders})")
        for i, sym in enumerate(symbols):
            params[f"sym_{i}"] = sym
    
    if start_date:
        conditions.append("d.time >= :start_date")
        params["start_date"] = pd.to_datetime(start_date).to_pydatetime()
    
    if end_date:
        conditions.append("d.time <= :end_date")
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
) -> dict:
    """执行技术指标预处理"""
    import pandas as pd
    from ..preprocessing.resample import ResampleProcessor, ResampleFreq
    
    indicators = indicators or DEFAULT_INDICATORS
    freqs = freqs or ["daily"]
    
    # 获取股票列表
    if not symbols:
        console.print("[cyan]获取股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols(db_manager)
        console.print(f"共 {len(symbols)} 只股票")
    
    # 初始化处理器和存储
    adjust_processor = AdjustProcessor()
    resample_processor = ResampleProcessor()
    storage = ProcessedDataStorage(db_manager)
    
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
        task = progress.add_task(
            "[cyan]预处理技术指标...",
            total=len(symbols)
        )
        
        # 分批处理
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            
            if verbose:
                progress.console.print(f"  批次 {i//batch_size + 1}: {len(batch_symbols)} 只股票")
            
            # 获取数据
            df = await _get_stock_data(
                db_manager,
                batch_symbols,
                start_date,
                end_date,
                include_adj_factor=True
            )
            
            if df.empty:
                progress.advance(task, len(batch_symbols))
                continue
            
            # 复权处理
            if adjust_type == "qfq":
                df = adjust_processor.adjust_qfq(df)
            elif adjust_type == "hfq":
                df = adjust_processor.adjust_hfq(df)
            
            # 计算技术指标
            for ind_name in indicators:
                try:
                    indicator = create_indicator(ind_name)
                    df = indicator.calculate(df)
                except Exception as e:
                    if verbose:
                        progress.console.print(f"  [yellow]跳过指标 {ind_name}: {e}[/yellow]")
            
            # 处理各个频率
            for freq in freqs:
                if freq.lower() == "daily":
                    data = df
                elif freq.lower() == "weekly":
                    data = resample_processor.resample(df, ResampleFreq.WEEKLY)
                    # 对重采样数据重新计算指标
                    for ind_name in indicators:
                        try:
                            indicator = create_indicator(ind_name)
                            data = indicator.calculate(data)
                        except:
                            pass
                elif freq.lower() == "monthly":
                    data = resample_processor.resample(df, ResampleFreq.MONTHLY)
                    for ind_name in indicators:
                        try:
                            indicator = create_indicator(ind_name)
                            data = indicator.calculate(data)
                        except:
                            pass
                else:
                    continue
                
                # 存储数据
                if not data.empty:
                    count = await storage.upsert(data, freq=freq, adjust_type=adjust_type)
                    total_records += count
            
            total_symbols += len(batch_symbols)
            progress.advance(task, len(batch_symbols))
    
    return {
        "symbols_processed": total_symbols,
        "records_processed": total_records,
    }


async def _run_fundamental_preprocess(
    db_manager: DatabaseManager,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    batch_size: int = 100,
    verbose: bool = False,
) -> dict:
    """执行基本面指标预处理"""
    import pandas as pd
    from ..preprocessing.fundamental.valuation import ValuationPercentile
    
    # 获取股票列表
    if not symbols:
        console.print("[cyan]获取股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols(db_manager)
        console.print(f"共 {len(symbols)} 只股票")
    
    # 初始化处理器和存储
    valuation = ValuationPercentile()
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
        task = progress.add_task(
            "[cyan]预处理基本面指标...",
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
            
            if start_date:
                conditions.append("time >= :start_date")
                params["start_date"] = pd.to_datetime(start_date).to_pydatetime()
            
            if end_date:
                conditions.append("time <= :end_date")
                params["end_date"] = pd.to_datetime(end_date).to_pydatetime()
            
            where_clause = " AND ".join(conditions)
            
            sql = f"""
                SELECT time, symbol, pe_ttm, pb, ps_ttm
                FROM daily_basic
                WHERE {where_clause}
                ORDER BY symbol, time
            """
            
            try:
                result = await db_manager.execute_raw_sql(sql, params)
                rows = result.fetchall()
                
                if not rows:
                    progress.advance(task, len(batch_symbols))
                    continue
                
                df = pd.DataFrame(rows, columns=["time", "symbol", "pe_ttm", "pb", "ps_ttm"])
                
                # 计算估值分位
                df = valuation.calculate(df)
                
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
) -> dict:
    """执行季度基本面指标预处理（F-Score等）"""
    import pandas as pd
    import json
    from ..preprocessing.fundamental.quality import FScoreCalculator
    from ..preprocessing.storage import QuarterlyFundamentalDataStorage
    
    # 获取股票列表
    if not symbols:
        console.print("[cyan]获取股票列表...[/cyan]")
        symbols = await _get_all_stock_symbols(db_manager)
        console.print(f"共 {len(symbols)} 只股票")
    
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
        task = progress.add_task(
            "[cyan]预处理季度F-Score...",
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
                # 获取 fina_indicator 数据
                fina_sql = f"""
                    SELECT ts_code, end_date, ann_date,
                           roe, roe_yearly, roe_dt, roa, grossprofit_margin, assets_turn,
                           current_ratio, debt_to_assets, netprofit_yoy
                    FROM fina_indicator
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                fina_result = await db_manager.execute_raw_sql(fina_sql, params)
                fina_rows = fina_result.fetchall()
                
                if not fina_rows:
                    progress.advance(task, len(batch_symbols))
                    continue
                
                fina_df = pd.DataFrame(fina_rows, columns=[
                    "ts_code", "end_date", "ann_date",
                    "roe", "roe_yearly", "roe_dt", "roa", "grossprofit_margin", "assets_turn",
                    "current_ratio", "debt_to_assets", "netprofit_yoy"
                ])
                
                # 获取 balancesheet 数据
                bs_sql = f"""
                    SELECT ts_code, end_date, f_ann_date,
                           total_assets, total_liab, total_ncl, total_cur_assets, total_cur_liab, total_share
                    FROM balancesheet
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                bs_result = await db_manager.execute_raw_sql(bs_sql, params)
                bs_rows = bs_result.fetchall()
                bs_df = pd.DataFrame(bs_rows, columns=[
                    "ts_code", "end_date", "f_ann_date",
                    "total_assets", "total_liab", "total_ncl", "total_cur_assets", "total_cur_liab", "total_share"
                ]) if bs_rows else pd.DataFrame()
                
                # 获取 cashflow 数据
                cf_sql = f"""
                    SELECT ts_code, end_date, n_cashflow_act
                    FROM cashflow
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                cf_result = await db_manager.execute_raw_sql(cf_sql, params)
                cf_rows = cf_result.fetchall()
                cf_df = pd.DataFrame(cf_rows, columns=[
                    "ts_code", "end_date", "n_cashflow_act"
                ]) if cf_rows else pd.DataFrame()
                
                # 获取 income 数据
                inc_sql = f"""
                    SELECT ts_code, end_date, n_income
                    FROM income
                    WHERE ts_code IN ({placeholders})
                    ORDER BY ts_code, end_date
                """
                inc_result = await db_manager.execute_raw_sql(inc_sql, params)
                inc_rows = inc_result.fetchall()
                inc_df = pd.DataFrame(inc_rows, columns=[
                    "ts_code", "end_date", "n_income"
                ]) if inc_rows else pd.DataFrame()
                
                # 计算 F-Score
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
                result = await _run_technical_preprocess(
                    db_manager,
                    symbols=symbol_list,
                    start_date=start_date,
                    end_date=end_date,
                    freqs=freq_list,
                    adjust_type=adjust,
                    batch_size=batch_size,
                    verbose=verbose,
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
