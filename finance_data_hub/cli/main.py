"""
CLI 主入口模块

提供 fdh-cli 命令行工具的入口点。
"""

from typing import Optional, List
from datetime import datetime, timedelta
import asyncio
import sys

# 必须在任何其他模块导入之前配置日志
from loguru import logger

# 默认日志配置 - 在命令执行前设置
logger.remove()
logger.add(
    sys.stderr,
    level="ERROR",  # 默认只显示 ERROR 及以上，避免INFO日志刷屏
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    colorize=True,
)

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, TaskProgressColumn, ProgressColumn
from rich.text import Text
from rich.syntax import Syntax
from rich import print as rprint

from finance_data_hub.config import get_settings, reload_settings
from finance_data_hub.update.updater import DataUpdater
from finance_data_hub.providers.base import ProviderError
from finance_data_hub.database.init_db import init_database
from finance_data_hub.database.cleanup_db import cleanup_database

# 创建 Typer 应用
app = typer.Typer(
    name="fdh-cli",
    help="FinanceDataHub CLI - 金融数据服务中心命令行工具",
    rich_markup_mode="rich"
)

console = Console()


class SymbolCountColumn(ProgressColumn):
    """自定义进度列：显示已下载/总数"""

    def __init__(self, symbol_type: str = "股票"):
        super().__init__()
        self.symbol_type = symbol_type

    def render(self, task: "Task") -> Text:
        """渲染进度文本"""
        completed = task.completed
        total = task.total if task.total > 0 else 1
        if completed == 0 and total == 100:
            return Text("—", style="dim")
        return Text(f"已下载 {completed:.0f}/{total:.0f} {self.symbol_type}", style="bold cyan")


def _setup_logging(verbose: bool = False):
    """配置日志级别

    默认使用 ERROR 级别，verbose 模式使用 INFO 级别。
    """
    log_level = "INFO" if verbose else "ERROR"

    # 更新现有处理器的级别
    logger.remove()
    logger.add(
        sys.stderr,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )


def version_callback(value: bool):
    """显示版本信息"""
    if value:
        console.print("[bold blue]FinanceDataHub CLI[/bold blue] v0.1.0")
        raise typer.Exit()


@app.command("update")
def update(
    asset_class: str = typer.Option(
        "stock",
        "--asset-class",
        "-a",
        help="资产类别 (stock, fund, index, etc.)"
    ),
    dataset: Optional[str] = typer.Option(
        None,
        "--dataset",
        "-d",
        help="数据类型 (daily, minute_1, minute_5, daily_basic, adj_factor, basic, gdp)。"
             "取代 --frequency 参数，提供更准确的描述。"
    ),
    frequency: Optional[str] = typer.Option(
        None,
        "--frequency",
        "-f",
        help="数据频率 (已废弃，使用 --dataset 替代)"
    ),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="股票代码列表，用逗号分隔，如: 600519.SH,000858.SZ"
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="开始日期 (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        help="结束日期 (YYYY-MM-DD)，默认为今天"
    ),
    adj: Optional[str] = typer.Option(
        None,
        "--adj",
        help="复权类型 (None=不复权, qfq=前复权, hfq=后复权)"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="强制更新模式，忽略数据库状态"
    ),
    trade_date: Optional[str] = typer.Option(
        None,
        "--trade-date",
        help="指定交易日，用于批量更新当日所有股票数据（Tushare专用）"
    ),
    config_file: Optional[str] = typer.Option(
        None,
        "--config",
        help="指定配置文件路径"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细输出"
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="安静模式，减少日志输出"
    ),
):
    """
    从数据源更新数据到数据库

    执行数据同步流程，从配置的数据源获取数据并存储到数据库。
    支持按资产类别、数据类型和特定股票代码进行筛选。

    数据类型 (--dataset):
      - daily: 日线行情数据
      - minute_1: 1分钟线数据
      - minute_5: 5分钟线数据
      - daily_basic: 每日基本面数据
      - adj_factor: 复权因子数据
      - basic: 股票基本信息（非时间序列，强制全量更新）
      - gdp: 中国GDP宏观经济数据
      - ppi: 中国PPI工业生产者出厂价格指数数据
      - m: 中国货币供应量数据（M0、M1、M2）
      - pmi: 中国PMI采购经理人指数数据
      - index_dailybasic: 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）
      - sw_daily: 申万行业日线行情数据（申万2021版行业指数）

      - fina_indicator: 上市公司财务指标数据（每股收益、ROE、资产负债率等）

      - cashflow: 上市公司现金流量表数据（经营活动、投资活动、筹资活动现金流量）

      - balancesheet: 上市公司资产负债表数据（资产、负债、股东权益）
      - income: 上市公司利润表数据（收入、成本和利润）

    财务数据使用说明:
    - 需要指定 --symbols 参数指定股票代码（支持逗号分隔多只股票）
    - 支持 --start-date 和 --end-date 指定报告期范围
    - 默认为智能下载模式，自动判断是全量获取还是增量更新
    - 使用 --force 参数可强制全量更新

        index_dailybasic使用说明:
        - 不指定--symbols时：获取当日所有指数数据（增量更新）
        - 指定--symbols时：获取该指数的历史数据（如 --symbols 000001.SH --start-date 2024-01-01 --end-date 2024-12-31）

        sw_daily使用说明:
        - 不指定--symbols时：获取所有行业的历史数据（智能更新）
        - 指定--symbols时：获取指定行业的日线数据（如 --symbols 801780.SI --start-date 2024-01-01 --end-date 2024-12-31）
        - 使用--trade-date参数：获取指定交易日所有行业数据（如 --trade-date 2024-06-28）

    更新策略:
      默认采用智能下载模式，系统会自动:
      - 对新symbol获取全量历史数据
      - 对已有symbol获取增量数据
      - 智能判断是否覆盖盘中数据

      注意：非时间序列数据（如basic）会自动使用强制更新模式，
      确保数据的完整性和一致性。

    强制更新模式:
      使用 --force 标志可以启用强制更新模式，忽略数据库状态，
      根据用户指定的日期范围进行更新。
    """
    # 配置日志级别（默认 ERROR，verbose 时使用 INFO）
    _setup_logging(verbose=verbose)

    # 安静模式下只显示简洁信息
    if not quiet:
        console.print("[bold green]开始数据更新流程[/bold green]\n")

    try:
        # 加载配置
        settings = get_settings()

        if config_file:
            console.print(f"[yellow]配置文件: {config_file}[/yellow]")

        # 处理 --dataset 和 --frequency 参数
        # 优先使用 --dataset，--frequency 用于向后兼容
        data_type = dataset if dataset else frequency

        if not data_type:
            console.print("[bold red]ERROR:[/bold red] 必须指定数据类型（--dataset 或 --frequency）")
            raise typer.Exit(1)

        # 检查 --frequency 是否被使用，如果是，显示废弃警告
        if frequency and not dataset:
            console.print("[yellow]⚠️  警告: --frequency 参数已废弃，请使用 --dataset 替代[/yellow]")
            console.print("[yellow]  例: --dataset daily 替代 --frequency daily[/yellow]\n")

        # 显示更新参数（非安静模式）
        if not quiet:
            # 根据数据类型自动推断正确的资产类别
            display_asset_class = asset_class
            if data_type == "gdp":
                display_asset_class = "macro"
            console.print(f"[cyan]资产类别:[/cyan] {display_asset_class}")
            console.print(f"[cyan]数据类型:[/cyan] {data_type}")
            if force:
                console.print(f"[cyan]更新模式:[/cyan] 强制更新")
            else:
                console.print(f"[cyan]更新模式:[/cyan] 智能下载")

            if symbols:
                console.print(f"[cyan]股票代码:[/cyan] {symbols}")

            if start_date:
                console.print(f"[cyan]开始日期:[/cyan] {start_date}")
            if end_date:
                console.print(f"[cyan]结束日期:[/cyan] {end_date}")
            if adj:
                console.print(f"[cyan]复权类型:[/cyan] {adj}")
            if trade_date:
                console.print(f"[cyan]交易日:[/cyan] {trade_date}")

        # 执行更新流程
        asyncio.run(_run_update(
            settings, asset_class, data_type, symbols,
            start_date, end_date, adj, force, trade_date, verbose, quiet
        ))

        if not quiet:
            console.print("\n[bold green][OK][/bold green] 数据更新完成")
        else:
            console.print("[bold green][OK][/bold green] 数据更新完成")

    except Exception as e:
        console.print(f"[bold red]ERROR:[/bold red] {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


def _is_timeseries_data(data_type: str) -> bool:
    """判断是否为时间序列数据

    时间序列数据支持智能下载（增量更新）
    非时间序列数据需要强制全量更新

    Args:
        data_type: 数据类型

    Returns:
        bool: True表示时间序列数据，False表示非时间序列数据
    """
    # 非时间序列数据类型
    non_timeseries_types = {"basic", "asset_basic"}
    return data_type not in non_timeseries_types


async def _run_update(
    settings,
    asset_class: str,
    data_type: str,
    symbols: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    adj: Optional[str],
    force: bool,
    trade_date: Optional[str],
    verbose: bool,
    quiet: bool = False,
):
    """执行实际的数据更新"""
    # 解析股票代码列表
    symbol_list = None
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

    # 设置默认日期
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # 更新策略矩阵：根据参数组合自动选择最优策略
    if trade_date:
        # 策略 1: trade_date 批量更新（Tushare专用）
        console.print("\n[bold yellow]使用交易日批量更新模式[/bold yellow]")
        await _run_trade_date_update(
            settings, asset_class, data_type, trade_date, verbose, quiet
        )
    elif force or start_date or not _is_timeseries_data(data_type):
        # 策略 2: 强制更新模式
        # 注意：非时间序列数据（如asset_basic）自动使用强制更新模式
        if not quiet:
            if not _is_timeseries_data(data_type):
                console.print("\n[bold yellow]使用强制更新模式[/bold yellow]")
                console.print("[dim]  非时间序列数据，自动使用强制全量更新[/dim]")
            else:
                console.print("\n[bold yellow]使用强制更新模式[/bold yellow]")
        await _run_force_update(
            settings, asset_class, data_type, symbol_list,
            start_date, end_date, adj, trade_date, verbose, quiet
        )
    else:
        # 策略 3: 智能下载模式（默认）
        console.print("\n[bold yellow]使用智能下载模式[/bold yellow]")
        await _run_smart_download(
            settings, asset_class, data_type, symbol_list,
            end_date, adj, trade_date, start_date, verbose, quiet
        )


async def _run_smart_download(
    settings,
    asset_class: str,
    data_type: str,
    symbol_list: Optional[List[str]],
    end_date: Optional[str],
    adj: Optional[str],
    trade_date: Optional[str],
    start_date: Optional[str],
    verbose: bool,
    quiet: bool = False,
):
    """智能下载模式：自动检测数据库状态，智能选择全量或增量下载"""
    # GDP 数据不需要 symbol，单独处理
    if data_type == "gdp":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - GDP是宏观经济数据，无需symbol")
            console.print("  - 自动获取最新季度数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取GDP数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_gdp(
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条GDP数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条GDP数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新GDP数据失败: {str(e)}")
                    raise

    # PPI 数据不需要 symbol，单独处理
    if data_type == "ppi":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - PPI是宏观经济数据，无需symbol")
            console.print("  - 自动获取最新月份数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取PPI数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_ppi(
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PPI数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PPI数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新PPI数据失败: {str(e)}")
                    raise

    # 货币供应量数据不需要 symbol，单独处理
    if data_type == "m":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 货币供应量是宏观经济数据，无需symbol")
            console.print("  - 自动获取最新月份数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取货币供应量数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_m(
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条货币供应量数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条货币供应量数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新货币供应量数据失败: {str(e)}")
                    raise

    # PMI 数据不需要 symbol，单独处理
    if data_type == "pmi":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - PMI是宏观经济数据，无需symbol")
            console.print("  - 自动获取最新月份数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取PMI数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_pmi(
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PMI数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PMI数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新PMI数据失败: {str(e)}")
                    raise

    # 大盘指数每日指标数据处理
    if data_type == "index_dailybasic":
        # 获取指数代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为指数代码列表

        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）")
            console.print("  - 自动获取最新数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取指数每日指标数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_index_dailybasic(
                        ts_code=ts_code_list[0] if ts_code_list else None,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数每日指标数据失败: {str(e)}")
                    raise

    # 申万行业日线行情数据处理
    if data_type == "sw_daily":
        # 获取行业代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为行业代码列表

        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 申万行业日线行情数据（申万2021版行业指数）")
            if ts_code_list:
                console.print(f"  - 将更新 {len(ts_code_list)} 个行业指数")
            console.print("")

        # 如果没有指定行业列表，获取申万行业分类列表中的行业数量
        industry_count = len(ts_code_list) if ts_code_list else None
        if industry_count is None:
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                # 获取申万行业分类列表（包含L1/L2/L3全部层级）
                industry_classify = await updater.data_ops.get_sw_industry_classify(level=None)
                industry_count = len(industry_classify) if industry_classify is not None else 511

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("行业指数"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "正在获取申万行业日线行情...",
                total=industry_count if industry_count else 511
            )

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_sw_daily(
                        ts_code_list=ts_code_list if ts_code_list else None,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=False,  # 智能下载模式
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=len(ts_code_list) if ts_code_list else 100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条申万行业日线行情数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条申万行业日线行情数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新申万行业日线行情数据失败: {str(e)}")
                    raise

    # 财务指标数据处理
    if data_type == "fina_indicator":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 财务指标数据（上市公司财务报表关键指标）")
            console.print("  - 按股票代码获取历史财务数据")
            console.print("")

        # 获取股票列表（不更新，仅查询）
        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not symbol_list:
                symbols_db = await updater.data_ops.get_symbol_list()
                symbol_list = symbols_db
                if not quiet and symbol_list:
                    console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

            if not symbol_list:
                if not quiet:
                    console.print("[yellow]数据库中没有股票信息，请先运行 fdh-cli update --dataset basic[/yellow]")
                else:
                    console.print("[yellow]没有股票可更新[/yellow]")
                return 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取财务指标数据...", total=len(symbol_list))

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current)

                try:
                    count = await updater.update_fina_indicator(
                        symbols=symbol_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条财务指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条财务指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新财务指标数据失败: {str(e)}")
                    raise

    # 现金流量表数据处理
    if data_type == "cashflow":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 现金流量表数据（上市公司三大活动现金流量）")
            console.print("  - 按股票代码获取历史财务数据")
            console.print("")

        # 获取股票列表（不更新，仅查询）
        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not symbol_list:
                symbols_db = await updater.data_ops.get_symbol_list()
                symbol_list = symbols_db
                if not quiet and symbol_list:
                    console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

            if not symbol_list:
                if not quiet:
                    console.print("[yellow]数据库中没有股票信息，请先运行 fdh-cli update --dataset basic[/yellow]")
                else:
                    console.print("[yellow]没有股票可更新[/yellow]")
                return 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取现金流量表数据...", total=len(symbol_list))

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current)

                try:
                    count = await updater.update_cashflow(
                        symbols=symbol_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条现金流量表数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条现金流量表数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新现金流量表数据失败: {str(e)}")
                    raise

    # 资产负债表数据处理
    if data_type == "balancesheet":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 资产负债表数据（上市公司资产、负债和股东权益）")
            console.print("  - 按股票代码获取历史财务数据")
            console.print("")

        # 获取股票列表（不更新，仅查询）
        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not symbol_list:
                symbols_db = await updater.data_ops.get_symbol_list()
                symbol_list = symbols_db
                if not quiet and symbol_list:
                    console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

            if not symbol_list:
                if not quiet:
                    console.print("[yellow]数据库中没有股票信息，请先运行 fdh-cli update --dataset basic[/yellow]")
                else:
                    console.print("[yellow]没有股票可更新[/yellow]")
                return 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取资产负债表数据...", total=len(symbol_list))

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current)

                try:
                    count = await updater.update_balancesheet(
                        symbols=symbol_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条资产负债表数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条资产负债表数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新资产负债表数据失败: {str(e)}")
                    raise

    # 利润表数据处理
    if data_type == "income":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 利润表数据（上市公司收入、成本和利润）")
            console.print("  - 按股票代码获取历史财务数据")
            console.print("")

        # 获取股票列表（不更新，仅查询）
        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not symbol_list:
                symbols_db = await updater.data_ops.get_symbol_list()
                symbol_list = symbols_db
                if not quiet and symbol_list:
                    console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

            if not symbol_list:
                if not quiet:
                    console.print("[yellow]数据库中没有股票信息，请先运行 fdh-cli update --dataset basic[/yellow]")
                else:
                    console.print("[yellow]没有股票可更新[/yellow]")
                return 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取利润表数据...", total=len(symbol_list))

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current)

                try:
                    count = await updater.update_income(
                        symbols=symbol_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条利润表数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条利润表数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新利润表数据失败: {str(e)}")
                    raise

    # 申万行业分类数据处理
    if data_type == "sw_industry_classify":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 申万行业分类数据（一级/二级/三级行业）")
            console.print("  - 获取申万2021年版行业分类")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取申万行业分类...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_sw_industry_classify(
                        level="L1",
                        src="SW2021",
                        force_update=False,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条行业分类数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条行业分类数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新行业分类数据失败: {str(e)}")
                    raise

    # 申万行业成分股数据处理
    if data_type == "sw_industry_member":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 申万行业成分股数据")
            console.print("  - 按一级行业逐个下载成分股")
            console.print("  - 进度按行业计算")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            TextColumn("({task.completed}/{task.total} 行业)"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取申万行业成分股...", total=31)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_sw_industry_members(
                        l1_code=None,  # 下载所有行业
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=31)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条成分股数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条成分股数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新成分股数据失败: {str(e)}")
                    raise

    if not quiet:
        console.print("[bold]智能下载策略:[/bold]")
        console.print("  - 自动检测symbol是否存在于数据库")
        console.print("  - 新symbol：获取全量历史数据")
        console.print("  - 已有symbol：获取增量数据（从最后记录+1天开始）")
        console.print("  - 智能判断是否覆盖盘中数据")
        console.print("")

    # 初始化更新器
    async with DataUpdater(settings, config_path="sources.yml") as updater:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            try:
                # 如果没有指定symbol，先更新股票列表
                if not symbol_list:
                    if not quiet:
                        console.print("[yellow]先更新股票列表...[/yellow]")
                    basic_count = await updater.update_stock_basic()
                    if not quiet:
                        console.print(f"[green][OK][/green] 更新了 {basic_count} 条股票基本信息")

                    symbols_db = await updater.data_ops.get_symbol_list()
                    symbol_list = symbols_db
                    if not quiet:
                        console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

                total_updated = 0
                total_errors = 0

                task = progress.add_task("正在智能下载...", total=len(symbol_list))

                for idx, symbol in enumerate(symbol_list):
                    try:
                        if verbose:
                            console.print(
                                f"[cyan]处理 {symbol} ({idx + 1}/{len(symbol_list)})[/cyan]"
                            )

                        # 调用相应的更新方法，使用智能下载（force_update=False, start_date=None）
                        if data_type == "daily":
                            count = await updater.update_daily_data(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                adj=adj,
                                force_update=False,
                            )
                        elif data_type.startswith("minute"):
                            # 从 data_type 中提取频率
                            freq_map = {
                                "minute_1": "1m",
                                "minute_5": "5m",
                                "minute_15": "15m",
                                "minute_30": "30m",
                                "minute_60": "60m",
                                "minute": "1m",  # 默认
                            }
                            actual_freq = freq_map.get(data_type, "1m")

                            if verbose:
                                console.print(f"[dim]  频率映射: {data_type} -> {actual_freq}[/dim]")

                            count = await updater.update_minute_data(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                freq=actual_freq,
                                force_update=False,
                            )
                        elif data_type == "daily_basic":
                            count = await updater.update_daily_basic(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                force_update=False,
                            )
                        elif data_type == "adj_factor":
                            count = await updater.update_adj_factor(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                force_update=False,
                            )
                        elif data_type in ("basic", "asset_basic"):
                            # asset_basic 是非时间序列数据，不会进入智能下载模式
                            # 这里添加是为了代码完整性，但实际上不会执行到此处
                            count = await updater.update_stock_basic(market=None)
                        else:
                            console.print(f"[bold red]不支持的数据类型: {data_type}[/bold red]")
                            raise typer.Exit(1)

                        total_updated += count

                        # 更新进度（直接使用计数而非百分比）
                        progress.update(task, completed=idx + 1)

                    except Exception as e:
                        total_errors += 1
                        if not quiet:
                            console.print(f"[red]更新 {symbol} 失败: {str(e)}[/red]")
                        continue

                if not quiet:
                    console.print(f"\n[bold]智能下载完成:[/bold]")
                    console.print(f"  更新记录: {total_updated}")
                    console.print(f"  失败数量: {total_errors}")
                else:
                    console.print(f"[bold]完成:[/bold] 更新 {total_updated} 条记录, 失败 {total_errors}")

            except ProviderError as e:
                console.print(f"\n[bold red]ERROR:[/bold red] 数据源错误: {str(e)}")
                raise
            except Exception as e:
                console.print(f"\n[bold red]ERROR:[/bold red] 更新失败: {str(e)}")
                raise


async def _run_force_update(
    settings,
    asset_class: str,
    data_type: str,
    symbol_list: Optional[List[str]],
    start_date: Optional[str],
    end_date: Optional[str],
    adj: Optional[str],
    trade_date: Optional[str],
    verbose: bool,
    quiet: bool = False,
):
    """强制更新模式：忽略数据库状态，使用指定日期范围"""

    # GDP 数据不需要 symbol，单独处理
    if data_type == "gdp":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - GDP是宏观经济数据，无需symbol")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取GDP数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_gdp(
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条GDP数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条GDP数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新GDP数据失败: {str(e)}")
                    raise

    # PPI 数据不需要 symbol，单独处理
    if data_type == "ppi":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - PPI是宏观经济数据，无需symbol")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取PPI数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_ppi(
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PPI数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PPI数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新PPI数据失败: {str(e)}")
                    raise

    # 货币供应量数据不需要 symbol，单独处理
    if data_type == "m":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 货币供应量是宏观经济数据，无需symbol")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取货币供应量数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_m(
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条货币供应量数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条货币供应量数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新货币供应量数据失败: {str(e)}")
                    raise

    # PMI 数据不需要 symbol，单独处理
    if data_type == "pmi":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - PMI是宏观经济数据，无需symbol")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取PMI数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_pmi(
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PMI数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条PMI数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新PMI数据失败: {str(e)}")
                    raise

    # 大盘指数每日指标数据处理
    if data_type == "index_dailybasic":
        # 获取指数代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为指数代码列表

        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取指数每日指标数据...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_index_dailybasic(
                        ts_code=ts_code_list[0] if ts_code_list else None,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数每日指标数据失败: {str(e)}")
                    raise

    # 申万行业日线行情数据处理
    if data_type == "sw_daily":
        # 获取行业代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为行业代码列表

        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 申万行业日线行情数据（申万2021版行业指数）")
            if ts_code_list:
                console.print(f"  - 将更新 {len(ts_code_list)} 个行业指数")
            console.print("  - 使用指定的日期范围")
            console.print("")

        # 如果没有指定行业列表，先获取申万行业分类列表中的行业数量
        industry_count = len(ts_code_list) if ts_code_list else None
        if industry_count is None:
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                # 获取申万行业分类列表（包含L1/L2/L3全部层级）
                industry_classify = await updater.data_ops.get_sw_industry_classify(level=None)
                industry_count = len(industry_classify) if industry_classify is not None else 511  # 默认511（L1+L2+L3）
                if not quiet:
                    console.print(f"[yellow]将更新 {industry_count} 个行业指数（L1+L2+L3）[/yellow]\n")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("行业指数"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "正在获取申万行业日线行情...",
                total=industry_count if industry_count else 511
            )

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_sw_daily(
                        ts_code_list=ts_code_list if ts_code_list else None,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=industry_count if industry_count else 500)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条申万行业日线行情数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条申万行业日线行情数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新申万行业日线行情数据失败: {str(e)}")
                    raise

    # 财务指标数据处理
    if data_type == "fina_indicator":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 财务指标数据（上市公司财务报表关键指标）")
            console.print("  - 使用指定的日期范围")
            console.print("")

        # 获取股票列表（不更新，仅查询）
        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not symbol_list:
                symbols_db = await updater.data_ops.get_symbol_list()
                symbol_list = symbols_db
                if not quiet and symbol_list:
                    console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

            if not symbol_list:
                if not quiet:
                    console.print("[yellow]数据库中没有股票信息，请先运行 fdh-cli update --dataset basic[/yellow]")
                else:
                    console.print("[yellow]没有股票可更新[/yellow]")
                return 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取财务指标数据...", total=len(symbol_list))

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current)

                try:
                    count = await updater.update_fina_indicator(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条财务指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条财务指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新财务指标数据失败: {str(e)}")
                    raise

    # 申万行业分类数据处理（强制更新）
    if data_type == "sw_industry_classify":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 申万行业分类数据（一级/二级/三级行业）")
            console.print("  - 强制重新获取所有数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取申万行业分类...", total=100)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_sw_industry_classify(
                        level="L1",
                        src="SW2021",
                        force_update=True,
                    )
                    progress.update(task, completed=100)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条行业分类数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条行业分类数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新行业分类数据失败: {str(e)}")
                    raise

    # 申万行业成分股数据处理（强制更新）
    if data_type == "sw_industry_member":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 申万行业成分股数据")
            console.print("  - 按一级行业逐个下载成分股")
            console.print("  - 强制重新获取所有数据")
            console.print("")

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            TextColumn("({task.completed}/{task.total} 行业)"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取申万行业成分股...", total=31)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_sw_industry_members(
                        l1_code=None,  # 下载所有行业
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=31)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条成分股数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条成分股数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新成分股数据失败: {str(e)}")
                    raise

    if not quiet:
        console.print("[bold]强制更新策略:[/bold]")
        console.print("  - 忽略数据库现有状态")
        console.print("  - 使用用户指定的日期范围")
        console.print("  - 覆盖现有数据")
        console.print("")

    # 初始化更新器
    async with DataUpdater(settings, config_path="sources.yml") as updater:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            try:
                # 如果没有指定symbol，先更新股票列表
                if not symbol_list:
                    if not quiet:
                        console.print("[yellow]先更新股票列表...[/yellow]")
                    basic_count = await updater.update_stock_basic()
                    if not quiet:
                        console.print(f"[green][OK][/green] 更新了 {basic_count} 条股票基本信息")

                    symbols_db = await updater.data_ops.get_symbol_list()
                    symbol_list = symbols_db
                    if not quiet:
                        console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]\n")

                total_updated = 0
                total_errors = 0

                task = progress.add_task("正在强制更新...", total=len(symbol_list))

                for idx, symbol in enumerate(symbol_list):
                    try:
                        if verbose:
                            console.print(
                                f"[cyan]处理 {symbol} ({idx + 1}/{len(symbol_list)})[/cyan]"
                            )

                        # 调用相应的更新方法，使用强制更新（force_update=True）
                        if data_type == "daily":
                            count = await updater.update_daily_data(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                adj=adj,
                                force_update=True,
                            )
                        elif data_type.startswith("minute"):
                            # 从 data_type 中提取频率
                            freq_map = {
                                "minute_1": "1m",
                                "minute_5": "5m",
                                "minute_15": "15m",
                                "minute_30": "30m",
                                "minute_60": "60m",
                                "minute": "1m",  # 默认
                            }
                            actual_freq = freq_map.get(data_type, "1m")

                            if verbose:
                                console.print(f"[dim]  频率映射: {data_type} -> {actual_freq}[/dim]")

                            count = await updater.update_minute_data(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                freq=actual_freq,
                                force_update=True,
                            )
                        elif data_type == "daily_basic":
                            count = await updater.update_daily_basic(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                force_update=True,
                            )
                        elif data_type == "adj_factor":
                            count = await updater.update_adj_factor(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                force_update=True,
                            )
                        elif data_type in ("basic", "asset_basic"):
                            # asset_basic 是非时间序列数据，使用强制全量更新
                            # 注意：asset_basic 不需要按 symbol 更新，只需要调用一次即可
                            count = await updater.update_stock_basic(market=None)

                            # 一次性更新所有股票基本信息后，跳出循环
                            if not quiet:
                                console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")
                                console.print("[yellow]股票基本信息为全量数据，无需按symbol逐一更新[/yellow]\n")
                            else:
                                console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")

                            # 记录总更新数并跳出 symbol 循环
                            total_updated += count
                            break
                        else:
                            console.print(f"[bold red]不支持的数据类型: {data_type}[/bold red]")
                            raise typer.Exit(1)

                        total_updated += count

                        # 更新进度（直接使用计数而非百分比）
                        progress.update(task, completed=idx + 1)

                    except Exception as e:
                        total_errors += 1
                        if not quiet:
                            console.print(f"[red]更新 {symbol} 失败: {str(e)}[/red]")
                        continue

                if not quiet:
                    console.print(f"\n[bold]强制更新完成:[/bold]")
                    console.print(f"  更新记录: {total_updated}")
                    console.print(f"  失败数量: {total_errors}")
                else:
                    console.print(f"[bold]完成:[/bold] 更新 {total_updated} 条记录, 失败 {total_errors}")

            except ProviderError as e:
                console.print(f"\n[bold red]ERROR:[/bold red] 数据源错误: {str(e)}")
                raise
            except Exception as e:
                console.print(f"\n[bold red]ERROR:[/bold red] 更新失败: {str(e)}")
                raise


async def _run_trade_date_update(
    settings,
    asset_class: str,
    data_type: str,
    trade_date: str,
    verbose: bool,
    quiet: bool = False,
):
    """交易日批量更新模式：使用Tushare的trade_date参数批量更新当日所有股票"""
    # 转换日期格式从 YYYY-MM-DD (CLI格式) 到 YYYYMMDD (Tushare API格式)
    trade_date_api = trade_date.replace("-", "")

    if not quiet:
        console.print("[bold]交易日批量更新策略:[/bold]")
        console.print(f"  - 使用交易日: {trade_date} (API格式: {trade_date_api})")
        console.print("  - 批量更新当日所有股票数据")
        console.print("  - 适用于Tushare数据源")
        console.print("")

    try:
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
        from rich.console import Group
        from rich.panel import Panel
        from loguru import logger

        # 初始化更新器
        updater = DataUpdater(settings)
        await updater.initialize()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取交易日数据...", total=None)

            # 根据数据类型选择不同的方法和表
            if data_type == "daily":
                # 日线行情数据
                method_name = "get_daily_data"
            elif data_type == "daily_basic":
                # 每日指标数据
                method_name = "get_daily_basic"
            elif data_type == "index_dailybasic":
                # 大盘指数每日指标数据
                method_name = "get_index_dailybasic"
            elif data_type == "sw_daily":
                # 申万行业日线行情数据
                method_name = "get_sw_daily"
            else:
                console.print(f"[bold red]不支持的数据类型: {data_type}[/bold red]")
                raise typer.Exit(1)

            # 通过路由器获取数据
            # 注意：index_dailybasic和sw_daily使用asset_class="index"，其他使用"stock"
            if data_type in ["index_dailybasic", "sw_daily"]:
                asset_class = "index"
                router_data_type = "sw_daily" if data_type == "sw_daily" else "dailybasic"
            else:
                asset_class = "stock"
                router_data_type = data_type
            df = updater.router.route(
                asset_class=asset_class,
                data_type=router_data_type,
                method_name=method_name,
                trade_date=trade_date_api,
            )

            if df.empty:
                console.print(f"[yellow]指定交易日 {trade_date} 没有数据[/yellow]")
                return 0

            progress.update(task, description="正在插入数据库...", total=100)

            # 判断是股票还是指数数据
            is_index = (data_type in ["index_dailybasic", "sw_daily"])
            if is_index:
                unique_codes = df["ts_code"].unique()
                code_label = "行业指数" if data_type == "sw_daily" else "指数"
            else:
                unique_codes = df["symbol"].unique()
                code_label = "股票"

            total_records = len(df)
            total_inserted = 0
            total_errors = 0

            if not quiet:
                console.print(f"\n[bold]开始批量插入 {trade_date} 的 {len(unique_codes)} 只{code_label}数据[/bold]")
                console.print(f"总记录数: {total_records}\n")

            # 分批插入，每批1000条记录
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                try:
                    if data_type == "daily":
                        count = await updater.data_ops.insert_symbol_daily_batch(batch_df)
                    elif data_type == "daily_basic":
                        count = await updater.data_ops.insert_daily_basic_batch(batch_df)
                    elif data_type == "index_dailybasic":
                        count = await updater.data_ops.insert_index_dailybasic_batch(batch_df)
                    elif data_type == "sw_daily":
                        count = await updater.data_ops.insert_sw_daily_batch(batch_df)
                    else:
                        console.print(f"[bold red]不支持的数据类型: {data_type}[/bold red]")
                        raise typer.Exit(1)

                    total_inserted += count
                    progress.update(
                        task,
                        completed=((i + len(batch_df)) / total_records) * 100
                    )

                    # 显示进度（非安静模式）
                    if not quiet:
                        if is_index:
                            current_code = batch_df["ts_code"].iloc[0] if len(batch_df) > 0 else "unknown"
                        else:
                            current_code = batch_df["symbol"].iloc[0] if len(batch_df) > 0 else "unknown"
                        console.print(
                            f"[green]✓[/green] 批次 {i // batch_size + 1}: "
                            f"插入 {count} 条记录 ({current_code} 等)"
                        )

                except Exception as e:
                    total_errors += 1
                    if not quiet:
                        console.print(f"[red]✗[/red] 批次 {i // batch_size + 1} 失败: {str(e)}")
                    logger.error(f"Batch insert failed: {str(e)}", exc_info=True)
                    continue

            if not quiet:
                console.print(f"\n[bold]交易日批量更新完成:[/bold]")
                console.print(f"  交易日: {trade_date}")
                console.print(f"  {code_label}数量: {len(unique_codes)}")
                console.print(f"  总记录数: {total_records}")
                console.print(f"  插入记录: {total_inserted}")
                console.print(f"  失败数量: {total_errors}")
            else:
                console.print(f"[bold]完成:[/bold] 插入 {total_inserted}/{total_records} 条记录, 失败 {total_errors}")

            return total_inserted

    except ProviderError as e:
        console.print(f"\n[bold red]ERROR:[/bold red] 数据源错误: {str(e)}")
        raise
    except Exception as e:
        console.print(f"\n[bold red]ERROR:[/bold red] 更新失败: {str(e)}")
        raise



@app.command("etl")
def etl(
    from_date: Optional[str] = typer.Option(
        None,
        "--from-date",
        help="ETL 开始日期 (YYYY-MM-DD)"
    ),
    to_date: Optional[str] = typer.Option(
        None,
        "--to-date",
        help="ETL 结束日期 (YYYY-MM-DD)"
    ),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="股票代码列表"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="试运行，不执行实际 ETL"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细输出"
    ),
):
    """
    执行 ETL 流程

    将数据从 PostgreSQL 主存储同步到 Parquet+DuckDB 分析存储。
    支持指定日期范围和股票代码进行选择性 ETL。
    """
    console.print("[bold blue]开始 ETL 流程[/bold blue]")

    try:
        settings = get_settings()

        # 显示 ETL 参数
        if from_date:
            console.print(f"[cyan]开始日期:[/cyan] {from_date}")
        if to_date:
            console.print(f"[cyan]结束日期:[/cyan] {to_date}")
        if symbols:
            console.print(f"[cyan]股票代码:[/cyan] {symbols}")
        if dry_run:
            console.print("[yellow]⚠️  试运行模式，不会执行实际 ETL[/yellow]")

        # 显示输出路径
        console.print(f"[cyan]ETL 数据目录:[/cyan] {settings.etl.data_path}")
        console.print(f"[cyan]Parquet 目录:[/cyan] {settings.etl.parquet_path}")
        console.print(f"[cyan]批处理大小:[/cyan] {settings.etl.batch_size}")

        # TODO: 实现实际 ETL 逻辑
        console.print("\n[bold yellow]⚠️  功能待实现[/bold yellow]")
        console.print("此命令将在 Phase 3 中实现数据访问 SDK 后完成")

        # 创建一个简单的配置示例
        console.print("\n[bold]ETL 配置示例:[/bold]")
        config_example = """
# 创建一个 Parquet 文件
CREATE TABLE IF NOT EXISTS symbol_daily_etl AS
SELECT * FROM symbol_daily
WHERE time BETWEEN '2024-01-01' AND '2024-12-31';

# 使用 DuckDB 查询
SELECT symbol, AVG(close) as avg_close
FROM 'symbol_daily.parquet'
GROUP BY symbol
ORDER BY avg_close DESC
LIMIT 10;
"""
        console.print(Syntax(config_example, "sql", theme="monokai"))

        console.print("\n[bold green][OK][/bold green] 配置验证成功")

    except Exception as e:
        console.print(f"[bold red]ERROR:[/bold red] {str(e)}")
        raise typer.Exit(1)


@app.command("status")
def status(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
    format: str = typer.Option(
        "table",
        "--format",
        help="输出格式 (table, json)"
    ),
):
    """
    显示系统状态和数据完整性

    检查数据库连接、数据新鲜度、服务健康状态等信息。
    """
    console.print("[bold magenta]系统状态检查[/bold magenta]\n")

    try:
        settings = get_settings()

        # 创建状态表格
        table = Table(title="FinanceDataHub 系统状态")
        table.add_column("组件", style="cyan", no_wrap=True)
        table.add_column("状态", style="green")
        table.add_column("信息", style="yellow")

        # 数据库状态
        db_url = settings.database.url
        db_status = "[OK] 正常" if "localhost" in db_url or "postgresql" in db_url else "[WARNING] 请检查"
        db_info = "已连接" if "localhost" in db_url else db_url
        table.add_row("PostgreSQL", db_status, db_info)

        # Redis 状态
        redis_status = "[OK] 正常" if "redis://" in settings.redis.url else "[WARNING] 请检查"
        redis_info = "Redis 7.x" if "redis://" in settings.redis.url else settings.redis.url
        table.add_row("Redis", redis_status, redis_info)

        # 数据源状态
        tushare_status = "[OK] 已配置" if settings.data_source.tushare_token else "[WARNING] 未配置"
        tushare_info = "Tushare Pro API" if settings.data_source.tushare_token else "缺少 TUSHARE_TOKEN"
        table.add_row("Tushare", tushare_status, tushare_info)

        xtquant_status = "[OK] 已配置" if settings.data_source.xtquant_api_url else "[WARNING] 未配置"
        xtquant_info = settings.data_source.xtquant_api_url or "未配置"
        table.add_row("XTQuant", xtquant_status, xtquant_info)

        # 输出表格
        console.print(table)

        if verbose:
            console.print("\n[bold]详细信息:[/bold]")
            console.print(f"• 日志级别: {settings.logging.level}")
            console.print(f"• 配置文件: {settings.data_source.sources_config_path}")
            console.print(f"• ETL 数据路径: {settings.etl.data_path}")
            console.print(f"• Parquet 数据路径: {settings.etl.parquet_path}")

        # 虚拟环境信息
        import sys
        console.print(f"\n[dim]Python 版本: {sys.version.split()[0]}[/dim]")
        console.print(f"[dim]配置文件: .env 或环境变量[/dim]")

    except Exception as e:
        console.print(f"[bold red]ERROR:[/bold red] {str(e)}")
        raise typer.Exit(1)


@app.command("init")
def init_db(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
):
    """
    初始化数据库

    创建必要的数据库表、索引和扩展。
    必须在首次使用系统前执行此命令。
    """
    console.print("[bold blue]数据库初始化[/bold blue]\n")

    try:
        settings = get_settings()

        if verbose:
            console.print("[yellow]显示详细信息[/yellow]\n")
            console.print(f"[cyan]数据库URL:[/cyan] {settings.database.url}")
            console.print(f"[cyan]SQL脚本目录:[/cyan] sql/init/")

        # 执行数据库初始化
        asyncio.run(init_database(settings, verbose=verbose))

    except Exception as e:
        console.print(f"[bold red]ERROR:[/bold red] {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command("cleanup")
def cleanup_db(
    mode: str = typer.Option(
        "all",
        "--mode",
        "-m",
        help="清理模式: all-删除所有对象, data_only-只清空数据, aggregates-只删除连续聚合"
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="跳过确认直接执行"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
):
    """
    清理数据库

    删除或清空数据库对象。注意：此操作不可逆！

    清理模式:
      - all: 删除所有数据对象（表、视图、函数、连续聚合），完全重置数据库
      - data_only: 只清空数据，保留表结构和函数
      - aggregates: 只删除连续聚合视图
    """
    console.print("[bold red]警告：数据库清理操作不可逆！[/bold red]\n")

    # 模式说明
    mode_descriptions = {
        "all": "删除所有数据对象（表、视图、函数、连续聚合），完全重置数据库",
        "data_only": "只清空数据（TRUNCATE），保留表结构和函数",
        "aggregates": "只删除连续聚合视图，保留基表"
    }

    console.print(f"[bold]清理模式:[/bold] {mode}")
    console.print(f"[bold]说明:[/bold] {mode_descriptions.get(mode, '未知模式')}\n")

    if not yes:
        # 确认提示
        console.print("[bold yellow]请确认是否继续？[/bold yellow]")
        console.print("  输入 [bold]y[/bold] 继续，任意键取消: ", end="")
        try:
            import sys
            confirm = sys.stdin.readline().strip().lower()
        except KeyboardInterrupt:
            console.print("\n[yellow]已取消[/yellow]")
            raise typer.Exit(0)

        if confirm != 'y':
            console.print("[yellow]已取消[/yellow]")
            raise typer.Exit(0)

    try:
        settings = get_settings()

        if verbose:
            console.print("[yellow]显示详细信息[/yellow]\n")
            console.print(f"[cyan]数据库URL:[/cyan] {settings.database.url}")

        console.print("[bold]开始清理数据库...[/bold]\n")

        # 执行清理
        result = asyncio.run(cleanup_database(settings, mode=mode, verbose=verbose))

        # 显示结果
        console.print("\n[bold]清理结果:[/bold]")

        if mode in ("all", "data_only"):
            if result.get("continuous_aggregates"):
                console.print(f"  删除连续聚合: {', '.join(result['continuous_aggregates'])}")
            if result.get("functions"):
                console.print(f"  删除函数: {', '.join(result['functions'])}")
            if result.get("views"):
                console.print(f"  删除视图: {', '.join(result['views'])}")
            if result.get("truncated"):
                console.print(f"  清空表数据: {', '.join(result['truncated'])}")
            if result.get("tables"):
                console.print(f"  删除表: {', '.join(result['tables'])}")
        elif mode == "aggregates":
            console.print(f"  删除连续聚合: {', '.join(result.get('continuous_aggregates', []))}")

        if result.get("errors"):
            console.print("\n[bold yellow]警告:[/bold yellow]")
            for error in result["errors"]:
                console.print(f"  - {error}")

        console.print("\n[bold green][OK][/bold green] 数据库清理完成")

    except Exception as e:
        console.print(f"\n[bold red]ERROR:[/bold red] {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command("config")
def config_show(
    reload: bool = typer.Option(
        False,
        "--reload",
        help="重新加载配置文件"
    ),
):
    """
    显示当前配置信息

    显示当前加载的所有配置项（敏感信息将被隐藏）。
    """
    console.print("[bold cyan]当前配置[/bold cyan]\n")

    try:
        if reload:
            settings = reload_settings()
            console.print("[green][OK][/green] 配置已重新加载\n")
        else:
            settings = get_settings()

        # 创建配置表格
        table = Table(title="配置详情")
        table.add_column("配置项", style="cyan")
        table.add_column("值", style="yellow")

        # 数据库配置
        table.add_row("[bold]数据库配置[/bold]", "")
        db_url = settings.database.url
        if "@" in db_url:
            parts = db_url.split("@")
            hidden_url = parts[0].split("://")[0] + "://***:***@" + parts[1] if len(parts) == 2 else db_url
        else:
            hidden_url = db_url
        table.add_row("  URL", hidden_url)
        table.add_row("  池大小", str(settings.database.pool_size))
        table.add_row("  最大溢出", str(settings.database.max_overflow))

        # Redis 配置
        table.add_row("", "")
        table.add_row("[bold]Redis 配置[/bold]", "")
        table.add_row("  URL", settings.redis.url)
        table.add_row("  最大连接", str(settings.redis.max_connections))

        # 数据源配置
        table.add_row("", "")
        table.add_row("[bold]数据源配置[/bold]", "")
        tushare_token = settings.data_source.tushare_token or "未设置"
        tushare_display = tushare_token[:10] + "***" if tushare_token != "未设置" else tushare_token
        table.add_row("  Tushare Token", tushare_display)
        table.add_row("  XTQuant API URL", settings.data_source.xtquant_api_url)
        table.add_row("  Sources Config", settings.data_source.sources_config_path)

        # ETL 配置
        table.add_row("", "")
        table.add_row("[bold]ETL 配置[/bold]", "")
        table.add_row("  数据目录", settings.etl.data_path)
        table.add_row("  Parquet 目录", settings.etl.parquet_path)
        table.add_row("  批处理大小", str(settings.etl.batch_size))

        console.print(table)

    except Exception as e:
        console.print(f"[bold red]ERROR:[/bold red] {str(e)}")
        raise typer.Exit(1)


@app.command("refresh-aggregates")
def refresh_aggregates(
    table_name: str = typer.Option(
        ...,
        "--table",
        "-t",
        help="要刷新的连续聚合表名 (symbol_weekly, symbol_monthly, daily_basic_weekly, daily_basic_monthly, adj_factor_weekly, adj_factor_monthly)"
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start",
        help="刷新开始日期 (YYYY-MM-DD)，默认为空（刷新所有历史数据）"
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end",
        help="刷新结束日期 (YYYY-MM-DD)，默认为空（刷新到最新）"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细日志"
    ),
):
    """
    手动刷新连续聚合

    强制刷新指定的连续聚合视图，可指定日期范围。
    用于在新数据插入后立即更新聚合，或修复聚合数据。
    """
    console.print(f"[bold cyan]刷新连续聚合: {table_name}[/bold cyan]\n")

    try:
        settings = get_settings()

        # 构建刷新 SQL
        if start_date and end_date:
            refresh_sql = f"CALL refresh_continuous_aggregate('{table_name}', '{start_date}', '{end_date}');"
            console.print(f"刷新范围: {start_date} 到 {end_date}")
        elif start_date:
            refresh_sql = f"CALL refresh_continuous_aggregate('{table_name}', '{start_date}', NULL);"
            console.print(f"刷新范围: {start_date} 到最新")
        elif end_date:
            refresh_sql = f"CALL refresh_continuous_aggregate('{table_name}', NULL, '{end_date}');"
            console.print(f"刷新范围: 所有历史到 {end_date}")
        else:
            refresh_sql = f"CALL refresh_continuous_aggregate('{table_name}', NULL, NULL);"
            console.print("刷新范围: 所有历史数据")

        console.print("")

        # 执行刷新
        from sqlalchemy import text
        from finance_data_hub.database.manager import DatabaseManager
        from sqlalchemy.ext.asyncio import AsyncEngine

        async def _refresh():
            db_manager = DatabaseManager(settings)
            await db_manager.initialize()

            # 使用连接执行，refresh_continuous_aggregate 不能在事务中运行
            if verbose:
                console.print("[bold]执行SQL:[/bold]")
                console.print(f"  {refresh_sql}\n")

            console.print("[bold]正在刷新聚合...[/bold]")
            # 使用原始 asyncpg 连接执行，绕过 SQLAlchemy 事务管理
            async with db_manager._engine.connect() as conn:
                # 获取原始的 asyncpg 连接
                raw_conn = await conn.get_raw_connection()
                # 访问实际的 asyncpg 连接（通过适配器）
                pg_conn = raw_conn._connection
                # 在 autocommit 模式下执行
                await pg_conn.execute(refresh_sql)

            console.print("[green][OK][/green] 刷新完成！\n")

            await db_manager.close()

        asyncio.run(_refresh())

        # 显示结果
        console.print("[bold]聚合刷新成功！[/bold]")

    except Exception as e:
        console.print(f"\n[bold red]ERROR:[/bold red] 刷新失败: {str(e)}")
        if verbose:
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.command("status")
def status_show(
    aggregates: bool = typer.Option(
        False,
        "--aggregates",
        "-a",
        help="显示连续聚合状态信息"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
):
    """
    显示系统状态

    包括数据库连接状态、表信息和可选的连续聚合状态。
    """
    console.print("[bold cyan]系统状态检查[/bold cyan]\n")

    try:
        settings = get_settings()

        if aggregates:
            # 显示聚合状态
            console.print("[bold]连续聚合状态[/bold]\n")

            from sqlalchemy import text
            from finance_data_hub.database.manager import DatabaseManager

            async def _check_aggregates():
                db_manager = DatabaseManager(settings)
                await db_manager.initialize()

                # 使用连接而不是事务上下文以保持连接活跃
                async with db_manager._engine.connect() as conn:
                    # 查询聚合列表
                    result = await conn.execute(text("""
                        SELECT view_name, view_owner
                        FROM timescaledb_information.continuous_aggregates
                        WHERE view_name IN ('symbol_weekly', 'symbol_monthly', 'daily_basic_weekly', 'daily_basic_monthly', 'adj_factor_weekly', 'adj_factor_monthly')
                        ORDER BY view_name
                    """))

                    if not result.rowcount:
                        console.print("[yellow]未找到连续聚合[/yellow]")
                        return

                    # 创建表格
                    table = Table(title="连续聚合列表")
                    table.add_column("聚合名称", style="cyan")
                    table.add_column("状态", style="green")
                    table.add_column("大小", style="yellow")
                    table.add_column("最后刷新", style="blue")

                    for row in result.fetchall():
                        view_name = row.view_name

                        # 查询聚合大小
                        size_result = await conn.execute(text(f"""
                            SELECT pg_size_pretty(pg_total_relation_size('{view_name}')) AS size
                        """))
                        size_row = size_result.fetchone()
                        size_str = size_row.size if size_row else "未知"

                        # 最后刷新时间 - 简化显示（TimescaleDB版本兼容性）
                        last_refresh = "后台自动刷新"

                        # 确定状态
                        status = "[green]活跃[/green]"

                        table.add_row(view_name, status, size_str, last_refresh)

                console.print(table)

                # 显示刷新策略（在同一个连接中）
                try:
                    console.print("\n[bold]刷新策略[/bold]\n")

                    async with db_manager._engine.connect() as conn2:
                        policy_result = await conn2.execute(text("""
                            SELECT view_name, refresh_lag, end_offset, schedule_interval
                            FROM timescaledb_information.continuous_aggregates ca
                            JOIN timescaledb_information.continuous_aggregate_policies cap
                              ON ca.view_name = cap.view_name
                            WHERE ca.view_name IN ('symbol_weekly', 'symbol_monthly', 'daily_basic_weekly', 'daily_basic_monthly', 'adj_factor_weekly', 'adj_factor_monthly')
                            ORDER BY ca.view_name
                        """))

                        if policy_result.rowcount:
                            policy_table = Table(title="刷新策略")
                            policy_table.add_column("聚合名称", style="cyan")
                            policy_table.add_column("刷新滞后", style="yellow")
                            policy_table.add_column("结束偏移", style="yellow")
                            policy_table.add_column("调度间隔", style="yellow")

                            for row in policy_result.fetchall():
                                policy_table.add_row(
                                    row.view_name,
                                    str(row.refresh_lag),
                                    str(row.end_offset),
                                    str(row.schedule_interval)
                                )

                            console.print(policy_table)
                        else:
                            console.print("[yellow]未找到刷新策略信息[/yellow]")
                except Exception as policy_error:
                    # 忽略 TimescaleDB 版本兼容性错误
                    if 'does not exist' in str(policy_error) or 'undefined' in str(policy_error).lower():
                        console.print("[yellow]（刷新策略查询不可用，请使用 TimescaleDB 2.0+ 版本）[/yellow]")
                    else:
                        console.print(f"[yellow]刷新策略查询失败: {str(policy_error)[:100]}[/yellow]")

                await db_manager.close()

            asyncio.run(_check_aggregates())

        else:
            # 显示基本状态
            console.print("[yellow]使用 --aggregates 参数查看连续聚合状态[/yellow]")

    except Exception as e:
        console.print(f"\n[bold red]ERROR:[/bold red] {str(e)}")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="显示版本信息"
    )
):
    """
    FinanceDataHub - 综合性金融数据服务中心

    提供数据更新、ETL 和状态监控等功能。
    """
    pass


if __name__ == "__main__":
    app()
