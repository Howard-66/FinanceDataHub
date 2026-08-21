"""
CLI 主入口模块

提供 fdh-cli 命令行工具的入口点。
"""

from typing import Optional, List
from datetime import datetime, timedelta
import asyncio
import sys
import os

# 强制 Windows 控制台使用 UTF-8 编码
if sys.platform == "win32":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    # 启用 Windows 终端的 UTF-8 模式
    if "WT_SESSION" in os.environ:
        os.environ["PYTHONUTF8"] = "1"
    os.system("chcp 65001 > nul 2>&1") if 0 else None  # 静默设置代码页

# 必须在任何其他模块导入之前配置日志
from loguru import logger

# 默认日志配置 - 在命令执行前设置
logger.remove()
logger.add(
    sys.stderr,
    level="ERROR",  # 默认只显示 ERROR，便于安静输出
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

from finance_data_hub.providers.tushare import (
    SUPPORTED_INDEX_CODES,
    TUSHARE_INDEX_MARKETS,
)
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

# 导入并注册 schedule 子命令
from finance_data_hub.cli.schedule import schedule_app
app.add_typer(schedule_app, name="schedule", help="调度器管理命令")

# 导入并注册 preprocess 子命令
from finance_data_hub.cli.preprocess import preprocess_app
app.add_typer(preprocess_app, name="preprocess", help="数据预处理命令")

console = Console(legacy_windows=False)


def get_spinner():
    """获取与平台兼容的 spinner 文本

    在 Windows 上使用 ASCII spinner 避免编码问题
    """
    if sys.platform == "win32":
        return SpinnerColumn(spinner_name="line")
    return SpinnerColumn()


class SymbolCountColumn(ProgressColumn):
    """自定义进度列：显示已下载/总数"""

    def __init__(self, symbol_type: str = "股票"):
        super().__init__()
        self.symbol_type = symbol_type

    def render(self, task: "Task") -> Text:
        """渲染进度文本"""
        completed = task.completed
        if task.total is None:
            return Text(f"已下载 {completed:.0f} {self.symbol_type}", style="bold cyan")
        total = task.total if task.total > 0 else 1
        if completed == 0 and total == 100:
            return Text("-", style="dim")
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
    logger.debug(f"Log level set to: {log_level}")


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
    market: str = typer.Option(
        "CN",
        "--market",
        help="宽市场代码 (CN, HK, ALL)。默认 CN，保持既有 A 股行为"
    ),
    dataset: Optional[str] = typer.Option(
        None,
        "--dataset",
        "-d",
        help="数据类型 (daily, minute_1, minute_5, minute_15, minute_30, minute_60, daily_basic, adj_factor, basic, fund_basic, etf_basic, etf_index, fund_daily, fund_adj, etf_share_size, etf_sh_cons, etf_sz_cons, idx_anns, fund_company, fund_manager, fund_share, fund_nav, fund_div, mkt_idx_bmk, fund_portfolio, index_basic, gdp)。"
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
        help="代码列表，用逗号分隔，如: 600519.SH,000858.SZ；期货、指数及 moneyflow 支持 --symbols all 表示更新全量代码池"
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
      - minute_15: 15分钟线数据
      - minute_30: 30分钟线数据
      - minute_60: 60分钟线数据
      - daily_basic: 每日基本面数据
      - adj_factor: 复权因子数据
      - basic: 股票基本信息（非时间序列，强制全量更新）
      - gdp: 中国GDP宏观经济数据
      - ppi: 中国PPI工业生产者出厂价格指数数据
      - m: 中国货币供应量数据（M0、M1、M2）
      - pmi: 中国PMI采购经理人指数数据
      - fund_basic: 公募基金基础信息（非时间序列，按场内/场外市场全量刷新）
      - etf_basic: ETF基础信息（非时间序列，默认全量刷新）
      - etf_index: ETF基准指数列表（无可靠变更时间，智能模式执行快照 upsert）
      - fund_daily: ETF日线行情（支持全量与智能增量）
      - fund_adj: 基金复权因子（支持全量与智能增量）
      - etf_share_size: ETF份额规模（支持全量与智能增量）
      - etf_sh_cons/etf_sz_cons: 沪深 ETF 每日持仓组合
      - idx_anns: 指数公告（按自然月窗口全量/智能增量）
      - fund_company: 公募基金管理人目录（非时间序列，全量刷新）
      - fund_manager: 基金经理任职与简历（支持全量分页或按基金代码、公告日筛选）
      - fund_share: 公募基金规模（支持基金代码、交易日或日期区间）
      - fund_nav: 公募基金净值（支持基金代码、净值日或日期区间）
      - fund_div: 公募基金分红（支持基金代码或公告/除息/派息日）
      - mkt_idx_bmk: ETF业绩比较基准库（非时间序列）
      - fund_portfolio: 公募基金季度持仓（支持全量与智能增量下载）
      - index_basic: 指数基本信息（非时间序列，按 Tushare 指数市场全量刷新）
      - index_daily: 指数日线行情数据（沪深300、中证500、上证50、上证综指等）
      - index_dailybasic: 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）
      - index_weight: 指数成分和权重数据（月度数据，沪深300、中证500等）
      - sw_daily: 申万行业日线行情数据（申万2021版行业指数）
      - trade_cal: 交易日历数据（SSE/SZSE/CFFEX/SHFE/CZCE/DCE/INE）
      - future/basic, future/daily 等: 使用 --asset-class future，并通过 --dataset 指定
        basic, mapping, daily, minute_1, settle, index_daily, spot_basis, inventory

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
        - 使用--trade-date参数：获取指定交易日所有指数数据（如 --trade-date 2024-11-27）

        index_daily使用说明:
        - 不指定--symbols或指定--symbols all时：从本地 index_basic 获取有效指数目录（申万行业指数请使用 sw_daily）
        - 指定--symbols时：获取指定指数的历史数据（如 --symbols 000300.SH）
        - 支持 --start-date 和 --end-date 指定日期范围
        - 使用--trade-date参数：按指定交易日批量获取所有有效指数日线

        index_basic使用说明:
        - 不指定--symbols或指定--symbols all时：刷新全部 Tushare 指数市场的基础信息
        - 指定--symbols时：按 Tushare 市场代码筛选，如 --symbols SSE,SW
        - 支持的市场代码：MSCI, CSI, SSE, SZSE, CICC, SW, OTH
        - 基础信息不是时间序列，不支持 --trade-date、--start-date 或 --end-date

        fund_basic使用说明:
        - 不指定--symbols或指定--symbols all时：同步场内 E 和场外 O 的全部公募基金
        - 指定--symbols时：按基金交易市场筛选，如 --symbols E,O
        - 基础信息不是时间序列，不支持 --trade-date、--start-date 或 --end-date

        etf_basic使用说明:
        - 不指定--symbols或指定--symbols all时：分页同步全部沪深 ETF 基础信息
        - 指定--symbols时：按单个 ETF TS 代码筛选，如 --symbols 510300.SH
        - 基础信息不是时间序列，不支持 --trade-date、--start-date 或 --end-date

        fund_company使用说明:
        - 不指定--symbols或指定--symbols all时：同步全部基金管理人
        - 非时间序列，不支持 --trade-date、--start-date 或 --end-date

        fund_manager使用说明:
        - 不指定--symbols或指定--symbols all时：分页同步全部基金经理
        - 指定--symbols时：按基金 TS 代码筛选，可逗号分隔多个代码
        - --trade-date 可指定公告日期（YYYY-MM-DD 或 YYYYMMDD）

        fund_share/fund_nav/fund_div使用说明:
        - --symbols 可传基金代码（逗号分隔）；--trade-date 分别映射交易日、净值日和公告日
        - fund_share 与 fund_nav 支持 --start-date、--end-date；fund_div 全量模式也支持日期范围
        - 三者均支持 --symbols all：从本地 fund_basic 最早基金日期按对应日期字段全量下载；可用日期范围缩小范围

        mkt_idx_bmk使用说明:
        - 不指定--symbols或指定--symbols all时：同步全部ETF业绩比较基准
        - 指定--symbols时：按指数TS代码筛选，如 --symbols 000300.SH
        - 基准库不是时间序列，不支持 --trade-date、--start-date 或 --end-date

        fund_portfolio使用说明:
        - --symbols all --force：按公告日回补全量数据；--start-date/--end-date 可缩小范围
        - 不传 --symbols、--trade-date 或 --force：按本地最新公告日智能增量（首次自动回补全量）
        - 指定基金代码：--symbols 001753.OF（可逗号分隔多个基金）
        - 按报告期同步：--trade-date 2024-06-30

        index_weight使用说明:
        - 不指定--symbols或指定--symbols all时：从本地 index_basic 获取有效指数目录（智能更新）
        - 指定--symbols时：获取指定指数的数据（如 --symbols 000300.SH,000905.SH）
        - 使用--trade-date参数：获取指定日期所有指数的成分权重（如 --trade-date 2024-06-30）
        - 支持 --start-date 和 --end-date 指定日期范围

        sw_daily使用说明:
        - 不指定--symbols时：获取所有行业的历史数据（智能更新）
        - 指定--symbols时：获取指定行业的日线数据（如 --symbols 801780.SI --start-date 2024-01-01 --end-date 2024-12-31）
        - 使用--trade-date参数：获取指定交易日所有行业数据（如 --trade-date 2024-06-28）

        trade_cal使用说明:
        - 不指定--symbols时：获取所有7个交易所的日历数据（SSE/SZSE/CFFEX/SHFE/CZCE/DCE/INE）
        - 指定--symbols时：获取指定交易所的日历（如 --symbols SSE,SZSE）
        - 支持 --start-date 和 --end-date 指定日期范围

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
            console.print(f"[cyan]市场:[/cyan] {market}")
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
        update_result = asyncio.run(_run_update(
            settings, asset_class, data_type, symbols,
            start_date, end_date, adj, force, trade_date, market, verbose, quiet
        ))
        partial_failure = (
            isinstance(update_result, dict)
            and update_result.get("partial_failure", False)
        )

        if not quiet:
            if partial_failure:
                console.print("\n[bold yellow][PARTIAL][/bold yellow] 数据更新完成（有失败合约）")
            else:
                console.print("\n[bold green][OK][/bold green] 数据更新完成")
        else:
            if partial_failure:
                console.print("[bold yellow][PARTIAL][/bold yellow] 数据更新完成（有失败合约）")
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
    non_timeseries_types = {
        "basic", "asset_basic", "fund_basic", "etf_basic", "etf_index", "fund_company", "fund_manager",
        "mkt_idx_bmk", "index_basic",
    }
    return data_type not in non_timeseries_types


def _is_symbols_all(symbol_list: Optional[List[str]]) -> bool:
    if not symbol_list:
        return False
    lowered = {symbol.lower() for symbol in symbol_list}
    return lowered == {"all"}


def _validate_symbols_all(symbol_list: Optional[List[str]], scope_name: str) -> None:
    if not symbol_list:
        return
    lowered = {symbol.lower() for symbol in symbol_list}
    if "all" in lowered and len(lowered) > 1:
        raise ValueError(f"{scope_name} --symbols all 不能与其他代码混用")


FUTURE_DATASETS = {
    "basic",
    "mapping",
    "daily",
    "weekly",
    "monthly",
    "minute",
    "minute_1",
    "minute_5",
    "minute_15",
    "minute_30",
    "minute_60",
    "settle",
    "index_daily",
    "spot_basis",
    "inventory",
    "term_metrics",
}

MAINLINE_RAW_DATASETS = {
    "stock_st", "stock_namechange", "stock_suspend", "stock_dividend",
    "stock_repurchase", "margin_detail", "moneyflow_hsgt", "moneyflow",
}


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
    market: str,
    verbose: bool,
    quiet: bool = False,
):
    """执行实际的数据更新"""
    # 解析股票代码列表
    symbol_list = None
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if asset_class == "future" and symbol_list:
        _validate_symbols_all(symbol_list, "期货模式下")
    if data_type in {
        "index_basic", "index_daily", "index_weight", "fund_basic", "fund_company",
        "fund_manager", "etf_basic", "etf_index", "mkt_idx_bmk", "sw_daily",
        "fund_daily", "fund_adj", "etf_share_size", "etf_sh_cons", "etf_sz_cons",
        "idx_anns", *MAINLINE_RAW_DATASETS,
    } and symbol_list:
        _validate_symbols_all(symbol_list, "指数/基金模式下")

    if data_type in {"index_basic", "fund_basic", "etf_basic", "fund_company", "mkt_idx_bmk"} and (trade_date or start_date or end_date):
        raise ValueError(
            f"{data_type} 是非时间序列数据，不支持 --trade-date、--start-date 或 --end-date"
        )
    if data_type == "fund_manager" and (start_date or end_date):
        raise ValueError("fund_manager 不支持 --start-date 或 --end-date；可使用 --trade-date 指定公告日")

    # 设置默认日期；trade_date 模式不能自动注入 end_date，否则会破坏互斥参数。
    if not end_date and not trade_date and data_type != "trade_cal":
        end_date = datetime.now().strftime("%Y-%m-%d")

    if asset_class == "future":
        return await _run_future_update(
            settings,
            data_type,
            symbol_list,
            start_date,
            end_date,
            force,
            trade_date,
            verbose,
            quiet,
        )

    # 公募基金规模/净值/分红使用接口自己的筛选与分页，不能走股票智能下载。
    if data_type in {
        "etf_basic", "fund_company", "fund_manager", "fund_share", "fund_nav", "fund_div",
        "fund_portfolio", "etf_index", "fund_daily", "fund_adj", "etf_share_size",
        "etf_sh_cons", "etf_sz_cons", "idx_anns", *MAINLINE_RAW_DATASETS,
    }:
        return await _run_force_update(
            settings, asset_class, data_type, symbol_list,
            start_date, end_date, adj, trade_date, market, verbose, quiet,
            force_update=force,
        )
    # 更新策略矩阵：根据参数组合自动选择最优策略
    elif trade_date:
        # 策略 1: trade_date 批量更新（Tushare专用）
        console.print("\n[bold yellow]使用交易日批量更新模式[/bold yellow]")
        await _run_trade_date_update(
            settings, asset_class, data_type, trade_date, market, verbose, quiet
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
            start_date, end_date, adj, trade_date, market, verbose, quiet,
            force_update=force,
        )
    else:
        # 策略 3: 智能下载模式（默认）
        console.print("\n[bold yellow]使用智能下载模式[/bold yellow]")
        await _run_smart_download(
            settings, asset_class, data_type, symbol_list,
            end_date, adj, trade_date, start_date, market, verbose, quiet
        )


async def _run_future_update(
    settings,
    data_type: str,
    symbol_list: Optional[List[str]],
    start_date: Optional[str],
    end_date: Optional[str],
    force: bool,
    trade_date: Optional[str],
    verbose: bool,
    quiet: bool = False,
):
    """执行期货数据更新。"""
    if data_type not in FUTURE_DATASETS:
        console.print(f"[bold red]不支持的期货数据类型: {data_type}[/bold red]")
        raise typer.Exit(1)

    freq_map = {
        "minute": "1m",
        "minute_1": "1m",
        "minute_5": "5m",
        "minute_15": "15m",
        "minute_30": "30m",
        "minute_60": "60m",
    }
    task_total = len(symbol_list) if symbol_list else 1
    if data_type in {"daily", "weekly", "monthly", "minute", "minute_1", "minute_5", "minute_15", "minute_30", "minute_60", "settle", "mapping"} and symbol_list:
        task_total = len(symbol_list)

    if not quiet:
        console.print("[bold]期货更新策略:[/bold]")
        console.print("  - 使用 futures schema 专用表")
        if symbol_list:
            console.print(f"  - 输入代码数量: {len(symbol_list)}")
            if len(symbol_list) == 1 and str(symbol_list[0]).lower() == "all":
                console.print("  - 合约范围: 全量合约池（基于 futures.contract_basic 展开）")
        if trade_date:
            console.print(f"  - 交易日模式: {trade_date}")
        elif force:
            console.print("  - 强制更新模式")
        else:
            console.print("  - 智能增量模式")
        console.print("")

    with Progress(
        get_spinner(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("正在更新期货数据...", total=task_total)
        latest_progress_total = task_total

        async with DataUpdater(settings, config_path="sources.yml") as updater:
            def progress_callback(current, total):
                nonlocal latest_progress_total
                latest_progress_total = total
                progress.update(task, completed=current, total=total)

            try:
                futures_summary = None
                summary_label = "期货数据"
                if data_type == "basic":
                    count = await updater.update_futures_basic()
                elif data_type == "mapping":
                    count = await updater.update_futures_mapping(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                elif data_type == "daily":
                    count = await updater.update_futures_daily(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                elif data_type == "weekly":
                    count = await updater.update_futures_weekly(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=None if trade_date else start_date,
                        end_date=None if trade_date else end_date,
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                    futures_summary = updater.__dict__.get(
                        "last_futures_period_summary"
                    )
                    summary_label = "周线"
                elif data_type == "monthly":
                    count = await updater.update_futures_monthly(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=None if trade_date else start_date,
                        end_date=None if trade_date else end_date,
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                    futures_summary = updater.__dict__.get(
                        "last_futures_period_summary"
                    )
                    summary_label = "月线"
                elif data_type.startswith("minute"):
                    count = await updater.update_futures_minute(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        freq=freq_map.get(data_type, "1m"),
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                    futures_summary = updater.__dict__.get(
                        "last_futures_minute_summary"
                    )
                    summary_label = "分钟线"
                elif data_type == "settle":
                    count = await updater.update_futures_settle(
                        symbols=symbol_list,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                        progress_callback=progress_callback,
                    )
                elif data_type == "index_daily":
                    count = await updater.update_futures_index_daily(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                    )
                elif data_type == "spot_basis":
                    count = await updater.update_futures_spot_basis(
                        product_codes=symbol_list,
                        trade_date=trade_date,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                    )
                elif data_type == "inventory":
                    count = await updater.update_futures_inventory(
                        product_codes=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=force,
                    )
                else:
                    count = await updater.preprocess_futures_term_metrics(
                        product_codes=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        progress_callback=progress_callback,
                    )

                completed_total = (
                    futures_summary["total_symbols"]
                    if futures_summary and futures_summary.get("total_symbols")
                    else latest_progress_total
                )
                progress.update(
                    task, completed=completed_total, total=completed_total
                )
                failed_symbols = (
                    futures_summary.get("failed_symbols", [])
                    if futures_summary
                    else []
                )
                if failed_symbols:
                    console.print(
                        f"[yellow][PARTIAL][/yellow] 已更新 {count} 条期货数据，"
                        f"{len(failed_symbols)}/{futures_summary['total_symbols']} 个合约失败"
                    )
                    console.print(
                        f"[yellow]  {summary_label}摘要:[/yellow] "
                        f"成功 {futures_summary['inserted_symbols']}，"
                        f"空数据 {futures_summary['empty_symbols']}，"
                        f"已是最新 {futures_summary['up_to_date_symbols']}，"
                        f"失败 {len(failed_symbols)}"
                    )
                    sample = ", ".join(item["symbol"] for item in failed_symbols[:10])
                    suffix = "..." if len(failed_symbols) > 10 else ""
                    console.print(f"[yellow]  失败样例:[/yellow] {sample}{suffix}")
                    error_samples = []
                    seen_errors = set()
                    for item in failed_symbols:
                        error = str(item.get("error") or "").strip()
                        if not error or error in seen_errors:
                            continue
                        seen_errors.add(error)
                        error_samples.append(error[:500])
                        if len(error_samples) >= 3:
                            break
                    for error in error_samples:
                        console.print("[yellow]  错误样例:[/yellow]", Text(error))
                    return {"count": count, "partial_failure": True}
                else:
                    console.print(f"[green][OK][/green] 已更新 {count} 条期货数据")
                return count
            except Exception as e:
                progress.update(task, failed=True)
                console.print(f"[bold red]ERROR:[/bold red] 更新期货数据失败: {str(e)}")
                if verbose:
                    import traceback
                    console.print(traceback.format_exc())
                raise


async def _run_smart_download(
    settings,
    asset_class: str,
    data_type: str,
    symbol_list: Optional[List[str]],
    end_date: Optional[str],
    adj: Optional[str],
    trade_date: Optional[str],
    start_date: Optional[str],
    market: str,
    verbose: bool,
    quiet: bool = False,
):
    """智能下载模式：自动检测数据库状态，智能选择全量或增量下载"""
    if data_type in ("basic", "asset_basic"):
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 股票基本信息为非时间序列数据")
            console.print("  - 直接执行全量刷新，无需预先读取数据库股票池")
            console.print("")

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在更新股票基本信息...", total=1)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_stock_basic(market=market)
                    progress.update(task, completed=1)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新股票基本信息失败: {str(e)}")
                    raise

    # GDP 数据不需要 symbol，单独处理
    if data_type == "gdp":
        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - GDP是宏观经济数据，无需symbol")
            console.print("  - 自动获取最新季度数据")
            console.print("")

        with Progress(
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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

    # 指数日线行情数据处理
    if data_type == "index_daily":
        ts_code_list = None if _is_symbols_all(symbol_list) else symbol_list

        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not quiet:
                console.print("[bold]智能下载策略:[/bold]")
                console.print("  - 指数日线行情数据（来自本地 index_basic，有效指数；申万行业指数请使用 sw_daily）")
                if ts_code_list:
                    console.print(f"  - 将更新 {len(ts_code_list)} 个指数")
                else:
                    console.print("  - 将按本地交易日批量更新")
                console.print("")

            index_count = len(ts_code_list) if ts_code_list else None
            progress_unit = "指数" if ts_code_list else "交易日"

            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn(progress_unit),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数日线行情数据...", total=index_count)

                latest_total = index_count

                def progress_callback(current, total):
                    nonlocal latest_total
                    latest_total = total
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_daily(
                        ts_code_list=ts_code_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    if latest_total is not None:
                        progress.update(task, completed=latest_total, total=latest_total)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数日线行情数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数日线行情数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数日线行情数据失败: {str(e)}")
                    raise

    # 大盘指数每日指标数据处理
    if data_type == "index_dailybasic":
        # 获取指数代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为指数代码列表

        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）")
            if ts_code_list:
                console.print(f"  - 将更新 {len(ts_code_list)} 个指数")
            else:
                console.print(f"  - 将更新 {len(SUPPORTED_INDEX_CODES)} 个指数")
            console.print("")

        # 确定要更新的指数列表
        index_count = len(ts_code_list) if ts_code_list else len(SUPPORTED_INDEX_CODES)

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("指数"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取指数每日指标数据...", total=index_count)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_dailybasic(
                        ts_code_list=ts_code_list if ts_code_list else SUPPORTED_INDEX_CODES,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=index_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数每日指标数据失败: {str(e)}")
                    raise

    # 指数成分权重数据处理
    if data_type == "index_weight":
        # 获取指数代码列表
        index_code_list = None if _is_symbols_all(symbol_list) else symbol_list

        async with DataUpdater(settings, config_path="sources.yml") as updater:
            resolved_index_codes = (
                await updater.resolve_index_weight_codes(active_date=end_date)
                if index_code_list is None
                else index_code_list
            )

            if not quiet:
                console.print("[bold]智能下载策略:[/bold]")
                console.print("  - 指数成分和权重数据（月度数据，来自本地 index_basic 有效指数目录）")
                console.print(f"  - 将更新 {len(resolved_index_codes)} 个指数")
                console.print("")

            index_count = len(resolved_index_codes)

            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn("指数"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数成分权重数据...", total=index_count)

                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_weight(
                        index_list=resolved_index_codes,
                        start_date=start_date,  # 智能下载时为None
                        end_date=end_date,
                        trade_date=None,  # 智能更新不使用trade_date
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=index_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数成分权重数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数成分权重数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数成分权重数据失败: {str(e)}")
                    raise

    # 申万行业日线行情数据处理
    if data_type == "sw_daily":
        # 获取行业代码列表
        ts_code_list = None if _is_symbols_all(symbol_list) else symbol_list

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
            get_spinner(),
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

    # 交易日历数据处理
    if data_type == "trade_cal":
        # 获取交易所代码列表（使用symbol_list存储）
        exchange_list = symbol_list  # symbol_list 存储交易所代码（如 SSE, SZSE）

        if not quiet:
            console.print("[bold]智能下载策略:[/bold]")
            console.print("  - 交易日历数据（SSE/SZSE/CFFEX/SHFE/CZCE/DCE/INE）")
            if exchange_list:
                console.print(f"  - 将更新 {len(exchange_list)} 个交易所")
            else:
                console.print("  - 将更新 7 个交易所")
            console.print("")

        # 确定要更新的交易所列表
        from finance_data_hub.providers.tushare import SUPPORTED_EXCHANGES
        exchange_count = len(exchange_list) if exchange_list else len(SUPPORTED_EXCHANGES)

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("交易所"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取交易日历数据...", total=exchange_count)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_trade_cal(
                        exchange_list=exchange_list if exchange_list else None,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=False,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=exchange_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条交易日历数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条交易日历数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新交易日历数据失败: {str(e)}")
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            try:
                # 如果没有指定symbol，从数据库获取股票列表
                if not symbol_list:
                    symbol_limit = 10 if data_type.startswith("minute") else None
                    symbols_db = await updater.data_ops.get_symbol_list(
                        market=market,
                        limit=symbol_limit,
                    )
                    if symbols_db:
                        symbol_list = symbols_db
                        if not quiet:
                            console.print(f"[yellow]从数据库获取到 {len(symbol_list)} 只股票[/yellow]\n")
                    else:
                        if not quiet:
                            console.print("[bold red]数据库中没有股票列表，请先执行: fdh-cli update --dataset basic[/bold red]")
                        else:
                            console.print("[bold red]请先执行: fdh-cli update --dataset basic[/bold red]")
                        return 0

                total_updated = 0
                total_errors = 0
                single_symbol_adj_factor = (
                    data_type == "adj_factor" and len(symbol_list) == 1
                )

                task = progress.add_task("正在智能下载...", total=len(symbol_list))

                if data_type == "adj_factor":
                    def progress_callback(current, total):
                        progress.update(task, completed=current, total=total)

                    count = await updater.update_adj_factor(
                        symbols=symbol_list,
                        start_date=None,  # 智能下载
                        end_date=end_date,
                        force_update=False,
                        market=market,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=len(symbol_list), total=len(symbol_list))
                    if not quiet:
                        console.print(f"\n[bold]智能下载完成:[/bold]")
                        console.print(f"  更新记录: {count}")
                    else:
                        console.print(f"[bold]完成:[/bold] 更新 {count} 条记录")
                    return count

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
                                market=market,
                            )
                        elif data_type.startswith("minute"):
                            # 从 data_type 中提取频率
                            freq_map = {
                                "minute_1": "1m",
                                "minute_5": "5m",
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
                                market=market,
                            )
                        elif data_type == "daily_basic":
                            count = await updater.update_daily_basic(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                force_update=False,
                                market=market,
                            )
                        elif data_type == "adj_factor":
                            count = await updater.update_adj_factor(
                                symbols=[symbol],
                                start_date=None,  # 智能下载
                                end_date=end_date,
                                force_update=False,
                                market=market,
                            )
                        elif data_type in ("basic", "asset_basic"):
                            # asset_basic 是非时间序列数据，不会进入智能下载模式
                            # 这里添加是为了代码完整性，但实际上不会执行到此处
                            count = await updater.update_stock_basic(market=market)
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
                        if single_symbol_adj_factor:
                            raise
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
    market: str,
    verbose: bool,
    quiet: bool = False,
    force_update: bool = False,
):
    """强制更新模式：忽略数据库状态，使用指定日期范围"""
    if data_type in MAINLINE_RAW_DATASETS:
        selected_symbols = None if _is_symbols_all(symbol_list) else symbol_list
        count_unit = {
            "stock_dividend": "股票",
            "margin_detail": "日期",
            "moneyflow": "股票",
        }.get(data_type)
        count_columns = [SymbolCountColumn(count_unit)] if count_unit else []
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"),
            BarColumn(), *count_columns, TimeElapsedColumn(), console=console,
        ) as progress:
            task = progress.add_task(f"正在更新 {data_type} ...", total=1)
            final_scope_total = 1

            def update_progress(completed: int, total: int) -> None:
                nonlocal final_scope_total
                final_scope_total = total
                progress.update(task, completed=completed, total=total)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                count = await updater.update_mainline_raw(
                    dataset=data_type,
                    symbols=selected_symbols,
                    trade_date=trade_date,
                    start_date=start_date,
                    end_date=end_date,
                    progress_callback=update_progress,
                )
                progress.update(
                    task, completed=final_scope_total, total=final_scope_total
                )
                console.print(f"[green][OK][/green] 已更新 {count} 条 {data_type} 数据")
                return count

    if data_type in ("basic", "asset_basic"):
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 股票基本信息为非时间序列数据")
            console.print("  - 直接执行全量刷新，无需预先读取数据库股票池")
            console.print("")

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在强制更新股票基本信息...", total=1)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_stock_basic(market=market)
                    progress.update(task, completed=1)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")
                        console.print("[yellow]股票基本信息为全量数据，无需按symbol逐一更新[/yellow]\n")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条股票基本信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新股票基本信息失败: {str(e)}")
                    raise

    if data_type == "fund_basic":
        selected_markets = None if _is_symbols_all(symbol_list) else symbol_list
        if selected_markets:
            selected_markets = [item.upper() for item in selected_markets]
            supported_markets = {"E", "O"}
            invalid_markets = sorted(set(selected_markets) - supported_markets)
            if invalid_markets:
                raise ValueError(
                    "fund_basic 的 --symbols 仅支持基金交易市场代码 (E,O)；无效值: "
                    f"{', '.join(invalid_markets)}"
                )

        if not quiet:
            scope = ", ".join(selected_markets) if selected_markets else "场内 E 和场外 O"
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 公募基金基础信息为非时间序列数据，按市场全量刷新")
            console.print(f"  - 更新范围: {scope}")
            console.print("")

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在更新公募基金基本信息...", total=1)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_fund_basic(markets=selected_markets)
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条公募基金基本信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新公募基金基本信息失败: {str(e)}")
                    raise

    if data_type == "etf_basic":
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        if selected_codes and len(selected_codes) != 1:
            raise ValueError("etf_basic 的 --symbols 最多指定一个 ETF TS 代码")

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在更新 ETF 基础信息...", total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_etf_basic(
                        ts_code=selected_codes[0] if selected_codes else None
                    )
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条 ETF 基础信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新 ETF 基础信息失败: {str(e)}")
                    raise

    if data_type == "etf_index":
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        if selected_codes and len(selected_codes) != 1:
            raise ValueError("etf_index 的 --symbols 最多指定一个指数代码")
        with Progress(get_spinner(), TextColumn("[bold blue]{task.description}"),
                      BarColumn(), TimeElapsedColumn(), console=console) as progress:
            task = progress.add_task("正在刷新 ETF 基准指数列表...", total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                count = await updater.update_etf_index(
                    ts_code=selected_codes[0] if selected_codes else None
                )
                progress.update(task, completed=1)
                console.print(f"[green][OK][/green] 已更新 {count} 条 ETF 基准指数")
                return count

    if data_type == "fund_company":
        if symbol_list and not _is_symbols_all(symbol_list):
            raise ValueError("fund_company 不接受 --symbols；请省略或使用 --symbols all")
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TimeElapsedColumn(), console=console,
        ) as progress:
            task = progress.add_task("正在更新公募基金管理人...", total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_fund_company()
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条公募基金管理人")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新公募基金管理人失败: {str(e)}")
                    raise

    if data_type == "fund_manager":
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TimeElapsedColumn(), console=console,
        ) as progress:
            task = progress.add_task("正在更新基金经理信息...", total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_fund_manager(
                        fund_codes=selected_codes,
                        ann_date=trade_date,
                    )
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条基金经理信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新基金经理信息失败: {str(e)}")
                    raise

    if data_type == "mkt_idx_bmk":
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        if selected_codes and len(selected_codes) != 1:
            raise ValueError("mkt_idx_bmk 的 --symbols 最多指定一个指数 TS 代码")
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TimeElapsedColumn(), console=console,
        ) as progress:
            task = progress.add_task("正在更新 ETF 业绩比较基准库...", total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_mkt_idx_bmk(
                        ts_code=selected_codes[0] if selected_codes else None
                    )
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条 ETF 业绩比较基准")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新 ETF 业绩比较基准失败: {str(e)}")
                    raise

    if data_type in {
        "fund_daily", "fund_adj", "etf_share_size", "etf_sh_cons", "etf_sz_cons",
    }:
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        # A supplied trade_date is an explicit all-market date query, even when
        # the user also writes `--symbols all`. It must not be classified as a
        # catalog-wide full-history download.
        all_data = not trade_date and (
            _is_symbols_all(symbol_list)
            or force_update and not selected_codes
        )
        smart_incremental = not all_data and not selected_codes and not trade_date
        progress_unit = (
            "ETF代码" if (
                data_type in {"etf_sh_cons", "etf_sz_cons"}
                or all_data and data_type in {"fund_daily", "etf_share_size"}
            )
            else "交易日"
        )
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TextColumn(f"[bold cyan]已下载 {{task.completed:.0f}}/{{task.total:.0f}} {progress_unit}"),
            TimeElapsedColumn(), console=console,
        ) as progress:
            mode = "全量" if all_data else ("智能增量" if smart_incremental else "指定范围")
            task = progress.add_task(f"正在{mode}下载 {data_type} ...", total=1)

            def update_progress(completed: int, total: int) -> None:
                progress.update(task, completed=completed, total=total)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                method = getattr(updater, f"update_{data_type}")
                kwargs = {
                    "fund_codes": selected_codes,
                    "trade_date": trade_date,
                    "start_date": start_date,
                    "end_date": end_date,
                    "all_funds": all_data,
                    "smart_incremental": smart_incremental,
                    "progress_callback": update_progress,
                }
                if data_type == "etf_share_size":
                    kwargs["exchange"] = market if market in {"SH", "SZ"} else None
                count = await method(**kwargs)
                console.print(f"[green][OK][/green] 已更新 {count} 条 {data_type} 数据")
                return count

    if data_type == "idx_anns":
        if symbol_list and not _is_symbols_all(symbol_list):
            raise ValueError("idx_anns 不接受指数代码；SDK 可使用 src 筛选公告来源")
        all_data = _is_symbols_all(symbol_list) or (force_update and not trade_date)
        smart_incremental = not all_data and not trade_date
        with Progress(
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
            TextColumn("[bold cyan]已下载 {task.completed:.0f}/{task.total:.0f} 月度窗口"),
            TimeElapsedColumn(), console=console,
        ) as progress:
            task = progress.add_task("正在同步指数公告...", total=1)

            def update_progress(completed: int, total: int) -> None:
                progress.update(task, completed=completed, total=total)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                count = await updater.update_idx_anns(
                    ann_date=trade_date, start_date=start_date, end_date=end_date,
                    all_data=all_data, smart_incremental=smart_incremental,
                    progress_callback=update_progress,
                )
                console.print(f"[green][OK][/green] 已更新 {count} 条 idx_anns 数据")
                return count

    if data_type in {"fund_share", "fund_nav", "fund_div"}:
        all_fund_dataset = (
            data_type in {"fund_share", "fund_nav", "fund_div"}
            and _is_symbols_all(symbol_list)
        )
        if _is_symbols_all(symbol_list) and not all_fund_dataset:
            raise ValueError(f"{data_type} 不支持 --symbols all")
        fund_codes = (
            None if all_fund_dataset else (",".join(symbol_list) if symbol_list else None)
        )
        full_date_label = "公告日" if data_type == "fund_div" else "交易日"
        progress_columns = [
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
        ]
        if all_fund_dataset:
            progress_columns.append(
                TextColumn(
                    f"[bold cyan]已下载 {{task.completed:.0f}}/{{task.total:.0f}} {full_date_label}"
                )
            )
        progress_columns.append(TimeElapsedColumn())

        with Progress(*progress_columns, console=console) as progress:
            task_description = (
                f"正在按日期全量下载 {data_type} ..."
                if all_fund_dataset
                else f"正在更新 {data_type} ..."
            )
            task = progress.add_task(task_description, total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    full_update_kwargs = {}
                    if all_fund_dataset:
                        def update_progress(completed: int, total: int) -> None:
                            progress.update(task, completed=completed, total=total)

                        full_update_kwargs = {
                            "all_funds": True,
                            "progress_callback": update_progress,
                        }

                    if data_type == "fund_share":
                        share_kwargs = dict(
                            ts_code=fund_codes, trade_date=trade_date,
                            start_date=start_date, end_date=end_date,
                            market=market if market in {"SH", "SZ"} else None,
                        )
                        share_kwargs.update(full_update_kwargs)
                        count = await updater.update_fund_share(**share_kwargs)
                    elif data_type == "fund_nav":
                        nav_kwargs = dict(
                            ts_code=fund_codes, nav_date=trade_date,
                            market=market if market in {"E", "O"} else None,
                            start_date=start_date, end_date=end_date,
                        )
                        nav_kwargs.update(full_update_kwargs)
                        count = await updater.update_fund_nav(**nav_kwargs)
                    else:
                        div_kwargs = dict(
                            ts_code=fund_codes, ann_date=trade_date,
                        )
                        if all_fund_dataset:
                            div_kwargs.update(
                                start_date=start_date,
                                end_date=end_date,
                            )
                        div_kwargs.update(full_update_kwargs)
                        count = await updater.update_fund_div(**div_kwargs)
                    if not all_fund_dataset:
                        progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条 {data_type} 数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新 {data_type} 失败: {str(e)}")
                    raise

    if data_type == "fund_portfolio":
        selected_codes = None if _is_symbols_all(symbol_list) else symbol_list
        all_fund_dataset = _is_symbols_all(symbol_list) and not trade_date
        # ``--force`` without a more specific scope is a full backfill.  A
        # scope-less ordinary invocation instead resumes from its checkpoint.
        if force_update and not selected_codes and not trade_date:
            all_fund_dataset = True
        smart_incremental = (
            not all_fund_dataset and not selected_codes and not trade_date
        )

        progress_columns = [
            get_spinner(), TextColumn("[bold blue]{task.description}"), BarColumn(),
        ]
        if all_fund_dataset or smart_incremental:
            progress_columns.append(
                TextColumn("[bold cyan]已下载 {task.completed:.0f}/{task.total:.0f} 公告日")
            )
        progress_columns.append(TimeElapsedColumn())

        with Progress(*progress_columns, console=console) as progress:
            task_description = (
                "正在按公告日全量下载 fund_portfolio ..."
                if all_fund_dataset
                else (
                    "正在智能增量下载 fund_portfolio ..."
                    if smart_incremental
                    else "正在更新公募基金持仓..."
                )
            )
            task = progress.add_task(task_description, total=1)
            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    update_kwargs = {
                        "fund_codes": selected_codes,
                        "period": trade_date,
                        "start_date": start_date,
                        "end_date": end_date,
                        "all_funds": all_fund_dataset,
                        "smart_incremental": smart_incremental,
                    }
                    if all_fund_dataset or smart_incremental:
                        def update_progress(completed: int, total: int) -> None:
                            progress.update(task, completed=completed, total=total)

                        update_kwargs["progress_callback"] = update_progress

                    count = await updater.update_fund_portfolio(
                        **update_kwargs,
                    )
                    if not (all_fund_dataset or smart_incremental):
                        progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条公募基金持仓")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新公募基金持仓失败: {str(e)}")
                    raise

    if data_type == "index_basic":
        selected_markets = None if _is_symbols_all(symbol_list) else symbol_list
        if selected_markets:
            selected_markets = [item.upper() for item in selected_markets]
            invalid_markets = sorted(
                set(selected_markets) - set(TUSHARE_INDEX_MARKETS)
            )
            if invalid_markets:
                raise ValueError(
                    "index_basic 的 --symbols 仅支持 Tushare 指数市场代码 "
                    f"({', '.join(TUSHARE_INDEX_MARKETS)})；无效值: "
                    f"{', '.join(invalid_markets)}"
                )

        if not quiet:
            scope = ", ".join(selected_markets) if selected_markets else "全部市场"
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 指数基本信息为非时间序列数据，按市场全量刷新")
            console.print(f"  - 更新范围: {scope}")
            console.print("")

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在更新指数基本信息...", total=1)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                try:
                    count = await updater.update_index_basic(markets=selected_markets)
                    progress.update(task, completed=1)
                    console.print(f"[green][OK][/green] 已更新 {count} 条指数基本信息")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数基本信息失败: {str(e)}")
                    raise

    # GDP 数据不需要 symbol，单独处理
    if data_type == "gdp":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - GDP是宏观经济数据，无需symbol")
            console.print("  - 使用指定的日期范围")
            console.print("")

        with Progress(
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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

    # 指数日线行情数据处理
    if data_type == "index_daily":
        ts_code_list = None if _is_symbols_all(symbol_list) else symbol_list

        async with DataUpdater(settings, config_path="sources.yml") as updater:
            if not quiet:
                console.print("[bold]强制更新策略:[/bold]")
                console.print("  - 指数日线行情数据（来自本地 index_basic；申万行业指数请使用 sw_daily）")
                console.print("  - 使用指定的日期范围")
                if ts_code_list:
                    console.print(f"  - 将更新 {len(ts_code_list)} 个指数")
                else:
                    console.print("  - 将按本地交易日批量更新")
                console.print("")

            index_count = len(ts_code_list) if ts_code_list else None
            progress_unit = "指数" if ts_code_list else "交易日"

            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn(progress_unit),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数日线行情数据...", total=index_count)

                latest_total = index_count

                def progress_callback(current, total):
                    nonlocal latest_total
                    latest_total = total
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_daily(
                        ts_code_list=ts_code_list,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    if latest_total is not None:
                        progress.update(task, completed=latest_total, total=latest_total)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数日线行情数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数日线行情数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数日线行情数据失败: {str(e)}")
                    raise

    # 大盘指数每日指标数据处理
    if data_type == "index_dailybasic":
        # 获取指数代码列表
        ts_code_list = symbol_list  # 使用symbol_list作为指数代码列表

        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 大盘指数每日指标数据（上证综指、深证成指、上证50、中证500等）")
            console.print("  - 使用指定的日期范围")
            if ts_code_list:
                console.print(f"  - 将更新 {len(ts_code_list)} 个指数")
            else:
                console.print(f"  - 将更新 {len(SUPPORTED_INDEX_CODES)} 个指数")
            console.print("")

        # 确定要更新的指数列表
        index_count = len(ts_code_list) if ts_code_list else len(SUPPORTED_INDEX_CODES)

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("指数"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取指数每日指标数据...", total=index_count)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_dailybasic(
                        ts_code_list=ts_code_list if ts_code_list else SUPPORTED_INDEX_CODES,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=index_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数每日指标数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数每日指标数据失败: {str(e)}")
                    raise

    # 指数成分权重数据处理
    if data_type == "index_weight":
        # 获取指数代码列表
        index_code_list = None if _is_symbols_all(symbol_list) else symbol_list

        async with DataUpdater(settings, config_path="sources.yml") as updater:
            resolved_index_codes = (
                await updater.resolve_index_weight_codes(active_date=start_date)
                if index_code_list is None
                else index_code_list
            )

            if not quiet:
                console.print("[bold]强制更新策略:[/bold]")
                console.print("  - 指数成分和权重数据（月度数据，来自本地 index_basic）")
                console.print("  - 使用指定的日期范围")
                console.print(f"  - 将更新 {len(resolved_index_codes)} 个指数")
                console.print("")

            index_count = len(resolved_index_codes)

            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn("指数"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数成分权重数据...", total=index_count)

                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_index_weight(
                        index_list=resolved_index_codes,
                        start_date=start_date,
                        end_date=end_date,
                        trade_date=None,  # 强制更新不使用trade_date
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=index_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数成分权重数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条指数成分权重数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新指数成分权重数据失败: {str(e)}")
                    raise

    # 申万行业日线行情数据处理
    if data_type == "sw_daily":
        # 获取行业代码列表
        ts_code_list = None if _is_symbols_all(symbol_list) else symbol_list

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
            get_spinner(),
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

    # 交易日历数据处理
    if data_type == "trade_cal":
        # 获取交易所代码列表
        exchange_list = symbol_list  # symbol_list 存储交易所代码

        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 交易日历数据（SSE/SZSE/CFFEX/SHFE/CZCE/DCE/INE）")
            if exchange_list:
                console.print(f"  - 将更新 {len(exchange_list)} 个交易所")
            else:
                console.print("  - 将更新 7 个交易所")
            console.print("  - 使用指定的日期范围")
            console.print("")

        # 确定要更新的交易所列表
        from finance_data_hub.providers.tushare import SUPPORTED_EXCHANGES
        exchange_count = len(exchange_list) if exchange_list else len(SUPPORTED_EXCHANGES)

        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("交易所"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在获取交易日历数据...", total=exchange_count)

            async with DataUpdater(settings, config_path="sources.yml") as updater:
                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                try:
                    count = await updater.update_trade_cal(
                        exchange_list=exchange_list if exchange_list else None,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=exchange_count)
                    if not quiet:
                        console.print(f"[green][OK][/green] 已更新 {count} 条交易日历数据")
                    else:
                        console.print(f"[green][OK][/green] 已更新 {count} 条交易日历数据")
                    return count
                except Exception as e:
                    progress.update(task, failed=True)
                    console.print(f"[bold red]ERROR:[/bold red] 更新交易日历数据失败: {str(e)}")
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
            get_spinner(),
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
            get_spinner(),
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
            get_spinner(),
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

    # 利润表数据处理
    if data_type == "income":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 利润表数据（上市公司收入、成本和利润）")
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
            get_spinner(),
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
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
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

    # 现金流量表数据处理
    if data_type == "cashflow":
        if not quiet:
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 现金流量表数据（上市公司三大活动现金流量）")
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
            get_spinner(),
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
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
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
            console.print("[bold]强制更新策略:[/bold]")
            console.print("  - 资产负债表数据（上市公司资产、负债和股东权益）")
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
            get_spinner(),
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
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
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

    if not quiet:
        console.print("[bold]强制更新策略:[/bold]")
        console.print("  - 忽略数据库现有状态")
        console.print("  - 使用用户指定的日期范围")
        console.print("  - 覆盖现有数据")
        console.print("")

    # 初始化更新器
    async with DataUpdater(settings, config_path="sources.yml") as updater:
        with Progress(
            get_spinner(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            SymbolCountColumn("股票"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            try:
                # 如果没有指定symbol，从数据库获取股票列表
                if not symbol_list:
                    symbol_limit = 10 if data_type.startswith("minute") else None
                    symbols_db = await updater.data_ops.get_symbol_list(
                        market=market,
                        limit=symbol_limit,
                    )
                    if symbols_db:
                        symbol_list = symbols_db
                        if not quiet:
                            console.print(f"[yellow]从数据库获取到 {len(symbol_list)} 只股票[/yellow]\n")
                    else:
                        if not quiet:
                            console.print("[bold red]数据库中没有股票列表，请先执行: fdh-cli update --dataset basic[/bold red]")
                        else:
                            console.print("[bold red]请先执行: fdh-cli update --dataset basic[/bold red]")
                        return 0

                total_updated = 0
                total_errors = 0
                single_symbol_adj_factor = (
                    data_type == "adj_factor" and len(symbol_list) == 1
                )

                task = progress.add_task("正在强制更新...", total=len(symbol_list))

                if data_type == "adj_factor":
                    def progress_callback(current, total):
                        progress.update(task, completed=current, total=total)

                    count = await updater.update_adj_factor(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        force_update=True,
                        market=market,
                        progress_callback=progress_callback,
                    )
                    progress.update(task, completed=len(symbol_list), total=len(symbol_list))
                    if not quiet:
                        console.print(f"\n[bold]强制更新完成:[/bold]")
                        console.print(f"  更新记录: {count}")
                    else:
                        console.print(f"[bold]完成:[/bold] 更新 {count} 条记录")
                    return count

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
                                market=market,
                            )
                        elif data_type.startswith("minute"):
                            # 从 data_type 中提取频率
                            freq_map = {
                                "minute_1": "1m",
                                "minute_5": "5m",
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
                                market=market,
                            )
                        elif data_type == "daily_basic":
                            count = await updater.update_daily_basic(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                force_update=True,
                                market=market,
                            )
                        elif data_type == "adj_factor":
                            count = await updater.update_adj_factor(
                                symbols=[symbol],
                                start_date=start_date,
                                end_date=end_date,
                                force_update=True,
                                market=market,
                            )
                        elif data_type in ("basic", "asset_basic"):
                            # asset_basic 是非时间序列数据，使用强制全量更新
                            # 注意：asset_basic 不需要按 symbol 更新，只需要调用一次即可
                            count = await updater.update_stock_basic(market=market)

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
                        if single_symbol_adj_factor:
                            raise
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
    market: str,
    verbose: bool,
    quiet: bool = False,
):
    """交易日批量更新模式：使用Tushare的trade_date参数批量更新当日所有股票"""
    # 转换日期格式从 YYYY-MM-DD (CLI格式) 到 YYYYMMDD (Tushare API格式)
    trade_date_api = trade_date.replace("-", "")
    market_code = market.upper()

    if not quiet:
        console.print("[bold]交易日批量更新策略:[/bold]")
        console.print(f"  - 使用交易日: {trade_date} (API格式: {trade_date_api})")
        console.print(f"  - 市场: {market_code}")
        if data_type == "index_weight":
            console.print("  - 批量更新当日所有有效指数成分权重")
        else:
            console.print("  - 批量更新当日所有股票数据")
            console.print("  - CN 使用 Tushare 批量接口，HK 使用 XTQuant 逐股票接口")
        console.print("")

    try:
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
        from rich.console import Group
        from rich.panel import Panel
        from loguru import logger

        # 初始化更新器
        updater = DataUpdater(settings)
        await updater.initialize()

        if market_code in {"HK", "ALL"} and data_type in {"daily", "adj_factor"}:
            if data_type == "daily":
                with Progress(
                    get_spinner(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("正在获取港股日线数据...", total=None)
                    latest_total = None

                    def progress_callback(current, total):
                        nonlocal latest_total
                        latest_total = total
                        progress.update(task, completed=current, total=total)

                    count = await updater.update_daily_data(
                        trade_date=trade_date,
                        market=market_code,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    if latest_total is not None:
                        progress.update(task, completed=latest_total, total=latest_total)
            else:
                with Progress(
                    get_spinner(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TimeElapsedColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task("正在获取港股复权因子...", total=None)
                    latest_total = None

                    def progress_callback(current, total):
                        nonlocal latest_total
                        latest_total = total
                        progress.update(task, completed=current, total=total)

                    count = await updater.update_adj_factor(
                        trade_date=trade_date,
                        market=market_code,
                        force_update=True,
                        progress_callback=progress_callback,
                    )
                    if latest_total is not None:
                        progress.update(task, completed=latest_total, total=latest_total)
            if not quiet:
                console.print(f"[green][OK][/green] 已更新 {count} 条{data_type}数据")
            else:
                console.print(f"[green][OK][/green] 已更新 {count} 条数据")
            await updater.close()
            return count

        if data_type == "index_daily":
            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn("交易日"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数日线行情数据...", total=1)

                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                count = await updater.update_index_daily(
                    trade_date=trade_date,
                    force_update=True,
                    progress_callback=progress_callback,
                )
                progress.update(task, completed=1, total=1)

            if not quiet:
                console.print(f"[green][OK][/green] 已更新 {count} 条指数日线行情数据")
            else:
                console.print(f"[green][OK][/green] 已更新 {count} 条数据")
            await updater.close()
            return count

        if data_type == "index_weight":
            index_codes = await updater.resolve_index_weight_codes(active_date=trade_date)

            if not quiet:
                console.print(f"  - 将按本地 index_basic 更新 {len(index_codes)} 个有效指数")
                console.print("")

            with Progress(
                get_spinner(),
                TextColumn("[bold blue]{task.description}"),
                BarColumn(),
                SymbolCountColumn("指数"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("正在获取指数成分权重数据...", total=len(index_codes))

                def progress_callback(current, total):
                    progress.update(task, completed=current, total=total)

                count = await updater.update_index_weight(
                    index_list=index_codes,
                    trade_date=trade_date,
                    force_update=True,
                    progress_callback=progress_callback,
                )
                progress.update(task, completed=len(index_codes))

            if not quiet:
                console.print(f"[green][OK][/green] 已更新 {count} 条指数成分权重数据")
            else:
                console.print(f"[green][OK][/green] 已更新 {count} 条数据")
            await updater.close()
            return count

        with Progress(
            get_spinner(),
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
            elif data_type == "index_daily":
                method_name = "get_index_daily"
            elif data_type == "index_dailybasic":
                # 大盘指数每日指标数据
                method_name = "get_index_dailybasic"
            elif data_type == "sw_daily":
                # 申万行业日线行情数据
                method_name = "get_sw_daily"
            elif data_type == "adj_factor":
                # 复权因子数据
                method_name = "get_adj_factor"
            elif data_type == "index_weight":
                # 指数成分权重数据
                method_name = "get_index_weight"
            else:
                console.print(f"[bold red]不支持的数据类型: {data_type}[/bold red]")
                raise typer.Exit(1)

            # 通过路由器获取数据
            # 注意：指数类数据使用 asset_class="index"，其他使用 "stock"
            if data_type in ["index_daily", "index_dailybasic", "sw_daily", "index_weight"]:
                asset_class = "index"
                if data_type == "sw_daily":
                    router_data_type = "sw_daily"
                elif data_type == "index_daily":
                    router_data_type = "daily"
                elif data_type == "index_weight":
                    router_data_type = "index_weight"
                else:
                    router_data_type = "dailybasic"
            else:
                asset_class = "stock"
                router_data_type = data_type
            df = updater.router.route(
                asset_class=asset_class,
                data_type=router_data_type,
                method_name=method_name,
                trade_date=trade_date_api,
                market=market_code,
            )

            if df.empty:
                console.print(f"[yellow]指定交易日 {trade_date} 没有数据[/yellow]")
                return 0

            progress.update(task, description="正在插入数据库...", total=100)

            # 判断是股票还是指数数据
            is_index = (data_type in ["index_daily", "index_dailybasic", "sw_daily"])
            is_index_weight = (data_type == "index_weight")

            if is_index_weight:
                # 指数成分权重数据使用 index_code 列
                unique_codes = df["index_code"].unique()
                code_label = "指数成分"
            elif is_index:
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
                    elif data_type == "index_daily":
                        count = await updater.data_ops.insert_index_daily_batch(batch_df)
                    elif data_type == "index_dailybasic":
                        count = await updater.data_ops.insert_index_dailybasic_batch(batch_df)
                    elif data_type == "sw_daily":
                        count = await updater.data_ops.insert_sw_daily_batch(batch_df)
                    elif data_type == "adj_factor":
                        count = await updater.data_ops.insert_adj_factor_batch(batch_df)
                    elif data_type == "index_weight":
                        count = await updater.data_ops.insert_index_weight_batch(batch_df)
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
                        if is_index_weight:
                            current_code = batch_df["index_code"].iloc[0] if len(batch_df) > 0 else "unknown"
                        elif is_index:
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
        table.add_row("  读查询并发", str(settings.database.query_max_concurrency))
        table.add_row("  重查询并发", str(settings.database.heavy_query_max_concurrency))

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
        table.add_row(
            "  期货分钟下载并发",
            str(settings.data_source.futures_minute_max_workers),
        )

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
        help="要刷新的连续聚合表名 (symbol_weekly, symbol_monthly, daily_basic_weekly, daily_basic_monthly, adj_factor_weekly, adj_factor_monthly, futures.minute_15m, futures.minute_30m, futures.minute_60m)"
    ),
    start_date: Optional[str] = typer.Option(
        None,
        "--start-date",
        help="刷新开始日期 (YYYY-MM-DD)，默认为空（刷新所有历史数据）"
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
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
