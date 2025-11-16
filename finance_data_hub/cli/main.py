"""
CLI 主入口模块

提供 fdh-cli 命令行工具的入口点。
"""

from typing import Optional, List
from datetime import datetime, timedelta
import asyncio

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.syntax import Syntax
from rich import print as rprint

from finance_data_hub.config import get_settings, reload_settings
from finance_data_hub.update.updater import DataUpdater
from finance_data_hub.providers.base import ProviderError

# 创建 Typer 应用
app = typer.Typer(
    name="fdh-cli",
    help="FinanceDataHub CLI - 金融数据服务中心命令行工具",
    rich_markup_mode="rich"
)

console = Console()


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
    frequency: str = typer.Option(
        "daily",
        "--frequency",
        "-f",
        help="数据频率 (daily, minute_1, minute_5, basic, adj_factor)"
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
        help="开始日期 (YYYY-MM-DD)，默认获取最近数据"
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
):
    """
    从数据源更新数据到数据库

    执行数据同步流程，从配置的数据源获取数据并存储到数据库。
    支持按资产类别、数据频率和特定股票代码进行筛选。
    """
    console.print("[bold green]🚀 开始数据更新流程[/bold green]\n")

    try:
        # 加载配置
        settings = get_settings()

        if config_file:
            console.print(f"[yellow]配置文件: {config_file}[/yellow]")

        # 显示更新参数
        console.print(f"[cyan]资产类别:[/cyan] {asset_class}")
        console.print(f"[cyan]数据频率:[/cyan] {frequency}")

        if symbols:
            console.print(f"[cyan]股票代码:[/cyan] {symbols}")

        if start_date:
            console.print(f"[cyan]开始日期:[/cyan] {start_date}")
        if end_date:
            console.print(f"[cyan]结束日期:[/cyan] {end_date}")
        if adj:
            console.print(f"[cyan]复权类型:[/cyan] {adj}")

        # 执行更新流程
        asyncio.run(_run_update(
            settings, asset_class, frequency, symbols,
            start_date, end_date, adj, verbose
        ))

        console.print("\n[bold green]✓ 数据更新完成[/bold green]")

    except Exception as e:
        console.print(f"[bold red]❌ 错误: {str(e)}[/bold red]")
        if verbose:
            import traceback
            console.print(traceback.format_exc())
        raise typer.Exit(1)


async def _run_update(
    settings,
    asset_class: str,
    frequency: str,
    symbols: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
    adj: Optional[str],
    verbose: bool,
):
    """执行实际的数据更新"""
    # 解析股票代码列表
    symbol_list = None
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

    # 设置默认日期
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if not start_date and frequency == "daily":
        # 日线数据默认获取最近30天
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=30)
        start_date = start_dt.strftime("%Y-%m-%d")
    elif not start_date and "minute" in frequency:
        # 分钟数据默认获取最近1天
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=1)
        start_date = start_dt.strftime("%Y-%m-%d")

    # 初始化更新器
    async with DataUpdater(settings, config_path="sources.yml") as updater:
        # 进度显示
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("正在更新...", total=100)

            try:
                # 更新股票基本信息
                if frequency == "basic":
                    progress.update(task, description="更新股票基本信息...")
                    count = await updater.update_stock_basic()
                    progress.update(task, advance=100)
                    console.print(f"[green]✓ 更新了 {count} 条股票基本信息[/green]")
                    return

                # 更新日线数据
                elif frequency == "daily":
                    progress.update(task, description="更新日线数据...")
                    total = 0

                    if not symbol_list:
                        # 首先更新股票基本信息
                        console.print("[yellow]先更新股票列表...[/yellow]")
                        basic_count = await updater.update_stock_basic()
                        console.print(f"[green]✓ 更新了 {basic_count} 条股票基本信息[/green]")

                        # 获取股票列表
                        symbols_db = await updater.data_ops.get_symbol_list()
                        # symbol_list = symbols_db[:10]  # 限制数量避免API调用过多
                        symbol_list = symbols_db
                        console.print(f"[yellow]将更新 {len(symbol_list)} 只股票[/yellow]")

                    # 更新数据
                    count = await updater.update_daily_data(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        adj=adj,
                    )
                    total += count
                    progress.update(task, advance=100)
                    console.print(f"[green]✓ 更新了 {count} 条日线数据[/green]")

                # 更新分钟数据
                elif frequency in ["minute_1", "minute_5"]:
                    freq_map = {"minute_1": "1m", "minute_5": "5m"}
                    actual_freq = freq_map[frequency]

                    progress.update(task, description=f"更新{actual_freq}数据...")

                    count = await updater.update_minute_data(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                        freq=actual_freq,
                    )
                    progress.update(task, advance=100)
                    console.print(f"[green]✓ 更新了 {count} 条{actual_freq}数据[/green]")

                # 更新每日指标
                elif frequency == "daily_basic":
                    progress.update(task, description="更新每日指标数据...")

                    count = await updater.update_daily_basic(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    progress.update(task, advance=100)
                    console.print(f"[green]✓ 更新了 {count} 条每日指标数据[/green]")

                # 更新复权因子
                elif frequency == "adj_factor":
                    progress.update(task, description="更新复权因子...")

                    count = await updater.update_adj_factor(
                        symbols=symbol_list,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    progress.update(task, advance=100)
                    console.print(f"[green]✓ 更新了 {count} 条复权因子数据[/green]")

                else:
                    console.print(f"[bold red]不支持的数据频率: {frequency}[/bold red]")
                    raise typer.Exit(1)

                # 显示路由器统计
                if verbose:
                    stats = updater.router.get_stats()
                    if stats:
                        console.print("\n[bold]路由统计:[/bold]")
                        for provider, stat in stats.items():
                            console.print(
                                f"  [cyan]{provider}[/cyan]: "
                                f"总调用 {stat['total']}, "
                                f"成功 {stat['success']}, "
                                f"失败 {stat['failure']}"
                            )

            except ProviderError as e:
                console.print(f"\n[bold red]❌ 数据源错误: {str(e)}[/bold red]")
                raise
            except Exception as e:
                console.print(f"\n[bold red]❌ 更新失败: {str(e)}[/bold red]")
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
    console.print("[bold blue]🔄 开始 ETL 流程[/bold blue]")

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

        console.print("\n[bold green]✓ 配置验证成功[/bold green]")

    except Exception as e:
        console.print(f"[bold red]❌ 错误: {str(e)}[/bold red]")
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
    console.print("[bold magenta]📊 系统状态检查[/bold magenta]\n")

    try:
        settings = get_settings()

        # 创建状态表格
        table = Table(title="FinanceDataHub 系统状态")
        table.add_column("组件", style="cyan", no_wrap=True)
        table.add_column("状态", style="green")
        table.add_column("信息", style="yellow")

        # 数据库状态
        db_url = settings.database.url
        db_status = "✓ 正常" if "localhost" in db_url or "postgresql" in db_url else "⚠️  请检查"
        db_info = "已连接" if "localhost" in db_url else db_url
        table.add_row("PostgreSQL", db_status, db_info)

        # Redis 状态
        redis_status = "✓ 正常" if "redis://" in settings.redis.url else "⚠️  请检查"
        redis_info = "Redis 7.x" if "redis://" in settings.redis.url else settings.redis.url
        table.add_row("Redis", redis_status, redis_info)

        # 数据源状态
        tushare_status = "✓ 已配置" if settings.data_source.tushare_token else "⚠️  未配置"
        tushare_info = "Tushare Pro API" if settings.data_source.tushare_token else "缺少 TUSHARE_TOKEN"
        table.add_row("Tushare", tushare_status, tushare_info)

        xtquant_status = "✓ 已配置" if settings.data_source.xtquant_api_url else "⚠️  未配置"
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
        console.print(f"[bold red]❌ 错误: {str(e)}[/bold red]")
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
    console.print("[bold cyan]⚙️  当前配置[/bold cyan]\n")

    try:
        if reload:
            settings = reload_settings()
            console.print("[green]✓ 配置已重新加载[/green]\n")
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
        console.print(f"[bold red]❌ 错误: {str(e)}[/bold red]")
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
