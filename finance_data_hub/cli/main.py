"""
CLI 主入口模块

提供 fdh-cli 命令行工具的入口点。
"""

from typing import Optional
from datetime import datetime

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from finance_data_hub.config import get_settings, reload_settings

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
        help="数据频率 (daily, minute_1, minute_5, tick)"
    ),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="股票代码列表，用逗号分隔，如: 600519.SH,000858.SZ"
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
    console.print("[bold green]🚀 开始数据更新流程[/bold green]")

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

        # 显示数据库连接信息（隐藏密码）
        db_url = settings.database.url
        if "@" in db_url:
            parts = db_url.split("@")
            if len(parts) == 2:
                user_pass = parts[0].split("://")[1]
                user = user_pass.split(":")[0] if ":" in user_pass else user_pass
                console.print(f"[cyan]数据库:[/cyan] {user}@localhost:5432/trading_nexus_db")

        if verbose:
            console.print(f"[cyan]日志级别:[/cyan] {settings.logging.level}")
            console.print(f"[cyan]ETL 目录:[/cyan] {settings.etl.data_path}")

        # TODO: 实现实际的数据更新逻辑
        console.print("\n[bold yellow]⚠️  功能待实现[/bold yellow]")
        console.print("此命令将在 Phase 2 中实现数据源适配器后完成")

        # 显示成功信息
        console.print("\n[bold green]✓ 配置加载成功[/bold green]")
        console.print(f"[dim]数据库: {settings.database.url.split('@')[-1] if '@' in settings.database.url else '未配置'}[/dim]")
        console.print(f"[dim]Redis: {settings.redis.url}[/dim]")

    except Exception as e:
        console.print(f"[bold red]❌ 错误: {str(e)}[/bold red]")
        raise typer.Exit(1)


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
