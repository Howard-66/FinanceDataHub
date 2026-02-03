"""
调度器 CLI 命令

提供 fdh-cli schedule 子命令：
- start: 启动调度器
- stop: 停止调度器
- status: 显示调度器状态
- run: 立即执行指定任务
- pause: 暂停指定任务
- resume: 恢复指定任务
- history: 查看任务执行历史
- list: 列出所有任务
"""

from typing import Optional
import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from pathlib import Path
import os
import signal
import sys

from ..scheduler import ScheduleManager
from ..config import get_settings

console = Console()

# 创建子应用
schedule_app = typer.Typer(
    name="schedule",
    help="调度器管理命令",
    rich_markup_mode="rich"
)

# 全局调度管理器（用于停止命令）
_manager: Optional[ScheduleManager] = None
_pid_file = Path("/tmp/fdh_scheduler.pid")


def _get_config_path() -> str:
    """获取调度配置文件路径"""
    # 优先使用环境变量
    config_path = os.environ.get("FDH_SCHEDULES_PATH")
    if config_path and Path(config_path).exists():
        return config_path
    
    # 默认路径
    default_paths = [
        Path.cwd() / "schedules.yml",
        Path(__file__).parent.parent.parent / "schedules.yml",
    ]
    
    for path in default_paths:
        if path.exists():
            return str(path)
    
    return "schedules.yml"


def _get_database_url() -> Optional[str]:
    """获取数据库连接 URL"""
    try:
        settings = get_settings()
        return settings.database.url
    except Exception:
        return None


def _write_pid() -> None:
    """写入 PID 文件"""
    with open(_pid_file, "w") as f:
        f.write(str(os.getpid()))


def _read_pid() -> Optional[int]:
    """读取 PID 文件"""
    try:
        if _pid_file.exists():
            with open(_pid_file) as f:
                return int(f.read().strip())
    except Exception:
        pass
    return None


def _remove_pid() -> None:
    """删除 PID 文件"""
    try:
        if _pid_file.exists():
            _pid_file.unlink()
    except Exception:
        pass


@schedule_app.command("start")
def start(
    daemon: bool = typer.Option(
        False,
        "--daemon",
        "-d",
        help="以守护进程模式运行"
    ),
    config: Optional[str] = typer.Option(
        None,
        "--config",
        "-c",
        help="调度配置文件路径"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细日志"
    ),
):
    """
    启动调度器

    启动后台调度器，按配置执行定时任务。
    使用 --daemon 以守护进程模式运行。
    """
    global _manager
    
    config_path = config or _get_config_path()
    
    console.print("[bold blue]启动调度器[/bold blue]\n")
    console.print(f"配置文件: {config_path}")
    
    if not Path(config_path).exists():
        console.print(f"[bold red]错误:[/bold red] 配置文件不存在: {config_path}")
        raise typer.Exit(1)
    
    # 检查是否已有调度器运行
    existing_pid = _read_pid()
    if existing_pid:
        try:
            os.kill(existing_pid, 0)
            console.print(f"[yellow]调度器已在运行 (PID: {existing_pid})[/yellow]")
            raise typer.Exit(1)
        except OSError:
            # 进程不存在，清理 PID 文件
            _remove_pid()
    
    try:
        _manager = ScheduleManager(
            config_path=config_path,
            database_url=_get_database_url(),
            project_root=str(Path.cwd())
        )
        
        # 加载配置
        config_obj = _manager.load_config()
        console.print(f"已加载 {len(config_obj.jobs)} 个任务\n")
        
        # 显示任务列表
        enabled_count = sum(1 for j in config_obj.jobs.values() if j.enabled)
        console.print(f"启用任务: {enabled_count}/{len(config_obj.jobs)}")
        
        if verbose:
            for job_id, job_config in config_obj.jobs.items():
                status = "[green]启用[/green]" if job_config.enabled else "[red]禁用[/red]"
                console.print(f"  - {job_id}: {status}")
        
        # 注册信号处理
        def signal_handler(signum, frame):
            console.print("\n[yellow]收到停止信号，正在关闭调度器...[/yellow]")
            if _manager:
                _manager.stop()
            _remove_pid()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # 写入 PID 文件
        _write_pid()
        
        console.print("\n[bold green]调度器启动成功[/bold green]")
        
        if daemon:
            console.print("[dim]守护进程模式，按 Ctrl+C 停止[/dim]")
        else:
            console.print("[dim]前台模式，按 Ctrl+C 停止[/dim]")
        
        # 启动调度器
        _manager.start(daemon=daemon)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]调度器已停止[/yellow]")
        _remove_pid()
    except Exception as e:
        console.print(f"[bold red]启动失败:[/bold red] {e}")
        _remove_pid()
        raise typer.Exit(1)


@schedule_app.command("stop")
def stop(
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="强制停止"
    ),
):
    """
    停止调度器

    停止正在运行的调度器进程。
    """
    console.print("[bold blue]停止调度器[/bold blue]\n")
    
    pid = _read_pid()
    
    if not pid:
        console.print("[yellow]调度器未在运行[/yellow]")
        return
    
    try:
        # 检查进程是否存在
        os.kill(pid, 0)
        
        # 发送停止信号
        sig = signal.SIGKILL if force else signal.SIGTERM
        os.kill(pid, sig)
        
        console.print(f"[green]已发送停止信号到进程 {pid}[/green]")
        _remove_pid()
        
    except OSError:
        console.print(f"[yellow]进程 {pid} 不存在，清理 PID 文件[/yellow]")
        _remove_pid()


@schedule_app.command("status")
def status(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细信息"
    ),
):
    """
    显示调度器状态

    显示调度器运行状态和任务概览。
    """
    console.print("[bold cyan]调度器状态[/bold cyan]\n")
    
    # 检查调度器进程
    pid = _read_pid()
    if pid:
        try:
            os.kill(pid, 0)
            console.print(f"[green]● 调度器运行中[/green] (PID: {pid})")
        except OSError:
            console.print("[red]● 调度器未运行[/red] (PID 文件已过期)")
            _remove_pid()
    else:
        console.print("[yellow]● 调度器未运行[/yellow]")
    
    # 加载配置显示任务信息
    config_path = _get_config_path()
    if Path(config_path).exists():
        try:
            manager = ScheduleManager(config_path=config_path)
            config_obj = manager.load_config()
            
            console.print(f"\n[bold]配置文件:[/bold] {config_path}")
            console.print(f"[bold]时区:[/bold] {config_obj.scheduler.timezone}")
            console.print(f"[bold]最大并发:[/bold] {config_obj.scheduler.max_concurrent_jobs}")
            
            # 任务统计
            total_jobs = len(config_obj.jobs)
            enabled_jobs = sum(1 for j in config_obj.jobs.values() if j.enabled)
            download_jobs = sum(
                1 for j in config_obj.jobs.values() 
                if j.enabled and j.type.value == "download"
            )
            preprocess_jobs = sum(
                1 for j in config_obj.jobs.values() 
                if j.enabled and j.type.value == "preprocess"
            )
            
            console.print(f"\n[bold]任务统计:[/bold]")
            console.print(f"  总任务: {total_jobs}")
            console.print(f"  启用: {enabled_jobs}")
            console.print(f"  下载任务: {download_jobs}")
            console.print(f"  预处理任务: {preprocess_jobs}")
            
            if verbose:
                console.print("\n[bold]任务列表:[/bold]")
                for job_id, job_config in config_obj.jobs.items():
                    status_icon = "✓" if job_config.enabled else "✗"
                    status_color = "green" if job_config.enabled else "red"
                    deps = f" (依赖: {', '.join(job_config.depends_on)})" if job_config.depends_on else ""
                    console.print(f"  [{status_color}]{status_icon}[/{status_color}] {job_id}: {job_config.type.value}{deps}")
                    
        except Exception as e:
            console.print(f"[red]加载配置失败: {e}[/red]")


@schedule_app.command("run")
def run_job(
    job: str = typer.Option(
        ...,
        "--job",
        "-j",
        help="要执行的任务 ID"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细日志"
    ),
):
    """
    立即执行指定任务

    手动触发任务立即执行，不影响调度计划。
    """
    console.print(f"[bold blue]执行任务: {job}[/bold blue]\n")
    
    config_path = _get_config_path()
    
    try:
        manager = ScheduleManager(
            config_path=config_path,
            database_url=_get_database_url(),
            project_root=str(Path.cwd())
        )
        
        log = manager.run_job(job)
        
        if log is None:
            console.print(f"[red]任务不存在: {job}[/red]")
            raise typer.Exit(1)
        
        # 显示执行结果
        if log.status == "completed":
            console.print(f"[green]✓ 任务执行成功[/green]")
            if log.records_processed:
                console.print(f"  处理记录: {log.records_processed}")
            if log.symbols_count:
                console.print(f"  处理股票: {log.symbols_count}")
        elif log.status == "failed":
            console.print(f"[red]✗ 任务执行失败[/red]")
            if log.error_message:
                console.print(f"  错误: {log.error_message}")
        else:
            console.print(f"[yellow]任务状态: {log.status}[/yellow]")
        
        duration = (log.end_time - log.start_time).total_seconds() if log.end_time else 0
        console.print(f"  耗时: {duration:.2f}秒")
        
    except Exception as e:
        console.print(f"[bold red]执行失败:[/bold red] {e}")
        raise typer.Exit(1)


@schedule_app.command("list")
def list_jobs(
    enabled_only: bool = typer.Option(
        False,
        "--enabled",
        "-e",
        help="只显示启用的任务"
    ),
    job_type: Optional[str] = typer.Option(
        None,
        "--type",
        "-t",
        help="按类型过滤 (download, preprocess)"
    ),
):
    """
    列出所有任务

    显示配置中的所有调度任务。
    """
    console.print("[bold cyan]调度任务列表[/bold cyan]\n")
    
    config_path = _get_config_path()
    
    try:
        manager = ScheduleManager(config_path=config_path)
        config_obj = manager.load_config()
        
        # 创建表格
        table = Table(title="任务列表")
        table.add_column("任务 ID", style="cyan", no_wrap=True)
        table.add_column("类型", style="magenta")
        table.add_column("状态", style="green")
        table.add_column("数据集/类别")
        table.add_column("调度规则")
        table.add_column("依赖")
        
        for job_id, job_config in config_obj.jobs.items():
            # 过滤
            if enabled_only and not job_config.enabled:
                continue
            if job_type and job_config.type.value != job_type:
                continue
            
            status = "[green]启用[/green]" if job_config.enabled else "[red]禁用[/red]"
            
            # 数据集或类别
            if job_config.type.value == "download":
                dataset = job_config.dataset
                if isinstance(dataset, list):
                    dataset = ", ".join(dataset[:2])
                    if len(job_config.get_datasets()) > 2:
                        dataset += "..."
                dataset_str = dataset or "-"
            else:
                dataset_str = job_config.category or "-"
            
            # 调度规则
            schedule = job_config.schedule
            schedule_type = schedule.get("type", "cron")
            if schedule_type == "cron":
                hour = schedule.get("hour", "*")
                minute = schedule.get("minute", 0)
                day = schedule.get("day", "*")
                day_of_week = schedule.get("day_of_week", "*")
                schedule_str = f"{day_of_week} {day} {hour}:{minute:02d}" if isinstance(minute, int) else f"{day_of_week} {day} {hour}:{minute}"
            else:
                schedule_str = schedule_type
            
            # 依赖
            deps = ", ".join(job_config.depends_on) if job_config.depends_on else "-"
            
            table.add_row(
                job_id,
                job_config.type.value,
                status,
                dataset_str,
                schedule_str,
                deps
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]加载配置失败:[/bold red] {e}")
        raise typer.Exit(1)


@schedule_app.command("history")
def history(
    job: Optional[str] = typer.Option(
        None,
        "--job",
        "-j",
        help="指定任务 ID"
    ),
    limit: int = typer.Option(
        10,
        "--limit",
        "-n",
        help="显示条数"
    ),
):
    """
    查看任务执行历史

    显示任务的最近执行记录。
    """
    console.print("[bold cyan]任务执行历史[/bold cyan]\n")
    
    # 注意：这里需要从数据库读取历史记录
    # 当前实现只能显示内存中的记录
    console.print("[yellow]提示: 执行历史需要调度器运行后才能记录[/yellow]")
    console.print("[dim]历史记录存储在 preprocess_execution_log 表中[/dim]\n")
    
    # TODO: 实现从数据库读取历史记录
    console.print(f"查询参数: job={job}, limit={limit}")


@schedule_app.command("pause")
def pause_job(
    job: str = typer.Option(
        ...,
        "--job",
        "-j",
        help="要暂停的任务 ID"
    ),
):
    """
    暂停指定任务

    暂停任务的定时执行，不影响其他任务。
    """
    console.print(f"[bold yellow]暂停任务: {job}[/bold yellow]")
    
    # 需要调度器运行中才能暂停
    pid = _read_pid()
    if not pid:
        console.print("[red]调度器未运行，无法暂停任务[/red]")
        raise typer.Exit(1)
    
    console.print("[yellow]提示: 暂停功能需要通过 API 与运行中的调度器通信[/yellow]")
    console.print("[dim]当前版本请手动修改 schedules.yml 并重启调度器[/dim]")


@schedule_app.command("resume")
def resume_job(
    job: str = typer.Option(
        ...,
        "--job",
        "-j",
        help="要恢复的任务 ID"
    ),
):
    """
    恢复指定任务

    恢复已暂停任务的定时执行。
    """
    console.print(f"[bold green]恢复任务: {job}[/bold green]")
    
    # 需要调度器运行中才能恢复
    pid = _read_pid()
    if not pid:
        console.print("[red]调度器未运行，无法恢复任务[/red]")
        raise typer.Exit(1)
    
    console.print("[yellow]提示: 恢复功能需要通过 API 与运行中的调度器通信[/yellow]")
    console.print("[dim]当前版本请手动修改 schedules.yml 并重启调度器[/dim]")


@schedule_app.command("init")
def init_schedule(
    full: bool = typer.Option(
        False,
        "--full",
        help="执行全量数据初始化"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="显示详细日志"
    ),
):
    """
    初始化调度系统

    首次运行时初始化全量数据。
    """
    console.print("[bold blue]初始化调度系统[/bold blue]\n")
    
    if full:
        console.print("[yellow]全量数据初始化可能需要较长时间...[/yellow]\n")
        console.print("建议使用以下命令手动初始化各数据集:\n")
        console.print("  fdh-cli update --dataset daily --force")
        console.print("  fdh-cli update --dataset daily_basic --force")
        console.print("  fdh-cli update --dataset adj_factor --force")
        console.print("  fdh-cli update --dataset fina_indicator --force")
        console.print("  # ... 其他数据集\n")
        console.print("数据初始化完成后，启动调度器进行增量更新:")
        console.print("  fdh-cli schedule start --daemon")
    else:
        console.print("使用 --full 参数执行全量数据初始化")
        console.print("或直接启动调度器进行增量更新:\n")
        console.print("  fdh-cli schedule start --daemon")
