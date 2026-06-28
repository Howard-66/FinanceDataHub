"""调度管理器

提供高级调度管理功能：
- 加载配置并初始化调度器
- 任务依赖管理
- 任务状态监控
- CLI 命令集成
"""

import os
import sys
import traceback
import json
from typing import Optional, Dict, List, Any, Set
from datetime import datetime, date, time, timedelta
from pathlib import Path
from loguru import logger

from .models import ScheduleConfig, JobConfig, JobExecutionLog, JobType
from .engine import SchedulerEngine
from .executor import TaskExecutor, RetryExecutor


# 配置日志输出到文件
def _setup_file_logging():
    """设置日志文件输出"""
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "scheduler.log"
    
    # 移除默认的 stderr handler，添加文件 handler
    logger.add(
        str(log_file),
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # 线程安全
    )
    return log_file

# 在模块加载时设置日志
_log_file = _setup_file_logging()


# 全局任务注册表，用于 APScheduler 序列化任务时查找
# 结构: {job_id: (manager_ref, job_config)}
_job_registry: Dict[str, tuple] = {}


def _job_dispatcher(dispatcher_job_id: str, **kwargs) -> JobExecutionLog:
    """
    任务调度器的全局入口函数
    
    APScheduler 使用 SQLAlchemy jobstore 时需要序列化任务函数。
    局部闭包无法序列化，因此使用此全局函数作为入口，
    通过 job_id 查找对应的 manager 和 config 来执行任务。
    
    Args:
        dispatcher_job_id: 任务 ID
        **kwargs: APScheduler 传递的额外参数（如 trade_date）
        
    Returns:
        任务执行日志
    """
    try:
        if dispatcher_job_id not in _job_registry:
            error_msg = f"Job {dispatcher_job_id} not found in registry. Registry contains: {list(_job_registry.keys())}"
            logger.error(f"[Scheduler] {error_msg}")
            raise ValueError(error_msg)
        
        manager_ref, job_config = _job_registry[dispatcher_job_id]
        
        return manager_ref._execute_job(dispatcher_job_id, job_config, kwargs)
        
    except Exception as e:
        error_msg = f"任务执行失败: {dispatcher_job_id}\n{traceback.format_exc()}"
        logger.error(f"[Scheduler] {error_msg}")
        raise


class ScheduleManager:
    """调度管理器"""
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        database_url: Optional[str] = None,
        project_root: Optional[str] = None
    ):
        """
        初始化调度管理器
        
        Args:
            config_path: 调度配置文件路径
            database_url: 数据库连接 URL
            project_root: 项目根目录
        """
        self.config_path = config_path or "schedules.yml"
        self.database_url = database_url
        self.project_root = project_root
        
        self._config: Optional[ScheduleConfig] = None
        self._engine: Optional[SchedulerEngine] = None
        self._executor: Optional[RetryExecutor] = None
        self._execution_logs: List[JobExecutionLog] = []
        self._job_dependencies: Dict[str, Set[str]] = {}
        self._pending_jobs: Dict[str, Dict[str, Any]] = {}
        
    def load_config(self) -> ScheduleConfig:
        """加载调度配置"""
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Schedule config not found: {self.config_path}")
        
        self._config = ScheduleConfig.from_yaml(str(config_file))
        logger.info(f"Loaded schedule config with {len(self._config.jobs)} jobs")
        
        # 构建依赖关系图
        self._build_dependency_graph()
        
        return self._config
    
    def _build_dependency_graph(self) -> None:
        """构建任务依赖关系图"""
        self._job_dependencies.clear()
        
        for job_id, job_config in self._config.jobs.items():
            self._job_dependencies[job_id] = set(job_config.depends_on)
            
        logger.debug(f"Built dependency graph: {self._job_dependencies}")
    
    def initialize(self) -> None:
        """初始化调度管理器"""
        if self._config is None:
            self.load_config()
        
        # 创建调度引擎
        self._engine = SchedulerEngine(
            config=self._config.scheduler,
            database_url=self.database_url
        )
        self._engine.initialize()
        
        # 创建任务执行器
        base_executor = TaskExecutor(project_root=self.project_root)
        self._executor = RetryExecutor(base_executor)
        
        # 注册所有任务
        self._register_jobs()
        
        logger.info("Schedule manager initialized")
    
    def _register_jobs(self) -> None:
        """注册所有任务"""
        _job_registry.clear()
        enabled_job_ids: Set[str] = set()
        for job_id, job_config in self._config.jobs.items():
            if not job_config.enabled:
                logger.debug(f"Skipping disabled job: {job_id}")
                continue
            enabled_job_ids.add(job_id)
            
            # 将任务注册到全局注册表（供序列化后的任务查找）
            _job_registry[job_id] = (self, job_config)
            
            # 使用模块级别的 _job_dispatcher 函数，避免序列化问题
            # APScheduler 会将 dispatcher_job_id 作为 kwargs 传递
            self._engine.add_job(
                job_id=job_id,
                func=_job_dispatcher,
                job_config=job_config,
                dispatcher_job_id=job_id  # 作为 kwargs 传递给 _job_dispatcher
            )
            
            # 注册任务监听器
            self._engine.register_listener(job_id, self._on_job_event)

        self._remove_stale_persisted_jobs(enabled_job_ids)

    def _remove_stale_persisted_jobs(self, enabled_job_ids: Set[str]) -> None:
        """Remove obsolete APScheduler jobs left in persistent job stores.

        PostgreSQL job stores keep old cron/date jobs across config changes.
        Without reconciliation, removed jobs still fire and fail before the
        in-memory registry can find them. Catch-up/pending one-time jobs are
        intentionally transient, so they are dropped on every scheduler start.
        """
        if self._engine is None:
            return

        for job in list(self._engine.get_jobs()):
            job_id = getattr(job, "id", "")
            if job_id in enabled_job_ids:
                continue
            if job_id.startswith(("catchup:", "pending:")):
                logger.warning(f"Removing transient persisted scheduler job: {job_id}")
            else:
                logger.warning(f"Removing stale scheduler job not in config: {job_id}")
            self._engine.remove_job(job_id)
    
    def _execute_job(
        self,
        job_id: str,
        job_config: JobConfig,
        execution_params: Optional[Dict[str, Any]] = None,
    ) -> JobExecutionLog:
        """执行任务（带依赖检查）"""
        execution_params = dict(execution_params or {})
        scheduled_date = self._extract_scheduled_date(execution_params)
        is_catchup_run = bool(execution_params.pop("_is_catchup_run", False))
        execution_params.pop("_is_pending_run", None)
        execution_params.pop("dispatcher_job_id", None)

        resolved_params = self._resolve_execution_params(job_config, execution_params)
        run_context = {
            "scheduled_date": scheduled_date.isoformat(),
            "catchup": is_catchup_run,
        }

        # 检查依赖
        if not self._check_dependencies(job_id, scheduled_date):
            logger.warning(
                f"Job {job_id} has unmet dependencies for {scheduled_date}, adding to pending"
            )
            pending_key = f"{job_id}:{scheduled_date.isoformat()}"
            self._pending_jobs[pending_key] = {
                "job_id": job_id,
                "params": resolved_params,
                "scheduled_date": scheduled_date.isoformat(),
            }
            log = JobExecutionLog(
                job_id=job_id,
                job_name=job_id,
                job_type=job_config.type,
                status="pending",
                start_time=datetime.now(),
                error_message="Waiting for dependencies",
                config={"params": resolved_params, "scheduler": run_context},
            )
            self._save_execution_log(log)
            return log
        
        logger.info(
            f"[Scheduler] 开始执行任务: {job_id}, scheduled_date={scheduled_date}"
        )
        
        # 执行任务
        log = self._executor.execute_with_retry(job_id, job_config, **resolved_params)
        log.config = {"params": resolved_params, "scheduler": run_context}
        self._execution_logs.append(log)
        
        # 保存执行日志到数据库
        self._save_execution_log(log)
        
        logger.info(f"[Scheduler] 任务 {job_id} 执行完成，状态: {log.status}")
        
        # 如果成功，检查是否有等待此任务的其他任务
        if log.status == "completed":
            self._trigger_dependent_jobs(job_id, scheduled_date)
        elif (
            log.status == "failed"
            and not is_catchup_run
            and job_config.catchup_on_failure
        ):
            self._schedule_next_day_catchup(
                job_id=job_id,
                job_config=job_config,
                resolved_params=resolved_params,
                scheduled_date=scheduled_date,
            )
        
        return log

    def _extract_scheduled_date(self, execution_params: Dict[str, Any]) -> date:
        raw_value = execution_params.pop("_scheduled_date", None)
        if isinstance(raw_value, date):
            return raw_value
        if raw_value:
            return datetime.strptime(str(raw_value)[:10], "%Y-%m-%d").date()
        return datetime.now().date()

    def _resolve_execution_params(
        self,
        job_config: JobConfig,
        execution_params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """将调度占位参数解析成当前执行使用的具体参数。"""
        base_params = {**job_config.params, **execution_params}
        if self._executor is None:
            return base_params
        return self._executor.executor.resolve_params(job_config, base_params)

    def _schedule_next_day_catchup(
        self,
        job_id: str,
        job_config: JobConfig,
        resolved_params: Dict[str, Any],
        scheduled_date: date,
    ) -> None:
        """安排 T+1 凌晨补跑，补跑时使用 T 日已解析参数。"""
        if self._engine is None:
            return

        offset_minutes = 30 + self._catchup_stagger_minutes(job_id)
        run_date = datetime.combine(
            scheduled_date + timedelta(days=1),
            time(hour=0, minute=0),
        ) + timedelta(minutes=offset_minutes)

        now = datetime.now()
        if run_date <= now:
            logger.warning(
                f"[Scheduler] 跳过过期补跑: job={job_id}, "
                f"scheduled_date={scheduled_date}, original_run_date={run_date}, now={now}"
            )
            return

        catchup_job_id = f"catchup:{job_id}:{scheduled_date.isoformat()}"
        kwargs = {
            "dispatcher_job_id": job_id,
            **resolved_params,
            "_scheduled_date": scheduled_date.isoformat(),
            "_is_catchup_run": True,
        }

        _job_registry[job_id] = (self, job_config)
        self._engine.add_one_time_job(
            job_id=catchup_job_id,
            func=_job_dispatcher,
            run_date=run_date,
            kwargs=kwargs,
            name=f"{job_id} catchup {scheduled_date.isoformat()}",
        )
        logger.warning(
            f"[Scheduler] 任务 {job_id} 多次重试失败，已安排补跑: "
            f"run_date={run_date}, scheduled_date={scheduled_date}, params={resolved_params}"
        )

    def _catchup_stagger_minutes(self, job_id: str) -> int:
        """按配置顺序错峰补跑，避免凌晨所有失败任务同时启动。"""
        if self._config is None:
            return 0
        try:
            job_index = list(self._config.jobs.keys()).index(job_id)
        except ValueError:
            job_index = 0
        return (job_index * 3) % 90
    
    def _save_execution_log(self, log: JobExecutionLog) -> None:
        """将执行日志保存到数据库"""
        if not self.database_url:
            logger.debug("No database URL configured, skipping log save")
            return
        
        try:
            from sqlalchemy import create_engine, text
            engine = create_engine(self._sync_database_url())
            
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO preprocess_execution_log 
                        (job_id, job_type, status, started_at, ended_at, 
                         symbols_count, records_processed, error_message, parameters)
                        VALUES (:job_id, :job_type, :status, :started_at, :ended_at,
                                :symbols_count, :records_processed, :error_message,
                                CAST(:parameters AS JSONB))
                    """),
                    {
                        "job_id": log.job_id,
                        "job_type": log.job_type.value,
                        "status": log.status,
                        "started_at": log.start_time,
                        "ended_at": log.end_time,
                        "symbols_count": log.symbols_count,
                        "records_processed": log.records_processed,
                        "error_message": log.error_message,
                        "parameters": json.dumps(log.config or {}, default=str),
                    }
                )
                conn.commit()
                logger.debug(f"Saved execution log for job {log.job_id}")
        except Exception as e:
            logger.error(f"Failed to save execution log: {e}")

    def _sync_database_url(self) -> str:
        """Return a SQLAlchemy sync URL for scheduler bookkeeping queries."""
        if not self.database_url:
            return ""
        return self.database_url.replace(
            "postgresql+asyncpg://",
            "postgresql://",
            1,
        )
    
    def _check_dependencies(
        self,
        job_id: str,
        scheduled_date: Optional[date] = None,
    ) -> bool:
        """检查任务依赖是否满足"""
        dependencies = self._job_dependencies.get(job_id, set())
        
        if not dependencies:
            return True
        
        # 检查最近的执行日志
        target_date = scheduled_date or datetime.now().date()
        
        for dep_job_id in dependencies:
            # 查找今天该依赖任务的成功执行记录
            found = False
            for log in reversed(self._execution_logs):
                log_scheduled_date = (
                    (log.config or {})
                    .get("scheduler", {})
                    .get("scheduled_date")
                )
                if (log.job_id == dep_job_id and 
                    log.status == "completed" and
                    (
                        log.start_time.date() == target_date
                        or log_scheduled_date == target_date.isoformat()
                    )):
                    found = True
                    break
            
            if not found:
                found = self._dependency_completed_today(dep_job_id, target_date)

            if not found:
                logger.debug(
                    f"Dependency {dep_job_id} not satisfied for {job_id} on {target_date}"
                )
                return False
        
        return True

    def _dependency_completed_today(self, job_id: str, today) -> bool:
        """兼容旧测试/调用：检查数据库中指定日期是否已有依赖任务成功记录。"""
        return self._dependency_completed_for_date(job_id, today)

    def _dependency_completed_for_date(self, job_id: str, target_date: date) -> bool:
        """检查数据库中指定调度日期是否已有依赖任务成功记录。

        调度器可能因为容器重启丢失内存执行日志；依赖状态以数据库执行日志兜底。
        """
        if not self.database_url:
            return False

        try:
            from sqlalchemy import create_engine, text

            engine = create_engine(
                self._sync_database_url(),
                pool_pre_ping=True,
                connect_args={"connect_timeout": 2},
            )
            try:
                with engine.connect() as conn:
                    row = conn.execute(
                        text("""
                            SELECT 1
                            FROM preprocess_execution_log
                            WHERE job_id = :job_id
                              AND status = 'completed'
                              AND (
                                (started_at AT TIME ZONE 'Asia/Shanghai')::date = :target_date
                                OR parameters->'scheduler'->>'scheduled_date' = :target_date_text
                              )
                            LIMIT 1
                        """),
                        {
                            "job_id": job_id,
                            "target_date": target_date,
                            "target_date_text": target_date.isoformat(),
                        },
                    ).fetchone()
            finally:
                engine.dispose()
            return row is not None
        except Exception as exc:
            logger.debug(
                f"Dependency log lookup failed for {job_id}, using in-memory logs only: {exc}"
            )
            return False
    
    def _trigger_dependent_jobs(
        self,
        completed_job_id: str,
        scheduled_date: Optional[date] = None,
    ) -> None:
        """触发依赖已完成任务的待处理任务"""
        jobs_to_trigger = []
        
        for pending_key, pending_context in list(self._pending_jobs.items()):
            pending_job_id = pending_context["job_id"]
            deps = self._job_dependencies.get(pending_job_id, set())
            pending_date = datetime.strptime(
                pending_context["scheduled_date"], "%Y-%m-%d"
            ).date()
            if (
                completed_job_id in deps
                and (scheduled_date is None or pending_date == scheduled_date)
                and self._check_dependencies(pending_job_id, pending_date)
            ):
                jobs_to_trigger.append((pending_job_id, pending_context))
                self._pending_jobs.pop(pending_key, None)
        
        for job_id, pending_context in jobs_to_trigger:
            params = pending_context["params"]
            scheduled_date_text = pending_context["scheduled_date"]
            one_time_id = f"pending:{job_id}:{scheduled_date_text}:{int(datetime.now().timestamp())}"
            logger.info(
                f"Triggering pending job: {job_id}, scheduled_date={scheduled_date_text}"
            )
            self._engine.add_one_time_job(
                job_id=one_time_id,
                func=_job_dispatcher,
                run_date=datetime.now() + timedelta(seconds=1),
                kwargs={
                    "dispatcher_job_id": job_id,
                    **params,
                    "_scheduled_date": scheduled_date_text,
                    "_is_pending_run": True,
                },
                name=f"{job_id} pending {scheduled_date_text}",
            )
    
    def _on_job_event(self, event, status: str) -> None:
        """任务事件回调"""
        job_id = event.job_id
        
        if status == "success":
            result_status = getattr(getattr(event, "retval", None), "status", None)
            if result_status == "pending":
                logger.debug(f"Job {job_id} deferred: waiting for dependencies")
            elif result_status and result_status != "completed":
                logger.debug(f"Job {job_id} finished with task status: {result_status}")
            else:
                logger.debug(f"Job {job_id} completed successfully")
        elif status == "error":
            logger.debug(f"Job {job_id} failed: {event.exception}")
        elif status == "missed":
            logger.debug(f"Job {job_id} missed scheduled execution")
    
    def start(self, daemon: bool = False) -> None:
        """
        启动调度器
        
        Args:
            daemon: 是否以守护进程模式运行
        """
        if self._engine is None:
            self.initialize()
        
        self._engine.start()
        
        if not daemon:
            # 非守护模式，保持主线程运行
            import time
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self.stop()
    
    def stop(self) -> None:
        """停止调度器"""
        if self._engine is not None:
            self._engine.shutdown()
            logger.info("Scheduler stopped")
    
    def pause(self) -> None:
        """暂停调度器"""
        if self._engine is not None:
            self._engine.pause()
    
    def resume(self) -> None:
        """恢复调度器"""
        if self._engine is not None:
            self._engine.resume()
    
    def run_job(self, job_id: str) -> Optional[JobExecutionLog]:
        """立即执行指定任务"""
        if self._config is None:
            self.load_config()
        
        job_config = self._config.jobs.get(job_id)
        if job_config is None:
            logger.error(f"Job not found: {job_id}")
            return None
        
        if self._executor is None:
            base_executor = TaskExecutor(project_root=self.project_root)
            self._executor = RetryExecutor(base_executor)
        
        log = self._executor.execute_with_retry(job_id, job_config)
        self._execution_logs.append(log)
        
        # 保存执行日志到数据库
        self._save_execution_log(log)

        if log.status == "completed" and self._engine is not None:
            self._trigger_dependent_jobs(job_id)
        
        return log
    
    def pause_job(self, job_id: str) -> None:
        """暂停指定任务"""
        if self._engine is not None:
            self._engine.pause_job(job_id)
    
    def resume_job(self, job_id: str) -> None:
        """恢复指定任务"""
        if self._engine is not None:
            self._engine.resume_job(job_id)
    
    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        status = {
            "running": self._engine.running if self._engine else False,
            "jobs_count": len(self._config.jobs) if self._config else 0,
            "enabled_jobs": 0,
            "pending_jobs": list(self._pending_jobs.keys()),
            "recent_executions": []
        }
        
        if self._config:
            status["enabled_jobs"] = sum(
                1 for j in self._config.jobs.values() if j.enabled
            )
        
        # 最近 10 条执行记录
        for log in self._execution_logs[-10:]:
            status["recent_executions"].append({
                "job_id": log.job_id,
                "status": log.status,
                "start_time": log.start_time.isoformat(),
                "end_time": log.end_time.isoformat() if log.end_time else None,
                "error": log.error_message
            })
        
        return status
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """获取所有任务信息"""
        jobs = []
        
        if self._config is None:
            return jobs
        
        for job_id, job_config in self._config.jobs.items():
            job_info = {
                "id": job_id,
                "enabled": job_config.enabled,
                "type": job_config.type.value,
                "dataset": job_config.dataset,
                "category": job_config.category,
                "depends_on": job_config.depends_on,
                "next_run": None
            }
            
            # 获取下次执行时间
            if self._engine:
                job = self._engine.get_job(job_id)
                if job and job.next_run_time:
                    job_info["next_run"] = job.next_run_time.isoformat()
            
            jobs.append(job_info)
        
        return jobs
    
    def get_history(
        self, 
        job_id: Optional[str] = None, 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """获取任务执行历史"""
        logs = self._execution_logs
        
        if job_id:
            logs = [log for log in logs if log.job_id == job_id]
        
        # 按时间倒序，取最近 N 条
        logs = sorted(logs, key=lambda x: x.start_time, reverse=True)[:limit]
        
        return [
            {
                "job_id": log.job_id,
                "job_type": log.job_type.value,
                "status": log.status,
                "start_time": log.start_time.isoformat(),
                "end_time": log.end_time.isoformat() if log.end_time else None,
                "duration": (
                    (log.end_time - log.start_time).total_seconds()
                    if log.end_time else None
                ),
                "records_processed": log.records_processed,
                "symbols_count": log.symbols_count,
                "error": log.error_message
            }
            for log in logs
        ]
