"""
调度管理器

提供高级调度管理功能：
- 加载配置并初始化调度器
- 任务依赖管理
- 任务状态监控
- CLI 命令集成
"""

from typing import Optional, Dict, List, Any, Set
from datetime import datetime
from pathlib import Path
from loguru import logger

from .models import ScheduleConfig, JobConfig, JobExecutionLog, JobType
from .engine import SchedulerEngine
from .executor import TaskExecutor, RetryExecutor


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
    if dispatcher_job_id not in _job_registry:
        raise ValueError(f"Job {dispatcher_job_id} not found in registry")
    
    manager_ref, job_config = _job_registry[dispatcher_job_id]
    return manager_ref._execute_job(dispatcher_job_id, job_config)


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
        self._pending_jobs: Set[str] = set()
        
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
        for job_id, job_config in self._config.jobs.items():
            if not job_config.enabled:
                logger.debug(f"Skipping disabled job: {job_id}")
                continue
            
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
    
    def _execute_job(self, job_id: str, job_config: JobConfig) -> JobExecutionLog:
        """执行任务（带依赖检查）"""
        # 检查依赖
        if not self._check_dependencies(job_id):
            logger.warning(f"Job {job_id} has unmet dependencies, adding to pending")
            self._pending_jobs.add(job_id)
            return JobExecutionLog(
                job_id=job_id,
                job_name=job_id,
                job_type=job_config.type,
                status="pending",
                start_time=datetime.now(),
                error_message="Waiting for dependencies"
            )
        
        # 执行任务
        log = self._executor.execute_with_retry(job_id, job_config)
        self._execution_logs.append(log)
        
        # 如果成功，检查是否有等待此任务的其他任务
        if log.status == "completed":
            self._trigger_dependent_jobs(job_id)
        
        return log
    
    def _check_dependencies(self, job_id: str) -> bool:
        """检查任务依赖是否满足"""
        dependencies = self._job_dependencies.get(job_id, set())
        
        if not dependencies:
            return True
        
        # 检查最近的执行日志
        today = datetime.now().date()
        
        for dep_job_id in dependencies:
            # 查找今天该依赖任务的成功执行记录
            found = False
            for log in reversed(self._execution_logs):
                if (log.job_id == dep_job_id and 
                    log.status == "completed" and
                    log.start_time.date() == today):
                    found = True
                    break
            
            if not found:
                logger.debug(f"Dependency {dep_job_id} not satisfied for {job_id}")
                return False
        
        return True
    
    def _trigger_dependent_jobs(self, completed_job_id: str) -> None:
        """触发依赖已完成任务的待处理任务"""
        jobs_to_trigger = []
        
        for pending_job_id in list(self._pending_jobs):
            deps = self._job_dependencies.get(pending_job_id, set())
            if completed_job_id in deps and self._check_dependencies(pending_job_id):
                jobs_to_trigger.append(pending_job_id)
                self._pending_jobs.discard(pending_job_id)
        
        for job_id in jobs_to_trigger:
            logger.info(f"Triggering pending job: {job_id}")
            self._engine.run_job_now(job_id)
    
    def _on_job_event(self, event, status: str) -> None:
        """任务事件回调"""
        job_id = event.job_id
        
        if status == "success":
            logger.info(f"Job {job_id} completed successfully")
        elif status == "error":
            logger.error(f"Job {job_id} failed: {event.exception}")
        elif status == "missed":
            logger.warning(f"Job {job_id} missed scheduled execution")
    
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
            "pending_jobs": list(self._pending_jobs),
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
