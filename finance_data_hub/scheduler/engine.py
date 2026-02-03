"""
APScheduler 引擎封装

提供调度器的核心功能：
- 任务注册与管理
- 调度器生命周期控制
- 任务状态持久化
"""

from typing import Optional, Callable, Dict, Any
from datetime import datetime
import asyncio
from loguru import logger

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import (
    EVENT_JOB_EXECUTED, 
    EVENT_JOB_ERROR, 
    EVENT_JOB_MISSED,
    JobExecutionEvent
)

from .models import (
    SchedulerConfig, 
    JobConfig, 
    CronSchedule, 
    IntervalSchedule, 
    DateSchedule,
    ScheduleType
)


class SchedulerEngine:
    """APScheduler 引擎封装"""
    
    def __init__(
        self, 
        config: SchedulerConfig,
        database_url: Optional[str] = None,
        async_mode: bool = False
    ):
        """
        初始化调度器引擎
        
        Args:
            config: 调度器配置
            database_url: 数据库连接 URL（用于持久化任务）
            async_mode: 是否使用异步调度器
        """
        self.config = config
        self.database_url = database_url
        self.async_mode = async_mode
        self._scheduler: Optional[BackgroundScheduler] = None
        self._job_listeners: Dict[str, Callable] = {}
        
    def _create_jobstores(self) -> Dict[str, Any]:
        """创建任务存储"""
        if self.config.job_store == "postgresql" and self.database_url:
            return {
                "default": SQLAlchemyJobStore(url=self.database_url)
            }
        else:
            return {
                "default": MemoryJobStore()
            }
    
    def _create_executors(self) -> Dict[str, Any]:
        """创建执行器"""
        return {
            "default": ThreadPoolExecutor(self.config.max_concurrent_jobs),
            "processpool": ProcessPoolExecutor(2)
        }
    
    def _create_job_defaults(self) -> Dict[str, Any]:
        """创建任务默认配置"""
        return {
            "coalesce": True,  # 合并错过的执行
            "max_instances": 1,  # 每个任务最多同时运行一个实例
            "misfire_grace_time": self.config.misfire_grace_time
        }
    
    def initialize(self) -> None:
        """初始化调度器"""
        if self._scheduler is not None:
            logger.warning("Scheduler already initialized")
            return
            
        jobstores = self._create_jobstores()
        executors = self._create_executors()
        job_defaults = self._create_job_defaults()
        
        if self.async_mode:
            self._scheduler = AsyncIOScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone=self.config.timezone
            )
        else:
            self._scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone=self.config.timezone
            )
        
        # 注册事件监听器
        self._scheduler.add_listener(
            self._on_job_executed,
            EVENT_JOB_EXECUTED
        )
        self._scheduler.add_listener(
            self._on_job_error,
            EVENT_JOB_ERROR
        )
        self._scheduler.add_listener(
            self._on_job_missed,
            EVENT_JOB_MISSED
        )
        
        logger.info(f"Scheduler initialized with timezone: {self.config.timezone}")
    
    def start(self, paused: bool = False) -> None:
        """启动调度器"""
        if self._scheduler is None:
            self.initialize()
            
        if not self._scheduler.running:
            self._scheduler.start(paused=paused)
            logger.info("Scheduler started")
        else:
            logger.warning("Scheduler is already running")
    
    def shutdown(self, wait: bool = True) -> None:
        """关闭调度器"""
        if self._scheduler is not None and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler shut down")
    
    def pause(self) -> None:
        """暂停调度器"""
        if self._scheduler is not None:
            self._scheduler.pause()
            logger.info("Scheduler paused")
    
    def resume(self) -> None:
        """恢复调度器"""
        if self._scheduler is not None:
            self._scheduler.resume()
            logger.info("Scheduler resumed")
    
    @property
    def running(self) -> bool:
        """调度器是否运行中"""
        return self._scheduler is not None and self._scheduler.running
    
    def add_job(
        self,
        job_id: str,
        func: Callable,
        job_config: JobConfig,
        **kwargs
    ) -> None:
        """
        添加任务
        
        Args:
            job_id: 任务 ID
            func: 任务函数
            job_config: 任务配置
            **kwargs: 传递给任务函数的额外参数
        """
        if self._scheduler is None:
            raise RuntimeError("Scheduler not initialized. Call initialize() first.")
        
        if not job_config.enabled:
            logger.debug(f"Job {job_id} is disabled, skipping")
            return
        
        # 获取调度配置
        schedule_config = job_config.get_schedule_config()
        trigger = self._create_trigger(schedule_config)
        
        # 合并参数
        job_kwargs = {**job_config.params, **kwargs}
        
        # 添加任务
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            name=job_id,
            kwargs=job_kwargs,
            replace_existing=True
        )
        
        logger.info(f"Added job: {job_id}")
    
    def remove_job(self, job_id: str) -> None:
        """移除任务"""
        if self._scheduler is not None:
            try:
                self._scheduler.remove_job(job_id)
                logger.info(f"Removed job: {job_id}")
            except Exception as e:
                logger.warning(f"Failed to remove job {job_id}: {e}")
    
    def pause_job(self, job_id: str) -> None:
        """暂停任务"""
        if self._scheduler is not None:
            self._scheduler.pause_job(job_id)
            logger.info(f"Paused job: {job_id}")
    
    def resume_job(self, job_id: str) -> None:
        """恢复任务"""
        if self._scheduler is not None:
            self._scheduler.resume_job(job_id)
            logger.info(f"Resumed job: {job_id}")
    
    def run_job_now(self, job_id: str) -> None:
        """立即执行任务"""
        if self._scheduler is not None:
            job = self._scheduler.get_job(job_id)
            if job:
                job.modify(next_run_time=datetime.now())
                logger.info(f"Triggered immediate execution of job: {job_id}")
            else:
                logger.warning(f"Job not found: {job_id}")
    
    def get_jobs(self) -> list:
        """获取所有任务"""
        if self._scheduler is not None:
            return self._scheduler.get_jobs()
        return []
    
    def get_job(self, job_id: str):
        """获取指定任务"""
        if self._scheduler is not None:
            return self._scheduler.get_job(job_id)
        return None
    
    def _create_trigger(self, schedule_config):
        """创建触发器"""
        if isinstance(schedule_config, CronSchedule):
            return CronTrigger(**schedule_config.to_apscheduler_kwargs())
        elif isinstance(schedule_config, IntervalSchedule):
            return IntervalTrigger(**schedule_config.to_apscheduler_kwargs())
        elif isinstance(schedule_config, DateSchedule):
            return DateTrigger(**schedule_config.to_apscheduler_kwargs())
        else:
            raise ValueError(f"Unknown schedule config type: {type(schedule_config)}")
    
    def register_listener(self, job_id: str, callback: Callable) -> None:
        """注册任务监听器"""
        self._job_listeners[job_id] = callback
    
    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        """任务执行完成回调"""
        job_id = event.job_id
        logger.info(f"Job executed successfully: {job_id}")
        
        if job_id in self._job_listeners:
            try:
                self._job_listeners[job_id](event, "success")
            except Exception as e:
                logger.error(f"Error in job listener for {job_id}: {e}")
    
    def _on_job_error(self, event: JobExecutionEvent) -> None:
        """任务执行错误回调"""
        job_id = event.job_id
        logger.error(f"Job execution failed: {job_id}, error: {event.exception}")
        
        if job_id in self._job_listeners:
            try:
                self._job_listeners[job_id](event, "error")
            except Exception as e:
                logger.error(f"Error in job listener for {job_id}: {e}")
    
    def _on_job_missed(self, event: JobExecutionEvent) -> None:
        """任务错过执行回调"""
        job_id = event.job_id
        logger.warning(f"Job execution missed: {job_id}")
        
        if job_id in self._job_listeners:
            try:
                self._job_listeners[job_id](event, "missed")
            except Exception as e:
                logger.error(f"Error in job listener for {job_id}: {e}")
