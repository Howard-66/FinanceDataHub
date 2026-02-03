"""
任务执行器

负责执行具体的数据下载和预处理任务：
- 下载任务：调用 fdh-cli update 命令
- 预处理任务：调用预处理模块
"""

import subprocess
import sys
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
from pathlib import Path
from loguru import logger

from .models import JobConfig, JobType, JobExecutionLog


class TaskExecutor:
    """任务执行器"""
    
    def __init__(
        self,
        project_root: Optional[str] = None,
        python_path: Optional[str] = None
    ):
        """
        初始化任务执行器
        
        Args:
            project_root: 项目根目录
            python_path: Python 解释器路径
        """
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.python_path = python_path or sys.executable
        
    def execute(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs
    ) -> JobExecutionLog:
        """
        执行任务
        
        Args:
            job_id: 任务 ID
            job_config: 任务配置
            **kwargs: 额外参数
            
        Returns:
            任务执行日志
        """
        log = JobExecutionLog(
            job_id=job_id,
            job_name=job_id,
            job_type=job_config.type,
            status="running",
            start_time=datetime.now(),
            config=job_config.model_dump()
        )
        
        try:
            if job_config.type == JobType.DOWNLOAD:
                result = self._execute_download(job_id, job_config, **kwargs)
            elif job_config.type == JobType.PREPROCESS:
                result = self._execute_preprocess(job_id, job_config, **kwargs)
            else:
                raise ValueError(f"Unknown job type: {job_config.type}")
            
            log.status = "completed"
            log.records_processed = result.get("records_processed", 0)
            log.symbols_count = result.get("symbols_count", 0)
            
        except Exception as e:
            log.status = "failed"
            log.error_message = str(e)
            logger.error(f"Job {job_id} failed: {e}")
            
        finally:
            log.end_time = datetime.now()
            
        return log
    
    def _execute_download(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行下载任务
        
        调用 fdh-cli update 命令
        """
        datasets = job_config.get_datasets()
        params = {**job_config.params, **kwargs}
        
        total_records = 0
        total_symbols = 0
        
        for dataset in datasets:
            cmd = self._build_download_command(dataset, params)
            logger.info(f"Executing download command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=str(self.project_root),
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                raise RuntimeError(f"Download failed for {dataset}: {error_msg}")
            
            logger.info(f"Download completed for {dataset}")
            # TODO: 解析输出获取记录数
            
        return {
            "records_processed": total_records,
            "symbols_count": total_symbols
        }
    
    def _build_download_command(
        self,
        dataset: str,
        params: Dict[str, Any]
    ) -> List[str]:
        """构建下载命令"""
        # 使用 fdh-cli 命令（已安装的入口点）
        # 如果在虚拟环境中，使用虚拟环境的 fdh-cli
        venv_fdh_cli = self.project_root / ".venv" / "bin" / "fdh-cli"
        if venv_fdh_cli.exists():
            cmd = [str(venv_fdh_cli), "update"]
        else:
            # 回退到使用 python -m
            cmd = [self.python_path, "-m", "finance_data_hub.cli.main", "update"]
        
        # 添加 dataset 参数
        cmd.extend(["--dataset", dataset])
        
        # 处理 trade_date 参数
        trade_date = params.get("trade_date")
        if trade_date:
            if trade_date == "latest":
                # 获取最新交易日
                trade_date = self._get_latest_trade_date()
            if trade_date:
                cmd.extend(["--trade-date", trade_date])
        
        # 处理 symbols 参数
        symbols = params.get("symbols")
        if symbols:
            if isinstance(symbols, list):
                symbols = ",".join(symbols)
            cmd.extend(["--symbols", symbols])
        
        # 处理 force 参数
        if params.get("force"):
            cmd.append("--force")
        
        # 添加 verbose 参数
        if params.get("verbose"):
            cmd.append("--verbose")
        
        return cmd
    
    def _get_latest_trade_date(self) -> Optional[str]:
        """获取最新交易日"""
        today = date.today()
        # 简单判断：周六周日不是交易日
        weekday = today.weekday()
        if weekday == 5:  # 周六
            trade_date = today.replace(day=today.day - 1)
        elif weekday == 6:  # 周日
            trade_date = today.replace(day=today.day - 2)
        else:
            trade_date = today
        
        return trade_date.strftime("%Y-%m-%d")
    
    def _execute_preprocess(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs
    ) -> Dict[str, Any]:
        """
        执行预处理任务
        
        调用预处理模块
        """
        category = job_config.category
        params = {**job_config.params, **kwargs}
        
        logger.info(f"Executing preprocess task: {job_id}, category: {category}")
        
        # TODO: 实现预处理逻辑（等预处理模块完成后集成）
        # 目前先返回空结果
        
        if category == "technical":
            return self._preprocess_technical(params)
        elif category == "fundamental":
            return self._preprocess_fundamental(params)
        else:
            raise ValueError(f"Unknown preprocess category: {category}")
    
    def _preprocess_technical(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行技术指标预处理"""
        # TODO: 实现技术指标预处理
        logger.info(f"Technical preprocessing with params: {params}")
        
        # 占位实现
        return {
            "records_processed": 0,
            "symbols_count": 0
        }
    
    def _preprocess_fundamental(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行基本面指标预处理"""
        # TODO: 实现基本面指标预处理
        logger.info(f"Fundamental preprocessing with params: {params}")
        
        # 占位实现
        return {
            "records_processed": 0,
            "symbols_count": 0
        }


class RetryExecutor:
    """带重试机制的任务执行器"""
    
    def __init__(self, executor: TaskExecutor):
        self.executor = executor
        
    def execute_with_retry(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs
    ) -> JobExecutionLog:
        """
        带重试机制执行任务
        
        Args:
            job_id: 任务 ID
            job_config: 任务配置
            **kwargs: 额外参数
            
        Returns:
            任务执行日志
        """
        import time
        
        max_retries = job_config.retry.max_retries
        retry_delay = job_config.retry.delay
        
        last_log = None
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"Retrying job {job_id}, attempt {attempt + 1}/{max_retries + 1}")
                time.sleep(retry_delay)
            
            log = self.executor.execute(job_id, job_config, **kwargs)
            last_log = log
            
            if log.status == "completed":
                return log
        
        # 所有重试都失败
        logger.error(f"Job {job_id} failed after {max_retries + 1} attempts")
        return last_log
