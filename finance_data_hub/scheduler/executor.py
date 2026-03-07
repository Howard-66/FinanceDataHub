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


class NoDataAvailableError(Exception):
    """当 API 返回空数据时抛出的异常，可触发重试"""
    pass


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
            
            # 记录命令输出，以便调试
            output = result.stdout.strip() if result.stdout else ""
            if output:
                logger.info(f"Command output for {dataset}:\n{output}")
            
            # 检测"没有数据"的情况，抛出异常以触发重试
            # 这通常发生在 Tushare 数据尚未准备好的情况下
            if "没有数据" in output:
                logger.warning(f"No data available for {dataset}, will retry later")
                raise NoDataAvailableError(
                    f"数据集 {dataset} 暂无数据可用（Tushare 数据可能尚未更新），将触发重试"
                )
            
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
        from datetime import timedelta
        
        today = date.today()
        # 简单判断：周六周日不是交易日
        weekday = today.weekday()
        if weekday == 5:  # 周六
            trade_date = today - timedelta(days=1)
        elif weekday == 6:  # 周日
            trade_date = today - timedelta(days=2)
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

        调用 fdh-cli preprocess 命令
        """
        category = job_config.category
        params = {**job_config.params, **kwargs}

        logger.info(f"Executing preprocess task: {job_id}, category: {category}")

        if category == "technical":
            return self._preprocess_technical(params)
        elif category == "fundamental":
            return self._preprocess_fundamental(params)
        elif category == "quarterly_fundamental":
            return self._preprocess_quarterly_fundamental(params)
        elif category == "industry_valuation":
            return self._preprocess_industry_valuation(params)
        else:
            raise ValueError(f"Unknown preprocess category: {category}")

    def _preprocess_technical(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行技术指标预处理"""
        cmd = self._build_preprocess_command("technical", params)
        logger.info(f"Executing technical preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Technical preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Technical preprocess output:\n{output}")

        # 解析输出获取处理记录数
        records_processed = self._parse_preprocess_output(output)

        return {
            "records_processed": records_processed,
            "symbols_count": 0  # TODO: 可以从输出解析
        }

    def _preprocess_fundamental(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行基本面指标预处理"""
        cmd = self._build_preprocess_command("fundamental", params)
        logger.info(f"Executing fundamental preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Fundamental preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Fundamental preprocess output:\n{output}")

        records_processed = self._parse_preprocess_output(output)

        return {
            "records_processed": records_processed,
            "symbols_count": 0
        }

    def _preprocess_quarterly_fundamental(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行季度基本面指标预处理（F-Score等）"""
        cmd = self._build_preprocess_command("quarterly_fundamental", params)
        logger.info(f"Executing quarterly fundamental preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Quarterly fundamental preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Quarterly fundamental preprocess output:\n{output}")

        records_processed = self._parse_preprocess_output(output)

        return {
            "records_processed": records_processed,
            "symbols_count": 0
        }

    def _preprocess_industry_valuation(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行行业差异化估值预处理（根据行业自动选择核心估值指标）"""
        cmd = self._build_preprocess_command("industry_valuation", params)
        logger.info(f"Executing industry valuation preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Industry valuation preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Industry valuation preprocess output:\n{output}")

        records_processed = self._parse_preprocess_output(output)

        return {
            "records_processed": records_processed,
            "symbols_count": 0
        }

    def _build_preprocess_command(
        self,
        category: str,
        params: Dict[str, Any]
    ) -> List[str]:
        """构建预处理命令"""
        # 使用 fdh-cli 命令
        venv_fdh_cli = self.project_root / ".venv" / "bin" / "fdh-cli"
        if venv_fdh_cli.exists():
            cmd = [str(venv_fdh_cli), "preprocess", "run"]
        else:
            cmd = [self.python_path, "-m", "finance_data_hub.cli.main", "preprocess", "run"]

        # 添加 category 参数
        cmd.extend(["--category", category])

        # 处理 all 参数（处理全部股票）
        if params.get("all"):
            cmd.append("--all")

        # 处理 freq 参数
        freq = params.get("freq")
        if freq:
            cmd.extend(["--freq", freq])

        # 处理 adjust 参数
        adjust = params.get("adjust")
        if adjust:
            cmd.extend(["--adjust", adjust])

        # 处理 force 参数（全量重新计算）
        if params.get("force"):
            cmd.append("--force")

        # 处理 symbols 参数
        symbols = params.get("symbols")
        if symbols:
            if isinstance(symbols, list):
                symbols = ",".join(symbols)
            cmd.extend(["--symbols", symbols])

        # 处理 start_date 参数
        start_date = params.get("start_date")
        if start_date:
            cmd.extend(["--start-date", start_date])

        # 处理 end_date 参数
        end_date = params.get("end_date")
        if end_date:
            cmd.extend(["--end-date", end_date])

        # 添加 verbose 参数
        if params.get("verbose"):
            cmd.append("--verbose")

        return cmd

    def _parse_preprocess_output(self, output: str) -> int:
        """解析预处理命令输出，获取处理记录数"""
        import re

        # 尝试匹配 "总处理记录: X" 或 "Total upserted: X" 等格式
        patterns = [
            r"总处理记录:\s*(\d+)",
            r"Total upserted:\s*(\d+)",
            r"records_processed:\s*(\d+)",
            r"处理记录:\s*(\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                return int(match.group(1))

        return 0


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
