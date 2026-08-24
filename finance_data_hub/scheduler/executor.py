"""
任务执行器

负责执行具体的数据下载和预处理任务：
- 下载任务：调用 fdh-cli update 命令
- 预处理任务：调用预处理模块
"""

import subprocess
import sys
import re
import threading
import json
import os
import time
import uuid
from contextlib import contextmanager
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date, timedelta
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine, text

from .models import JobConfig, JobType, JobExecutionLog


class NoDataAvailableError(Exception):
    """当 API 返回空数据时抛出的异常，可触发重试"""
    pass


_resource_locks: Dict[str, threading.Lock] = {}
_resource_locks_guard = threading.Lock()


def _get_resource_lock(resource_group: str) -> threading.Lock:
    with _resource_locks_guard:
        lock = _resource_locks.get(resource_group)
        if lock is None:
            lock = threading.Lock()
            _resource_locks[resource_group] = lock
        return lock


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

    @contextmanager
    def resource_guard(self, job_id: str, job_config: JobConfig):
        """Serialize jobs that share an external resource such as xtquant_helper."""
        resource_group = (job_config.resource_group or "").strip()
        if not resource_group:
            yield
            return

        lock = _get_resource_lock(resource_group)
        wait_started = datetime.now()
        logger.info(f"Job {job_id} waiting for scheduler resource group: {resource_group}")
        lock.acquire()
        waited = (datetime.now() - wait_started).total_seconds()
        logger.info(
            f"Job {job_id} acquired scheduler resource group: "
            f"{resource_group} after {waited:.3f}s"
        )
        try:
            yield
        finally:
            lock.release()
            logger.info(f"Job {job_id} released scheduler resource group: {resource_group}")
        
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
            elif job_config.type == JobType.AGGREGATE:
                result = self._execute_aggregate(job_id, job_config, **kwargs)
            elif job_config.type == JobType.DESKTOP_AUTOMATION:
                result = self._execute_desktop_automation(
                    job_id, job_config, **kwargs
                )
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
                error_msg = self._format_subprocess_error(result)
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
            self._raise_if_download_fully_failed(dataset, output)
            
            logger.info(f"Download completed for {dataset}")
            
            # TODO: 解析输出获取记录数
            
        return {
            "records_processed": total_records,
            "symbols_count": total_symbols
        }

    def _execute_desktop_automation(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs,
    ) -> Dict[str, Any]:
        """Submit a desktop task for the macOS worker and wait for its result.

        The production scheduler runs inside Docker and cannot control desktop
        applications directly.  The queue lives under the existing ``data``
        bind mount, so a macOS LaunchAgent can claim the request, operate Excel,
        and publish an auditable result without exposing a network service.
        """
        params = {**job_config.params, **kwargs}
        action = str(params.get("action", "")).strip()
        if action != "wind_excel_refresh":
            raise ValueError(
                "desktop_automation only supports action=wind_excel_refresh"
            )

        queue_root_value = params.get(
            "queue_root", self.project_root / "data" / "desktop_automation"
        )
        queue_root = Path(str(queue_root_value))
        if not queue_root.is_absolute():
            queue_root = self.project_root / queue_root

        timeout_seconds = int(params.get("timeout_seconds", 900))
        if timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")

        request_dir = queue_root / "requests"
        result_dir = queue_root / "results"
        request_dir.mkdir(parents=True, exist_ok=True)
        result_dir.mkdir(parents=True, exist_ok=True)

        requested_at = datetime.now().astimezone()
        request_id = (
            f"{job_id}-{requested_at.strftime('%Y%m%dT%H%M%S')}-"
            f"{uuid.uuid4().hex[:12]}"
        )
        worker_params = {
            key: value
            for key, value in params.items()
            if key not in {"queue_root", "timeout_seconds"}
        }
        request_payload = {
            "request_id": request_id,
            "job_id": job_id,
            "action": action,
            "requested_at": requested_at.isoformat(),
            "params": worker_params,
        }

        request_path = request_dir / f"{request_id}.json"
        temporary_path = request_dir / f".{request_id}.tmp"
        temporary_path.write_text(
            json.dumps(request_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary_path, request_path)
        logger.info(
            f"Queued desktop automation request {request_id} at {request_path}"
        )

        result_path = result_dir / f"{request_id}.json"
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if result_path.exists():
                result = json.loads(result_path.read_text(encoding="utf-8"))
                status = str(result.get("status", "failed"))
                if status != "completed":
                    error = result.get("error") or "desktop worker failed"
                    raise RuntimeError(
                        f"Desktop automation {request_id} failed: {error}"
                    )
                details = result.get("details") or {}
                logger.info(
                    f"Desktop automation {request_id} completed: {details}"
                )
                return {
                    "records_processed": int(
                        details.get("records_processed", 0)
                    ),
                    "symbols_count": 0,
                }
            time.sleep(1)

        raise TimeoutError(
            f"Desktop automation {request_id} did not finish within "
            f"{timeout_seconds} seconds"
        )

    @staticmethod
    def _format_subprocess_error(
        result: subprocess.CompletedProcess,
        max_chars_per_stream: int = 12000,
    ) -> str:
        """Preserve CLI errors from stdout while retaining provider logs from stderr."""
        streams = []
        for name, value in (("stdout", result.stdout), ("stderr", result.stderr)):
            content = str(value or "").strip()
            if not content:
                continue
            if len(content) > max_chars_per_stream:
                content = content[-max_chars_per_stream:]
                content = f"...[truncated to last {max_chars_per_stream} chars]\n{content}"
            streams.append(f"{name}:\n{content}")
        return "\n\n".join(streams) or f"process exited with code {result.returncode}"

    def _raise_if_download_fully_failed(self, dataset: str, output: str) -> None:
        """Treat complete per-symbol failures as scheduler failures."""
        failed_ratio = re.search(r"(\d+)/(\d+)\s*个合约失败", output)
        if failed_ratio:
            failed = int(failed_ratio.group(1))
            total = int(failed_ratio.group(2))
            if total > 0 and failed >= total:
                raise RuntimeError(
                    f"Download failed for {dataset}: all {total} futures symbols failed"
                )

        summary = re.search(
            r"分钟线摘要:\s*成功\s*(\d+)，空数据\s*(\d+)，已是最新\s*(\d+)，失败\s*(\d+)",
            output,
        )
        if summary:
            inserted = int(summary.group(1))
            empty = int(summary.group(2))
            up_to_date = int(summary.group(3))
            failed = int(summary.group(4))
            if failed > 0 and inserted == 0 and empty == 0 and up_to_date == 0:
                raise RuntimeError(
                    f"Download failed for {dataset}: all attempted futures symbols failed"
                )

    def _execute_aggregate(
        self,
        job_id: str,
        job_config: JobConfig,
        **kwargs
    ) -> Dict[str, Any]:
        """执行连续聚合刷新任务。"""
        tables = job_config.get_datasets()
        params = {**job_config.params, **kwargs}

        for table in tables:
            cmd = self._build_aggregate_command(table, params)
            logger.info(f"Executing aggregate refresh command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                cwd=str(self.project_root),
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout
                raise RuntimeError(f"Aggregate refresh failed for {table}: {error_msg}")

            output = result.stdout.strip() if result.stdout else ""
            if output:
                logger.info(f"Aggregate refresh output for {table}:\n{output}")

        return {
            "records_processed": 0,
            "symbols_count": 0
        }

    def _build_aggregate_command(
        self,
        table: str,
        params: Dict[str, Any]
    ) -> List[str]:
        """构建连续聚合刷新命令。"""
        venv_fdh_cli = self.project_root / ".venv" / "bin" / "fdh-cli"
        if venv_fdh_cli.exists():
            cmd = [str(venv_fdh_cli), "refresh-aggregates"]
        else:
            cmd = [
                self.python_path,
                "-m",
                "finance_data_hub.cli.main",
                "refresh-aggregates",
            ]

        cmd.extend(["--table", table])

        asset_class = params.get("asset_class")
        normalized_asset_class = (
            str(asset_class).strip().lower() if asset_class is not None else None
        )
        start_date = self._resolve_date_param(
            params.get("start_date"), normalized_asset_class
        )
        if start_date:
            cmd.extend(["--start-date", start_date])

        end_date = self._resolve_date_param(
            params.get("end_date"), normalized_asset_class
        )
        if end_date:
            cmd.extend(["--end-date", end_date])

        if params.get("verbose"):
            cmd.append("--verbose")

        return cmd
    
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
        
        asset_class = params.get("asset_class")
        normalized_asset_class = (
            str(asset_class).strip().lower() if asset_class is not None else None
        )

        # 添加 dataset 参数
        cmd.extend(["--dataset", dataset])

        # 处理 asset_class 参数
        if normalized_asset_class:
            cmd.extend(["--asset-class", normalized_asset_class])

        # 处理 market 参数
        market = params.get("market")
        if market:
            cmd.extend(["--market", str(market)])

        # 主线阶段可独立调度，避免每天无意重训月度模型。
        stage = params.get("stage")
        if stage:
            if isinstance(stage, list):
                stage = ",".join(stage)
            cmd.extend(["--stage", str(stage)])
        
        # 处理 trade_date 参数
        trade_date = self._resolve_date_param(
            params.get("trade_date"), normalized_asset_class
        )
        if trade_date:
            cmd.extend(["--trade-date", trade_date])
        
        # 处理 symbols 参数
        symbols = params.get("symbols")
        if symbols:
            if isinstance(symbols, list):
                symbols = ",".join(symbols)
            cmd.extend(["--symbols", symbols])

        # 处理 start_date 参数
        start_date = self._resolve_date_param(
            params.get("start_date"), normalized_asset_class
        )
        if start_date:
            cmd.extend(["--start-date", start_date])

        # 处理 end_date 参数
        end_date = self._resolve_date_param(
            params.get("end_date"), normalized_asset_class
        )
        if end_date:
            cmd.extend(["--end-date", end_date])
        
        # 处理 force 参数
        if params.get("force"):
            cmd.append("--force")
        
        # 添加 verbose 参数
        if params.get("verbose"):
            cmd.append("--verbose")
        
        return cmd

    def resolve_params(
        self,
        job_config: JobConfig,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Resolve scheduler date placeholders once for a concrete job run.

        The scheduler uses this before retries and next-day catch-up jobs so
        placeholders such as ``latest`` keep the business date of the original
        scheduled run instead of being re-evaluated later.
        """
        resolved = dict(params or job_config.params)
        asset_class = resolved.get("asset_class")
        normalized_asset_class = (
            str(asset_class).strip().lower() if asset_class is not None else None
        )

        for key in ("trade_date", "start_date", "end_date", "source_updated_since"):
            if key in resolved:
                resolved[key] = self._resolve_date_param(
                    resolved.get(key),
                    normalized_asset_class,
                )

        return resolved

    def _resolve_date_param(
        self,
        value: Optional[Any],
        asset_class: Optional[str] = None,
    ) -> Optional[str]:
        """Resolve scheduler-friendly date placeholders for CLI parameters."""
        if value is None:
            return None
        if not isinstance(value, str):
            return str(value)

        normalized = value.strip().lower()
        if not normalized:
            return None

        if normalized == "previous_month_last_trade_date":
            first_of_month = date.today().replace(day=1)
            month_end = first_of_month - timedelta(days=1)
            resolved_value = self._query_trade_calendar_date(
                asset_class=asset_class,
                as_of=month_end,
                previous_to=None,
            )
            if resolved_value:
                return resolved_value
            return self._fallback_latest_business_date(month_end).strftime("%Y-%m-%d")

        if normalized == "previous_month_start":
            first_of_month = date.today().replace(day=1)
            return (first_of_month - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")

        placeholder_match = re.fullmatch(
            r"(latest|previous_trade_date|today)(?:([+-])(\d+)(bd|d))?",
            normalized,
        )
        if placeholder_match:
            placeholder, sign, days, unit = placeholder_match.groups()
            if placeholder == "latest":
                resolved_value = self._get_latest_trade_date(asset_class=asset_class)
                if resolved_value is None:
                    return None
                resolved = datetime.strptime(resolved_value, "%Y-%m-%d").date()
            elif placeholder == "previous_trade_date":
                resolved_value = self._get_previous_trade_date(asset_class=asset_class)
                if resolved_value is None:
                    return None
                resolved = datetime.strptime(resolved_value, "%Y-%m-%d").date()
            else:
                # fund_div announcements can occur on weekends and holidays,
                # so daily jobs need the calendar date rather than latest.
                resolved = date.today()
            if days:
                offset = int(days)
                if unit == "bd":
                    resolved = self._shift_business_days(resolved, offset, sign)
                else:
                    delta = timedelta(days=offset)
                    resolved = resolved - delta if sign == "-" else resolved + delta
            return resolved.strftime("%Y-%m-%d")
        return value

    def _get_latest_trade_date(self, asset_class: Optional[str] = None) -> Optional[str]:
        """获取最新交易日。优先查询交易日历，失败时退回简单工作日规则。"""
        calendar_date = self._query_trade_calendar_date(
            asset_class=asset_class,
            as_of=date.today(),
            previous_to=None,
        )
        if calendar_date:
            return calendar_date
        return self._fallback_latest_business_date(date.today()).strftime("%Y-%m-%d")

    def _get_previous_trade_date(
        self,
        asset_class: Optional[str] = None,
    ) -> Optional[str]:
        """获取最新交易日前一交易日。优先查询交易日历，失败时退回工作日规则。"""
        latest = self._get_latest_trade_date(asset_class=asset_class)
        if latest is None:
            return None

        latest_date = datetime.strptime(latest, "%Y-%m-%d").date()
        calendar_date = self._query_trade_calendar_date(
            asset_class=asset_class,
            as_of=latest_date,
            previous_to=latest_date,
        )
        if calendar_date:
            return calendar_date
        return self._shift_business_days(latest_date, 1, "-").strftime("%Y-%m-%d")

    def _query_trade_calendar_date(
        self,
        asset_class: Optional[str],
        as_of: date,
        previous_to: Optional[date],
    ) -> Optional[str]:
        """Query trade_cal for the latest open date on or before ``as_of``."""
        try:
            from finance_data_hub.config import get_settings
        except Exception as exc:
            logger.debug(f"Unable to load settings for trade calendar lookup: {exc}")
            return None

        try:
            settings = get_settings()
            database_url = settings.database.url
            if database_url.startswith("postgresql+asyncpg://"):
                database_url = database_url.replace("postgresql+asyncpg://", "postgresql://")
            if not database_url.startswith("postgresql://"):
                return None

            exchanges = self._trade_calendar_exchanges(asset_class)
            query = """
                SELECT MAX((cal_date AT TIME ZONE 'Asia/Shanghai')::date) AS trade_date
                FROM trade_cal
                WHERE is_open = 1
                  AND exchange = ANY(:exchanges)
                  AND (cal_date AT TIME ZONE 'Asia/Shanghai')::date <= :as_of
            """
            params: Dict[str, Any] = {
                "exchanges": exchanges,
                "as_of": as_of,
            }
            if previous_to is not None:
                query += (
                    " AND (cal_date AT TIME ZONE 'Asia/Shanghai')::date < :previous_to"
                )
                params["previous_to"] = previous_to

            engine = create_engine(
                database_url,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 2},
            )
            try:
                with engine.connect() as conn:
                    row = conn.execute(text(query), params).fetchone()
            finally:
                engine.dispose()

            if row and row.trade_date:
                return self._date_to_str(row.trade_date)
        except Exception as exc:
            logger.debug(f"Trade calendar lookup failed, falling back to weekdays: {exc}")
        return None

    def _trade_calendar_exchanges(self, asset_class: Optional[str]) -> List[str]:
        normalized = str(asset_class or "").strip().lower()
        if normalized == "future":
            return ["CFFEX", "SHFE", "CZCE", "DCE", "INE", "GFEX"]
        return ["SSE", "SZSE"]

    def _fallback_latest_business_date(self, current_date: date) -> date:
        # 简单判断：周六周日不是交易日
        weekday = current_date.weekday()
        if weekday == 5:  # 周六
            trade_date = current_date - timedelta(days=1)
        elif weekday == 6:  # 周日
            trade_date = current_date - timedelta(days=2)
        else:
            trade_date = current_date
        return trade_date

    def _date_to_str(self, value: Any) -> str:
        if isinstance(value, datetime):
            return value.date().strftime("%Y-%m-%d")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return str(value)[:10]

    def _shift_business_days(self, value: date, days: int, sign: str) -> date:
        step = -1 if sign == "-" else 1
        resolved = value
        for _ in range(days):
            resolved += timedelta(days=step)
            while resolved.weekday() >= 5:
                resolved += timedelta(days=step)
        return resolved
    
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
        elif category == "valuation_fill":
            return self._preprocess_valuation_fill(params)
        elif category == "fundamental":
            return self._preprocess_fundamental(params)
        elif category == "quarterly_fundamental":
            return self._preprocess_quarterly_fundamental(params)
        elif category == "industry_valuation":
            return self._preprocess_industry_valuation(params)
        elif category == "macro_cycle":
            return self._preprocess_macro_cycle(params)
        elif category == "mainline":
            return self._preprocess_mainline(params)
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

    def _preprocess_valuation_fill(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行估值缺失补值预处理"""
        cmd = self._build_preprocess_command("valuation_fill", params)
        logger.info(f"Executing valuation fill preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Valuation fill preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Valuation fill preprocess output:\n{output}")

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

    def _preprocess_macro_cycle(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行中国宏观周期预处理。"""
        cmd = self._build_preprocess_command("macro_cycle", params)
        logger.info(f"Executing macro cycle preprocess command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(self.project_root),
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            error_msg = result.stderr or result.stdout
            raise RuntimeError(f"Macro cycle preprocess failed: {error_msg}")

        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Macro cycle preprocess output:\n{output}")

        records_processed = self._parse_preprocess_output(output)

        return {
            "records_processed": records_processed,
            "symbols_count": 0
        }

    def _preprocess_mainline(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """执行量化主线事实与因子预处理。"""
        cmd = self._build_preprocess_command("mainline", params)
        logger.info(f"Executing mainline preprocess command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd, cwd=str(self.project_root), capture_output=True, text=True
        )
        if result.returncode != 0:
            error_msg = self._format_subprocess_error(result)
            raise RuntimeError(f"Mainline preprocess failed: {error_msg}")
        output = result.stdout.strip() if result.stdout else ""
        if output:
            logger.info(f"Mainline preprocess output:\n{output}")
        return {
            "records_processed": self._parse_preprocess_output(output),
            "symbols_count": 0,
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

        # 处理 market 参数
        market = params.get("market")
        if market:
            cmd.extend(["--market", str(market)])

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

        # Mainline materializers are intentionally split into explicit stages.
        # Without forwarding this option, every scheduled mainline job falls
        # back to the CLI default and silently runs the wrong layers.
        stage = params.get("stage")
        if stage:
            if isinstance(stage, list):
                stage = ",".join(stage)
            cmd.extend(["--stage", str(stage)])

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

        source_updated_since = params.get("source_updated_since")
        if source_updated_since:
            cmd.extend(["--source-updated-since", source_updated_since])

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
        
        with self.executor.resource_guard(job_id, job_config):
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
