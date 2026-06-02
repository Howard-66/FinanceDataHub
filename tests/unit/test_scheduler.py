"""
调度模块单元测试
"""

import pytest
import os
from pathlib import Path
from datetime import datetime, date


class TestJobConfig:
    """任务配置测试"""
    
    def test_job_config_creation(self):
        """测试任务配置创建"""
        from finance_data_hub.scheduler.models import (
            JobConfig, JobType, RetryConfig
        )
        
        config = JobConfig(
            enabled=True,
            type=JobType.DOWNLOAD,
            dataset="daily",
            schedule={
                "type": "cron",
                "hour": 17,
                "minute": 0,
                "day_of_week": "mon-fri"
            },
            retry=RetryConfig()
        )
        
        assert config.enabled is True
        assert config.type == JobType.DOWNLOAD
        assert config.dataset == "daily"
    
    def test_job_config_with_list_dataset(self):
        """测试多数据集任务配置"""
        from finance_data_hub.scheduler.models import (
            JobConfig, JobType, RetryConfig
        )
        
        config = JobConfig(
            enabled=True,
            type=JobType.DOWNLOAD,
            dataset=["fina_indicator", "cashflow", "balancesheet"],
            schedule={
                "type": "cron",
                "day": "1,15",
                "hour": 6,
                "minute": 0
            },
            retry=RetryConfig()
        )
        
        datasets = config.get_datasets()
        assert len(datasets) == 3
        assert "fina_indicator" in datasets
    
    def test_preprocess_job_config(self):
        """测试预处理任务配置"""
        from finance_data_hub.scheduler.models import (
            JobConfig, JobType, RetryConfig
        )
        
        config = JobConfig(
            enabled=True,
            type=JobType.PREPROCESS,
            category="technical",
            schedule={
                "type": "cron",
                "hour": 18,
                "minute": 0,
                "day_of_week": "mon-fri"
            },
            params={
                "freq": ["daily", "weekly"],
                "indicators": {"ma": [5, 10, 20]}
            },
            retry=RetryConfig()
        )
        
        assert config.type == JobType.PREPROCESS
        assert config.category == "technical"
        assert "freq" in config.params

    def test_aggregate_job_config(self):
        """测试连续聚合刷新任务配置"""
        from finance_data_hub.scheduler.models import (
            JobConfig, JobType, RetryConfig
        )

        config = JobConfig(
            enabled=True,
            type=JobType.AGGREGATE,
            dataset="futures.minute_15m",
            schedule={
                "type": "cron",
                "hour": 17,
                "minute": 15,
            },
            retry=RetryConfig()
        )

        assert config.type == JobType.AGGREGATE
        assert config.get_datasets() == ["futures.minute_15m"]
    
    def test_get_schedule_config(self):
        """测试获取调度配置对象"""
        from finance_data_hub.scheduler.models import JobConfig, CronSchedule
        
        config = JobConfig(
            type="download",
            dataset="daily",
            schedule={
                "type": "cron",
                "hour": 17,
                "minute": 30,
            }
        )
        
        schedule = config.get_schedule_config()
        assert isinstance(schedule, CronSchedule)
        assert schedule.hour == 17
        assert schedule.minute == 30


class TestScheduleConfig:
    """调度配置测试"""
    
    def test_cron_schedule(self):
        """测试 Cron 调度"""
        from finance_data_hub.scheduler.models import ScheduleType, CronSchedule
        
        schedule = CronSchedule(
            type=ScheduleType.CRON,
            hour=17,
            minute=30,
            day_of_week="mon-fri"
        )
        
        assert schedule.type == ScheduleType.CRON
        assert schedule.hour == 17
        assert schedule.minute == 30
    
    def test_interval_schedule(self):
        """测试间隔调度"""
        from finance_data_hub.scheduler.models import ScheduleType, IntervalSchedule
        
        schedule = IntervalSchedule(
            type=ScheduleType.INTERVAL,
            hours=1
        )
        
        assert schedule.type == ScheduleType.INTERVAL
        assert schedule.hours == 1
    
    def test_cron_to_apscheduler_kwargs(self):
        """测试转换为 APScheduler 参数"""
        from finance_data_hub.scheduler.models import CronSchedule
        
        schedule = CronSchedule(
            hour=17,
            minute=30,
            day_of_week="mon-fri"
        )
        
        kwargs = schedule.to_apscheduler_kwargs()
        assert kwargs["hour"] == 17
        assert kwargs["minute"] == 30
        assert kwargs["day_of_week"] == "mon-fri"


class TestScheduleConfigLoader:
    """配置加载测试"""
    
    @pytest.fixture
    def sample_config_path(self, tmp_path):
        """创建临时配置文件"""
        config_content = """
scheduler:
  timezone: "Asia/Shanghai"
  max_concurrent_jobs: 3

jobs:
  test_job:
    enabled: true
    type: download
    dataset: daily
    schedule:
      type: cron
      hour: 17
      minute: 0
      day_of_week: "mon-fri"
    retry:
      max_retries: 3
      delay: 300
"""
        config_file = tmp_path / "schedules.yml"
        config_file.write_text(config_content)
        return str(config_file)
    
    def test_load_config_from_yaml(self, sample_config_path):
        """测试从 YAML 加载配置"""
        from finance_data_hub.scheduler.models import ScheduleConfig
        
        config = ScheduleConfig.from_yaml(sample_config_path)
        
        assert config.scheduler.timezone == "Asia/Shanghai"
        assert config.scheduler.max_concurrent_jobs == 3
        assert "test_job" in config.jobs
        assert config.jobs["test_job"].type.value == "download"

    def test_load_repository_schedule_config(self):
        """仓库内置 schedules.yml 应保持可解析。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml("schedules.yml")

        assert "futures_minute_5m_update" in config.jobs
        assert "futures_minute_5m_saturday_update" in config.jobs
        assert config.jobs["futures_minute_15m_refresh"].type.value == "aggregate"
        assert config.jobs["futures_daily_update"].params["trade_date"] == "previous_trade_date"
        assert config.jobs["futures_term_metrics_saturday_update"].dataset == "term_metrics"


class TestTaskExecutor:
    """任务执行器测试"""
    
    def test_executor_creation(self):
        """测试执行器创建"""
        from finance_data_hub.scheduler.executor import TaskExecutor
        
        executor = TaskExecutor()
        assert executor is not None
        assert executor.project_root == Path.cwd()
    
    def test_executor_with_custom_path(self, tmp_path):
        """测试自定义路径的执行器"""
        from finance_data_hub.scheduler.executor import TaskExecutor
        
        executor = TaskExecutor(project_root=str(tmp_path))
        assert executor.project_root == tmp_path
    
    def test_get_latest_trade_date(self):
        """测试获取最新交易日"""
        from finance_data_hub.scheduler.executor import TaskExecutor
        
        executor = TaskExecutor()
        trade_date = executor._get_latest_trade_date()

        assert trade_date is not None
        # 格式应该是 YYYY-MM-DD

    def test_build_preprocess_command_for_macro_cycle(self):
        """测试宏观周期预处理命令构造。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        cmd = executor._build_preprocess_command("macro_cycle", {"all": True})

        joined = " ".join(cmd)
        assert "--category macro_cycle" in joined
        assert "--all" in joined

    def test_build_download_command_includes_market(self):
        """下载命令应透传 market 参数。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        cmd = executor._build_download_command("daily", {"market": "HK", "trade_date": "2024-01-02"})

        joined = " ".join(cmd)
        assert "--dataset daily" in joined
        assert "--market HK" in joined

    def test_build_download_command_includes_asset_class_and_dates(self):
        """下载命令应透传 asset_class 与日期窗口参数。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        cmd = executor._build_download_command(
            "index_daily",
            {
                "asset_class": "future",
                "start_date": "2024-04-30",
                "end_date": "2024-04-30",
            },
        )

        joined = " ".join(cmd)
        assert "--dataset index_daily" in joined
        assert "--asset-class future" in joined
        assert "--start-date 2024-04-30" in joined
        assert "--end-date 2024-04-30" in joined

    def test_build_download_command_resolves_latest_date_placeholders(self):
        """下载命令应把 latest 占位展开为交易日日期。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        executor._get_latest_trade_date = lambda asset_class=None: "2024-04-30"

        cmd = executor._build_download_command(
            "inventory",
            {
                "asset_class": "future",
                "start_date": "latest",
                "end_date": "latest",
            },
        )

        joined = " ".join(cmd)
        assert "--asset-class future" in joined
        assert "--start-date 2024-04-30" in joined
        assert "--end-date 2024-04-30" in joined

    def test_build_download_command_resolves_relative_latest_date_placeholders(self):
        """下载命令应支持 latest/previous_trade_date 与相对日期占位。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        executor._get_latest_trade_date = lambda asset_class=None: "2024-04-30"
        executor._get_previous_trade_date = lambda asset_class=None: "2024-04-29"

        cmd = executor._build_download_command(
            "minute_5",
            {
                "asset_class": "future",
                "start_date": "previous_trade_date",
                "end_date": "latest+1d",
            },
        )

        joined = " ".join(cmd)
        assert "--start-date 2024-04-29" in joined
        assert "--end-date 2024-05-01" in joined

        executor._get_latest_trade_date = lambda asset_class=None: "2024-05-06"
        executor._get_previous_trade_date = lambda asset_class=None: "2024-05-03"
        cmd = executor._build_download_command(
            "minute_5",
            {
                "asset_class": "future",
                "start_date": "previous_trade_date",
                "end_date": "latest",
            },
        )

        joined = " ".join(cmd)
        assert "--start-date 2024-05-03" in joined
        assert "--end-date 2024-05-06" in joined

    def test_download_output_all_futures_failures_raises(self):
        """期货分钟线全合约失败时调度任务应失败，避免误报 completed。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        output = """
        [PARTIAL] 已更新 0 条期货数据，1080/1080 个合约失败
          分钟线摘要: 成功 0，空数据 0，已是最新 0，失败 1080
        """

        with pytest.raises(RuntimeError, match="all 1080 futures symbols failed"):
            executor._raise_if_download_fully_failed("minute_1", output)

    def test_download_output_partial_futures_failures_do_not_raise(self):
        """少量合约失败时保留 partial 输出，不让调度器整批重试。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        output = """
        [PARTIAL] 已更新 100 条期货数据，2/1080 个合约失败
          分钟线摘要: 成功 100，空数据 20，已是最新 958，失败 2
        """

        executor._raise_if_download_fully_failed("minute_1", output)

    def test_trade_calendar_date_lookup_uses_database(self, monkeypatch):
        """latest/previous_trade_date 优先来自 trade_cal。"""
        from finance_data_hub.scheduler import executor as executor_module
        from finance_data_hub.scheduler.executor import TaskExecutor

        class FakeRow:
            trade_date = "2024-05-06"

        class FakeResult:
            def fetchone(self):
                return FakeRow()

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, params):
                assert params["exchanges"] == ["CFFEX", "SHFE", "CZCE", "DCE", "INE", "GFEX"]
                return FakeResult()

        class FakeEngine:
            def connect(self):
                return FakeConnection()

            def dispose(self):
                pass

        monkeypatch.setattr(executor_module, "create_engine", lambda *args, **kwargs: FakeEngine())

        executor = TaskExecutor()
        assert executor._query_trade_calendar_date("future", date(2024, 5, 7), None) == "2024-05-06"

    def test_build_aggregate_command(self):
        """连续聚合刷新命令应透传表名与日期窗口。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        executor._get_latest_trade_date = lambda asset_class=None: "2024-04-30"
        executor._get_previous_trade_date = lambda asset_class=None: "2024-04-29"

        cmd = executor._build_aggregate_command(
            "futures.minute_15m",
            {
                "asset_class": "future",
                "start_date": "previous_trade_date",
                "end_date": "latest+1d",
            },
        )

        joined = " ".join(cmd)
        assert "refresh-aggregates" in joined
        assert "--table futures.minute_15m" in joined
        assert "--start 2024-04-29" in joined
        assert "--end 2024-05-01" in joined

    def test_build_preprocess_command_includes_market(self):
        """预处理命令应透传 market 参数。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        cmd = executor._build_preprocess_command("technical", {"all": True, "market": "HK"})

        joined = " ".join(cmd)
        assert "--category technical" in joined
        assert "--market HK" in joined


class TestScheduleManager:
    """调度管理器测试"""
    
    @pytest.fixture
    def sample_config_path(self, tmp_path):
        """创建临时配置文件"""
        config_content = """
scheduler:
  timezone: "Asia/Shanghai"
  max_concurrent_jobs: 3
  misfire_grace_time: 300

jobs:
  test_download:
    enabled: true
    type: download
    dataset: daily
    schedule:
      type: cron
      hour: 17
      minute: 0
      day_of_week: "mon-fri"
    retry:
      max_retries: 3
      delay: 300
      
  test_preprocess:
    enabled: true
    type: preprocess
    category: technical
    schedule:
      type: cron
      hour: 18
      minute: 0
      day_of_week: "mon-fri"
    depends_on: [test_download]
    retry:
      max_retries: 2
      delay: 600
"""
        config_file = tmp_path / "schedules.yml"
        config_file.write_text(config_content)
        return str(config_file)
    
    def test_manager_creation(self, sample_config_path):
        """测试调度管理器创建"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        
        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        assert config is not None
    
    def test_manager_config_jobs(self, sample_config_path):
        """测试调度管理器配置中的任务"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        
        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        
        # 配置中应该有两个任务
        assert len(config.jobs) == 2
        assert "test_download" in config.jobs
        assert "test_preprocess" in config.jobs
    
    def test_manager_job_dependency(self, sample_config_path):
        """测试任务依赖配置"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        
        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        
        preprocess_config = config.jobs["test_preprocess"]
        assert "test_download" in preprocess_config.depends_on


class TestRetryExecutor:
    """重试执行器测试"""
    
    def test_retry_executor_creation(self):
        """测试重试执行器创建"""
        from finance_data_hub.scheduler.executor import TaskExecutor, RetryExecutor
        
        executor = TaskExecutor()
        retry_executor = RetryExecutor(executor)
        
        assert retry_executor.executor is executor
