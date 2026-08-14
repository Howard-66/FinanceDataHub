"""
调度模块单元测试
"""

import pytest
import os
from pathlib import Path
from datetime import datetime, date, timedelta
from unittest.mock import Mock, patch


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
        assert config.resource_group is None
        assert config.catchup_on_failure is True

    def test_job_config_resource_group_and_catchup_flag(self):
        """任务可声明互斥资源组，并关闭失败后的次日补跑。"""
        from finance_data_hub.scheduler.models import JobConfig, JobType

        config = JobConfig(
            enabled=True,
            type=JobType.DOWNLOAD,
            dataset="minute_1",
            resource_group="xtquant_helper",
            catchup_on_failure=False,
            schedule={
                "type": "cron",
                "hour": 3,
                "minute": 30,
            },
        )

        assert config.resource_group == "xtquant_helper"
        assert config.catchup_on_failure is False
    
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
        assert "futures_minute_1m_update" in config.jobs
        assert "futures_minute_15m_refresh" in config.jobs
        assert "futures_minute_5m_night_update" in config.jobs
        assert "futures_minute_5m_saturday_update" in config.jobs
        assert config.jobs["futures_minute_15m_night_refresh"].type.value == "aggregate"
        assert config.jobs["futures_minute_5m_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_5m_update"].catchup_on_failure is False
        assert config.jobs["futures_minute_5m_update"].params["trade_date"] == "latest"
        assert "futures_daily_update" in config.jobs["futures_minute_5m_update"].depends_on
        assert config.jobs["futures_minute_1m_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_1m_update"].catchup_on_failure is False
        assert config.jobs["futures_minute_1m_update"].params["trade_date"] == "latest"
        assert (
            "futures_minute_5m_update"
            in config.jobs["futures_minute_1m_update"].depends_on
        )
        assert config.jobs["futures_minute_5m_night_update"].enabled is False
        assert config.jobs["futures_minute_5m_night_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_5m_night_update"].catchup_on_failure is False
        assert config.jobs["futures_minute_1m_night_update"].enabled is False
        assert config.jobs["futures_minute_1m_night_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_1m_night_update"].catchup_on_failure is False
        assert (
            "futures_minute_5m_night_update"
            in config.jobs["futures_minute_1m_night_update"].depends_on
        )
        assert config.jobs["futures_minute_5m_saturday_update"].enabled is False
        assert config.jobs["futures_minute_5m_saturday_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_5m_saturday_update"].catchup_on_failure is False
        assert config.jobs["futures_minute_1m_saturday_update"].enabled is False
        assert config.jobs["futures_minute_1m_saturday_update"].resource_group == "xtquant_helper"
        assert config.jobs["futures_minute_1m_saturday_update"].catchup_on_failure is False
        assert (
            "futures_minute_5m_saturday_update"
            in config.jobs["futures_minute_1m_saturday_update"].depends_on
        )
        assert config.jobs["futures_daily_update"].params["trade_date"] == "latest"
        assert config.jobs["fund_nav_update"].enabled is True
        assert config.jobs["fund_nav_update"].params["trade_date"] == "latest"
        assert config.jobs["fund_share_update"].enabled is True
        assert config.jobs["fund_share_update"].params["trade_date"] == "latest"
        assert config.jobs["fund_div_update"].enabled is True
        assert config.jobs["fund_div_update"].params["trade_date"] == "today"
        assert "sw_daily_update" not in config.jobs
        assert config.jobs["futures_term_metrics_saturday_update"].dataset == "term_metrics"
        desktop_job = config.jobs["basisflow_wind_excel_refresh"]
        assert desktop_job.type.value == "desktop_automation"
        assert desktop_job.schedule["hour"] == 20
        assert desktop_job.schedule["minute"] == 45
        assert "day_of_week" not in desktop_job.schedule
        assert desktop_job.params["action"] == "wind_excel_refresh"
        assert desktop_job.resource_group == "mac_excel"
        assert desktop_job.catchup_on_failure is False


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

    def test_build_download_command_resolves_today_calendar_date_placeholder(self):
        """fund_div 调度的 today 应保留自然日，而不是退化为最近交易日。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        with patch("finance_data_hub.scheduler.executor.date") as mock_date:
            mock_date.today.return_value = date(2024, 5, 4)  # Saturday
            cmd = executor._build_download_command(
                "fund_div", {"trade_date": "today"}
            )

        assert "--dataset fund_div" in " ".join(cmd)
        assert "--trade-date 2024-05-04" in " ".join(cmd)

    def test_resolve_params_freezes_scheduler_date_placeholders(self):
        """调度执行前应先冻结日期占位，供重试和次日补跑复用。"""
        from finance_data_hub.scheduler.executor import TaskExecutor
        from finance_data_hub.scheduler.models import JobConfig

        executor = TaskExecutor()
        executor._get_latest_trade_date = lambda asset_class=None: "2024-05-06"
        executor._get_previous_trade_date = lambda asset_class=None: "2024-05-03"
        job_config = JobConfig(
            type="download",
            dataset="minute_5",
            schedule={"type": "cron", "hour": 17},
            params={
                "asset_class": "future",
                "start_date": "previous_trade_date",
                "end_date": "latest+1d",
            },
        )

        resolved = executor.resolve_params(job_config)

        assert resolved["start_date"] == "2024-05-03"
        assert resolved["end_date"] == "2024-05-07"

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

    def test_download_failure_preserves_stdout_and_stderr(self):
        """下载失败时应同时保留 CLI traceback 和 provider 日志。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        result = Mock(
            returncode=1,
            stdout="ERROR: relation futures.weekly does not exist",
            stderr="Registered provider: tushare",
        )

        message = TaskExecutor._format_subprocess_error(result)

        assert "stdout:" in message
        assert "relation futures.weekly does not exist" in message
        assert "stderr:" in message
        assert "Registered provider: tushare" in message

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
        assert "--start-date 2024-04-29" in joined
        assert "--end-date 2024-05-01" in joined

    def test_build_preprocess_command_includes_market(self):
        """预处理命令应透传 market 参数。"""
        from finance_data_hub.scheduler.executor import TaskExecutor

        executor = TaskExecutor()
        cmd = executor._build_preprocess_command("technical", {"all": True, "market": "HK"})

        joined = " ".join(cmd)
        assert "--category technical" in joined
        assert "--market HK" in joined

    def test_desktop_automation_uses_shared_queue(self, tmp_path):
        """Docker 调度器应通过共享目录等待 Mac 执行器结果。"""
        import json
        import threading
        import time

        from finance_data_hub.scheduler.executor import TaskExecutor
        from finance_data_hub.scheduler.models import JobConfig

        queue_root = tmp_path / "desktop_queue"
        config = JobConfig(
            type="desktop_automation",
            schedule={"type": "cron", "hour": 20, "minute": 45},
            params={
                "action": "wind_excel_refresh",
                "queue_root": str(queue_root),
                "timeout_seconds": 5,
                "workbook_path": "/tmp/workbook.xlsx",
                "excel_app_path": "/tmp/Microsoft Excel.app",
            },
        )

        def publish_result():
            deadline = time.monotonic() + 3
            request_files = []
            while time.monotonic() < deadline and not request_files:
                request_files = list((queue_root / "requests").glob("*.json"))
                time.sleep(0.01)
            assert request_files
            request = json.loads(request_files[0].read_text(encoding="utf-8"))
            result_path = queue_root / "results" / request_files[0].name
            result_path.write_text(
                json.dumps(
                    {
                        "request_id": request["request_id"],
                        "status": "completed",
                        "details": {"records_processed": 1},
                    }
                ),
                encoding="utf-8",
            )

        responder = threading.Thread(target=publish_result)
        responder.start()
        result = TaskExecutor(project_root=str(tmp_path))._execute_desktop_automation(
            "basisflow_wind_excel_refresh", config
        )
        responder.join(timeout=2)

        assert result == {"records_processed": 1, "symbols_count": 0}


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

    def test_dependency_check_uses_persistent_log_fallback(
        self, sample_config_path, monkeypatch
    ):
        """容器重启导致内存日志为空时，应查询数据库执行日志兜底。"""
        from finance_data_hub.scheduler.manager import ScheduleManager

        manager = ScheduleManager(
            config_path=sample_config_path,
            database_url="postgresql+asyncpg://user:pass@localhost/db",
        )
        manager.load_config()

        monkeypatch.setattr(
            manager,
            "_dependency_completed_today",
            lambda job_id, today: job_id == "test_download",
        )

        assert manager._check_dependencies("test_preprocess") is True

    def test_sync_database_url_converts_asyncpg_url(self, sample_config_path):
        """调度器持久化查询使用同步 SQLAlchemy URL。"""
        from finance_data_hub.scheduler.manager import ScheduleManager

        manager = ScheduleManager(
            config_path=sample_config_path,
            database_url="postgresql+asyncpg://user:pass@localhost/db",
        )

        assert manager._sync_database_url() == "postgresql://user:pass@localhost/db"

    def test_industry_valuation_depends_on_fundamental_preprocess_only(self):
        """行业估值预处理只依赖基本面预处理，行业分类使用最新成分映射。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        industry = config.jobs["industry_valuation_preprocess"]

        assert industry.depends_on == ["fundamental_preprocess"]

    def test_production_misfire_grace_covers_short_scheduler_restart(self):
        """生产调度应覆盖十几分钟级别的容器重启/恢复。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        assert config.scheduler.misfire_grace_time >= 1800

    def test_quarterly_preprocess_matches_financial_update_cycle(self):
        """季度预处理依赖月度财务更新时，调度周期也应一致。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        financial = config.jobs["financial_update"]
        quarterly = config.jobs["quarterly_fundamental_preprocess"]

        assert "financial_update" in quarterly.depends_on
        assert quarterly.schedule["day"] == financial.schedule["day"]
        assert "day_of_week" not in quarterly.schedule

    def test_production_schedule_hk_adj_factor_waits_for_hk_daily(self):
        """港股复权因子需要本地港股日线交易日序列，必须声明依赖。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        hk_adj = config.jobs["hk_adj_factor_update"]
        hk_technical = config.jobs["hk_technical_preprocess"]

        assert "hk_daily_update" in hk_adj.depends_on
        assert "hk_adj_factor_update" in hk_technical.depends_on
        hk_adj_time = hk_adj.schedule["hour"] * 60 + hk_adj.schedule["minute"]
        hk_technical_time = (
            hk_technical.schedule["hour"] * 60 + hk_technical.schedule["minute"]
        )

        assert hk_technical_time - hk_adj_time >= 90

    def test_macro_cycle_dependencies_share_same_monthly_cycle(self):
        """宏观周期 15 号运行时，申万成分股依赖也应在 15 号更新。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        macro = config.jobs["macro_cycle_preprocess"]
        sw_classify = config.jobs["sw_classify_update"]
        sw_member = config.jobs["sw_member_update"]

        assert "sw_member_update" in macro.depends_on
        assert sw_classify.schedule["day"] == "1,15"
        assert sw_member.schedule["day"] == "1,15"

    def test_remove_stale_persisted_jobs_drops_deleted_and_transient_jobs(
        self, sample_config_path
    ):
        """启动时应清理 PostgreSQL job store 里已删除或临时补跑任务。"""
        from finance_data_hub.scheduler.manager import ScheduleManager

        class FakeJob:
            def __init__(self, job_id):
                self.id = job_id

        manager = ScheduleManager(config_path=sample_config_path)
        manager._engine = Mock()
        manager._engine.get_jobs.return_value = [
            FakeJob("test_download"),
            FakeJob("futures_minute_5m_update"),
            FakeJob("catchup:test_download:2026-06-05"),
            FakeJob("pending:test_preprocess:2026-06-05:1"),
        ]

        manager._remove_stale_persisted_jobs({"test_download"})

        removed = [call.args[0] for call in manager._engine.remove_job.call_args_list]
        assert removed == [
            "futures_minute_5m_update",
            "catchup:test_download:2026-06-05",
            "pending:test_preprocess:2026-06-05:1",
        ]

    def test_start_reconciles_persisted_jobs_after_scheduler_start(
        self, sample_config_path
    ):
        """应在暂停启动后清理持久化旧任务，再恢复调度。"""
        from finance_data_hub.scheduler.manager import ScheduleManager

        manager = ScheduleManager(config_path=sample_config_path)
        manager.load_config()
        manager._engine = Mock()

        events = []
        manager._engine.start.side_effect = lambda paused=False: events.append(
            ("start", paused)
        )
        manager._remove_stale_persisted_jobs = Mock(
            side_effect=lambda job_ids: events.append(("cleanup", set(job_ids)))
        )
        manager._engine.resume.side_effect = lambda: events.append(("resume", None))

        manager.start(daemon=True)

        assert events == [
            ("start", True),
            ("cleanup", {"test_download", "test_preprocess"}),
            ("resume", None),
        ]

    def test_futures_minute_aggregates_leave_retry_window(self):
        """5m 原始分钟线调度后应给重试窗口留出时间再刷新聚合。"""
        from finance_data_hub.scheduler.models import ScheduleConfig

        config = ScheduleConfig.from_yaml(
            str(Path(__file__).resolve().parents[2] / "schedules.yml")
        )

        windows = [
            ("futures_minute_5m_update", "futures_minute_15m_refresh"),
        ]

        for minute_job_id, refresh_job_id in windows:
            minute_5m = config.jobs[minute_job_id]
            refresh_15m = config.jobs[refresh_job_id]

            minute_time = minute_5m.schedule["hour"] * 60 + minute_5m.schedule["minute"]
            refresh_time = refresh_15m.schedule["hour"] * 60 + refresh_15m.schedule["minute"]

            assert minute_job_id in refresh_15m.depends_on
            assert refresh_time - minute_time >= 30

    def test_failed_scheduled_job_creates_next_day_catchup_with_resolved_params(
        self, sample_config_path, monkeypatch
    ):
        """重试耗尽后应安排 T+1 补跑，且补跑使用 T 日已解析参数。"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        from finance_data_hub.scheduler.executor import TaskExecutor, RetryExecutor
        from finance_data_hub.scheduler.models import JobExecutionLog

        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        job_config = config.jobs["test_download"]
        job_config.params = {"market": "CN", "trade_date": "latest"}

        base_executor = TaskExecutor()
        base_executor._get_latest_trade_date = lambda asset_class=None: "2026-06-05"
        manager._executor = RetryExecutor(base_executor)

        failed_log = JobExecutionLog(
            job_id="test_download",
            job_name="test_download",
            job_type=job_config.type,
            status="failed",
            start_time=datetime(2026, 6, 5, 17, 10),
            end_time=datetime(2026, 6, 5, 17, 40),
        )
        manager._executor.execute_with_retry = Mock(return_value=failed_log)
        manager._save_execution_log = Mock()
        manager._engine = Mock()

        captured = {}
        manager._engine.add_one_time_job = Mock(
            side_effect=lambda **kwargs: captured.update(kwargs)
        )
        monkeypatch.setattr(
            "finance_data_hub.scheduler.manager.datetime",
            Mock(
                now=Mock(return_value=datetime(2026, 6, 5, 17, 40)),
                combine=datetime.combine,
                strptime=datetime.strptime,
            ),
        )

        manager._execute_job(
            "test_download",
            job_config,
            {"_scheduled_date": "2026-06-05"},
        )

        assert captured["job_id"] == "catchup:test_download:2026-06-05"
        assert captured["run_date"].date() == date(2026, 6, 6)
        assert captured["kwargs"]["trade_date"] == "2026-06-05"
        assert captured["kwargs"]["_scheduled_date"] == "2026-06-05"
        assert captured["kwargs"]["_is_catchup_run"] is True

    def test_failed_job_with_catchup_disabled_does_not_schedule_catchup(
        self, sample_config_path
    ):
        """catchup_on_failure=false 的任务失败后不应进入补跑队列。"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        from finance_data_hub.scheduler.executor import TaskExecutor, RetryExecutor
        from finance_data_hub.scheduler.models import JobExecutionLog

        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        job_config = config.jobs["test_download"]
        job_config.catchup_on_failure = False
        manager._executor = RetryExecutor(TaskExecutor())

        failed_log = JobExecutionLog(
            job_id="test_download",
            job_name="test_download",
            job_type=job_config.type,
            status="failed",
            start_time=datetime(2026, 6, 5, 17, 10),
            end_time=datetime(2026, 6, 5, 17, 40),
        )
        manager._executor.execute_with_retry = Mock(return_value=failed_log)
        manager._save_execution_log = Mock()
        manager._engine = Mock()
        manager._engine.add_one_time_job = Mock()

        manager._execute_job(
            "test_download",
            job_config,
            {"_scheduled_date": "2026-06-05"},
        )

        manager._engine.add_one_time_job.assert_not_called()

    def test_past_next_day_catchup_is_skipped(
        self, sample_config_path, monkeypatch
    ):
        """调度器重启后不应把历史补跑改成当前立即执行。"""
        from finance_data_hub.scheduler.manager import ScheduleManager

        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        job_config = config.jobs["test_download"]
        manager._engine = Mock()
        manager._engine.add_one_time_job = Mock()

        monkeypatch.setattr(
            "finance_data_hub.scheduler.manager.datetime",
            Mock(
                now=Mock(return_value=datetime(2026, 6, 7, 9, 0)),
                combine=datetime.combine,
                strptime=datetime.strptime,
            ),
        )

        manager._schedule_next_day_catchup(
            job_id="test_download",
            job_config=job_config,
            resolved_params={"trade_date": "2026-06-05"},
            scheduled_date=date(2026, 6, 5),
        )

        manager._engine.add_one_time_job.assert_not_called()

    def test_catchup_run_does_not_schedule_another_catchup(
        self, sample_config_path
    ):
        """补跑失败不应无限递归安排补跑。"""
        from finance_data_hub.scheduler.manager import ScheduleManager
        from finance_data_hub.scheduler.executor import TaskExecutor, RetryExecutor
        from finance_data_hub.scheduler.models import JobExecutionLog

        manager = ScheduleManager(config_path=sample_config_path)
        config = manager.load_config()
        job_config = config.jobs["test_download"]
        manager._executor = RetryExecutor(TaskExecutor())

        failed_log = JobExecutionLog(
            job_id="test_download",
            job_name="test_download",
            job_type=job_config.type,
            status="failed",
            start_time=datetime(2026, 6, 6, 0, 30),
            end_time=datetime(2026, 6, 6, 0, 45),
        )
        manager._executor.execute_with_retry = Mock(return_value=failed_log)
        manager._save_execution_log = Mock()
        manager._engine = Mock()
        manager._engine.add_one_time_job = Mock()

        manager._execute_job(
            "test_download",
            job_config,
            {
                "_scheduled_date": "2026-06-05",
                "_is_catchup_run": True,
            },
        )

        manager._engine.add_one_time_job.assert_not_called()


class TestRetryExecutor:
    """重试执行器测试"""
    
    def test_retry_executor_creation(self):
        """测试重试执行器创建"""
        from finance_data_hub.scheduler.executor import TaskExecutor, RetryExecutor
        
        executor = TaskExecutor()
        retry_executor = RetryExecutor(executor)
        
        assert retry_executor.executor is executor
