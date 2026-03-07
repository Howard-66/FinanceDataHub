"""
调度任务配置数据模型

使用 Pydantic 定义任务配置的数据结构，支持 YAML 配置文件解析。
"""

from enum import Enum
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class ScheduleType(str, Enum):
    """调度类型枚举"""
    CRON = "cron"
    INTERVAL = "interval"
    DATE = "date"


class JobType(str, Enum):
    """任务类型枚举"""
    DOWNLOAD = "download"
    PREPROCESS = "preprocess"


class RetryConfig(BaseModel):
    """重试配置"""
    max_retries: int = Field(default=3, ge=0, le=10, description="最大重试次数")
    delay: int = Field(default=300, ge=0, description="重试间隔（秒）")
    
    
class CronSchedule(BaseModel):
    """Cron 调度配置"""
    type: ScheduleType = ScheduleType.CRON
    year: Optional[Union[int, str]] = None
    month: Optional[Union[int, str]] = None
    day: Optional[Union[int, str]] = None
    week: Optional[Union[int, str]] = None
    day_of_week: Optional[str] = None
    hour: Optional[Union[int, str]] = None
    minute: Optional[Union[int, str]] = Field(default=0)
    second: Optional[Union[int, str]] = Field(default=0)
    
    def to_apscheduler_kwargs(self) -> Dict[str, Any]:
        """转换为 APScheduler cron trigger 参数"""
        kwargs = {}
        for field_name in ["year", "month", "day", "week", "day_of_week", 
                          "hour", "minute", "second"]:
            value = getattr(self, field_name)
            if value is not None:
                kwargs[field_name] = value
        return kwargs


class IntervalSchedule(BaseModel):
    """间隔调度配置"""
    type: ScheduleType = ScheduleType.INTERVAL
    weeks: int = Field(default=0, ge=0)
    days: int = Field(default=0, ge=0)
    hours: int = Field(default=0, ge=0)
    minutes: int = Field(default=0, ge=0)
    seconds: int = Field(default=0, ge=0)
    
    def to_apscheduler_kwargs(self) -> Dict[str, Any]:
        """转换为 APScheduler interval trigger 参数"""
        return {
            "weeks": self.weeks,
            "days": self.days,
            "hours": self.hours,
            "minutes": self.minutes,
            "seconds": self.seconds,
        }


class DateSchedule(BaseModel):
    """单次执行调度配置"""
    type: ScheduleType = ScheduleType.DATE
    run_date: datetime
    
    def to_apscheduler_kwargs(self) -> Dict[str, Any]:
        """转换为 APScheduler date trigger 参数"""
        return {"run_date": self.run_date}


ScheduleUnion = Union[CronSchedule, IntervalSchedule, DateSchedule]


class JobConfig(BaseModel):
    """任务配置"""
    enabled: bool = Field(default=True, description="是否启用")
    type: JobType = Field(default=JobType.DOWNLOAD, description="任务类型")
    dataset: Optional[Union[str, List[str]]] = Field(
        default=None, 
        description="数据集名称（download 类型）"
    )
    category: Optional[str] = Field(
        default=None,
        description="预处理类别（preprocess 类型）: technical, fundamental, quarterly_fundamental, industry_valuation"
    )
    schedule: Dict[str, Any] = Field(..., description="调度配置")
    params: Dict[str, Any] = Field(default_factory=dict, description="任务参数")
    retry: RetryConfig = Field(default_factory=RetryConfig, description="重试配置")
    depends_on: List[str] = Field(default_factory=list, description="依赖的任务列表")
    
    @field_validator("schedule", mode="before")
    @classmethod
    def parse_schedule(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """解析调度配置"""
        return v
    
    def get_schedule_config(self) -> ScheduleUnion:
        """获取调度配置对象"""
        schedule_type = self.schedule.get("type", "cron")
        
        if schedule_type == "cron":
            return CronSchedule(**self.schedule)
        elif schedule_type == "interval":
            return IntervalSchedule(**self.schedule)
        elif schedule_type == "date":
            return DateSchedule(**self.schedule)
        else:
            raise ValueError(f"Unknown schedule type: {schedule_type}")
    
    def get_datasets(self) -> List[str]:
        """获取数据集列表"""
        if self.dataset is None:
            return []
        if isinstance(self.dataset, str):
            return [self.dataset]
        return self.dataset


class SchedulerConfig(BaseModel):
    """调度器全局配置"""
    timezone: str = Field(default="Asia/Shanghai", description="时区")
    job_store: str = Field(default="memory", description="任务存储类型")
    max_concurrent_jobs: int = Field(default=3, ge=1, description="最大并发任务数")
    misfire_grace_time: int = Field(default=300, description="错过执行的容忍时间（秒）")


class ScheduleConfig(BaseModel):
    """完整调度配置"""
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    jobs: Dict[str, JobConfig] = Field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ScheduleConfig":
        """从 YAML 文件加载配置"""
        import yaml
        import os
        
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        
        # 处理环境变量
        data = cls._expand_env_vars(data)
        
        return cls(**data)
    
    @classmethod
    def _expand_env_vars(cls, data: Any) -> Any:
        """递归展开环境变量"""
        import os
        import re
        
        if isinstance(data, str):
            # 匹配 ${VAR} 或 ${VAR:-default}
            pattern = r'\$\{([^}:]+)(?::-([^}]*))?\}'
            
            def replace(match):
                var_name = match.group(1)
                default = match.group(2) or ""
                return os.environ.get(var_name, default)
            
            return re.sub(pattern, replace, data)
        elif isinstance(data, dict):
            return {k: cls._expand_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [cls._expand_env_vars(item) for item in data]
        else:
            return data


class JobExecutionLog(BaseModel):
    """任务执行日志"""
    job_id: str
    job_name: str
    job_type: JobType
    status: str  # pending, running, completed, failed
    start_time: datetime
    end_time: Optional[datetime] = None
    symbols_count: Optional[int] = None
    records_processed: Optional[int] = None
    error_message: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
