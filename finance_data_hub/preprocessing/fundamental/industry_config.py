"""
行业配置加载器

集中管理 industry_config.json 的访问，提供行业差异化配置查询接口。
支持单例模式，避免重复加载配置文件。

配置文件格式：
{
    "行业名称": {
        "macro_cycle": "RECOVERY" | "STAGFLATION" | "OVERHEAT" | "RECESSION",
        "core_indicator": "PE" | "PB" | "PS" | "PEG",
        "ref_indicator": "PE" | "PB" | "PS" | "PEG",
        "logic": "行业逻辑说明",
        "exemptions": ["f_score_cfo", "f_score_leverage", ...]
    }
}

行业名称对应 sw_industry_members 表的 l2_name 字段。
"""

from typing import Dict, List, Optional, Any
from pathlib import Path
import json
from loguru import logger


class IndustryConfigLoader:
    """
    行业配置加载器（单例模式）

    集中管理行业差异化配置，包括：
    - core_indicator: 核心估值指标（PE/PB/PS/PEG）
    - ref_indicator: 参考估值指标
    - exemptions: 豁免规则列表（用于F-Score等）

    示例:
        >>> loader = IndustryConfigLoader()
        >>> loader.get_core_indicator("银行")
        'PB'
        >>> loader.get_exemptions("水产养殖")
        ['f_score_cfo']
    """

    _instance: Optional["IndustryConfigLoader"] = None
    _config: Dict[str, Dict[str, Any]] = {}

    def __new__(cls, config_path: Optional[str] = None):
        """单例模式：确保全局只有一个配置加载实例"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        """初始化配置加载器

        Args:
            config_path: 配置文件路径，默认使用项目根目录下的 industry_config.json
        """
        if self._initialized and config_path is None:
            return

        if config_path is None:
            # 默认路径：项目根目录下的 industry_config.json
            project_root = Path(__file__).parent.parent.parent.parent
            config_path = str(project_root / "industry_config.json")

        self._load_config(config_path)
        self._initialized = True

    def _load_config(self, config_path: str) -> None:
        """加载配置文件

        Args:
            config_path: 配置文件路径
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self._config = json.load(f)
            logger.info(f"已加载行业配置: {len(self._config)} 个行业, 路径: {config_path}")
        except FileNotFoundError:
            logger.warning(f"行业配置文件不存在: {config_path}, 使用默认配置")
            self._config = {}
        except json.JSONDecodeError as e:
            logger.error(f"行业配置文件格式错误: {e}")
            self._config = {}

    @property
    def config(self) -> Dict[str, Dict[str, Any]]:
        """获取完整配置字典"""
        return self._config

    def get_industry_config(self, l2_name: Optional[str]) -> Dict[str, Any]:
        """获取行业配置

        Args:
            l2_name: 二级行业名称

        Returns:
            行业配置字典，未配置时返回默认值
        """
        if l2_name is None or l2_name not in self._config:
            return self._get_default_config()
        return self._config[l2_name]

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            "core_indicator": "PE",
            "ref_indicator": "PB",
            "exemptions": [],
        }

    def get_core_indicator(self, l2_name: Optional[str]) -> str:
        """获取核心估值指标类型

        Args:
            l2_name: 二级行业名称

        Returns:
            核心指标类型 (PE/PB/PS/PEG)，默认 PE
        """
        return self.get_industry_config(l2_name).get("core_indicator", "PE")

    def get_ref_indicator(self, l2_name: Optional[str]) -> str:
        """获取参考估值指标类型

        Args:
            l2_name: 二级行业名称

        Returns:
            参考指标类型 (PE/PB/PS/PEG)，默认 PB
        """
        return self.get_industry_config(l2_name).get("ref_indicator", "PB")

    def get_exemptions(self, l2_name: Optional[str]) -> List[str]:
        """获取行业豁免规则列表

        Args:
            l2_name: 二级行业名称

        Returns:
            豁免规则列表，如 ["f_score_cfo", "f_score_leverage"]
        """
        return self.get_industry_config(l2_name).get("exemptions", [])

    def get_macro_cycle(self, l2_name: Optional[str]) -> Optional[str]:
        """获取行业宏观周期定位

        Args:
            l2_name: 二级行业名称

        Returns:
            宏观周期 (RECOVERY/STAGFLATION/OVERHEAT/RECESSION)，未配置返回 None
        """
        return self.get_industry_config(l2_name).get("macro_cycle")

    def get_logic(self, l2_name: Optional[str]) -> Optional[str]:
        """获取行业投资逻辑说明

        Args:
            l2_name: 二级行业名称

        Returns:
            投资逻辑说明文字
        """
        return self.get_industry_config(l2_name).get("logic")

    def has_industry(self, l2_name: str) -> bool:
        """检查行业是否已配置

        Args:
            l2_name: 二级行业名称

        Returns:
            是否已配置
        """
        return l2_name in self._config

    def get_all_industries(self) -> List[str]:
        """获取所有已配置的行业名称列表

        Returns:
            行业名称列表
        """
        return list(self._config.keys())

    def get_industries_by_indicator(self, indicator: str) -> List[str]:
        """获取使用指定核心指标的行业列表

        Args:
            indicator: 指标类型 (PE/PB/PS/PEG)

        Returns:
            使用该指标作为核心指标的行业列表
        """
        return [
            name for name, cfg in self._config.items()
            if cfg.get("core_indicator") == indicator
        ]

    @classmethod
    def reset(cls) -> None:
        """重置单例实例（主要用于测试）"""
        cls._instance = None


# 模块级便捷函数
_loader: Optional[IndustryConfigLoader] = None


def get_industry_config_loader(config_path: Optional[str] = None) -> IndustryConfigLoader:
    """获取行业配置加载器单例

    Args:
        config_path: 配置文件路径（仅首次调用时生效）

    Returns:
        IndustryConfigLoader 实例
    """
    global _loader
    if _loader is None:
        _loader = IndustryConfigLoader(config_path)
    return _loader