"""
行业差异化估值计算器

根据行业配置自动选择核心估值指标（PE/PB/PS/PEG），并计算：
1. 自身历史分位数：当前值在历史数据中的位置
2. 行业内相对分位：当前值在同行业股票中的相对位置

使用场景：
- 银行股使用PB估值，科技股使用PE估值
- 成长股使用PEG估值，亏损企业使用PS估值
- 某些行业有指标豁免规则（如金融股豁免现金流检查）

数据来源：
- processed_valuation_pct: 估值数据（PE/PB/PS/PEG/分位数）
- sw_industry_member: 行业分类数据（l1_name/l2_name/l3_name）
- industry_config.json: 行业配置（core_indicator/ref_indicator/exemptions）
"""

from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from loguru import logger

from .industry_config import IndustryConfigLoader, get_industry_config_loader


class IndustryValuationCalculator:
    """
    行业差异化估值计算器

    根据行业配置自动选择核心/参考估值指标，并计算历史分位和行业内分位。

    示例:
        >>> calculator = IndustryValuationCalculator()
        >>> result = calculator.calculate(
        ...     valuation_df=valuation_data,
        ...     industry_members_df=industry_data
        ... )
        >>> # 银行股应使用PB
        >>> bank_result = result[result['l2_name'] == '银行']
        >>> print(bank_result['core_indicator_type'].unique())  # ['PB']
    """

    # 指标类型到列名的映射
    INDICATOR_MAP = {
        "PE": "pe_ttm",
        "PB": "pb",
        "PS": "ps_ttm",
        "PEG": "peg",
    }

    # 反向映射
    COLUMN_TO_INDICATOR = {v: k for k, v in INDICATOR_MAP.items()}

    # 输出列
    OUTPUT_COLUMNS = [
        "time", "symbol",
        "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
        "core_indicator_type", "core_indicator_value",
        "core_indicator_pct_1250d", "core_indicator_industry_pct",
        "ref_indicator_type", "ref_indicator_value",
        "ref_indicator_pct_1250d", "ref_indicator_industry_pct",
        "pe_ttm", "pb", "ps_ttm", "peg", "dv_ttm",
        "is_exempted", "exemption_reason", "fscore_exemptions",
    ]

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化行业估值计算器

        Args:
            config_path: 行业配置文件路径，默认使用项目根目录下的 industry_config.json
        """
        self.config_loader = get_industry_config_loader(config_path)

    def calculate(
        self,
        valuation_df: pd.DataFrame,
        industry_members_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        计算行业差异化估值

        Args:
            valuation_df: 估值数据，需包含 time, symbol, pe_ttm, pb, ps_ttm, peg, dv_ttm
                         及其分位列（如 pe_ttm_pct_1250d）
            industry_members_df: 行业分类数据，需包含 ts_code, l1_code, l1_name,
                                l2_code, l2_name, l3_code, l3_name

        Returns:
            包含行业差异化估值指标的 DataFrame
        """
        if valuation_df.empty:
            logger.warning("估值数据为空")
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        # 1. 准备数据
        df = self._prepare_data(valuation_df, industry_members_df)

        # 2. 为每行确定核心/参考指标类型
        df = self._assign_indicator_types(df)

        # 3. 提取指标值
        df = self._extract_indicator_values(df)

        # 4. 计算自身历史分位（复用已有的分位数据）
        df = self._assign_self_percentile(df)

        # 5. 计算行业内相对分位
        df = self._calculate_industry_percentile(df)

        # 6. 处理豁免情况
        df = self._handle_exemptions(df)

        # 7. 添加 F-Score 豁免规则
        df = self._add_fscore_exemptions(df)

        # 8. 选择输出列
        result = self._select_output_columns(df)

        logger.info(
            f"行业差异化估值计算完成: {len(result)} 条记录, "
            f"豁免 {result['is_exempted'].sum()} 条"
        )
        return result

    def _prepare_data(
        self,
        valuation_df: pd.DataFrame,
        industry_members_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """准备数据：合并估值与行业分类"""
        # 确保时间列格式正确
        if "time" not in valuation_df.columns and "trade_date" in valuation_df.columns:
            valuation_df = valuation_df.rename(columns={"trade_date": "time"})

        if valuation_df["time"].dtype == "object":
            valuation_df = valuation_df.copy()
            valuation_df["time"] = pd.to_datetime(valuation_df["time"])

        # 获取当前有效的行业成分股（is_new='Y' 或 in_date <= time < out_date）
        # 简化处理：只取最新的行业分类（is_new='Y'）
        if "is_new" in industry_members_df.columns:
            industry_current = industry_members_df[
                industry_members_df["is_new"] == "Y"
            ].copy()
        else:
            industry_current = industry_members_df.copy()

        # 行业分类列
        industry_cols = ["ts_code"]
        for col in ["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"]:
            if col in industry_current.columns:
                industry_cols.append(col)

        # 合并估值数据与行业分类
        df = valuation_df.merge(
            industry_current[industry_cols],
            left_on="symbol",
            right_on="ts_code",
            how="left"
        )

        # 记录未匹配的行业
        no_industry = df["l2_name"].isna().sum()
        if no_industry > 0:
            logger.info(f"有 {no_industry} 条记录无行业分类")

        return df

    def _assign_indicator_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """为每行确定核心/参考指标类型（向量化版本）"""
        df = df.copy()

        # 构建行业 -> 指标类型的映射字典（使用 l3_name 查找配置）
        core_map = {}
        ref_map = {}
        for l3_name in df["l3_name"].dropna().unique():
            core_map[l3_name] = self.config_loader.get_core_indicator(l3_name)
            ref_map[l3_name] = self.config_loader.get_ref_indicator(l3_name)

        # 使用 map 进行向量化查找（比 apply 快 10x+）
        df["core_indicator_type"] = df["l3_name"].map(core_map).fillna("PE")
        df["ref_indicator_type"] = df["l3_name"].map(ref_map).fillna("PB")

        return df

    def _extract_indicator_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """提取核心/参考指标值（向量化版本）"""
        df = df.copy()

        # 使用 np.select 进行向量化条件选择
        core_values = self._get_indicator_values_vectorized(df, "core_indicator_type")
        ref_values = self._get_indicator_values_vectorized(df, "ref_indicator_type")

        df["core_indicator_value"] = core_values
        df["ref_indicator_value"] = ref_values

        return df

    def _get_indicator_values_vectorized(
        self,
        df: pd.DataFrame,
        type_col: str
    ) -> pd.Series:
        """
        向量化提取指标值

        根据指标类型列，从对应的原始数据列中提取值。
        PE/PB/PS 必须 > 0 才有效，PEG 可以为任意值。
        """
        result = pd.Series(np.nan, index=df.index)

        # 按指标类型分组处理
        for indicator_type, col_name in self.INDICATOR_MAP.items():
            mask = df[type_col] == indicator_type
            if not mask.any():
                continue

            values = df.loc[mask, col_name]

            if indicator_type in ["PE", "PB", "PS"]:
                # PE/PB/PS 必须为正值
                valid = (values.notna()) & (values > 0)
                # 显式转换为 float，避免 Decimal 类型不兼容警告
                result.loc[mask & valid] = values[valid].astype(float)
            else:
                # PEG 可以为任意值
                valid = values.notna()
                result.loc[mask & valid] = values[valid].astype(float)

        return result

    def _assign_self_percentile(self, df: pd.DataFrame) -> pd.DataFrame:
        """从估值数据中提取自身历史分位（向量化版本）"""
        df = df.copy()

        # 使用向量化方法提取分位值
        df["core_indicator_pct_1250d"] = self._get_percentile_values_vectorized(
            df, "core_indicator_type"
        )
        df["ref_indicator_pct_1250d"] = self._get_percentile_values_vectorized(
            df, "ref_indicator_type"
        )

        return df

    def _get_percentile_values_vectorized(
        self,
        df: pd.DataFrame,
        type_col: str
    ) -> pd.Series:
        """
        向量化提取历史分位值

        根据指标类型列，从对应的分位列中提取值。
        """
        result = pd.Series(np.nan, index=df.index)

        # 按指标类型分组处理
        for indicator_type, col_name in self.INDICATOR_MAP.items():
            pct_col = f"{col_name}_pct_1250d"
            if pct_col not in df.columns:
                continue

            mask = df[type_col] == indicator_type
            if not mask.any():
                continue

            values = df.loc[mask, pct_col]
            valid = values.notna()
            # 显式转换为 float，避免 Decimal 类型不兼容警告
            result.loc[mask & valid] = values[valid].astype(float)

        return result

    def _calculate_industry_percentile(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算行业内相对分位

        对于每个交易日和每个行业，计算股票在行业内的相对分位。
        """
        df = df.copy()

        # 初始化分位列
        df["core_indicator_industry_pct"] = np.nan
        df["ref_indicator_industry_pct"] = np.nan

        # 按日期和行业分组计算
        # 只有当行业成员数 >= 3 时才计算行业内分位
        df = df.sort_values(["time", "l2_name", "symbol"])

        # 核心指标行业内分位
        df["core_indicator_industry_pct"] = self._calc_cross_sectional_percentile(
            df, "core_indicator_value"
        )

        # 参考指标行业内分位
        df["ref_indicator_industry_pct"] = self._calc_cross_sectional_percentile(
            df, "ref_indicator_value"
        )

        return df

    def _calc_cross_sectional_percentile(
        self,
        df: pd.DataFrame,
        value_col: str,
    ) -> pd.Series:
        """
        计算截面分位数（行业内相对分位）

        对于每个日期和行业组合，计算值在组内的分位。

        Args:
            df: 数据框
            value_col: 值列名

        Returns:
            分位数序列 (0-100)
        """
        def calc_group_pct(group: pd.DataFrame) -> pd.Series:
            """计算组内分位"""
            values = group[value_col]
            valid = values.dropna()

            # 至少需要3个有效值才计算
            if len(valid) < 3:
                return pd.Series([np.nan] * len(group), index=group.index)

            # 计算分位：每个值在组内的排名
            ranks = valid.rank(pct=True) * 100
            return ranks.reindex(group.index)

        # 按日期和行业分组（include_groups=False 避免 FutureWarning）
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="DataFrameGroupBy.apply operated on the grouping columns")
            result = df.groupby(["time", "l2_name"], group_keys=False).apply(
                calc_group_pct, include_groups=False
            )

        return result

    def _handle_exemptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理豁免情况"""
        df = df.copy()

        # 初始化豁免标记
        df["is_exempted"] = False
        df["exemption_reason"] = None

        # 1. 无行业分类（使用 l3_name 判断，因为配置查找基于 l3_name）
        no_industry_mask = df["l3_name"].isna()
        df.loc[no_industry_mask, "is_exempted"] = True
        df.loc[no_industry_mask, "exemption_reason"] = "NO_INDUSTRY_CLASSIFICATION"

        # 2. 核心指标无效（如亏损企业PE为负）
        invalid_core_mask = (
            df["core_indicator_value"].isna() &
            (~no_industry_mask)  # 排除已标记无行业的
        )
        df.loc[invalid_core_mask, "is_exempted"] = True
        df.loc[invalid_core_mask, "exemption_reason"] = "INVALID_CORE_INDICATOR"

        # 统计豁免情况
        exempted_count = df["is_exempted"].sum()
        if exempted_count > 0:
            reasons = df[df["is_exempted"]]["exemption_reason"].value_counts()
            logger.info(f"豁免统计: {exempted_count} 条, 原因分布: {reasons.to_dict()}")

        return df

    def _add_fscore_exemptions(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加 F-Score 豁免规则列

        根据行业配置获取每只股票的 F-Score 豁免规则。
        """
        df = df.copy()

        # 构建 l3_name -> exemptions 映射
        exemptions_map = {}
        for l3_name in df["l3_name"].dropna().unique():
            exemptions_map[l3_name] = self.config_loader.get_exemptions(l3_name)

        # 使用 map 进行向量化查找
        df["fscore_exemptions"] = df["l3_name"].map(exemptions_map)

        # 对于无行业分类的，设置为空列表（fillna 不支持列表，使用 apply）
        df["fscore_exemptions"] = df["fscore_exemptions"].apply(
            lambda x: x if isinstance(x, list) else []
        )

        return df

    def _select_output_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """选择输出列"""
        # 确保所有列都存在
        output_cols = []
        for col in self.OUTPUT_COLUMNS:
            if col in df.columns:
                output_cols.append(col)
            else:
                # 添加空列
                df[col] = None
                output_cols.append(col)

        return df[output_cols]

    def get_indicator_for_symbol(
        self,
        symbol: str,
        l3_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        获取指定股票的核心/参考指标类型

        Args:
            symbol: 股票代码
            l3_name: 三级行业名称（可选）

        Returns:
            (核心指标类型, 参考指标类型)
        """
        core = self.config_loader.get_core_indicator(l3_name)
        ref = self.config_loader.get_ref_indicator(l3_name)
        return core, ref

    def get_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        获取计算结果摘要

        Args:
            df: 计算结果 DataFrame

        Returns:
            摘要统计字典
        """
        summary = {
            "total_records": len(df),
            "unique_symbols": df["symbol"].nunique() if "symbol" in df.columns else 0,
            "unique_dates": df["time"].nunique() if "time" in df.columns else 0,
            "exempted_count": df["is_exempted"].sum() if "is_exempted" in df.columns else 0,
            "exemption_rate": (
                df["is_exempted"].sum() / len(df) * 100
                if len(df) > 0 and "is_exempted" in df.columns
                else 0
            ),
            "industry_coverage": {},
        }

        # 按行业统计
        if "l2_name" in df.columns:
            industry_stats = df.groupby("l2_name").agg({
                "symbol": "nunique",
                "is_exempted": "sum"
            }).rename(columns={"symbol": "stock_count"})

            for l2_name, row in industry_stats.iterrows():
                summary["industry_coverage"][l2_name] = {
                    "stock_count": int(row["stock_count"]),
                    "exempted_count": int(row["is_exempted"]),
                }

        return summary