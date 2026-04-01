"""
中国宏观周期预处理计算器

基于 ValueInvesting 项目中的中国宏观周期算法，按月度对齐 M2/PPI/PMI/GDP，
并生成：
1. 原始宏观阶段 raw_phase
2. 平滑宏观阶段 stable_phase
3. 基于 industry_config.json 的行业匹配快照
"""

from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from ..fundamental.industry_config import get_industry_config_loader


CN_PHASE_METADATA: Dict[str, Dict[str, object]] = {
    "REFLATION": {"label": "再通胀", "color": "#60a5fa", "y": 0},
    "RECOVERY": {"label": "复苏", "color": "#4ade80", "y": 1},
    "OVERHEAT": {"label": "过热", "color": "#f87171", "y": 2},
    "STAGFLATION": {"label": "滞胀", "color": "#fbbf24", "y": 3},
}


class MacroCycleCalculator:
    """中国宏观周期计算器。"""

    PHASE_COLUMNS = [
        "time",
        "observation_time",
        "m2_yoy",
        "gdp_yoy",
        "ppi_yoy",
        "pmi",
        "credit_impulse",
        "raw_phase",
        "stable_phase",
        "raw_phase_changed",
        "stable_phase_changed",
    ]

    INDUSTRY_COLUMNS = [
        "time",
        "observation_time",
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "config_macro_cycle",
        "core_indicator",
        "ref_indicator",
        "logic",
        "fscore_exemptions",
        "is_present_in_sw_member",
        "matches_raw_phase",
        "matches_stable_phase",
    ]

    def __init__(self, config_path: Optional[str] = None):
        self.config_loader = get_industry_config_loader(config_path)

    def calculate(
        self,
        m_df: Optional[pd.DataFrame],
        ppi_df: Optional[pd.DataFrame],
        pmi_df: Optional[pd.DataFrame],
        gdp_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """计算中国宏观周期月度主表。"""
        aligned = self._align_macro_frames(m_df, ppi_df, pmi_df, gdp_df)
        if aligned.empty:
            return pd.DataFrame(columns=self.PHASE_COLUMNS)

        aligned = aligned.sort_values("observation_time").reset_index(drop=True)
        aligned["credit_impulse"] = aligned["m2_yoy"] - aligned["gdp_yoy"]
        aligned["raw_phase"] = aligned.apply(
            lambda row: self.determine_phase(
                row["credit_impulse"], row["ppi_yoy"], row["pmi"]
            ),
            axis=1,
        )
        aligned["time"] = self._shift_to_effective_month(aligned["observation_time"])
        aligned["stable_phase"] = self._build_stable_phases(
            aligned["raw_phase"].tolist()
        )
        aligned["raw_phase_changed"] = aligned["raw_phase"].ne(
            aligned["raw_phase"].shift(1)
        )
        aligned["stable_phase_changed"] = aligned["stable_phase"].ne(
            aligned["stable_phase"].shift(1)
        )
        aligned.loc[aligned.index[0], ["raw_phase_changed", "stable_phase_changed"]] = False

        logger.info(
            "中国宏观周期计算完成: {} 条月份记录，范围 {} ~ {}",
            len(aligned),
            aligned["observation_time"].min(),
            aligned["observation_time"].max(),
        )
        return aligned[self.PHASE_COLUMNS].copy()

    def build_industry_snapshot(
        self,
        phase_df: pd.DataFrame,
        industry_members_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """构建月度行业配置快照。"""
        if phase_df.empty:
            return pd.DataFrame(columns=self.INDUSTRY_COLUMNS)

        industry_dim = self._build_industry_dimension(industry_members_df)
        phase_base = phase_df[
            ["time", "observation_time", "raw_phase", "stable_phase"]
        ].copy()
        phase_base["_merge_key"] = 1
        industry_dim["_merge_key"] = 1

        snapshot = phase_base.merge(industry_dim, on="_merge_key", how="inner").drop(
            columns="_merge_key"
        )
        snapshot["matches_raw_phase"] = (
            snapshot["config_macro_cycle"] == snapshot["raw_phase"]
        )
        snapshot["matches_stable_phase"] = (
            snapshot["config_macro_cycle"] == snapshot["stable_phase"]
        )

        result = snapshot[self.INDUSTRY_COLUMNS].sort_values(
            ["time", "l3_name"]
        ).reset_index(drop=True)
        logger.info("行业快照生成完成: {} 条记录", len(result))
        return result

    @staticmethod
    def determine_phase(
        credit_impulse: float,
        ppi_yoy: float,
        pmi: float,
    ) -> str:
        """复刻 ValueInvesting 的中国宏观周期判定逻辑。"""
        if credit_impulse > 0:
            if ppi_yoy < 0 and pmi < 52:
                return "REFLATION"
            return "RECOVERY"

        if ppi_yoy > 2.0:
            if pmi > 50:
                return "OVERHEAT"
            return "STAGFLATION"

        if pmi < 50:
            return "STAGFLATION"
        return "RECOVERY"

    def _align_macro_frames(
        self,
        m_df: Optional[pd.DataFrame],
        ppi_df: Optional[pd.DataFrame],
        pmi_df: Optional[pd.DataFrame],
        gdp_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """按 observation_time 对齐四类宏观数据。"""
        if any(df is None or df.empty for df in [m_df, ppi_df, pmi_df, gdp_df]):
            logger.warning("宏观原始数据不完整，跳过宏观周期计算")
            return pd.DataFrame()

        m2 = m_df.copy()
        ppi = ppi_df.copy()
        pmi = pmi_df.copy()
        gdp = gdp_df.copy()

        m2["observation_time"] = self._normalize_month_end(m2["time"])
        ppi["observation_time"] = self._normalize_month_end(ppi["time"])
        pmi["observation_time"] = self._normalize_month_end(pmi["time"])
        gdp["quarter_time"] = self._normalize_month_end(gdp["time"])

        pmi_col = self._resolve_pmi_column(pmi)
        if pmi_col is None:
            logger.warning("PMI 数据中既没有 pmi010000 也没有 pmi030000，跳过计算")
            return pd.DataFrame()

        m2 = m2[["observation_time", "m2_yoy"]].dropna(subset=["m2_yoy"])
        ppi = ppi[["observation_time", "ppi_yoy"]].dropna(subset=["ppi_yoy"])
        pmi = pmi[["observation_time", pmi_col]].rename(columns={pmi_col: "pmi"})
        pmi = pmi.dropna(subset=["pmi"])
        gdp = gdp[["quarter_time", "gdp_yoy"]].dropna(subset=["gdp_yoy"])

        merged = pd.merge_asof(
            m2.sort_values("observation_time"),
            gdp.sort_values("quarter_time"),
            left_on="observation_time",
            right_on="quarter_time",
            direction="backward",
        ).drop(columns="quarter_time")

        merged = merged.merge(
            ppi.sort_values("observation_time"),
            on="observation_time",
            how="inner",
        )
        merged = merged.merge(
            pmi.sort_values("observation_time"),
            on="observation_time",
            how="inner",
        )
        merged = merged.dropna(subset=["m2_yoy", "gdp_yoy", "ppi_yoy", "pmi"])

        return merged

    def _build_industry_dimension(
        self,
        industry_members_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """构建配置驱动的三级行业维表。"""
        records: List[Dict[str, object]] = []
        for l3_name in sorted(self.config_loader.get_all_industries()):
            config = self.config_loader.get_industry_config(l3_name)
            records.append(
                {
                    "l3_name": l3_name,
                    "config_macro_cycle": config.get("macro_cycle"),
                    "core_indicator": config.get("core_indicator"),
                    "ref_indicator": config.get("ref_indicator"),
                    "logic": config.get("logic"),
                    "fscore_exemptions": config.get("exemptions", []),
                }
            )

        config_df = pd.DataFrame(records)
        mapping_df = self._build_sw_mapping(industry_members_df)
        industry_dim = config_df.merge(mapping_df, on="l3_name", how="left")
        industry_dim["is_present_in_sw_member"] = industry_dim["l3_code"].notna()

        return industry_dim[
            [
                "l1_code",
                "l1_name",
                "l2_code",
                "l2_name",
                "l3_code",
                "l3_name",
                "config_macro_cycle",
                "core_indicator",
                "ref_indicator",
                "logic",
                "fscore_exemptions",
                "is_present_in_sw_member",
            ]
        ].copy()

    @staticmethod
    def _build_sw_mapping(
        industry_members_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """构建当前 SW 三级行业到层级编码的映射。"""
        columns = ["l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name"]
        if industry_members_df is None or industry_members_df.empty:
            return pd.DataFrame(columns=columns)

        current = industry_members_df.copy()
        if "is_new" in current.columns:
            current = current[current["is_new"] == "Y"]

        mapping = (
            current[columns]
            .dropna(subset=["l3_name"])
            .sort_values(["l3_name", "l3_code"])
            .drop_duplicates(subset=["l3_name"], keep="first")
        )
        return mapping

    @staticmethod
    def _resolve_pmi_column(pmi_df: pd.DataFrame) -> Optional[str]:
        """优先使用制造业 PMI，缺失时回退到综合 PMI。"""
        for col in ["pmi010000", "pmi030000"]:
            if col in pmi_df.columns and not pmi_df[col].dropna().empty:
                return col
        return None

    @staticmethod
    def _build_stable_phases(raw_phases: List[str]) -> List[str]:
        """构建两个月确认的平滑阶段。"""
        if not raw_phases:
            return []

        stable_phases = [raw_phases[0]]
        for i in range(1, len(raw_phases)):
            prev_stable = stable_phases[-1]
            current_raw = raw_phases[i]
            prev_raw = raw_phases[i - 1]

            if current_raw == prev_stable:
                stable_phases.append(prev_stable)
            elif current_raw == prev_raw:
                stable_phases.append(current_raw)
            else:
                stable_phases.append(prev_stable)

        return stable_phases

    @staticmethod
    def _normalize_month_end(series: pd.Series) -> pd.Series:
        """统一到 Asia/Shanghai 月末 15:00:00。"""
        dt = pd.to_datetime(series)
        if isinstance(dt.dtype, pd.DatetimeTZDtype):
            localized = dt.dt.tz_convert("Asia/Shanghai")
        else:
            localized = dt.dt.tz_localize("Asia/Shanghai")

        naive = localized.dt.tz_localize(None)
        month_end = naive.dt.to_period("M").dt.to_timestamp("M")
        month_end = month_end + pd.Timedelta(hours=15)
        return month_end.dt.tz_localize("Asia/Shanghai")

    @staticmethod
    def _shift_to_effective_month(observation_series: pd.Series) -> pd.Series:
        """整体后移一个月，作为可交易生效月份。"""
        dt = pd.to_datetime(observation_series)
        if isinstance(dt.dtype, pd.DatetimeTZDtype):
            localized = dt.dt.tz_convert("Asia/Shanghai")
        else:
            localized = dt.dt.tz_localize("Asia/Shanghai")

        shifted = localized.dt.tz_localize(None) + pd.offsets.MonthEnd(1)
        return shifted.dt.tz_localize("Asia/Shanghai")
