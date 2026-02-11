"""
Piotroski F-Score 财务质量评分及补充基本面指标

F-Score 是一个 0-9 分的评分系统，用于评估公司财务健康状况。
由 Joseph Piotroski 于 2000 年提出。

评分维度（共9项，每项1分）：

1. 盈利能力 (4分)
   - F_ROA: ROA TTM > 0
   - F_CFO: 经营现金流 TTM > 0
   - F_ΔROA: ROA TTM 同比增长
   - F_ACCRUAL: 经营现金流 TTM > 净利润 TTM

2. 财务杠杆/流动性 (3分)
   - F_ΔLEVER: 资产负债率下降
   - F_ΔLIQUID: 流动比率上升
   - F_EQ_OFFER: 未增发新股

3. 运营效率 (2分)
   - F_ΔMARGIN: 毛利率TTM上升（基于q_gsprofit_margin单季度数据）
   - F_ΔTURN: 资产周转率TTM上升

行业豁免规则:
- f_score_cfo_positive: 豁免 CFO > 0 评测
- f_score_cfo: 豁免 CFO > Net Income 评测
- f_score_leverage: 豁免负债率改善评测
- f_score_current_ratio: 豁免流动比率改善评测
- f_score_gross_margin: 豁免毛利率改善评测

数据来源：
- fina_indicator: ROA(累计→TTM), q_gsprofit_margin(单季度→TTM), 
                   assets_turn(累计→TTM), current_ratio(时点值), q_roe(单季度)
- balancesheet: 总资产, 总负债, 流动资产, 流动负债, 股本变化 (均为时点值)
- cashflow: 经营现金流 n_cashflow_act(累计→TTM)
- income: 净利润 n_income(累计→TTM)

TTM 处理说明：
- 累计值→TTM: Q4直接使用, Q1-Q3用 本期累计 + (上年年报 - 上年同期累计)
- 单季度值→TTM: 直接 rolling(4) 聚合（mean/sum）
"""

from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger
import json
from pathlib import Path


class FScoreCalculator:
    """
    Piotroski F-Score 计算器
    
    根据财务报表数据计算 F-Score，支持行业豁免规则。
    所有累计值指标均转换为TTM后再评分，消除季度间不可比性。
    
    示例:
        >>> calculator = FScoreCalculator()
        >>> result = calculator.calculate(
        ...     fina_indicator=fina_df,
        ...     balancesheet=bs_df,
        ...     cashflow=cf_df,
        ...     income=inc_df,
        ...     exemptions=["f_score_cfo_positive"]
        ... )
    """
    
    # F-Score 各项得分列名
    SCORE_COLUMNS = [
        "f_score",
        "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
        "f_delta_lever", "f_delta_liquid", "f_eq_offer",
        "f_delta_margin", "f_delta_turn"
    ]
    
    # 补充指标列名
    EXTRA_COLUMNS = [
        "roa_ttm", "roe_5y_avg", "ni_cfo_corr_3y", "debt_ratio", "current_ratio"
    ]
    
    def __init__(self, industry_config_path: Optional[str] = None):
        """
        初始化 F-Score 计算器
        
        Args:
            industry_config_path: 行业配置文件路径,默认使用项目根目录下的 industry_config.json
        """
        self.industry_config = {}
        if industry_config_path:
            self._load_industry_config(industry_config_path)
    
    def _load_industry_config(self, config_path: str):
        """加载行业配置"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.industry_config = json.load(f)
            logger.info(f"Loaded industry config with {len(self.industry_config)} industries")
        except Exception as e:
            logger.warning(f"Failed to load industry config: {e}")
    
    def get_exemptions_for_industry(self, industry_name: str) -> List[str]:
        """获取行业的豁免规则"""
        config = self.industry_config.get(industry_name, {})
        return config.get("exemptions", [])
    
    @property
    def columns(self) -> List[str]:
        return self.SCORE_COLUMNS + self.EXTRA_COLUMNS
    
    def calculate(
        self,
        fina_indicator: pd.DataFrame,
        balancesheet: pd.DataFrame,
        cashflow: pd.DataFrame,
        income: pd.DataFrame,
        exemptions: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        计算 F-Score 及补充指标
        
        Args:
            fina_indicator: 财务指标数据，需包含 ts_code, end_date, roa, 
                           q_gsprofit_margin, assets_turn, current_ratio, q_roe
            balancesheet: 资产负债表数据，需包含 ts_code, end_date, 
                         total_assets, total_liab, total_cur_assets, 
                         total_cur_liab, total_share
            cashflow: 现金流量表数据，需包含 ts_code, end_date, n_cashflow_act
            income: 利润表数据，需包含 ts_code, end_date, n_income
            exemptions: 豁免规则列表
            
        Returns:
            包含 F-Score 各项得分及补充指标的 DataFrame
        """
        exemptions = exemptions or []
        
        # 合并财务数据
        df = self._merge_financial_data(
            fina_indicator, balancesheet, cashflow, income
        )
        
        if df.empty:
            logger.warning("No valid financial data to calculate F-Score")
            return pd.DataFrame(columns=self.columns)
        
        # 按股票分组计算
        result_list = []
        
        for ts_code, group in df.groupby("ts_code"):
            scored = self._calc_single_stock(group, exemptions)
            result_list.append(scored)
        
        if not result_list:
            return pd.DataFrame(columns=self.columns)
        
        result = pd.concat(result_list, ignore_index=True)
        
        logger.debug(f"Calculated F-Score for {result['ts_code'].nunique()} stocks")
        return result
    
    def _merge_financial_data(
        self,
        fina_indicator: pd.DataFrame,
        balancesheet: pd.DataFrame,
        cashflow: pd.DataFrame,
        income: pd.DataFrame
    ) -> pd.DataFrame:
        """合并财务数据"""
        # 标准化日期列名
        def standardize_date(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            if "end_date" in df.columns and "end_date_time" not in df.columns:
                df["end_date_time"] = pd.to_datetime(df["end_date"])
            return df
        
        fina_indicator = standardize_date(fina_indicator)
        balancesheet = standardize_date(balancesheet)
        cashflow = standardize_date(cashflow)
        income = standardize_date(income)
        
        # 选择必要的列(来自fina_indicator)
        # q_gsprofit_margin: 单季度毛利率，用于 F_ΔMARGIN TTM
        # q_roe: 单季度ROE，用于 roe_5y_avg（20期滚动）
        fina_cols = ["ts_code", "end_date_time", "ann_date", "roa", "roe", "roe_yearly",
                     "grossprofit_margin", "q_gsprofit_margin", "q_roe",
                     "assets_turn", "current_ratio"]
        
        # 选择必要的列(来自balancesheet)
        bs_cols = ["ts_code", "end_date_time", "f_ann_date", "total_assets", 
                   "total_liab", "total_ncl", "total_cur_assets", "total_cur_liab", "total_share"]
        
        # 选择必要的列(来自cashflow)
        cf_cols = ["ts_code", "end_date_time", "f_ann_date", "n_cashflow_act"]
        
        # 选择必要的列(来自income)
        inc_cols = ["ts_code", "end_date_time", "f_ann_date", "n_income", "total_revenue"]
        
        # 只保留存在的列
        fina_cols = [c for c in fina_cols if c in fina_indicator.columns]
        bs_cols = [c for c in bs_cols if c in balancesheet.columns]
        cf_cols = [c for c in cf_cols if c in cashflow.columns]
        inc_cols = [c for c in inc_cols if c in income.columns]
        
        # 合并
        df = fina_indicator[fina_cols].merge(
            balancesheet[bs_cols], 
            on=["ts_code", "end_date_time"], 
            how="left",
            suffixes=('', '_bs')
        ).merge(
            cashflow[cf_cols], 
            on=["ts_code", "end_date_time"], 
            how="left",
            suffixes=('', '_cf')
        ).merge(
            income[inc_cols], 
            on=["ts_code", "end_date_time"], 
            how="left",
            suffixes=('', '_inc')
        )
        
        # 选择最准确的 f_ann_date (优先使用 cashflow/balancesheet)
        if "f_ann_date" in df.columns:
            df["f_ann_date_final"] = df["f_ann_date"]
        elif "f_ann_date_bs" in df.columns:
            df["f_ann_date_final"] = df["f_ann_date_bs"]
        elif "f_ann_date_cf" in df.columns:
            df["f_ann_date_final"] = df["f_ann_date_cf"]
        elif "ann_date" in df.columns:
            df["f_ann_date_final"] = df["ann_date"]
        
        return df.sort_values(["ts_code", "end_date_time"])
    
    def _calc_single_stock(
        self, 
        group: pd.DataFrame, 
        exemptions: List[str]
    ) -> pd.DataFrame:
        """计算单只股票的 F-Score 及补充指标"""
        result = group.copy()
        
        # 确保按时间排序
        result = result.sort_values("end_date_time")
        
        # === 预计算 TTM 值 ===
        # 累计值 → TTM（roa, n_cashflow_act, n_income, assets_turn）
        result["roa_ttm"] = self._calc_cumulative_to_ttm(result, "roa")
        result["cfo_ttm"] = self._calc_cumulative_to_ttm(result, "n_cashflow_act")
        result["ni_ttm"] = self._calc_cumulative_to_ttm(result, "n_income")
        result["at_ttm"] = self._calc_cumulative_to_ttm(result, "assets_turn")
        
        # 单季度值 → TTM（q_gsprofit_margin 用 rolling mean）
        result["gpm_ttm"] = self._calc_quarterly_ttm(result, "q_gsprofit_margin", agg="mean")
        
        # === 1. 盈利能力指标 (4分) ===
        
        # F_ROA: ROA TTM > 0
        result["f_roa"] = (result["roa_ttm"] > 0).fillna(False).astype(int)
        
        # F_CFO: 经营现金流 TTM > 0
        if "f_score_cfo_positive" in exemptions:
            result["f_cfo"] = 1  # 豁免,给满分
        elif "n_cashflow_act" in result.columns:
            result["f_cfo"] = (result["cfo_ttm"] > 0).fillna(False).astype(int)
        else:
            result["f_cfo"] = 0
        
        # F_ΔROA: ROA TTM 同比增长
        result["f_delta_roa"] = self._calc_yoy_improvement(result, "roa_ttm")
        
        # F_ACCRUAL: 经营现金流 TTM > 净利润 TTM
        if "f_score_cfo" in exemptions:
            result["f_accrual"] = 1  # 豁免,给满分
        elif "n_cashflow_act" in result.columns and "n_income" in result.columns:
            result["f_accrual"] = (
                result["cfo_ttm"] > result["ni_ttm"]
            ).fillna(False).astype(int)
        else:
            result["f_accrual"] = 0
        
        # === 2. 财务杠杆/流动性 (3分) ===
        
        # F_ΔLEVER: 非流动负债占比下降 (使用 total_ncl / total_assets, 时点值)
        if "f_score_leverage" in exemptions:
            result["f_delta_lever"] = 1  # 豁免,给满分
        elif "total_ncl" in result.columns and "total_assets" in result.columns:
            lever = result["total_ncl"] / result["total_assets"].replace(0, np.nan)
            result["f_delta_lever"] = self._calc_yoy_improvement(
                result.assign(_lever=lever), "_lever", lower_is_better=True
            )
        else:
            result["f_delta_lever"] = 0
        
        # F_ΔLIQUID: 流动比率上升 (时点值比率,无需TTM)
        if "f_score_current_ratio" in exemptions:
            result["f_delta_liquid"] = 1  # 豁免,给满分
        elif "current_ratio" in result.columns:
            result["f_delta_liquid"] = self._calc_yoy_improvement(result, "current_ratio")
        elif "total_cur_assets" in result.columns and "total_cur_liab" in result.columns:
            cr = result["total_cur_assets"] / result["total_cur_liab"].replace(0, np.nan)
            result["f_delta_liquid"] = self._calc_yoy_improvement(
                result.assign(_cr=cr), "_cr"
            )
        else:
            result["f_delta_liquid"] = 0
        
        # F_EQ_OFFER: 未增发新股 (时点值)
        if "total_share" in result.columns:
            result["f_eq_offer"] = self._calc_yoy_improvement(
                result, "total_share", lower_is_better=True, allow_equal=True
            )
        else:
            result["f_eq_offer"] = 1  # 默认假设未增发
        
        # === 3. 运营效率 (2分) ===
        
        # F_ΔMARGIN: 毛利率 TTM 上升（基于 q_gsprofit_margin 单季度 rolling mean）
        if "f_score_gross_margin" in exemptions:
            result["f_delta_margin"] = 1  # 豁免,给满分
        elif "gpm_ttm" in result.columns and result["gpm_ttm"].notna().any():
            result["f_delta_margin"] = self._calc_yoy_improvement(result, "gpm_ttm")
        elif "grossprofit_margin" in result.columns:
            # 降级方案：无 q_gsprofit_margin 时，使用累计值 TTM
            gpm_ttm_fallback = self._calc_cumulative_to_ttm(result, "grossprofit_margin")
            result["f_delta_margin"] = self._calc_yoy_improvement(
                result.assign(_gpm_ttm=gpm_ttm_fallback), "_gpm_ttm"
            )
        else:
            result["f_delta_margin"] = 0
        
        # F_ΔTURN: 资产周转率 TTM 上升
        if "assets_turn" in result.columns:
            result["f_delta_turn"] = self._calc_yoy_improvement(result, "at_ttm")
        elif "total_revenue" in result.columns and "total_assets" in result.columns:
            at = result["total_revenue"] / result["total_assets"].replace(0, np.nan)
            at_ttm_fallback = self._calc_cumulative_to_ttm(
                result.assign(_at_raw=at), "_at_raw"
            )
            result["f_delta_turn"] = self._calc_yoy_improvement(
                result.assign(_at_ttm=at_ttm_fallback), "_at_ttm"
            )
        else:
            result["f_delta_turn"] = 0
        
        # === 计算总分 ===
        f_cols = [
            "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
            "f_delta_lever", "f_delta_liquid", "f_eq_offer",
            "f_delta_margin", "f_delta_turn"
        ]
        result["f_score"] = result[f_cols].sum(axis=1)
        
        # === 补充指标计算 ===
        
        # 5年平均ROE（使用 q_roe 单季度数据，20期滚动求和/5）
        result["roe_5y_avg"] = self._calc_roe_5y_avg(result)
        
        # 3年净利润-经营现金流相关性（使用日期范围筛选）
        result["ni_cfo_corr_3y"] = self._calc_ni_cfo_corr_3y(result)
        
        # 资产负债率 (时点值)
        if "total_liab" in result.columns and "total_assets" in result.columns:
            result["debt_ratio"] = (
                result["total_liab"] / result["total_assets"].replace(0, np.nan) * 100
            )
        else:
            result["debt_ratio"] = np.nan
        
        # 流动比率 (时点值)
        if "current_ratio" in result.columns:
            result["current_ratio"] = result["current_ratio"]
        elif "total_cur_assets" in result.columns and "total_cur_liab" in result.columns:
            result["current_ratio"] = (
                result["total_cur_assets"] / result["total_cur_liab"].replace(0, np.nan)
            )
        
        return result
    
    def _calc_yoy_improvement(
        self, 
        df: pd.DataFrame, 
        col: str, 
        lower_is_better: bool = False,
        allow_equal: bool = False
    ) -> pd.Series:
        """
        计算同比改善 (与去年同期比较)
        
        Args:
            df: 数据框
            col: 列名
            lower_is_better: 是否越低越好
            allow_equal: 是否允许相等也算改善
        
        Returns:
            改善标志 (0 或 1)
        """
        if col not in df.columns:
            return pd.Series([0] * len(df), index=df.index)
        
        # 同比:与4个季度前比较
        prev = df[col].shift(4)
        current = df[col]
        
        if lower_is_better:
            if allow_equal:
                improved = current <= prev
            else:
                improved = current < prev
        else:
            if allow_equal:
                improved = current >= prev
            else:
                improved = current > prev
        
        return improved.fillna(False).astype(int)
    
    def _calc_cumulative_to_ttm(
        self, 
        df: pd.DataFrame,
        col: str
    ) -> pd.Series:
        """
        将当年累计值转换为 TTM（滚动12个月）值
        
        适用于所有累计值指标（roa, n_cashflow_act, n_income, assets_turn 等）。
        
        累计值转 TTM 公式:
            Q4(年报): TTM = 年报值本身
            Q1-Q3:   TTM = 本期累计 + (上年年报 - 上年同期累计)
        
        Args:
            df: 按时间排序的单只股票数据框，需包含目标列和 end_date_time
            col: 累计值列名
        
        Returns:
            TTM 值 Series
        """
        if col not in df.columns or "end_date_time" not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        result = pd.Series([np.nan] * len(df), index=df.index, dtype=float)
        
        # 提取季度信息
        end_dates = pd.to_datetime(df["end_date_time"])
        months = end_dates.dt.month
        years = end_dates.dt.year
        values = pd.to_numeric(df[col], errors="coerce")
        
        # 构建快速查找字典: (year, month) -> value
        lookup = {}
        for y, m, val in zip(years, months, values):
            if pd.notna(val):
                lookup[(int(y), int(m))] = val
        
        for i in range(len(df)):
            cur_month = int(months.iloc[i])
            cur_year = int(years.iloc[i])
            cur_val = values.iloc[i]
            
            if pd.isna(cur_val):
                continue
            
            if cur_month == 12:
                # Q4 年报: TTM 就是年报值本身
                result.iloc[i] = cur_val
            else:
                # Q1/Q2/Q3: TTM = 本期累计 + (上年年报 - 上年同期累计)
                prev_annual = lookup.get((cur_year - 1, 12))
                prev_same_q = lookup.get((cur_year - 1, cur_month))
                
                if prev_annual is not None and prev_same_q is not None:
                    result.iloc[i] = cur_val + (prev_annual - prev_same_q)
        
        return result
    
    def _calc_quarterly_ttm(
        self,
        df: pd.DataFrame,
        col: str,
        agg: str = "mean"
    ) -> pd.Series:
        """
        对单季度值计算 TTM（滚动4期聚合）
        
        适用于 q_ 开头的单季度数据（如 q_gsprofit_margin, q_roe）。
        这些数据已经是单季度值，无需从累计值反推，直接滚动聚合即可。
        
        Args:
            df: 按时间排序的单只股票数据框
            col: 单季度值列名
            agg: 聚合方式，"mean" 求均值（适合比率指标），"sum" 求和（适合绝对值）
        
        Returns:
            TTM 值 Series
        """
        if col not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        values = pd.to_numeric(df[col], errors="coerce")
        
        if agg == "mean":
            return values.rolling(window=4, min_periods=4).mean()
        elif agg == "sum":
            return values.rolling(window=4, min_periods=4).sum()
        else:
            raise ValueError(f"Unsupported aggregation: {agg}. Use 'mean' or 'sum'.")
    
    def _calc_roa_ttm(
        self, 
        df: pd.DataFrame
    ) -> pd.Series:
        """
        从累计 ROA 计算 TTM（滚动12个月）ROA
        
        保留此方法以兼容现有调用。内部委托给通用的 _calc_cumulative_to_ttm。
        
        Args:
            df: 按时间排序的单只股票数据框，需包含 roa 和 end_date_time
        
        Returns:
            TTM ROA Series
        """
        return self._calc_cumulative_to_ttm(df, "roa")
    
    def _calc_roe_5y_avg(
        self, 
        df: pd.DataFrame
    ) -> pd.Series:
        """
        计算5年平均ROE
        
        使用 q_roe（单季度ROE）滚动20期求和后除以5，得到5年年均ROE。
        相比 roe_yearly 的简单年化（如Q1×4），此方法更准确、无季节性偏差。
        
        降级方案：若无 q_roe，使用 roe_yearly 按年取末值求均值。
        
        Args:
            df: 按时间排序的单只股票数据框
        
        Returns:
            5年平均ROE Series
        """
        # 优先使用 q_roe（单季度值，20期滚动）
        if "q_roe" in df.columns and df["q_roe"].notna().any():
            q_roe = pd.to_numeric(df["q_roe"], errors="coerce")
            # 20期滚动求和 / 5 = 5年年均ROE
            return q_roe.rolling(window=20, min_periods=4).sum() / 5
        
        # 降级方案：使用 roe_yearly
        if "roe_yearly" not in df.columns and "roe" not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        actual_col = "roe_yearly" if "roe_yearly" in df.columns else "roe"
        
        result = []
        for idx, row in df.iterrows():
            current_date = row.get("end_date_time")
            if pd.isna(current_date):
                result.append(np.nan)
                continue
            
            # 筛选截至当前日期的数据
            df_temp = df[df["end_date_time"] <= current_date].copy()
            df_temp["year"] = pd.to_datetime(df_temp["end_date_time"]).dt.year
            
            # 每年取最后一条记录
            df_annual = df_temp.sort_values("end_date_time").groupby("year").tail(1)
            
            # 取最近5年
            df_recent = df_annual.tail(5)
            
            if len(df_recent) >= 1:
                avg = df_recent[actual_col].mean()
                result.append(avg)
            else:
                result.append(np.nan)
        
        return pd.Series(result, index=df.index)
    
    def _calc_ni_cfo_corr_3y(
        self, 
        df: pd.DataFrame
    ) -> pd.Series:
        """
        计算3年净利润-经营现金流相关性（参考实现：筛选最近3年数据，计算单个相关性）
        
        对于每一行，计算截至该行日期的3年相关性
        """
        if "n_income" not in df.columns or "n_cashflow_act" not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        
        result = []
        for idx, row in df.iterrows():
            current_date = row.get("end_date_time")
            if pd.isna(current_date):
                result.append(np.nan)
                continue
            
            # 计算3年前的日期
            three_years_ago = current_date - pd.DateOffset(years=3)
            
            # 筛选3年内的数据
            df_3y = df[
                (df["end_date_time"] > three_years_ago) & 
                (df["end_date_time"] <= current_date)
            ].copy()
            
            if len(df_3y) > 2:
                income_series = pd.to_numeric(df_3y["n_income"], errors="coerce")
                cflow_series = pd.to_numeric(df_3y["n_cashflow_act"], errors="coerce")
                
                valid_mask = ~(income_series.isna() | cflow_series.isna())
                if valid_mask.sum() > 2:
                    corr = float(income_series[valid_mask].corr(cflow_series[valid_mask]))
                    result.append(corr)
                else:
                    result.append(np.nan)
            else:
                result.append(np.nan)
        
        return pd.Series(result, index=df.index)
    
    def get_score_level(self, score: int) -> str:
        """
        获取 F-Score 等级描述
        
        Args:
            score: F-Score 分数 (0-9)
            
        Returns:
            等级描述
        """
        if score >= 8:
            return "优秀"
        elif score >= 5:
            return "良好"
        elif score >= 3:
            return "一般"
        else:
            return "较差"
    
    def get_score_breakdown(self, row: pd.Series) -> Dict[str, Any]:
        """
        获取 F-Score 详细分解
        
        Args:
            row: 包含 F-Score 各项得分的 Series
            
        Returns:
            详细分解字典
        """
        return {
            "总分": int(row.get("f_score", 0)),
            "等级": self.get_score_level(int(row.get("f_score", 0))),
            "盈利能力": {
                "ROA正值": bool(row.get("f_roa", 0)),
                "经营现金流正值": bool(row.get("f_cfo", 0)),
                "ROA增长": bool(row.get("f_delta_roa", 0)),
                "盈余质量": bool(row.get("f_accrual", 0)),
            },
            "财务安全": {
                "负债率下降": bool(row.get("f_delta_lever", 0)),
                "流动性改善": bool(row.get("f_delta_liquid", 0)),
                "未增发股份": bool(row.get("f_eq_offer", 0)),
            },
            "运营效率": {
                "毛利率提升": bool(row.get("f_delta_margin", 0)),
                "周转率提升": bool(row.get("f_delta_turn", 0)),
            },
            "补充指标": {
                "5年平均ROE": float(row.get("roe_5y_avg", 0)) if pd.notna(row.get("roe_5y_avg")) else None,
                "3年NI-CFO相关性": float(row.get("ni_cfo_corr_3y", 0)) if pd.notna(row.get("ni_cfo_corr_3y")) else None,
                "资产负债率": float(row.get("debt_ratio", 0)) if pd.notna(row.get("debt_ratio")) else None,
                "流动比率": float(row.get("current_ratio", 0)) if pd.notna(row.get("current_ratio")) else None,
            }
        }
