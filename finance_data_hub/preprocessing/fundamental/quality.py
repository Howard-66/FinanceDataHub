"""
Piotroski F-Score 财务质量评分

F-Score 是一个 0-9 分的评分系统，用于评估公司财务健康状况。
由 Joseph Piotroski 于 2000 年提出。

评分维度（共9项，每项1分）：

1. 盈利能力 (4分)
   - F_ROA: ROA > 0
   - F_CFO: 经营现金流 > 0
   - F_ΔROA: ROA 同比增长
   - F_ACCRUAL: 经营现金流 > 净利润

2. 财务杠杆/流动性 (3分)
   - F_ΔLEVER: 长期负债率下降
   - F_ΔLIQUID: 流动比率上升
   - F_EQ_OFFER: 未增发新股

3. 运营效率 (2分)
   - F_ΔMARGIN: 毛利率上升
   - F_ΔTURN: 资产周转率上升

评分解读：
- 8-9 分：财务状况优秀
- 5-7 分：财务状况良好
- 3-4 分：财务状况一般
- 0-2 分：财务状况较差

数据来源：
- fina_indicator: ROA, 毛利率, 资产周转率
- balancesheet: 长期负债, 流动比率, 股本变化
- cashflow: 经营现金流
- income: 净利润
"""

from typing import Optional, List, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger


class FScoreCalculator:
    """
    Piotroski F-Score 计算器
    
    根据财务报表数据计算 F-Score。
    
    示例:
        >>> calculator = FScoreCalculator()
        >>> result = calculator.calculate(
        ...     fina_indicator=fina_df,
        ...     balancesheet=bs_df,
        ...     cashflow=cf_df,
        ...     income=inc_df
        ... )
    """
    
    # F-Score 各项得分列名
    SCORE_COLUMNS = [
        "f_score",
        "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
        "f_delta_lever", "f_delta_liquid", "f_eq_offer",
        "f_delta_margin", "f_delta_turn"
    ]
    
    def __init__(self):
        pass
    
    @property
    def columns(self) -> List[str]:
        return self.SCORE_COLUMNS
    
    def calculate(
        self,
        fina_indicator: pd.DataFrame,
        balancesheet: pd.DataFrame,
        cashflow: pd.DataFrame,
        income: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算 F-Score
        
        Args:
            fina_indicator: 财务指标数据，需包含 ts_code, end_date, roa, 
                           grossprofit_margin, assets_turn, current_ratio
            balancesheet: 资产负债表数据，需包含 ts_code, end_date, 
                         lt_borr, total_assets, total_share
            cashflow: 现金流量表数据，需包含 ts_code, end_date, n_cashflow_act
            income: 利润表数据，需包含 ts_code, end_date, n_income
            
        Returns:
            包含 F-Score 各项得分的 DataFrame
        """
        # 合并财务数据
        df = self._merge_financial_data(
            fina_indicator, balancesheet, cashflow, income
        )
        
        if df.empty:
            logger.warning("No valid financial data to calculate F-Score")
            return pd.DataFrame(columns=self.SCORE_COLUMNS)
        
        # 按股票分组计算
        result_list = []
        
        for ts_code, group in df.groupby("ts_code"):
            scored = self._calc_single_stock(group)
            result_list.append(scored)
        
        if not result_list:
            return pd.DataFrame(columns=self.SCORE_COLUMNS)
        
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
        
        # 选择必要的列
        fina_cols = ["ts_code", "end_date_time", "roa", 
                     "grossprofit_margin", "assets_turn", "current_ratio"]
        bs_cols = ["ts_code", "end_date_time", "lt_borr", "total_assets", "total_share"]
        cf_cols = ["ts_code", "end_date_time", "n_cashflow_act"]
        inc_cols = ["ts_code", "end_date_time", "n_income"]
        
        # 只保留存在的列
        fina_cols = [c for c in fina_cols if c in fina_indicator.columns]
        bs_cols = [c for c in bs_cols if c in balancesheet.columns]
        cf_cols = [c for c in cf_cols if c in cashflow.columns]
        inc_cols = [c for c in inc_cols if c in income.columns]
        
        # 合并
        df = fina_indicator[fina_cols].merge(
            balancesheet[bs_cols], 
            on=["ts_code", "end_date_time"], 
            how="left"
        ).merge(
            cashflow[cf_cols], 
            on=["ts_code", "end_date_time"], 
            how="left"
        ).merge(
            income[inc_cols], 
            on=["ts_code", "end_date_time"], 
            how="left"
        )
        
        return df.sort_values(["ts_code", "end_date_time"])
    
    def _calc_single_stock(self, group: pd.DataFrame) -> pd.DataFrame:
        """计算单只股票的 F-Score"""
        result = group.copy()
        
        # 确保按时间排序
        result = result.sort_values("end_date_time")
        
        # 1. 盈利能力指标 (4分)
        
        # F_ROA: ROA > 0
        result["f_roa"] = (result["roa"] > 0).astype(int)
        
        # F_CFO: 经营现金流 > 0
        if "n_cashflow_act" in result.columns:
            result["f_cfo"] = (result["n_cashflow_act"] > 0).astype(int)
        else:
            result["f_cfo"] = 0
        
        # F_ΔROA: ROA 同比增长
        result["f_delta_roa"] = (result["roa"].diff() > 0).astype(int)
        
        # F_ACCRUAL: 经营现金流 > 净利润
        if "n_cashflow_act" in result.columns and "n_income" in result.columns:
            result["f_accrual"] = (
                result["n_cashflow_act"] > result["n_income"]
            ).astype(int)
        else:
            result["f_accrual"] = 0
        
        # 2. 财务杠杆/流动性 (3分)
        
        # F_ΔLEVER: 长期负债率下降
        if "lt_borr" in result.columns and "total_assets" in result.columns:
            lever = result["lt_borr"] / result["total_assets"].replace(0, np.nan)
            result["f_delta_lever"] = (lever.diff() < 0).astype(int)
        else:
            result["f_delta_lever"] = 0
        
        # F_ΔLIQUID: 流动比率上升
        if "current_ratio" in result.columns:
            result["f_delta_liquid"] = (
                result["current_ratio"].diff() > 0
            ).astype(int)
        else:
            result["f_delta_liquid"] = 0
        
        # F_EQ_OFFER: 未增发新股
        if "total_share" in result.columns:
            result["f_eq_offer"] = (
                result["total_share"].diff() <= 0
            ).astype(int)
        else:
            result["f_eq_offer"] = 1  # 默认假设未增发
        
        # 3. 运营效率 (2分)
        
        # F_ΔMARGIN: 毛利率上升
        if "grossprofit_margin" in result.columns:
            result["f_delta_margin"] = (
                result["grossprofit_margin"].diff() > 0
            ).astype(int)
        else:
            result["f_delta_margin"] = 0
        
        # F_ΔTURN: 资产周转率上升
        if "assets_turn" in result.columns:
            result["f_delta_turn"] = (
                result["assets_turn"].diff() > 0
            ).astype(int)
        else:
            result["f_delta_turn"] = 0
        
        # 计算总分
        f_cols = [
            "f_roa", "f_cfo", "f_delta_roa", "f_accrual",
            "f_delta_lever", "f_delta_liquid", "f_eq_offer",
            "f_delta_margin", "f_delta_turn"
        ]
        result["f_score"] = result[f_cols].sum(axis=1)
        
        # 处理首行 NaN（因为 diff 导致）
        result.loc[result.index[0], [
            "f_delta_roa", "f_delta_lever", "f_delta_liquid",
            "f_eq_offer", "f_delta_margin", "f_delta_turn"
        ]] = 0
        
        # 重新计算首行总分
        result.loc[result.index[0], "f_score"] = result.loc[result.index[0], f_cols].sum()
        
        return result
    
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
            }
        }
