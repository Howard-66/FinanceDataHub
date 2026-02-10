"""
F-Score ROA TTM 计算单元测试

测试 _calc_roa_ttm 方法和基于 TTM 的 f_delta_roa 评分逻辑。
"""

import pytest
import pandas as pd
import numpy as np
from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator


@pytest.fixture
def calculator():
    return FScoreCalculator()


def _make_fina_df(records):
    """
    构造 fina_indicator 格式的 DataFrame
    
    records: list of (end_date, roa) tuples
    """
    df = pd.DataFrame(records, columns=["end_date", "roa"])
    df["ts_code"] = "600519.SH"
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    df["ann_date"] = df["end_date"]  # 简化
    # 添加 F-Score 计算所需的其他列（最小化）
    df["roe"] = 10.0
    df["roe_yearly"] = 10.0
    df["grossprofit_margin"] = 50.0
    df["assets_turn"] = 0.5
    df["current_ratio"] = 1.5
    return df.sort_values("end_date_time").reset_index(drop=True)


def _make_balancesheet_df(end_dates):
    """构造最小化的资产负债表数据"""
    df = pd.DataFrame({
        "ts_code": ["600519.SH"] * len(end_dates),
        "end_date": end_dates,
        "f_ann_date": end_dates,
        "total_assets": [1000.0] * len(end_dates),
        "total_liab": [400.0] * len(end_dates),
        "total_cur_assets": [500.0] * len(end_dates),
        "total_cur_liab": [300.0] * len(end_dates),
        "total_share": [100.0] * len(end_dates),
    })
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    return df


def _make_cashflow_df(end_dates):
    """构造最小化的现金流量表数据"""
    df = pd.DataFrame({
        "ts_code": ["600519.SH"] * len(end_dates),
        "end_date": end_dates,
        "f_ann_date": end_dates,
        "n_cashflow_act": [100.0] * len(end_dates),
    })
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    return df


def _make_income_df(end_dates):
    """构造最小化的利润表数据"""
    df = pd.DataFrame({
        "ts_code": ["600519.SH"] * len(end_dates),
        "end_date": end_dates,
        "f_ann_date": end_dates,
        "n_income": [80.0] * len(end_dates),
        "total_revenue": [500.0] * len(end_dates),
    })
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    return df


class TestCalcRoaTtm:
    """测试 _calc_roa_ttm 方法"""
    
    def test_q4_annual_report(self, calculator):
        """Q4 年报: TTM = 年报值本身"""
        records = [
            ("20220331", 3.0),   # 2022 Q1
            ("20220630", 6.5),   # 2022 Q2
            ("20220930", 10.0),  # 2022 Q3
            ("20221231", 14.0),  # 2022 Q4 年报
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        # Q4 行：TTM 就是年报值
        assert result.iloc[3] == pytest.approx(14.0)
    
    def test_q1_ttm_calculation(self, calculator):
        """Q1: TTM = Q1本期累计 + (上年年报 - 上年Q1累计)"""
        records = [
            ("20220331", 3.0),    # 2022 Q1 累计
            ("20220630", 6.5),    # 2022 Q2 累计
            ("20220930", 10.0),   # 2022 Q3 累计
            ("20221231", 14.0),   # 2022 Q4 年报
            ("20230331", 3.5),    # 2023 Q1 累计
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        # 2023 Q1 TTM = 3.5 + (14.0 - 3.0) = 14.5
        assert result.iloc[4] == pytest.approx(14.5)
    
    def test_q2_ttm_calculation(self, calculator):
        """Q2: TTM = Q2本期累计 + (上年年报 - 上年Q2累计)"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
            ("20230630", 7.0),   # 2023 Q2 累计
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        # 2023 Q2 TTM = 7.0 + (14.0 - 6.5) = 14.5
        assert result.iloc[5] == pytest.approx(14.5)
    
    def test_q3_ttm_calculation(self, calculator):
        """Q3: TTM = Q3本期累计 + (上年年报 - 上年Q3累计)"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
            ("20230630", 7.0),
            ("20230930", 11.0),  # 2023 Q3 累计
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        # 2023 Q3 TTM = 11.0 + (14.0 - 10.0) = 15.0
        assert result.iloc[6] == pytest.approx(15.0)
    
    def test_insufficient_data_returns_nan(self, calculator):
        """数据不足时返回 NaN（如第一年的 Q1-Q3 没有上年数据）"""
        records = [
            ("20220331", 3.0),   # 2022 Q1 - 无上年数据
            ("20220630", 6.5),   # 2022 Q2 - 无上年数据
            ("20220930", 10.0),  # 2022 Q3 - 无上年数据
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        # 前3行均无上年数据，应为 NaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
    
    def test_nan_roa_handled(self, calculator):
        """roa 为 NaN 的行应输出 NaN"""
        records = [
            ("20220331", 3.0),
            ("20220630", np.nan),  # 缺失
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        assert pd.isna(result.iloc[1])
        assert result.iloc[3] == pytest.approx(14.0)  # Q4 正常


class TestFDeltaRoaWithTtm:
    """测试基于 TTM 的 f_delta_roa 评分"""
    
    def test_roa_ttm_improvement_scores_1(self, calculator):
        """ROA TTM 同比改善应得 1 分"""
        # 构造两年完整数据，第二年 TTM 高于第一年
        records = [
            # 2021 年
            ("20210331", 2.5),
            ("20210630", 5.5),
            ("20210930", 9.0),
            ("20211231", 12.0),
            # 2022 年
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            # 2023 年
            ("20230331", 3.5),
            ("20230630", 7.0),
            ("20230930", 11.0),
            ("20231231", 15.0),
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # 验证 roa_ttm 列存在
        assert "roa_ttm" in result.columns
        
        # 2023 Q4: TTM=15.0 vs 2022 Q4: TTM=14.0 -> 改善 -> f_delta_roa=1
        row_2023q4 = result[result["end_date_time"] == pd.Timestamp("20231231")].iloc[0]
        assert row_2023q4["f_delta_roa"] == 1
    
    def test_roa_ttm_decline_scores_0(self, calculator):
        """ROA TTM 同比下降应得 0 分"""
        records = [
            # 2021 年
            ("20210331", 3.5),
            ("20210630", 7.0),
            ("20210930", 11.0),
            ("20211231", 15.0),
            # 2022 年 (业绩下滑)
            ("20220331", 2.5),
            ("20220630", 5.5),
            ("20220930", 9.0),
            ("20221231", 12.0),
            # 2023 年 (继续下滑)
            ("20230331", 2.0),
            ("20230630", 4.5),
            ("20230930", 7.5),
            ("20231231", 10.0),
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # 2023 Q4: TTM=10.0 vs 2022 Q4: TTM=12.0 -> 下降 -> f_delta_roa=0
        row_2023q4 = result[result["end_date_time"] == pd.Timestamp("20231231")].iloc[0]
        assert row_2023q4["f_delta_roa"] == 0
    
    def test_roa_ttm_values_correctness(self, calculator):
        """验证各季度 TTM 值计算准确"""
        records = [
            # 2022 年
            ("20220331", 3.0),    # Q1
            ("20220630", 6.5),    # Q2 累计
            ("20220930", 10.0),   # Q3 累计
            ("20221231", 14.0),   # Q4 年报
            # 2023 年
            ("20230331", 3.5),    # Q1
            ("20230630", 7.5),    # Q2 累计
            ("20230930", 11.5),   # Q3 累计
            ("20231231", 16.0),   # Q4 年报
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # 2022 Q4: TTM = 14.0 (年报)
        assert result.iloc[3]["roa_ttm"] == pytest.approx(14.0)
        
        # 2023 Q1: TTM = 3.5 + (14.0 - 3.0) = 14.5
        assert result.iloc[4]["roa_ttm"] == pytest.approx(14.5)
        
        # 2023 Q2: TTM = 7.5 + (14.0 - 6.5) = 15.0
        assert result.iloc[5]["roa_ttm"] == pytest.approx(15.0)
        
        # 2023 Q3: TTM = 11.5 + (14.0 - 10.0) = 15.5
        assert result.iloc[6]["roa_ttm"] == pytest.approx(15.5)
        
        # 2023 Q4: TTM = 16.0 (年报)
        assert result.iloc[7]["roa_ttm"] == pytest.approx(16.0)
