"""
F-Score TTM 计算单元测试

测试 _calc_cumulative_to_ttm、_calc_quarterly_ttm 方法
和基于 TTM 的各项 F-Score 评分逻辑。
"""

import pytest
import pandas as pd
import numpy as np
from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator


@pytest.fixture
def calculator():
    return FScoreCalculator()


def _make_fina_df(records, q_gsprofit_margin=None, q_roe=None):
    """
    构造 fina_indicator 格式的 DataFrame
    
    records: list of (end_date, roa) tuples
    q_gsprofit_margin: list of float, 单季度毛利率（可选）
    q_roe: list of float, 单季度ROE（可选）
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
    
    if q_gsprofit_margin is not None:
        df["q_gsprofit_margin"] = q_gsprofit_margin
    if q_roe is not None:
        df["q_roe"] = q_roe
    
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


def _make_cashflow_df(end_dates, n_cashflow_act=None):
    """构造最小化的现金流量表数据"""
    if n_cashflow_act is None:
        n_cashflow_act = [100.0] * len(end_dates)
    df = pd.DataFrame({
        "ts_code": ["600519.SH"] * len(end_dates),
        "end_date": end_dates,
        "f_ann_date": end_dates,
        "n_cashflow_act": n_cashflow_act,
    })
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    return df


def _make_income_df(end_dates, n_income=None):
    """构造最小化的利润表数据"""
    if n_income is None:
        n_income = [80.0] * len(end_dates)
    df = pd.DataFrame({
        "ts_code": ["600519.SH"] * len(end_dates),
        "end_date": end_dates,
        "f_ann_date": end_dates,
        "n_income": n_income,
        "total_revenue": [500.0] * len(end_dates),
    })
    df["end_date_time"] = pd.to_datetime(df["end_date"])
    return df


# =====================================================
# _calc_cumulative_to_ttm 通用方法测试
# =====================================================


class TestCalcCumulativeToTtm:
    """测试通用累计→TTM 转换方法"""
    
    def test_q4_annual_report(self, calculator):
        """Q4 年报: TTM = 年报值本身"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
        assert result.iloc[3] == pytest.approx(14.0)
    
    def test_q1_ttm_calculation(self, calculator):
        """Q1: TTM = Q1本期累计 + (上年年报 - 上年Q1累计)"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
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
            ("20230630", 7.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
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
            ("20230930", 11.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
        # 2023 Q3 TTM = 11.0 + (14.0 - 10.0) = 15.0
        assert result.iloc[6] == pytest.approx(15.0)
    
    def test_insufficient_data_returns_nan(self, calculator):
        """数据不足时返回 NaN（如第一年的 Q1-Q3 没有上年数据）"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
    
    def test_nan_value_handled(self, calculator):
        """列值为 NaN 的行应输出 NaN"""
        records = [
            ("20220331", 3.0),
            ("20220630", np.nan),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "roa")
        
        assert pd.isna(result.iloc[1])
        assert result.iloc[3] == pytest.approx(14.0)
    
    def test_cashflow_ttm(self, calculator):
        """测试现金流累计→TTM转换"""
        # 构造现金流累计数据
        records = [
            ("20220331", 3.0),  # placeholder roa
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 4.0),
        ]
        df = _make_fina_df(records)
        # 模拟 n_cashflow_act 累计值
        df["n_cashflow_act"] = [50, 120, 200, 300, 60]
        
        result = calculator._calc_cumulative_to_ttm(df, "n_cashflow_act")
        
        # Q4 2022: TTM = 300
        assert result.iloc[3] == pytest.approx(300.0)
        # Q1 2023: TTM = 60 + (300 - 50) = 310
        assert result.iloc[4] == pytest.approx(310.0)
    
    def test_missing_column_returns_nan(self, calculator):
        """不存在的列应返回全 NaN"""
        records = [("20220331", 3.0)]
        df = _make_fina_df(records)
        
        result = calculator._calc_cumulative_to_ttm(df, "nonexistent_col")
        
        assert pd.isna(result.iloc[0])


# =====================================================
# _calc_quarterly_ttm 单季度 TTM 方法测试
# =====================================================


class TestCalcQuarterlyTtm:
    """测试单季度值→TTM 转换方法"""
    
    def test_rolling_mean_basic(self, calculator):
        """基本 rolling mean 计算"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
        ]
        q_gsprofit_margin = [40.0, 42.0, 38.0, 44.0, 41.0]
        df = _make_fina_df(records, q_gsprofit_margin=q_gsprofit_margin)
        
        result = calculator._calc_quarterly_ttm(df, "q_gsprofit_margin", agg="mean")
        
        # 前3期不足4期，应为NaN
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert pd.isna(result.iloc[2])
        # 第4期: mean(40, 42, 38, 44) = 41.0
        assert result.iloc[3] == pytest.approx(41.0)
        # 第5期: mean(42, 38, 44, 41) = 41.25
        assert result.iloc[4] == pytest.approx(41.25)
    
    def test_rolling_sum(self, calculator):
        """rolling sum 计算"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        q_roe = [2.5, 3.0, 2.8, 3.2]
        df = _make_fina_df(records, q_roe=q_roe)
        
        result = calculator._calc_quarterly_ttm(df, "q_roe", agg="sum")
        
        # 第4期: sum(2.5, 3.0, 2.8, 3.2) = 11.5
        assert result.iloc[3] == pytest.approx(11.5)
    
    def test_missing_column(self, calculator):
        """不存在的列应返回全 NaN"""
        records = [("20220331", 3.0)]
        df = _make_fina_df(records)
        
        result = calculator._calc_quarterly_ttm(df, "nonexistent", agg="mean")
        
        assert pd.isna(result.iloc[0])


# =====================================================
# _calc_roa_ttm 兼容性测试
# =====================================================


class TestCalcRoaTtm:
    """测试 _calc_roa_ttm 方法（应委托给 _calc_cumulative_to_ttm）"""
    
    def test_q4_annual_report(self, calculator):
        """Q4 年报: TTM = 年报值本身"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        assert result.iloc[3] == pytest.approx(14.0)
    
    def test_q1_ttm_calculation(self, calculator):
        """Q1: TTM = Q1本期累计 + (上年年报 - 上年Q1累计)"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        assert result.iloc[4] == pytest.approx(14.5)
    
    def test_nan_roa_handled(self, calculator):
        """roa 为 NaN 的行应输出 NaN"""
        records = [
            ("20220331", 3.0),
            ("20220630", np.nan),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        df = _make_fina_df(records)
        
        result = calculator._calc_roa_ttm(df)
        
        assert pd.isna(result.iloc[1])
        assert result.iloc[3] == pytest.approx(14.0)


# =====================================================
# F_ROA TTM 评分测试
# =====================================================


class TestFRoaWithTtm:
    """测试 F_ROA 使用 TTM 值判断"""
    
    def test_positive_roa_ttm_scores_1(self, calculator):
        """ROA TTM > 0 应得 1 分"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),  # TTM = 14 > 0
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_q4["f_roa"] == 1
    
    def test_negative_roa_ttm_scores_0(self, calculator):
        """ROA TTM < 0 应得 0 分（即使某些累计季度 > 0）"""
        # 构造上年有数据，今年Q1累计>0但TTM<0的情况
        records = [
            ("20210331", 5.0),
            ("20210630", 8.0),
            ("20210930", 10.0),
            ("20211231", -2.0),   # 年报亏损
            ("20220331", 0.5),    # Q1 累计 > 0 但 TTM = 0.5 + (-2.0 - 5.0) = -6.5
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q1 = result[result["end_date_time"] == pd.Timestamp("20220331")].iloc[0]
        assert row_q1["roa_ttm"] == pytest.approx(-6.5)
        assert row_q1["f_roa"] == 0


# =====================================================
# F_ΔROA TTM 同比测试
# =====================================================


class TestFDeltaRoaWithTtm:
    """测试基于 TTM 的 f_delta_roa 评分"""
    
    def test_roa_ttm_improvement_scores_1(self, calculator):
        """ROA TTM 同比改善应得 1 分"""
        records = [
            ("20210331", 2.5),
            ("20210630", 5.5),
            ("20210930", 9.0),
            ("20211231", 12.0),
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
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
        
        assert "roa_ttm" in result.columns
        
        row_2023q4 = result[result["end_date_time"] == pd.Timestamp("20231231")].iloc[0]
        assert row_2023q4["f_delta_roa"] == 1
    
    def test_roa_ttm_decline_scores_0(self, calculator):
        """ROA TTM 同比下降应得 0 分"""
        records = [
            ("20210331", 3.5),
            ("20210630", 7.0),
            ("20210930", 11.0),
            ("20211231", 15.0),
            ("20220331", 2.5),
            ("20220630", 5.5),
            ("20220930", 9.0),
            ("20221231", 12.0),
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
        
        row_2023q4 = result[result["end_date_time"] == pd.Timestamp("20231231")].iloc[0]
        assert row_2023q4["f_delta_roa"] == 0
    
    def test_roa_ttm_values_correctness(self, calculator):
        """验证各季度 TTM 值计算准确"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
            ("20230630", 7.5),
            ("20230930", 11.5),
            ("20231231", 16.0),
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        assert result.iloc[3]["roa_ttm"] == pytest.approx(14.0)
        assert result.iloc[4]["roa_ttm"] == pytest.approx(14.5)
        assert result.iloc[5]["roa_ttm"] == pytest.approx(15.0)
        assert result.iloc[6]["roa_ttm"] == pytest.approx(15.5)
        assert result.iloc[7]["roa_ttm"] == pytest.approx(16.0)


# =====================================================
# F_CFO TTM 评分测试
# =====================================================


class TestFCfoWithTtm:
    """测试 F_CFO 使用 TTM 值判断"""
    
    def test_positive_cfo_ttm_scores_1(self, calculator):
        """CFO TTM > 0 应得 1 分"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        end_dates = [r[0] for r in records]
        # 累计现金流：50, 120, 200, 300 → Q4 TTM = 300
        cfo_values = [50.0, 120.0, 200.0, 300.0]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates, n_cashflow_act=cfo_values)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_q4["cfo_ttm"] == pytest.approx(300.0)
        assert row_q4["f_cfo"] == 1
    
    def test_negative_cfo_ttm_scores_0(self, calculator):
        """CFO TTM < 0 应得 0 分"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
            ("20230331", 3.5),
        ]
        end_dates = [r[0] for r in records]
        # Q1 2023 累计 = -10, 上年Q1 = 50, 上年年报 = -100
        # TTM = -10 + (-100 - 50) = -160
        cfo_values = [50.0, 120.0, 200.0, -100.0, -10.0]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates, n_cashflow_act=cfo_values)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q1_2023 = result[result["end_date_time"] == pd.Timestamp("20230331")].iloc[0]
        assert row_q1_2023["cfo_ttm"] == pytest.approx(-160.0)
        assert row_q1_2023["f_cfo"] == 0


# =====================================================
# F_ACCRUAL TTM 评分测试
# =====================================================


class TestFAccrualWithTtm:
    """测试 F_ACCRUAL 使用 TTM 值比较 CFO > NI"""
    
    def test_cfo_ttm_gt_ni_ttm_scores_1(self, calculator):
        """CFO TTM > NI TTM 应得 1 分"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        end_dates = [r[0] for r in records]
        # CFO 累计: 50, 120, 200, 300 → TTM = 300
        cfo_values = [50.0, 120.0, 200.0, 300.0]
        # NI 累计: 40, 90, 150, 200 → TTM = 200
        ni_values = [40.0, 90.0, 150.0, 200.0]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates, n_cashflow_act=cfo_values)
        inc_df = _make_income_df(end_dates, n_income=ni_values)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_q4["f_accrual"] == 1
    
    def test_cfo_ttm_lt_ni_ttm_scores_0(self, calculator):
        """CFO TTM < NI TTM 应得 0 分"""
        records = [
            ("20220331", 3.0),
            ("20220630", 6.5),
            ("20220930", 10.0),
            ("20221231", 14.0),
        ]
        end_dates = [r[0] for r in records]
        # CFO 累计较小
        cfo_values = [20.0, 50.0, 80.0, 100.0]
        # NI 累计较大
        ni_values = [40.0, 100.0, 180.0, 250.0]
        
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates, n_cashflow_act=cfo_values)
        inc_df = _make_income_df(end_dates, n_income=ni_values)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_q4["f_accrual"] == 0


# =====================================================
# F_ΔMARGIN TTM 评分测试 (基于 q_gsprofit_margin)
# =====================================================


class TestFDeltaMarginWithTtm:
    """测试 F_ΔMARGIN 使用 q_gsprofit_margin rolling mean TTM"""
    
    def test_margin_ttm_improvement_scores_1(self, calculator):
        """毛利率 TTM 同比改善应得 1 分"""
        records = [
            # 2021: 4 quarters
            ("20210331", 2.5), ("20210630", 5.5),
            ("20210930", 9.0), ("20211231", 12.0),
            # 2022: 4 quarters
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        # q_gsprofit_margin: 2021 低, 2022 高 → 改善
        q_gpm = [38.0, 39.0, 37.0, 40.0,     # 2021 mean=38.5
                 42.0, 43.0, 41.0, 44.0]       # 2022 mean=42.5
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records, q_gsprofit_margin=q_gpm)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # 2022 Q4 TTM mean=42.5 vs 2021 Q4 TTM mean=38.5 → 改善
        row_2022q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_2022q4["f_delta_margin"] == 1
    
    def test_margin_ttm_decline_scores_0(self, calculator):
        """毛利率 TTM 同比下降应得 0 分"""
        records = [
            ("20210331", 2.5), ("20210630", 5.5),
            ("20210930", 9.0), ("20211231", 12.0),
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        # q_gsprofit_margin: 2021 高, 2022 低 → 下降
        q_gpm = [42.0, 43.0, 41.0, 44.0,     # 2021 mean=42.5
                 38.0, 39.0, 37.0, 40.0]       # 2022 mean=38.5
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records, q_gsprofit_margin=q_gpm)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        row_2022q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_2022q4["f_delta_margin"] == 0
    
    def test_fallback_to_grossprofit_margin_ttm(self, calculator):
        """无 q_gsprofit_margin 时降级为 grossprofit_margin 累计→TTM"""
        records = [
            ("20210331", 2.5), ("20210630", 5.5),
            ("20210930", 9.0), ("20211231", 12.0),
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        end_dates = [r[0] for r in records]
        
        # 不传 q_gsprofit_margin
        fina_df = _make_fina_df(records)
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # grossprofit_margin 默认都是50，累计→TTM YoY 相同 → 0
        row_2022q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_2022q4["f_delta_margin"] == 0


# =====================================================
# F_ΔTURN TTM 评分测试
# =====================================================


class TestFDeltaTurnWithTtm:
    """测试 F_ΔTURN 使用 assets_turn 累计→TTM"""
    
    def test_turn_ttm_improvement_scores_1(self, calculator):
        """资产周转率 TTM 同比改善应得 1 分"""
        records = [
            ("20210331", 2.5), ("20210630", 5.5),
            ("20210930", 9.0), ("20211231", 12.0),
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        end_dates = [r[0] for r in records]
        
        fina_df = _make_fina_df(records)
        # 2021 assets_turn 低, 2022 assets_turn 高
        fina_df["assets_turn"] = [0.10, 0.22, 0.35, 0.50,   # 2021
                                  0.12, 0.26, 0.40, 0.58]   # 2022
        bs_df = _make_balancesheet_df(end_dates)
        cf_df = _make_cashflow_df(end_dates)
        inc_df = _make_income_df(end_dates)
        
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)
        
        # 2022 Q4 TTM = 0.58, 2021 Q4 TTM = 0.50 → 改善
        row_2022q4 = result[result["end_date_time"] == pd.Timestamp("20221231")].iloc[0]
        assert row_2022q4["f_delta_turn"] == 1


# =====================================================
# ROE 5年平均 (q_roe) 测试
# =====================================================


class TestRoe5yAvg:
    """测试使用 q_roe 20期滚动计算5年平均ROE"""
    
    def test_q_roe_20_quarters(self, calculator):
        """完整20期 q_roe 数据应返回正确的5年平均"""
        # 构造5年（20期）数据
        dates = []
        roa_vals = []
        q_roe_vals = []
        for year in range(2019, 2024):
            for m in ["0331", "0630", "0930", "1231"]:
                dates.append(f"{year}{m}")
                roa_vals.append(3.0)
                q_roe_vals.append(2.5)  # 每季度ROE = 2.5
        
        records = list(zip(dates, roa_vals))
        fina_df = _make_fina_df(records, q_roe=q_roe_vals)
        
        result = calculator._calc_roe_5y_avg(fina_df)
        
        # sum(20 * 2.5) / 5 = 50 / 5 = 10.0
        assert result.iloc[-1] == pytest.approx(10.0)
    
    def test_insufficient_data_still_works(self, calculator):
        """不足20期但有4期以上应返回结果"""
        records = [
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        q_roe = [2.5, 3.0, 2.8, 3.2]
        fina_df = _make_fina_df(records, q_roe=q_roe)
        
        result = calculator._calc_roe_5y_avg(fina_df)
        
        # rolling(20, min_periods=4).sum() / 5
        # sum(2.5+3.0+2.8+3.2) / 5 = 11.5 / 5 = 2.3
        assert result.iloc[-1] == pytest.approx(2.3)
    
    def test_fallback_to_roe_yearly(self, calculator):
        """无 q_roe 时降级为 roe_yearly 方式"""
        records = [
            ("20210331", 2.5), ("20210630", 5.5),
            ("20210930", 9.0), ("20211231", 12.0),
            ("20220331", 3.0), ("20220630", 6.5),
            ("20220930", 10.0), ("20221231", 14.0),
        ]
        # 不传 q_roe
        fina_df = _make_fina_df(records)
        
        result = calculator._calc_roe_5y_avg(fina_df)
        
        # 应使用 roe_yearly 降级方案, 不应全为NaN
        assert result.notna().any()

import pytest
import pandas as pd
import numpy as np
from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator

def _make_minimal_df(end_date):
    """构造最小数据集"""
    df = pd.DataFrame([{
        "ts_code": "600519.SH",
        "end_date": end_date,
        "end_date_time": pd.to_datetime(end_date),
        "ann_date": end_date,
        "roa": 10.0,
        "grossprofit_margin": 50.0,
        "q_gsprofit_margin": 50.0,
        "assets_turn": 0.5,
        "current_ratio": 2.0,
        "n_cashflow_act": 100.0,
        "n_income": 80.0,
        "total_revenue": 500.0,
        "total_assets": 1000.0,
        "total_liab": 400.0,
        "total_cur_assets": 500.0,
        "total_cur_liab": 250.0,
        "total_share": 100.0
    }])
    return df

@pytest.fixture
def calculator():
    return FScoreCalculator()


def test_ttm_fields_in_output(calculator):
    """Verify cfo_ttm, ni_ttm, gpm_ttm, at_ttm exist in result DataFrame"""
    # Construct 4 quarters of data to generate valid TTM
    dfs = []
    for date in ["20220331", "20220630", "20220930", "20221231"]:
        dfs.append(_make_minimal_df(date))

    # Merge and split back to each table (simplified test)
    full_df = pd.concat(dfs).reset_index(drop=True)

    # Simulate input data
    fina_df = full_df[["ts_code", "end_date", "end_date_time", "ann_date", "roa", "grossprofit_margin", "q_gsprofit_margin", "assets_turn", "current_ratio"]].copy()
    bs_df = full_df[["ts_code", "end_date", "end_date_time", "total_assets", "total_liab", "total_cur_assets", "total_cur_liab", "total_share"]].copy()
    cf_df = full_df[["ts_code", "end_date", "end_date_time", "n_cashflow_act"]].copy()
    inc_df = full_df[["ts_code", "end_date", "end_date_time", "n_income", "total_revenue"]].copy()

    result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)

    expected_cols = ["cfo_ttm", "ni_ttm", "gpm_ttm", "at_ttm"]
    for col in expected_cols:
        assert col in result.columns, f"Missing output column: {col}"
        # Verify Q4 data is not empty (Q1-Q3 may be empty due to rolling logic, but Q4 should have value)
        q4_val = result.iloc[3][col]
        assert pd.notna(q4_val), f"Column {col} is NaN for Q4"

