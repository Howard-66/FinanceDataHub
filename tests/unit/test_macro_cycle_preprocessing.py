"""
中国宏观周期预处理单元测试
"""

import pandas as pd


def _ts(date_str: str) -> pd.Timestamp:
    return pd.Timestamp(date_str, tz="Asia/Shanghai")


class TestMacroCycleCalculator:
    """中国宏观周期计算器测试。"""

    def test_determine_phase_examples(self):
        """测试四阶段判定逻辑。"""
        from finance_data_hub.preprocessing.macro import MacroCycleCalculator

        calculator = MacroCycleCalculator()

        assert calculator.determine_phase(3.0, -1.0, 49.0) == "REFLATION"
        assert calculator.determine_phase(2.0, 1.5, 53.0) == "RECOVERY"
        assert calculator.determine_phase(-1.0, 3.0, 51.0) == "OVERHEAT"
        assert calculator.determine_phase(-2.0, 5.0, 48.0) == "STAGFLATION"

    def test_calculate_aligns_effective_month_and_smoothing(self):
        """测试月度对齐、生效月份后移和平滑阶段。"""
        from finance_data_hub.preprocessing.macro import MacroCycleCalculator

        calculator = MacroCycleCalculator()

        m_df = pd.DataFrame(
            {
                "time": [_ts("2024-01-31 15:00"), _ts("2024-02-29 15:00"), _ts("2024-03-31 15:00"), _ts("2024-04-30 15:00")],
                "m2_yoy": [8.0, 8.0, 9.0, 4.0],
            }
        )
        ppi_df = pd.DataFrame(
            {
                "time": [_ts("2024-01-31 15:00"), _ts("2024-02-29 15:00"), _ts("2024-03-31 15:00"), _ts("2024-04-30 15:00")],
                "ppi_yoy": [-1.0, 1.0, 1.0, 3.0],
            }
        )
        pmi_df = pd.DataFrame(
            {
                "time": [_ts("2024-01-31 15:00"), _ts("2024-02-29 15:00"), _ts("2024-03-31 15:00"), _ts("2024-04-30 15:00")],
                "pmi010000": [49.0, 53.0, 53.0, 51.0],
            }
        )
        gdp_df = pd.DataFrame(
            {
                "time": [_ts("2023-12-31 15:00"), _ts("2024-03-31 15:00")],
                "gdp_yoy": [5.0, 5.5],
            }
        )

        result = calculator.calculate(m_df, ppi_df, pmi_df, gdp_df)

        assert list(result["raw_phase"]) == ["REFLATION", "RECOVERY", "RECOVERY", "OVERHEAT"]
        assert list(result["stable_phase"]) == ["REFLATION", "REFLATION", "RECOVERY", "RECOVERY"]

        assert result.loc[0, "observation_time"] == _ts("2024-01-31 15:00")
        assert result.loc[0, "time"] == _ts("2024-02-29 15:00")
        assert result.loc[3, "time"] == _ts("2024-05-31 15:00")

        assert float(result.loc[0, "credit_impulse"]) == 3.0
        assert bool(result.loc[0, "raw_phase_changed"]) is False
        assert bool(result.loc[2, "stable_phase_changed"]) is True

    def test_calculate_falls_back_to_pmi030000(self):
        """测试 PMI 字段回退逻辑。"""
        from finance_data_hub.preprocessing.macro import MacroCycleCalculator

        calculator = MacroCycleCalculator()

        m_df = pd.DataFrame({"time": [_ts("2024-01-31 15:00")], "m2_yoy": [8.0]})
        ppi_df = pd.DataFrame({"time": [_ts("2024-01-31 15:00")], "ppi_yoy": [-1.0]})
        pmi_df = pd.DataFrame(
            {
                "time": [_ts("2024-01-31 15:00")],
                "pmi010000": [None],
                "pmi030000": [50.5],
            }
        )
        gdp_df = pd.DataFrame({"time": [_ts("2023-12-31 15:00")], "gdp_yoy": [5.0]})

        result = calculator.calculate(m_df, ppi_df, pmi_df, gdp_df)

        assert len(result) == 1
        assert float(result.loc[0, "pmi"]) == 50.5

    def test_build_industry_snapshot_uses_config_cycle(self):
        """测试行业快照按配置周期匹配。"""
        from finance_data_hub.preprocessing.fundamental.industry_config import (
            get_industry_config_loader,
        )
        from finance_data_hub.preprocessing.macro import MacroCycleCalculator

        calculator = MacroCycleCalculator()
        loader = get_industry_config_loader()
        reflation_name = next(
            name for name, cfg in loader.config.items() if cfg.get("macro_cycle") == "REFLATION"
        )
        recovery_name = next(
            name for name, cfg in loader.config.items() if cfg.get("macro_cycle") == "RECOVERY"
        )

        phase_df = pd.DataFrame(
            {
                "time": [_ts("2024-02-29 15:00")],
                "observation_time": [_ts("2024-01-31 15:00")],
                "raw_phase": ["REFLATION"],
                "stable_phase": ["REFLATION"],
            }
        )
        industry_df = pd.DataFrame(
            {
                "l1_code": ["801", "801"],
                "l1_name": ["一级", "一级"],
                "l2_code": ["80101", "80102"],
                "l2_name": ["二级A", "二级B"],
                "l3_code": ["80101A", "80102B"],
                "l3_name": [reflation_name, recovery_name],
                "is_new": ["Y", "Y"],
            }
        )

        snapshot = calculator.build_industry_snapshot(phase_df, industry_df)
        reflation_row = snapshot[snapshot["l3_name"] == reflation_name].iloc[0]
        recovery_row = snapshot[snapshot["l3_name"] == recovery_name].iloc[0]

        assert bool(reflation_row["matches_raw_phase"]) is True
        assert bool(recovery_row["matches_raw_phase"]) is False
        assert bool(reflation_row["is_present_in_sw_member"]) is True
