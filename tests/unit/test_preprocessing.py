"""
预处理模块单元测试
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestAdjustProcessor:
    """复权处理测试"""
    
    @pytest.fixture
    def sample_daily_data(self):
        """创建测试用日线数据"""
        dates = pd.date_range('2024-01-01', periods=10, freq='D')
        return pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * 10,
            'open': [100.0 + i for i in range(10)],
            'high': [102.0 + i for i in range(10)],
            'low': [98.0 + i for i in range(10)],
            'close': [101.0 + i for i in range(10)],
            'volume': [1000000 + i * 10000 for i in range(10)],
            'amount': [100000000.0 + i * 1000000 for i in range(10)],
            'adj_factor': [1.0] * 5 + [1.1] * 5  # 中间有除权
        })
    
    def test_qfq_adjustment(self, sample_daily_data):
        """测试前复权"""
        from finance_data_hub.preprocessing import AdjustProcessor, AdjustType
        
        processor = AdjustProcessor()
        result = processor.adjust(sample_daily_data, AdjustType.QFQ)
        
        # 验证结果包含 adjust_type 列
        assert 'adjust_type' in result.columns
        assert result['adjust_type'].iloc[0] == 'qfq'
        
        # 验证前复权后的价格
        # 前 5 天的价格应该被调整（乘以 1.0/1.1）
        latest_factor = sample_daily_data['adj_factor'].iloc[-1]
        original_close = sample_daily_data['close'].iloc[0]
        original_factor = sample_daily_data['adj_factor'].iloc[0]
        
        expected_close = original_close * (original_factor / latest_factor)
        assert abs(result['close'].iloc[0] - expected_close) < 0.01
    
    def test_hfq_adjustment(self, sample_daily_data):
        """测试后复权"""
        from finance_data_hub.preprocessing import AdjustProcessor, AdjustType
        
        processor = AdjustProcessor()
        result = processor.adjust(sample_daily_data, AdjustType.HFQ)
        
        assert 'adjust_type' in result.columns
        assert result['adjust_type'].iloc[0] == 'hfq'
        
        # 后复权：价格 * adj_factor
        original_close = sample_daily_data['close'].iloc[-1]
        factor = sample_daily_data['adj_factor'].iloc[-1]
        expected_close = original_close * factor
        
        assert abs(result['close'].iloc[-1] - expected_close) < 0.01
    
    def test_adjustment_preserves_volume(self, sample_daily_data):
        """测试复权不影响成交量"""
        from finance_data_hub.preprocessing import AdjustProcessor, AdjustType
        
        processor = AdjustProcessor()
        result = processor.adjust(sample_daily_data, AdjustType.QFQ)
        
        # 成交量应该保持不变
        assert list(result['volume']) == list(sample_daily_data['volume'])
    
    def test_empty_dataframe(self):
        """测试空数据框"""
        from finance_data_hub.preprocessing import AdjustProcessor, AdjustType
        
        processor = AdjustProcessor()
        empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor'])
        result = processor.adjust(empty_df, AdjustType.QFQ)
        
        assert result.empty


class TestResampleProcessor:
    """周期重采样测试"""
    
    @pytest.fixture
    def daily_data(self):
        """创建测试用日线数据（一个月）"""
        dates = pd.date_range('2024-01-01', periods=22, freq='B')  # 工作日
        return pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * 22,
            'open': [100.0 + i for i in range(22)],
            'high': [105.0 + i for i in range(22)],
            'low': [95.0 + i for i in range(22)],
            'close': [102.0 + i for i in range(22)],
            'volume': [1000000] * 22,
            'amount': [100000000.0] * 22,
        })
    
    def test_weekly_resample(self, daily_data):
        """测试周线重采样"""
        from finance_data_hub.preprocessing import ResampleProcessor, ResampleFreq
        
        processor = ResampleProcessor()
        result = processor.resample(daily_data, ResampleFreq.WEEKLY)
        
        # 应该有多个周线
        assert len(result) > 1
        assert len(result) < len(daily_data)
        
        # 每周的高点应该是该周日线高点的最大值
        # 每周的低点应该是该周日线低点的最小值
        assert 'open' in result.columns
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns
        assert 'volume' in result.columns
    
    def test_monthly_resample(self, daily_data):
        """测试月线重采样"""
        from finance_data_hub.preprocessing import ResampleProcessor, ResampleFreq
        
        processor = ResampleProcessor()
        result = processor.resample(daily_data, ResampleFreq.MONTHLY)
        
        # 只有一个月的数据，应该只有一条月线
        assert len(result) == 1
        
        # 月线的 high 应该是这个月所有日线 high 的最大值
        assert result['high'].iloc[0] == daily_data['high'].max()
        assert result['low'].iloc[0] == daily_data['low'].min()
    
    def test_volume_sum_in_resample(self, daily_data):
        """测试重采样后成交量应该是累加的"""
        from finance_data_hub.preprocessing import ResampleProcessor, ResampleFreq
        
        processor = ResampleProcessor()
        result = processor.resample(daily_data, ResampleFreq.MONTHLY)
        
        # 月线的成交量应该是所有日线成交量之和
        assert result['volume'].iloc[0] == daily_data['volume'].sum()


class TestTechnicalIndicators:
    """技术指标测试"""
    
    @pytest.fixture
    def price_data(self):
        """创建价格数据"""
        np.random.seed(42)
        n = 100
        dates = pd.date_range('2024-01-01', periods=n, freq='D')
        prices = 100 + np.cumsum(np.random.randn(n) * 2)
        
        return pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * n,
            'open': prices - 1,
            'high': prices + 2,
            'low': prices - 2,
            'close': prices,
            'volume': [1000000] * n,
        })
    
    def test_ma_indicator(self, price_data):
        """测试移动平均线"""
        from finance_data_hub.preprocessing.technical import MAIndicator
        
        indicator = MAIndicator(period=20)
        result = indicator.calculate(price_data)
        
        # 应该添加了 ma_20 列
        assert 'ma_20' in result.columns
        
        # MAIndicator 使用扩展均值，所以不会有 NaN
        # 验证有值
        assert not result['ma_20'].isna().all()
        
        # 验证最后的值接近真实均值
        expected_ma = price_data['close'].iloc[-20:].mean()
        assert abs(result['ma_20'].iloc[-1] - expected_ma) < 1.0
    
    def test_ema_indicator(self, price_data):
        """测试指数移动平均线"""
        from finance_data_hub.preprocessing.technical import EMAIndicator
        
        indicator = EMAIndicator(period=20)
        result = indicator.calculate(price_data)
        
        assert 'ema_20' in result.columns
        # EMA 从第一天就开始计算（虽然前几天不够准确）
        assert not result['ema_20'].isna().all()
    
    def test_macd_indicator(self, price_data):
        """测试 MACD 指标"""
        from finance_data_hub.preprocessing.technical import MACDIndicator
        
        indicator = MACDIndicator()
        result = indicator.calculate(price_data)
        
        # 应该添加 MACD 相关列
        assert 'macd_dif' in result.columns
        assert 'macd_dea' in result.columns
        assert 'macd_hist' in result.columns
    
    def test_rsi_indicator(self, price_data):
        """测试 RSI 指标"""
        from finance_data_hub.preprocessing.technical import RSIIndicator
        
        indicator = RSIIndicator(period=14)
        result = indicator.calculate(price_data)
        
        assert 'rsi_14' in result.columns
        
        # RSI 应该在 0-100 之间
        valid_rsi = result['rsi_14'].dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()
    
    def test_atr_indicator(self, price_data):
        """测试 ATR 指标"""
        from finance_data_hub.preprocessing.technical import ATRIndicator
        
        indicator = ATRIndicator(period=14)
        result = indicator.calculate(price_data)
        
        assert 'atr_14' in result.columns
        
        # ATR 应该是正数
        valid_atr = result['atr_14'].dropna()
        assert (valid_atr > 0).all()

    def test_nda_indicator(self, price_data):
        """测试 NDA 指标"""
        from finance_data_hub.preprocessing.technical import NDAIndicator

        result = NDAIndicator().calculate(price_data)

        assert 'nda_value' in result.columns
        assert 'volume_confirmed' in result.columns
        assert result['nda_value'].iloc[:19].isna().all()
        assert result['nda_value'].iloc[19:].notna().any()

    def test_nda_indicator_without_valid_volume_returns_empty_values(self, price_data):
        """无有效成交量时 NDA 应返回空值"""
        from finance_data_hub.preprocessing.technical import NDAIndicator

        price_data = price_data.copy()
        price_data["volume"] = np.nan
        result = NDAIndicator().calculate(price_data)

        assert result["nda_value"].isna().all()
        assert result["volume_confirmed"].isna().all()


class TestIndicatorRegistry:
    """指标注册表测试"""
    
    def test_create_indicator_by_name(self):
        """测试通过名称创建指标"""
        from finance_data_hub.preprocessing.technical.base import create_indicator
        
        ma_20 = create_indicator('ma_20')
        assert ma_20.name == 'ma_20'
        
        macd = create_indicator('macd')
        assert macd.name == 'macd'

        nda = create_indicator('nda')
        assert nda.name == 'nda'
    
    def test_unknown_indicator_raises(self):
        """测试创建未知指标抛出异常"""
        from finance_data_hub.preprocessing.technical.base import create_indicator
        
        with pytest.raises(KeyError):
            create_indicator('unknown_indicator')
    
    def test_list_indicators(self):
        """测试列出所有已注册指标"""
        from finance_data_hub.preprocessing.technical.base import indicator_registry
        
        indicators = indicator_registry.list_indicators()
        
        # 应该包含常用指标
        assert 'ma_20' in indicators
        assert 'macd' in indicators
        assert 'rsi_14' in indicators
        assert 'atr_14' in indicators
        assert 'nda' in indicators


class TestPreprocessPipeline:
    """预处理流水线测试"""
    
    @pytest.fixture
    def sample_data(self):
        """创建测试数据"""
        np.random.seed(42)
        n = 50
        dates = pd.date_range('2024-01-01', periods=n, freq='D')
        prices = 100 + np.cumsum(np.random.randn(n) * 2)
        
        return pd.DataFrame({
            'time': dates,
            'symbol': ['600519.SH'] * n,
            'open': prices - 1,
            'high': prices + 2,
            'low': prices - 2,
            'close': prices,
            'volume': [1000000] * n,
            'amount': [100000000.0] * n,
            'adj_factor': [1.0] * n,
        })
    
    def test_pipeline_chain_call(self, sample_data):
        """测试流水线链式调用"""
        from finance_data_hub.preprocessing import PreprocessPipeline, AdjustType
        
        pipeline = PreprocessPipeline()
        result = (
            pipeline
            .set_data(sample_data)
            .adjust(AdjustType.QFQ)
            .add_indicator('ma_20')
            .run()
        )
        
        assert 'ma_20' in result.columns
        assert 'adjust_type' in result.columns
    
    def test_pipeline_multiple_indicators(self, sample_data):
        """测试流水线添加多个指标"""
        from finance_data_hub.preprocessing import PreprocessPipeline, AdjustType
        
        pipeline = PreprocessPipeline()
        result = (
            pipeline
            .set_data(sample_data)
            .adjust(AdjustType.QFQ)
            .add_indicators(['ma_20', 'macd', 'rsi_14'])
            .run()
        )
        
        assert 'ma_20' in result.columns
        assert 'macd_dif' in result.columns
        assert 'rsi_14' in result.columns
    
    def test_pipeline_reset(self, sample_data):
        """测试流水线重置"""
        from finance_data_hub.preprocessing import PreprocessPipeline
        
        pipeline = PreprocessPipeline()
        pipeline.set_data(sample_data)
        pipeline.add_indicator('ma_20')
        
        pipeline.reset()
        
        with pytest.raises(ValueError):
            pipeline.run()  # 没有设置数据应该报错


class TestVectorizedMultiSymbol:
    """Phase 3: 多股票向量化指标测试"""

    @pytest.fixture
    def multi_symbol_data(self):
        """创建包含3只股票的价格数据"""
        np.random.seed(42)
        n = 60
        dates = pd.date_range('2024-01-01', periods=n, freq='D')
        symbols = ['600519.SH', '000858.SZ', '601398.SH']

        dfs = []
        for sym in symbols:
            prices = 100 + np.cumsum(np.random.randn(n) * 2)
            dfs.append(pd.DataFrame({
                'time': dates,
                'symbol': sym,
                'open': prices - 1,
                'high': prices + 2,
                'low': prices - 2,
                'close': prices,
                'volume': [1000000] * n,
            }))
        return pd.concat(dfs, ignore_index=True)

    def test_ma_multi_symbol(self, multi_symbol_data):
        """MA 在多股票 DataFrame 中正确按股票分组计算"""
        from finance_data_hub.preprocessing.technical import MAIndicator

        indicator = MAIndicator(period=20)
        result = indicator.calculate(multi_symbol_data)

        assert 'ma_20' in result.columns
        assert len(result) == len(multi_symbol_data)
        # 每只股票都有值
        for sym in ['600519.SH', '000858.SZ', '601398.SH']:
            sym_data = result[result['symbol'] == sym]
            assert not sym_data['ma_20'].isna().all()

    def test_macd_multi_symbol(self, multi_symbol_data):
        """MACD 在多股票 DataFrame 中正确按股票分组计算"""
        from finance_data_hub.preprocessing.technical import MACDIndicator

        indicator = MACDIndicator()
        result = indicator.calculate(multi_symbol_data)

        assert 'macd_dif' in result.columns
        assert 'macd_dea' in result.columns
        assert 'macd_hist' in result.columns
        assert len(result) == len(multi_symbol_data)

    def test_rsi_multi_symbol(self, multi_symbol_data):
        """RSI 在多股票 DataFrame 中正确按股票分组计算"""
        from finance_data_hub.preprocessing.technical import RSIIndicator

        indicator = RSIIndicator(period=14)
        result = indicator.calculate(multi_symbol_data)

        assert 'rsi_14' in result.columns
        # RSI 在 0-100 之间
        valid_rsi = result['rsi_14'].dropna()
        assert (valid_rsi >= 0).all()
        assert (valid_rsi <= 100).all()

    def test_atr_multi_symbol(self, multi_symbol_data):
        """ATR 在多股票 DataFrame 中正确按股票分组计算"""
        from finance_data_hub.preprocessing.technical import ATRIndicator

        indicator = ATRIndicator(period=14)
        result = indicator.calculate(multi_symbol_data)

        assert 'atr_14' in result.columns
        valid_atr = result['atr_14'].dropna()
        assert (valid_atr > 0).all()

    def test_single_vs_multi_consistency(self, multi_symbol_data):
        """向量化结果与逐股票单独计算结果一致"""
        from finance_data_hub.preprocessing.technical import MAIndicator

        indicator = MAIndicator(period=20)

        # 多股票批量计算
        result_batch = indicator.calculate(multi_symbol_data)

        # 逐股票单独计算
        for sym in ['600519.SH', '000858.SZ', '601398.SH']:
            single_df = multi_symbol_data[multi_symbol_data['symbol'] == sym].copy()
            result_single = indicator.calculate(single_df)
            batch_values = result_batch.loc[
                result_batch['symbol'] == sym, 'ma_20'
            ].values
            single_values = result_single['ma_20'].values
            np.testing.assert_allclose(batch_values, single_values, rtol=1e-10)

    def test_batch_compute_entry(self, multi_symbol_data):
        """批量计算入口函数 compute_indicators_batch 正常工作"""
        from finance_data_hub.preprocessing.technical.vectorized import compute_indicators_batch

        result = compute_indicators_batch(
            multi_symbol_data,
            ["ma_20", "macd", "rsi_14", "atr_14"]
        )
        assert 'ma_20' in result.columns
        assert 'macd_dif' in result.columns
        assert 'rsi_14' in result.columns
        assert 'atr_14' in result.columns
        assert len(result) == len(multi_symbol_data)


class TestFScoreTTMVectorized:
    """Phase 3: F-Score TTM 向量化测试"""

    @pytest.fixture
    def financial_data(self):
        """创建2只股票8个季度的财务数据"""
        ts_codes = ['600519.SH', '000858.SZ']
        end_dates = pd.to_datetime([
            '2022-03-31', '2022-06-30', '2022-09-30', '2022-12-31',
            '2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31',
        ])

        fina_rows = []
        bs_rows = []
        cf_rows = []
        inc_rows = []

        for ts_code in ts_codes:
            for i, ed in enumerate(end_dates):
                fina_rows.append({
                    'ts_code': ts_code, 'end_date': ed, 'ann_date': ed + pd.Timedelta(days=30),
                    'roa': 2.0 + i * 0.5, 'roe': 10.0 + i, 'roe_yearly': 10.0 + i,
                    'grossprofit_margin': 50.0 + i, 'q_gsprofit_margin': 50.0 + i * 0.3,
                    'q_roe': 3.0 + i * 0.2, 'assets_turn': 0.5 + i * 0.02,
                    'current_ratio': 1.5 + i * 0.1, 'roe_dt': 9.0 + i,
                    'debt_to_assets': 30.0, 'netprofit_yoy': 5.0 + i,
                })
                bs_rows.append({
                    'ts_code': ts_code, 'end_date': ed, 'f_ann_date': ed + pd.Timedelta(days=30),
                    'total_assets': 1000000 + i * 50000, 'total_liab': 400000 + i * 10000,
                    'total_ncl': 200000 + i * 5000, 'total_cur_assets': 500000 + i * 30000,
                    'total_cur_liab': 300000 + i * 10000, 'total_share': 100000,
                })
                cf_rows.append({
                    'ts_code': ts_code, 'end_date': ed,
                    'n_cashflow_act': 50000 + i * 5000,
                })
                inc_rows.append({
                    'ts_code': ts_code, 'end_date': ed,
                    'n_income': 40000 + i * 4000,
                })

        fina_df = pd.DataFrame(fina_rows)
        bs_df = pd.DataFrame(bs_rows)
        cf_df = pd.DataFrame(cf_rows)
        inc_df = pd.DataFrame(inc_rows)

        return fina_df, bs_df, cf_df, inc_df

    def test_fscore_calculates_with_multi_stock(self, financial_data):
        """F-Score 计算器在多股票数据上正常工作"""
        from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator

        fina_df, bs_df, cf_df, inc_df = financial_data
        calculator = FScoreCalculator()
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)

        assert not result.empty
        assert 'f_score' in result.columns
        # 两只股票都有结果
        assert result['ts_code'].nunique() == 2
        # F-Score 在 0-9 之间
        valid_scores = result['f_score'].dropna()
        assert (valid_scores >= 0).all()
        assert (valid_scores <= 9).all()

    def test_cumulative_to_ttm_vectorized(self, financial_data):
        """向量化 _calc_cumulative_to_ttm 产生有效 TTM 值"""
        from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator

        fina_df, bs_df, cf_df, inc_df = financial_data
        calculator = FScoreCalculator()
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)

        # roa_ttm 应包含有效值（至少 Q4 值直接可用）
        assert 'roa_ttm' in result.columns
        assert result['roa_ttm'].notna().any()

    def test_ni_cfo_corr_vectorized(self, financial_data):
        """向量化 _calc_ni_cfo_corr_3y 使用 rolling.corr 产生有效值"""
        from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator

        fina_df, bs_df, cf_df, inc_df = financial_data
        calculator = FScoreCalculator()
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)

        assert 'ni_cfo_corr_3y' in result.columns
        # 8 个季度中，至少后几个有 rolling corr 值
        assert result['ni_cfo_corr_3y'].notna().any()

    def test_roe_5y_avg_vectorized(self, financial_data):
        """向量化 _calc_roe_5y_avg 产生有效值"""
        from finance_data_hub.preprocessing.fundamental.quality import FScoreCalculator

        fina_df, bs_df, cf_df, inc_df = financial_data
        calculator = FScoreCalculator()
        result = calculator.calculate(fina_df, bs_df, cf_df, inc_df)

        assert 'roe_5y_avg' in result.columns
        assert result['roe_5y_avg'].notna().any()

    def test_nda_multi_symbol(self):
        """NDA 在多股票 DataFrame 中正确按股票分组计算"""
        from finance_data_hub.preprocessing.technical import NDAIndicator
        np.random.seed(42)
        n = 60
        dates = pd.date_range('2024-01-01', periods=n, freq='D')
        symbols = ['600519.SH', '000858.SZ', '601398.SH']
        frames = []
        for idx, sym in enumerate(symbols):
            closes = 100 + idx + np.arange(n) * 0.5
            frames.append(pd.DataFrame({
                'time': dates,
                'symbol': sym,
                'open': closes - 1.0,
                'high': closes + 2.0,
                'low': closes - 2.0,
                'close': closes,
                'volume': np.arange(1, n + 1) * (idx + 1),
            }))
        multi_symbol_data = pd.concat(frames, ignore_index=True)

        result = NDAIndicator().calculate(multi_symbol_data)

        assert 'nda_value' in result.columns
        assert 'volume_confirmed' in result.columns
        for sym in ['600519.SH', '000858.SZ', '601398.SH']:
            sym_data = result[result['symbol'] == sym]
            assert sym_data['nda_value'].iloc[:19].isna().all()
            assert sym_data['nda_value'].iloc[19:].notna().any()
