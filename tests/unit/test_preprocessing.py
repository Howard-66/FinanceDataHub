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


class TestIndicatorRegistry:
    """指标注册表测试"""
    
    def test_create_indicator_by_name(self):
        """测试通过名称创建指标"""
        from finance_data_hub.preprocessing.technical.base import create_indicator
        
        ma_20 = create_indicator('ma_20')
        assert ma_20.name == 'ma_20'
        
        macd = create_indicator('macd')
        assert macd.name == 'macd'
    
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
