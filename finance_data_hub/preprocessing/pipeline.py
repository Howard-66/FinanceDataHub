"""
预处理流水线

提供完整的数据预处理流程：
1. 复权处理
2. 周期重采样
3. 技术指标计算
4. 数据存储

支持批量处理和增量更新。
"""

from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
import pandas as pd
from loguru import logger

from .adjust import AdjustProcessor, AdjustType
from .resample import ResampleProcessor, ResampleFreq
from .technical import MAIndicator, MACDIndicator, RSIIndicator, ATRIndicator
from .technical.base import BaseIndicator, create_indicator


class PreprocessPipeline:
    """
    预处理流水线
    
    提供链式调用的数据预处理功能。
    
    示例:
        >>> pipeline = PreprocessPipeline()
        >>> result = (
        ...     pipeline
        ...     .set_data(df)
        ...     .adjust(AdjustType.QFQ)
        ...     .add_indicator("ma_20")
        ...     .add_indicator("macd")
        ...     .run()
        ... )
    """
    
    def __init__(self, db_operations=None):
        """
        初始化预处理流水线
        
        Args:
            db_operations: 数据库操作对象（可选，用于读取/存储数据）
        """
        self.db_operations = db_operations
        self._adjust_processor = AdjustProcessor(db_operations)
        self._resample_processor = ResampleProcessor(db_operations)
        
        # 流水线状态
        self._data: Optional[pd.DataFrame] = None
        self._adjust_type: Optional[AdjustType] = None
        self._indicators: List[BaseIndicator] = []
        self._resample_freqs: List[ResampleFreq] = []
        
    def set_data(self, df: pd.DataFrame) -> "PreprocessPipeline":
        """
        设置输入数据
        
        Args:
            df: 原始 OHLCV 数据
            
        Returns:
            self（支持链式调用）
        """
        self._data = df.copy()
        return self
    
    def adjust(
        self, 
        adjust_type: Union[AdjustType, str] = AdjustType.QFQ
    ) -> "PreprocessPipeline":
        """
        设置复权类型
        
        Args:
            adjust_type: 复权类型
            
        Returns:
            self
        """
        if isinstance(adjust_type, str):
            adjust_type = AdjustType(adjust_type)
        self._adjust_type = adjust_type
        return self
    
    def add_indicator(
        self, 
        indicator: Union[str, BaseIndicator]
    ) -> "PreprocessPipeline":
        """
        添加技术指标
        
        Args:
            indicator: 指标名称或指标实例
            
        Returns:
            self
        """
        if isinstance(indicator, str):
            indicator = create_indicator(indicator)
        self._indicators.append(indicator)
        return self
    
    def add_indicators(
        self, 
        indicators: List[Union[str, BaseIndicator]]
    ) -> "PreprocessPipeline":
        """
        批量添加技术指标
        
        Args:
            indicators: 指标名称或实例列表
            
        Returns:
            self
        """
        for ind in indicators:
            self.add_indicator(ind)
        return self
    
    def resample(
        self, 
        freq: Union[ResampleFreq, str]
    ) -> "PreprocessPipeline":
        """
        添加重采样频率
        
        Args:
            freq: 重采样频率
            
        Returns:
            self
        """
        if isinstance(freq, str):
            freq = ResampleFreq(freq)
        self._resample_freqs.append(freq)
        return self
    
    def run(self) -> pd.DataFrame:
        """
        执行预处理流水线
        
        Returns:
            预处理后的 DataFrame
        """
        if self._data is None:
            raise ValueError("No data set. Call set_data() first.")
        
        result = self._data.copy()
        
        # 1. 复权处理
        if self._adjust_type is not None:
            logger.info(f"Applying {self._adjust_type.value} adjustment")
            result = self._adjust_processor.adjust(result, self._adjust_type)
        
        # 2. 计算技术指标
        for indicator in self._indicators:
            logger.info(f"Calculating indicator: {indicator.name}")
            result = indicator.calculate(result)
        
        logger.info(f"Pipeline completed: {len(result)} records processed")
        return result
    
    def run_with_resample(self) -> Dict[str, pd.DataFrame]:
        """
        执行预处理流水线并进行多周期重采样
        
        Returns:
            周期 -> DataFrame 的字典
        """
        # 首先处理日线数据
        daily = self.run()
        
        result = {"daily": daily}
        
        # 对每个重采样频率进行处理
        for freq in self._resample_freqs:
            logger.info(f"Resampling to {freq.value}")
            
            # 重采样
            resampled = self._resample_processor.resample(daily, freq)
            
            # 对重采样后的数据重新计算指标
            for indicator in self._indicators:
                logger.info(f"Calculating {indicator.name} for {freq.value}")
                resampled = indicator.calculate(resampled)
            
            result[freq.value.lower()] = resampled
        
        return result
    
    def reset(self) -> "PreprocessPipeline":
        """
        重置流水线状态
        
        Returns:
            self
        """
        self._data = None
        self._adjust_type = None
        self._indicators = []
        self._resample_freqs = []
        return self


class BatchPreprocessor:
    """
    批量预处理器
    
    用于处理大量数据的批量预处理。
    
    示例:
        >>> processor = BatchPreprocessor(db_operations)
        >>> processor.process_all_symbols(
        ...     adjust_type=AdjustType.QFQ,
        ...     indicators=["ma_20", "macd", "rsi_14"],
        ...     freqs=["W", "M"]
        ... )
    """
    
    def __init__(self, db_operations):
        """
        初始化批量预处理器
        
        Args:
            db_operations: 数据库操作对象
        """
        self.db_operations = db_operations
        self._pipeline = PreprocessPipeline(db_operations)
        
    def process_symbols(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjust_type: AdjustType = AdjustType.QFQ,
        indicators: Optional[List[str]] = None,
        freqs: Optional[List[str]] = None,
        batch_size: int = 100
    ) -> Dict[str, int]:
        """
        批量处理多只股票
        
        Args:
            symbols: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            adjust_type: 复权类型
            indicators: 指标列表
            freqs: 重采样频率列表
            batch_size: 批处理大小
            
        Returns:
            处理统计 {"symbols_processed": N, "records_processed": M}
        """
        indicators = indicators or ["ma_20", "macd", "rsi_14", "atr_14"]
        freqs = freqs or []
        
        total_symbols = 0
        total_records = 0
        
        # 分批处理
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}, symbols: {len(batch_symbols)}")
            
            # 获取数据
            df = self._fetch_data(batch_symbols, start_date, end_date)
            
            if df.empty:
                continue
            
            # 配置流水线
            self._pipeline.reset()
            self._pipeline.set_data(df)
            self._pipeline.adjust(adjust_type)
            
            for ind in indicators:
                self._pipeline.add_indicator(ind)
            
            for freq in freqs:
                self._pipeline.resample(freq)
            
            # 执行预处理
            if freqs:
                results = self._pipeline.run_with_resample()
                for freq_name, result_df in results.items():
                    count = self._save_processed_data(result_df, freq_name, adjust_type)
                    total_records += count
            else:
                result = self._pipeline.run()
                count = self._save_processed_data(result, "daily", adjust_type)
                total_records += count
            
            total_symbols += len(batch_symbols)
        
        return {
            "symbols_processed": total_symbols,
            "records_processed": total_records
        }
    
    def _fetch_data(
        self,
        symbols: List[str],
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> pd.DataFrame:
        """获取原始数据"""
        # TODO: 实现数据获取
        # 这里需要调用 db_operations 的方法
        return pd.DataFrame()
    
    def _save_processed_data(
        self,
        df: pd.DataFrame,
        freq: str,
        adjust_type: AdjustType
    ) -> int:
        """保存预处理数据"""
        # TODO: 实现数据存储
        return len(df)
