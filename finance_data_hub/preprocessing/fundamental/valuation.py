"""
估值指标分位计算

计算 PE/PB/PS 在历史数据中的分位数，用于判断当前估值水平。
使用滚动窗口计算，支持多个时间跨度（1年/2年/3年/5年）。

使用场景：
- 判断当前估值在历史中的位置
- 高分位（>80%）可能存在高估风险
- 低分位（<20%）可能存在低估机会
- 结合行业特性和成长性综合判断
"""

from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np
from loguru import logger


class ValuationPercentile:
    """
    估值分位计算器
    
    计算 PE_TTM、PB、PS_TTM 等估值指标的历史分位数。
    
    示例:
        >>> calculator = ValuationPercentile(
        ...     metrics=["pe_ttm", "pb"],
        ...     windows=[250, 500]
        ... )
        >>> result = calculator.calculate(df)
    """
    
    # 默认估值指标
    DEFAULT_METRICS = ["pe_ttm", "pb", "ps_ttm", "dv_ttm"]
    
    # 默认滚动窗口（交易日）
    # 1250 ≈ 5年, 2500 ≈ 10年
    DEFAULT_WINDOWS = [1250]
    
    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        windows: Optional[List[int]] = None
    ):
        """
        初始化估值分位计算器
        
        Args:
            metrics: 估值指标列表
            windows: 滚动窗口列表（交易日）
        """
        self.metrics = metrics or self.DEFAULT_METRICS
        self.windows = windows or self.DEFAULT_WINDOWS
        
    @property
    def columns(self) -> List[str]:
        """输出列名"""
        cols = []
        for metric in self.metrics:
            for window in self.windows:
                cols.append(f"{metric}_pct_{window}d")
        return cols
    
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算估值分位
        
        Args:
            df: 包含 symbol, time, 估值指标的 DataFrame
            
        Returns:
            添加分位列后的 DataFrame
            
        说明:
            分位数范围 0-100，表示当前值在历史数据中的位置：
            - 0: 历史最低
            - 50: 历史中位数
            - 100: 历史最高
        """
        result = df.copy()
        
        for metric in self.metrics:
            if metric not in df.columns:
                logger.warning(f"Metric '{metric}' not found in DataFrame, skipping")
                continue
                
            for window in self.windows:
                col_name = f"{metric}_pct_{window}d"
                
                result[col_name] = (
                    df.groupby("symbol")[metric]
                    .transform(
                        lambda x: self._rolling_percentile(x, window)
                    )
                )
                
        logger.debug(f"Calculated valuation percentiles for {len(result)} records")
        return result
    
    def _rolling_percentile(
        self, 
        series: pd.Series, 
        window: int
    ) -> pd.Series:
        """
        计算滚动分位数
        
        使用滚动窗口计算当前值在历史数据中的分位。
        
        Args:
            series: 指标值序列
            window: 窗口大小
            
        Returns:
            分位数序列 (0-100)
        """
        def calc_percentile(x):
            """计算当前值在窗口中的分位"""
            # 排除 NaN 和 <= 0 的值（PE/PB/PS 为负或零无意义）
            valid = x[(~np.isnan(x)) & (x > 0)]
            
            if len(valid) < 2:
                return np.nan
            
            current = x.iloc[-1]
            
            if np.isnan(current) or current <= 0:
                return np.nan
            
            # 计算当前值在窗口中的分位
            # 使用 < 而非 <= 来排除当前值本身
            rank = (valid < current).sum()
            percentile = rank / len(valid) * 100
            
            return percentile
        
        return series.rolling(
            window=window, 
            min_periods=min(20, window)  # 最少需要 20 个有效数据点
        ).apply(calc_percentile, raw=False)
    
    def get_percentile_level(self, percentile: float) -> str:
        """
        获取分位水平描述
        
        Args:
            percentile: 分位值 (0-100)
            
        Returns:
            水平描述
        """
        if percentile >= 80:
            return "极高"
        elif percentile >= 60:
            return "偏高"
        elif percentile >= 40:
            return "适中"
        elif percentile >= 20:
            return "偏低"
        else:
            return "极低"
    
    def calculate_current_percentile(
        self, 
        df: pd.DataFrame, 
        metric: str,
        window: int = 250
    ) -> pd.DataFrame:
        """
        计算最新分位数
        
        只计算每只股票的最新分位数，用于实时监控。
        
        Args:
            df: 包含历史数据的 DataFrame
            metric: 估值指标
            window: 滚动窗口
            
        Returns:
            每只股票最新分位数的 DataFrame
        """
        result_list = []
        
        for symbol, group in df.groupby("symbol"):
            if len(group) < window:
                # 数据不足
                continue
            
            # 取最近 window 天的数据
            recent = group.sort_values("time").tail(window)
            
            # 获取最新值
            latest = recent.iloc[-1]
            current_value = latest[metric]
            
            if pd.isna(current_value) or current_value <= 0:
                continue
            
            # 计算分位
            valid = recent[metric][(~recent[metric].isna()) & (recent[metric] > 0)]
            rank = (valid < current_value).sum()
            percentile = rank / len(valid) * 100
            
            result_list.append({
                "symbol": symbol,
                "time": latest["time"],
                metric: current_value,
                f"{metric}_pct": percentile,
                f"{metric}_level": self.get_percentile_level(percentile)
            })
        
        return pd.DataFrame(result_list)


class PEGCalculator:
    """
    PEG 指标计算器
    
    PEG = PE_TTM / 净利润同比增速(%)
    
    使用场景:
    - PEG < 1: 可能被低估
    - PEG = 1: 合理估值
    - PEG > 1: 可能被高估
    
    注意:
    - 仅当净利润增速 > 0 时计算有意义
    - 增速为负时 PEG 无意义,返回 NaN
    """
    
    def __init__(self):
        pass
    
    def calculate(
        self, 
        daily_basic: pd.DataFrame, 
        fina_indicator: pd.DataFrame
    ) -> pd.DataFrame:
        """
        计算 PEG 指标
        
        Args:
            daily_basic: 日度估值数据,需包含 symbol, time, pe_ttm
            fina_indicator: 财务指标数据,需包含 ts_code, end_date, netprofit_yoy
            
        Returns:
            添加 peg 列的 DataFrame
        """
        result = daily_basic.copy()
        
        if "pe_ttm" not in daily_basic.columns:
            logger.warning("pe_ttm not found in daily_basic, skipping PEG calculation")
            result["peg"] = np.nan
            return result
        
        if fina_indicator.empty or "netprofit_yoy" not in fina_indicator.columns:
            logger.warning("netprofit_yoy not found in fina_indicator, skipping PEG calculation")
            result["peg"] = np.nan
            return result
        
        # 标准化列名
        if "symbol" not in daily_basic.columns and "ts_code" in daily_basic.columns:
            result["symbol"] = result["ts_code"]
        
        # 准备财务数据:按股票获取最新的净利润增速
        fina = fina_indicator.copy()
        if "end_date_time" not in fina.columns and "end_date" in fina.columns:
            fina["end_date_time"] = pd.to_datetime(fina["end_date"])
        
        # 获取公告日期列
        if "ann_date_time" not in fina.columns and "ann_date" in fina.columns:
            fina["ann_date_time"] = pd.to_datetime(fina["ann_date"])
        
        # 计算 PEG
        peg_values = []
        
        for idx, row in result.iterrows():
            symbol = row.get("symbol")
            trade_date = row.get("time")
            pe_ttm = row.get("pe_ttm")
            
            if pd.isna(pe_ttm) or pe_ttm <= 0:
                peg_values.append(np.nan)
                continue
            
            # 获取该股票在交易日之前最新的财务数据
            stock_fina = fina[fina["ts_code"] == symbol]
            
            if stock_fina.empty:
                peg_values.append(np.nan)
                continue
            
            # 使用公告日期筛选
            date_col = "ann_date_time" if "ann_date_time" in stock_fina.columns else "end_date_time"
            available = stock_fina[stock_fina[date_col] <= trade_date]
            
            if available.empty:
                peg_values.append(np.nan)
                continue
            
            # 获取最新的净利润增速
            latest = available.sort_values(date_col).iloc[-1]
            netprofit_yoy = latest.get("netprofit_yoy")
            
            if pd.isna(netprofit_yoy) or netprofit_yoy <= 0:
                # 增速为负或为零,PEG 无意义
                peg_values.append(np.nan)
                continue
            
            # 计算 PEG
            peg = pe_ttm / netprofit_yoy
            peg_values.append(peg)
        
        result["peg"] = peg_values
        
        logger.debug(f"Calculated PEG for {len(result)} records")
        return result
    
    def calculate_batch(
        self,
        daily_basic: pd.DataFrame,
        fina_indicator: pd.DataFrame
    ) -> pd.DataFrame:
        """
        批量计算 PEG (优化版本,使用向量化操作)

        通过将财务数据按公告日期合并到日度数据,避免逐行循环。

        Args:
            daily_basic: 日度估值数据
            fina_indicator: 财务指标数据

        Returns:
            添加 peg 列的 DataFrame
        """
        result = daily_basic.copy()

        if "pe_ttm" not in daily_basic.columns:
            result["peg"] = np.nan
            return result

        if fina_indicator.empty or "netprofit_yoy" not in fina_indicator.columns:
            result["peg"] = np.nan
            return result

        # 准备财务数据
        fina = fina_indicator[["ts_code", "ann_date_time", "netprofit_yoy"]].copy()
        # 确保时间列是 datetime 类型
        if fina["ann_date_time"].dtype == "object":
            fina["ann_date_time"] = pd.to_datetime(fina["ann_date_time"])
        fina = fina.sort_values(["ts_code", "ann_date_time"])

        # 调试：检查 netprofit_yoy 数据
        valid_yoy = fina[fina["netprofit_yoy"].notna() & (fina["netprofit_yoy"] > 0)]

        # 为每只股票创建增速时间序列用于asof merge
        peg_list = []

        for symbol in result["symbol"].unique():
            stock_daily = result[result["symbol"] == symbol].copy()
            stock_fina = fina[fina["ts_code"] == symbol].copy()

            if stock_fina.empty:
                stock_daily["peg"] = np.nan
                peg_list.append(stock_daily)
                continue

            # 确保 daily_basic 的 time 列是 datetime 类型
            if stock_daily["time"].dtype == "object":
                stock_daily["time"] = pd.to_datetime(stock_daily["time"])

            # 使用 merge_asof 进行时点匹配
            stock_daily = stock_daily.sort_values("time")
            stock_fina = stock_fina.rename(columns={"ann_date_time": "time_fina"})

            merged = pd.merge_asof(
                stock_daily,
                stock_fina[["time_fina", "netprofit_yoy"]],
                left_on="time",
                right_on="time_fina",
                direction="backward"
            )

            # 调试：检查合并后的数据
            matched = merged[merged["netprofit_yoy"].notna()]
            valid_peg = merged[(merged["netprofit_yoy"].notna()) & (merged["netprofit_yoy"] > 0) & (merged["pe_ttm"] > 0)]
            # print(f"[DEBUG] PEG计算: {symbol} 匹配 {len(matched)} 条, 有效PEG {len(valid_peg)} 条")

            # 计算 PEG
            merged["peg"] = np.where(
                (merged["pe_ttm"] > 0) & (merged["netprofit_yoy"] > 0),
                merged["pe_ttm"] / merged["netprofit_yoy"],
                np.nan
            )

            peg_list.append(merged.drop(columns=["time_fina", "netprofit_yoy"], errors="ignore"))

        if peg_list:
            result = pd.concat(peg_list, ignore_index=True)
        else:
            result["peg"] = np.nan

        return result
    
    def get_peg_level(self, peg: float) -> str:
        """
        获取 PEG 估值水平描述
        
        Args:
            peg: PEG 值
            
        Returns:
            水平描述
        """
        if pd.isna(peg):
            return "无效"
        elif peg < 0.5:
            return "极度低估"
        elif peg < 1.0:
            return "低估"
        elif peg < 1.5:
            return "合理"
        elif peg < 2.0:
            return "偏高"
        else:
            return "高估"

