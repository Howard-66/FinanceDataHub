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

from .ttm import calc_cumulative_to_ttm


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
        使用 raw=True 优化性能，传入 numpy array 而非 Series，提升 3-5x。

        Args:
            series: 指标值序列
            window: 窗口大小

        Returns:
            分位数序列 (0-100)
        """
        def calc_percentile(x):
            """计算当前值在窗口中的分位

            Args:
                x: numpy array (当 raw=True 时)
            """
            # 排除 NaN 和 <= 0 的值（PE/PB/PS 为负或零无意义）
            valid = x[(~np.isnan(x)) & (x > 0)]

            if len(valid) < 2:
                return np.nan

            current = x[-1]  # numpy array 直接索引，比 iloc 快

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
        ).apply(calc_percentile, raw=True)  # raw=True: 传入 numpy array，性能提升 3-5x
    
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


class ValuationFillCalculator:
    """
    Derive daily valuation fills from market value and announced financial data.

    The calculator emits derived values only; callers should keep raw daily_basic
    values as the source of truth and coalesce raw values before these fills.
    """

    METRIC_COLUMNS = [
        "pe", "pe_ttm", "pb", "ps", "ps_ttm", "peg", "dv_ratio", "dv_ttm"
    ]
    FORMULA_VERSION = "valuation_fill_v1"
    FINANCIAL_UNIT_SCALE = 10000.0  # statement yuan -> daily_basic total_mv 10k CNY

    def calculate(
        self,
        daily_basic: pd.DataFrame,
        income: pd.DataFrame,
        balancesheet: pd.DataFrame,
        fina_indicator: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Calculate derived valuation fills.

        Args:
            daily_basic: Daily rows with symbol, time, total_mv, total_share, close
                         and raw valuation columns when available.
            income: Income statement rows with n_income_attr_p, total_revenue.
            balancesheet: Balance sheet rows with total_hldr_eqy_exc_min_int.
            fina_indicator: Financial indicators with netprofit_yoy and bps.

        Returns:
            DataFrame with symbol, time, derived metrics, JSON metadata columns,
            and formula_version. Rows with no derived metric are omitted.
        """
        daily = self._prepare_daily(daily_basic)
        if daily.empty:
            return self._empty_result()

        income_ttm = self._prepare_income_ttm(income)
        income_annual = self._prepare_income_annual(income_ttm)
        balance = self._prepare_balance(balancesheet)
        fina = self._prepare_fina_indicator(fina_indicator)

        merged = self._merge_asof_by_symbol(
            daily,
            income_ttm,
            value_columns=[
                "income_effective_date", "income_end_date_time",
                "ni_attr_p_ttm", "revenue_ttm", "income_revenue_source",
            ],
        )
        merged = self._merge_asof_by_symbol(
            merged,
            income_annual,
            value_columns=[
                "annual_income_effective_date", "annual_income_end_date_time",
                "annual_ni_attr_p", "annual_revenue",
                "annual_revenue_source",
            ],
        )
        merged = self._merge_asof_by_symbol(
            merged,
            balance,
            value_columns=[
                "balance_effective_date", "balance_end_date_time",
                "parent_equity",
            ],
        )
        merged = self._merge_asof_by_symbol(
            merged,
            fina,
            value_columns=[
                "fina_effective_date", "fina_end_date_time",
                "netprofit_yoy", "bps",
            ],
        )

        return self._calculate_metrics(merged)

    def _empty_result(self) -> pd.DataFrame:
        columns = (
            ["time", "symbol"] +
            self.METRIC_COLUMNS +
            ["sources", "denominator_dates", "quality_flags", "formula_version"]
        )
        return pd.DataFrame(columns=columns)

    def _prepare_daily(self, daily_basic: pd.DataFrame) -> pd.DataFrame:
        daily = daily_basic.copy()
        if daily.empty:
            return daily

        if "time" not in daily.columns or "symbol" not in daily.columns:
            logger.warning("daily_basic must contain time and symbol for valuation fills")
            return pd.DataFrame()

        daily["time"] = self._to_local_naive_datetime(daily["time"])

        for metric in self.METRIC_COLUMNS:
            if metric not in daily.columns:
                daily[metric] = np.nan
            daily = daily.rename(columns={metric: f"{metric}_raw"})

        for col in ["total_mv", "total_share", "close"]:
            if col not in daily.columns:
                daily[col] = np.nan
            daily[col] = pd.to_numeric(daily[col], errors="coerce")

        total_mv = daily["total_mv"]
        fallback_mv = daily["close"] * daily["total_share"]
        daily["_market_value"] = np.where(
            total_mv > 0,
            total_mv,
            np.where(fallback_mv > 0, fallback_mv, np.nan),
        )
        daily["_market_value_source"] = np.where(
            total_mv > 0,
            "daily_basic.total_mv",
            np.where(
                fallback_mv > 0,
                "symbol_daily.close*daily_basic.total_share",
                None,
            ),
        )

        return daily.sort_values(["symbol", "time"]).reset_index(drop=True)

    def _prepare_income_ttm(self, income: pd.DataFrame) -> pd.DataFrame:
        inc = income.copy()
        if inc.empty or "ts_code" not in inc.columns:
            return pd.DataFrame()

        inc = self._ensure_statement_dates(inc)
        if inc.empty:
            return pd.DataFrame()

        for col in ["n_income_attr_p", "revenue", "total_revenue"]:
            if col not in inc.columns:
                inc[col] = np.nan
            inc[col] = pd.to_numeric(inc[col], errors="coerce")

        inc["ps_revenue_base"] = inc["revenue"].combine_first(inc["total_revenue"])
        inc["income_revenue_source"] = np.where(
            inc["revenue"].notna(),
            "revenue",
            np.where(inc["total_revenue"].notna(), "total_revenue", None),
        )

        pieces = []
        for _, group in inc.groupby("ts_code", sort=False):
            group = group.sort_values("end_date_time").copy()
            group["ni_attr_p_ttm"] = calc_cumulative_to_ttm(
                group, "n_income_attr_p"
            )
            group["revenue_ttm"] = calc_cumulative_to_ttm(
                group, "ps_revenue_base"
            )
            pieces.append(group)

        if not pieces:
            return pd.DataFrame()

        inc = pd.concat(pieces, ignore_index=True)
        inc = inc.rename(
            columns={
                "effective_date": "income_effective_date",
                "end_date_time": "income_end_date_time",
            }
        )
        return inc[
            [
                "ts_code", "income_effective_date", "income_end_date_time",
                "ni_attr_p_ttm", "revenue_ttm", "income_revenue_source",
            ]
        ].sort_values(["ts_code", "income_effective_date"])

    def _prepare_income_annual(self, income_ttm: pd.DataFrame) -> pd.DataFrame:
        if income_ttm.empty:
            return pd.DataFrame()

        end_dates = pd.to_datetime(income_ttm["income_end_date_time"])
        annual = income_ttm.loc[end_dates.dt.month == 12].copy()
        if annual.empty:
            return pd.DataFrame()

        annual = annual.rename(
            columns={
                "income_effective_date": "annual_income_effective_date",
                "income_end_date_time": "annual_income_end_date_time",
                "ni_attr_p_ttm": "annual_ni_attr_p",
                "revenue_ttm": "annual_revenue",
                "income_revenue_source": "annual_revenue_source",
            }
        )
        return annual[
            [
                "ts_code", "annual_income_effective_date",
                "annual_income_end_date_time", "annual_ni_attr_p",
                "annual_revenue", "annual_revenue_source",
            ]
        ].sort_values(["ts_code", "annual_income_effective_date"])

    def _prepare_balance(self, balancesheet: pd.DataFrame) -> pd.DataFrame:
        bs = balancesheet.copy()
        if bs.empty or "ts_code" not in bs.columns:
            return pd.DataFrame()

        bs = self._ensure_statement_dates(bs)
        if bs.empty:
            return pd.DataFrame()

        if "total_hldr_eqy_exc_min_int" not in bs.columns:
            bs["total_hldr_eqy_exc_min_int"] = np.nan
        bs["parent_equity"] = pd.to_numeric(
            bs["total_hldr_eqy_exc_min_int"], errors="coerce"
        )
        bs = bs.rename(
            columns={
                "effective_date": "balance_effective_date",
                "end_date_time": "balance_end_date_time",
            }
        )
        return bs[
            [
                "ts_code", "balance_effective_date", "balance_end_date_time",
                "parent_equity",
            ]
        ].sort_values(["ts_code", "balance_effective_date"])

    def _prepare_fina_indicator(self, fina_indicator: pd.DataFrame) -> pd.DataFrame:
        fina = fina_indicator.copy()
        if fina.empty or "ts_code" not in fina.columns:
            return pd.DataFrame()

        fina = self._ensure_statement_dates(fina, prefer_f_ann=False)
        if fina.empty:
            return pd.DataFrame()

        for col in ["netprofit_yoy", "bps"]:
            if col not in fina.columns:
                fina[col] = np.nan
            fina[col] = pd.to_numeric(fina[col], errors="coerce")

        fina = fina.rename(
            columns={
                "effective_date": "fina_effective_date",
                "end_date_time": "fina_end_date_time",
            }
        )
        return fina[
            [
                "ts_code", "fina_effective_date", "fina_end_date_time",
                "netprofit_yoy", "bps",
            ]
        ].sort_values(["ts_code", "fina_effective_date"])

    def _ensure_statement_dates(
        self,
        df: pd.DataFrame,
        prefer_f_ann: bool = True,
    ) -> pd.DataFrame:
        result = df.copy()

        if "end_date_time" not in result.columns and "end_date" in result.columns:
            result["end_date_time"] = result["end_date"]
        if "end_date_time" not in result.columns:
            return pd.DataFrame()

        result["end_date_time"] = self._to_local_naive_datetime(
            result["end_date_time"]
        )

        date_candidates = []
        if prefer_f_ann:
            date_candidates.extend(["f_ann_date_time", "f_ann_date"])
        date_candidates.extend(["ann_date_time", "ann_date"])

        effective = None
        for col in date_candidates:
            if col not in result.columns:
                continue
            values = self._to_local_naive_datetime(result[col])
            effective = values if effective is None else effective.combine_first(values)

        if effective is None:
            return pd.DataFrame()

        result["effective_date"] = effective.dt.normalize()
        result = result.dropna(subset=["effective_date", "end_date_time"])
        return result.sort_values(["ts_code", "end_date_time", "effective_date"])

    def _to_local_naive_datetime(self, series: pd.Series) -> pd.Series:
        def convert(value: Any) -> pd.Timestamp:
            if pd.isna(value):
                return pd.NaT
            ts = pd.to_datetime(value, errors="coerce")
            if pd.isna(ts):
                return pd.NaT
            if ts.tzinfo is not None:
                ts = ts.tz_convert("Asia/Shanghai").tz_localize(None)
            return ts

        return series.map(convert)

    def _merge_asof_by_symbol(
        self,
        daily: pd.DataFrame,
        financial: pd.DataFrame,
        value_columns: List[str],
    ) -> pd.DataFrame:
        if daily.empty:
            return daily

        if financial.empty:
            result = daily.copy()
            for col in value_columns:
                if col not in result.columns:
                    result[col] = np.nan
            return result

        effective_col = value_columns[0]
        frames = []
        for symbol, stock_daily in daily.groupby("symbol", sort=False):
            stock_daily = stock_daily.sort_values("time")
            stock_fin = financial[financial["ts_code"] == symbol].copy()
            if stock_fin.empty:
                stock_result = stock_daily.copy()
                for col in value_columns:
                    if col not in stock_result.columns:
                        stock_result[col] = np.nan
                frames.append(stock_result)
                continue

            stock_fin = stock_fin.sort_values(effective_col)
            merged = pd.merge_asof(
                stock_daily,
                stock_fin[value_columns],
                left_on="time",
                right_on=effective_col,
                direction="backward",
            )
            frames.append(merged)

        return pd.concat(frames, ignore_index=True) if frames else daily

    def _calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        mv = pd.to_numeric(result["_market_value"], errors="coerce")

        ni_ttm = self._statement_to_10k(self._get_series(result, "ni_attr_p_ttm"))
        revenue_ttm = self._statement_to_10k(self._get_series(result, "revenue_ttm"))
        annual_ni = self._statement_to_10k(self._get_series(result, "annual_ni_attr_p"))
        annual_revenue = self._statement_to_10k(self._get_series(result, "annual_revenue"))
        parent_equity = self._statement_to_10k(self._get_series(result, "parent_equity"))

        derived_pe_ttm = self._ratio(mv, ni_ttm)
        derived_ps_ttm = self._ratio(mv, revenue_ttm)
        result["pe"] = self._ratio(mv, annual_ni)
        result["ps"] = self._ratio(mv, annual_revenue)
        result["pe_ttm"] = derived_pe_ttm.combine_first(result["pe"])
        result["ps_ttm"] = derived_ps_ttm.combine_first(result["ps"])
        result["_pe_ttm_source"] = np.where(
            derived_pe_ttm.notna(),
            "derived_ttm_income.n_income_attr_p",
            np.where(result["pe"].notna(), "derived_from_lfy_pe_when_ttm_unavailable", None),
        )
        pb_from_equity = self._ratio(mv, parent_equity)

        bps_pb = self._ratio(
            self._get_series(result, "close"),
            self._get_series(result, "bps"),
        )
        result["pb"] = bps_pb.combine_first(pb_from_equity)
        result["_pb_source"] = np.where(
            bps_pb.notna(),
            "derived_fina_indicator.bps",
            np.where(pb_from_equity.notna(), "derived_balancesheet.total_hldr_eqy_exc_min_int", None),
        )
        result["_ps_source"] = result.apply(
            lambda row: self._income_source_label(row.get("annual_revenue_source"), "derived_lfy_income"),
            axis=1,
        )
        result["_ps_ttm_source"] = result.apply(
            lambda row: self._ps_ttm_source_label(
                row.get("income_revenue_source"),
                row.get("ps_ttm"),
                derived_ps_ttm.loc[row.name] if row.name in derived_ps_ttm.index else np.nan,
            ),
            axis=1,
        )

        pe_ttm_basis = result["pe_ttm"].combine_first(
            self._get_series(result, "pe_ttm_raw")
        )
        netprofit_yoy = self._get_series(result, "netprofit_yoy")
        result["peg"] = self._ratio(pe_ttm_basis, netprofit_yoy)
        result["dv_ratio"] = np.nan
        result["dv_ttm"] = np.nan

        result["sources"] = result.apply(self._build_sources, axis=1)
        result["denominator_dates"] = result.apply(
            self._build_denominator_dates, axis=1
        )
        result["quality_flags"] = result.apply(self._build_quality_flags, axis=1)
        result["formula_version"] = self.FORMULA_VERSION

        metric_has_value = result[self.METRIC_COLUMNS].notna().any(axis=1)
        result = result.loc[metric_has_value].copy()
        if result.empty:
            return self._empty_result()

        return result[
            [
                "time", "symbol", *self.METRIC_COLUMNS,
                "sources", "denominator_dates", "quality_flags",
                "formula_version",
            ]
        ].reset_index(drop=True)

    def _get_series(self, df: pd.DataFrame, col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(np.nan, index=df.index, dtype=float)
        return pd.to_numeric(df[col], errors="coerce")

    def _statement_to_10k(self, series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce") / self.FINANCIAL_UNIT_SCALE

    def _ratio(self, numerator: pd.Series, denominator: pd.Series) -> pd.Series:
        numerator = pd.to_numeric(numerator, errors="coerce")
        denominator = pd.to_numeric(denominator, errors="coerce")
        return pd.Series(
            np.where(
                (numerator > 0) & (denominator > 0),
                numerator / denominator,
                np.nan,
            ),
            index=numerator.index,
            dtype=float,
        )

    def _build_sources(self, row: pd.Series) -> Dict[str, str]:
        sources: Dict[str, str] = {}
        if pd.notna(row.get("pe_ttm")):
            sources["pe_ttm"] = row.get("_pe_ttm_source") or "derived_ttm_income.n_income_attr_p"
        if pd.notna(row.get("ps_ttm")):
            sources["ps_ttm"] = row.get("_ps_ttm_source") or "derived_ttm_income.revenue"
        if pd.notna(row.get("pe")):
            sources["pe"] = "derived_lfy_income.n_income_attr_p"
        if pd.notna(row.get("ps")):
            sources["ps"] = row.get("_ps_source") or "derived_lfy_income.revenue"
        if pd.notna(row.get("pb")):
            sources["pb"] = row.get("_pb_source") or "valuation_fill"
        if pd.notna(row.get("peg")):
            if pd.notna(row.get("pe_ttm")):
                sources["peg"] = "derived_fina_indicator.netprofit_yoy"
            else:
                sources["peg"] = "derived_from_raw_pe_ttm_and_netprofit_yoy"
        return sources

    def _build_denominator_dates(self, row: pd.Series) -> Dict[str, str]:
        dates: Dict[str, str] = {}
        if pd.notna(row.get("pe_ttm")):
            if row.get("_pe_ttm_source") == "derived_from_lfy_pe_when_ttm_unavailable":
                self._add_date(dates, "pe_ttm", row.get("annual_income_end_date_time"))
            else:
                self._add_date(dates, "pe_ttm", row.get("income_end_date_time"))
        if pd.notna(row.get("ps_ttm")):
            if row.get("_ps_ttm_source") == "derived_from_lfy_ps_when_ttm_unavailable":
                self._add_date(dates, "ps_ttm", row.get("annual_income_end_date_time"))
            else:
                self._add_date(dates, "ps_ttm", row.get("income_end_date_time"))
        if pd.notna(row.get("pe")):
            self._add_date(dates, "pe", row.get("annual_income_end_date_time"))
        if pd.notna(row.get("ps")):
            self._add_date(dates, "ps", row.get("annual_income_end_date_time"))
        if pd.notna(row.get("pb")):
            if row.get("_pb_source") == "derived_balancesheet.total_hldr_eqy_exc_min_int":
                self._add_date(dates, "pb", row.get("balance_end_date_time"))
            else:
                self._add_date(dates, "pb", row.get("fina_end_date_time"))
        if pd.notna(row.get("peg")):
            self._add_date(dates, "peg", row.get("fina_end_date_time"))
        return dates

    def _build_quality_flags(self, row: pd.Series) -> Dict[str, Any]:
        raw_missing = {
            metric: bool(pd.isna(row.get(f"{metric}_raw")))
            for metric in self.METRIC_COLUMNS
        }
        flags: Dict[str, Any] = {
            "market_value_source": row.get("_market_value_source"),
            "financial_statement_unit": "yuan_converted_to_10k_cny",
            "raw_missing": raw_missing,
        }
        if row.get("_pe_ttm_source") == "derived_from_lfy_pe_when_ttm_unavailable":
            flags["pe_ttm"] = "fallback_to_pe_due_to_incomplete_ttm_window"
        if row.get("_ps_ttm_source") == "derived_from_lfy_ps_when_ttm_unavailable":
            flags["ps_ttm"] = "fallback_to_ps_due_to_incomplete_ttm_window"
        if pd.isna(row.get("dv_ratio")):
            flags["dv_ratio"] = "not_derived_without_dividend_data"
        if pd.isna(row.get("dv_ttm")):
            flags["dv_ttm"] = "not_derived_without_dividend_data"
        return flags

    def _income_source_label(self, source: Any, prefix: str) -> Optional[str]:
        if source == "total_revenue":
            return f"{prefix}.total_revenue"
        if source == "revenue":
            return f"{prefix}.revenue"
        return None

    def _ps_ttm_source_label(
        self,
        source: Any,
        ps_ttm_value: Any,
        derived_ps_ttm_value: Any,
    ) -> Optional[str]:
        if pd.notna(derived_ps_ttm_value):
            return self._income_source_label(source, "derived_ttm_income")
        if pd.notna(ps_ttm_value):
            return "derived_from_lfy_ps_when_ttm_unavailable"
        return None

    def _add_date(self, target: Dict[str, str], key: str, value: Any) -> None:
        if pd.isna(value):
            return
        target[key] = pd.Timestamp(value).date().isoformat()
