"""
资金流向类指标

包含：
- NDA: 净派发/吸筹方向指标
"""

from math import ceil
from typing import List

import numpy as np
import pandas as pd

from .base import BaseIndicator, register_indicator


class NDAIndicator(BaseIndicator):
    """
    Net Distribution/Accumulation (NDA)

    逻辑与 ValueInvesting 前端图表保持一致：
    - 对每个 bar 回看最近 window 个周期
    - 选取成交量前 top_percentile 的 bar
    - 统计这些高量 bar 中上涨日与下跌日数量差
    - nda_value = up_days - down_days
    - volume_confirmed = nda_value >= 1
    """

    def __init__(self, window: int = 20, top_percentile: float = 0.25):
        self.window = int(window)
        self.top_percentile = float(top_percentile)

    @property
    def name(self) -> str:
        return "nda"

    @property
    def columns(self) -> List[str]:
        return ["nda_value", "volume_confirmed"]

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        required_columns = ["symbol", "open", "close"]
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"DataFrame must contain '{column}' column for NDA calculation")

        volume_col = None
        for candidate in ("volume", "vol"):
            if candidate in df.columns:
                volume_col = candidate
                break
        if volume_col is None:
            raise ValueError("DataFrame must contain 'volume' or 'vol' column for NDA calculation")

        top_count = max(1, ceil(self.window * self.top_percentile))

        def _calc_group(group: pd.DataFrame) -> pd.DataFrame:
            open_values = pd.to_numeric(group["open"], errors="coerce")
            close_values = pd.to_numeric(group["close"], errors="coerce")
            volume_values = pd.to_numeric(group[volume_col], errors="coerce")

            nda_values = pd.Series(np.nan, index=group.index, dtype="float64")
            volume_confirmed = pd.Series([None] * len(group), index=group.index, dtype="object")

            if volume_values.notna().sum() == 0:
                return pd.DataFrame(
                    {
                        "nda_value": nda_values,
                        "volume_confirmed": volume_confirmed,
                    },
                    index=group.index,
                )

            for end_pos in range(self.window - 1, len(group)):
                window_slice = slice(end_pos - self.window + 1, end_pos + 1)
                window_frame = pd.DataFrame(
                    {
                        "open": open_values.iloc[window_slice].to_numpy(),
                        "close": close_values.iloc[window_slice].to_numpy(),
                        "volume": volume_values.iloc[window_slice].to_numpy(),
                    }
                ).dropna(subset=["open", "close", "volume"])

                if window_frame.empty:
                    continue

                top_volume_days = window_frame.nlargest(min(top_count, len(window_frame)), "volume")
                up_days = (top_volume_days["close"] > top_volume_days["open"]).sum()
                nda_value = int(up_days) - (len(top_volume_days) - int(up_days))

                row_index = group.index[end_pos]
                nda_values.loc[row_index] = nda_value
                volume_confirmed.loc[row_index] = nda_value >= 1

            return pd.DataFrame(
                {
                    "nda_value": nda_values,
                    "volume_confirmed": volume_confirmed,
                },
                index=group.index,
            )

        result = df.copy()
        computed = df.groupby("symbol", group_keys=False).apply(
            _calc_group, include_groups=False
        )
        result["nda_value"] = computed["nda_value"]
        result["volume_confirmed"] = computed["volume_confirmed"]
        return result


register_indicator("nda", NDAIndicator)
