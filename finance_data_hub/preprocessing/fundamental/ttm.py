"""
Shared TTM helpers for quarterly cumulative financial statement fields.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def calc_cumulative_to_ttm(
    df: pd.DataFrame,
    col: str,
    date_col: str = "end_date_time",
) -> pd.Series:
    """
    Convert year-to-date quarterly values to trailing-twelve-month values.

    Formula:
    - Q4: TTM = annual report value
    - Q1-Q3: TTM = current YTD + previous annual - previous same-quarter YTD
    """
    if col not in df.columns or date_col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index)

    end_dates = pd.to_datetime(df[date_col])
    months = end_dates.dt.month
    years = end_dates.dt.year
    values = pd.to_numeric(df[col], errors="coerce")

    lookup_df = pd.DataFrame(
        {
            "year": years.values,
            "month": months.values,
            "val": values.values,
        },
        index=df.index,
    )

    annual_map = (
        lookup_df.loc[lookup_df["month"] == 12]
        .drop_duplicates(subset=["year"], keep="last")
        .set_index("year")["val"]
    )

    ym_map = (
        lookup_df.dropna(subset=["val"])
        .drop_duplicates(subset=["year", "month"], keep="last")
        .set_index(["year", "month"])["val"]
    )

    result = pd.Series(np.nan, index=df.index, dtype=float)

    q4_mask = (months == 12) & values.notna()
    result[q4_mask] = values[q4_mask]

    non_q4_mask = (months != 12) & values.notna()
    if non_q4_mask.any():
        prev_years = years[non_q4_mask] - 1
        cur_months = months[non_q4_mask]

        prev_annual = prev_years.map(annual_map)
        prev_same_q_keys = list(zip(prev_years.values, cur_months.values))
        prev_same_q = pd.Series(
            [ym_map.get(k) for k in prev_same_q_keys],
            index=prev_annual.index,
        )

        valid = prev_annual.notna() & prev_same_q.notna()
        if valid.any():
            combined_mask = non_q4_mask & valid.reindex(result.index, fill_value=False)
            ttm_vals = (
                values[combined_mask].values
                + prev_annual[valid].values
                - prev_same_q[valid].values
            )
            result.loc[combined_mask] = ttm_vals

    return result
