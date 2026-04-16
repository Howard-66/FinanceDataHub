#!/usr/bin/env python3
"""
读查询并发压测脚本

针对同一批 symbols 重复并发读取，比较不同 FDH 读并发阈值下的结果一致性。
当前默认压测行业估值重查询：FinanceDataHub.get_industry_valuation_async()
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
from typing import Iterable

import pandas as pd

from finance_data_hub.config import reload_settings
from finance_data_hub.sdk import FinanceDataHub


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FDH 读查询并发压测")
    parser.add_argument(
        "--symbols",
        required=True,
        help="逗号分隔的股票代码列表，如 600519.SH,000858.SZ",
    )
    parser.add_argument("--start-date", required=True, help="开始日期，格式 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--thresholds",
        default="1,4,8,16,32",
        help="要验证的 FDH 并发阈值列表，逗号分隔",
    )
    parser.add_argument(
        "--heavy-thresholds",
        default=None,
        help="重查询并发阈值列表，逗号分隔；默认与 --thresholds 相同",
    )
    parser.add_argument(
        "--app-concurrency",
        type=int,
        default=32,
        help="调用侧并发任务数，默认 32",
    )
    parser.add_argument(
        "--include-exempted",
        action="store_true",
        help="是否包含豁免样本",
    )
    return parser.parse_args()


def normalize_symbols(raw_symbols: str) -> list[str]:
    return [symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip()]


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        raise RuntimeError("query returned None")

    sort_columns = [
        column
        for column in ["symbol", "time", "l1_name", "l2_name", "l3_name"]
        if column in df.columns
    ]
    normalized = df.copy()
    if sort_columns:
        normalized = normalized.sort_values(sort_columns)
    normalized = normalized.sort_index(axis=1).reset_index(drop=True)
    normalized = normalized.where(pd.notna(normalized), None)
    return normalized


def dataframe_digest(df: pd.DataFrame) -> str:
    normalized = normalize_dataframe(df)
    payload = normalized.to_json(
        orient="split",
        date_format="iso",
        force_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def run_industry_valuation_once(
    fdh: FinanceDataHub,
    symbols: Iterable[str],
    start_date: str,
    end_date: str,
    include_exempted: bool,
) -> pd.DataFrame:
    result = await fdh.get_industry_valuation_async(
        symbols=list(symbols),
        start_date=start_date,
        end_date=end_date,
        include_exempted=include_exempted,
    )
    if result is None:
        raise RuntimeError("query returned None")
    return result


async def run_threshold(
    threshold: int,
    heavy_threshold: int,
    symbols: list[str],
    start_date: str,
    end_date: str,
    app_concurrency: int,
    include_exempted: bool,
) -> dict:
    os.environ["DATABASE_QUERY_MAX_CONCURRENCY"] = str(threshold)
    os.environ["DATABASE_HEAVY_QUERY_MAX_CONCURRENCY"] = str(heavy_threshold)

    settings = reload_settings()
    fdh = FinanceDataHub(settings, backend="postgresql")

    try:
        tasks = [
            run_industry_valuation_once(
                fdh=fdh,
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                include_exempted=include_exempted,
            )
            for _ in range(app_concurrency)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await fdh.close()

    exceptions = [result for result in results if isinstance(result, Exception)]
    if exceptions:
        first_error = exceptions[0]
        raise RuntimeError(
            f"threshold={threshold} failed with {len(exceptions)} exception(s): "
            f"{type(first_error).__name__}: {first_error}"
        ) from first_error

    digests = {dataframe_digest(result) for result in results}
    if len(digests) != 1:
        raise RuntimeError(
            f"threshold={threshold} produced inconsistent result digests: {sorted(digests)}"
        )

    sample_df = results[0]
    return {
        "threshold": threshold,
        "heavy_threshold": heavy_threshold,
        "rows": len(sample_df),
        "columns": len(sample_df.columns),
        "digest": next(iter(digests)),
    }


async def main() -> int:
    args = parse_args()
    symbols = normalize_symbols(args.symbols)
    thresholds = [int(item) for item in args.thresholds.split(",") if item.strip()]
    heavy_thresholds = (
        [int(item) for item in args.heavy_thresholds.split(",") if item.strip()]
        if args.heavy_thresholds
        else thresholds
    )

    if len(heavy_thresholds) != len(thresholds):
        raise RuntimeError("--heavy-thresholds 的数量必须与 --thresholds 一致")

    baseline_digest = None
    summaries = []

    for threshold, heavy_threshold in zip(thresholds, heavy_thresholds):
        summary = await run_threshold(
            threshold=threshold,
            heavy_threshold=heavy_threshold,
            symbols=symbols,
            start_date=args.start_date,
            end_date=args.end_date,
            app_concurrency=args.app_concurrency,
            include_exempted=args.include_exempted,
        )

        if baseline_digest is None:
            baseline_digest = summary["digest"]
        elif summary["digest"] != baseline_digest:
            raise RuntimeError(
                f"threshold={threshold} digest mismatch: "
                f"{summary['digest']} != baseline {baseline_digest}"
            )

        summaries.append(summary)

    print("Concurrency stress test passed.")
    for summary in summaries:
        print(
            "threshold={threshold} heavy_threshold={heavy_threshold} "
            "rows={rows} columns={columns} digest={digest}".format(
                **summary
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
