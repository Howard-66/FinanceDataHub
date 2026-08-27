"""量化主线策略事实层与因子宽表。

本模块只生产可复用的点时数据和因子，不计算策略分数、仓位或交易指令。
"""

from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

if TYPE_CHECKING:
    from finance_data_hub.database.manager import DatabaseManager


MAINLINE_TABLES = {
    "stock_daily": ("processed_mainline_stock_daily", "ts_code", "trade_date"),
    "market_daily": ("processed_mainline_market_daily", None, "trade_date"),
    "industry_daily": ("processed_mainline_industry_daily", "l2_code", "trade_date"),
    "etf_daily": ("processed_mainline_etf_daily", "ts_code", "trade_date"),
    "fund_crowding_monthly": (
        "processed_mainline_fund_crowding_monthly",
        "ts_code",
        "report_period",
    ),
    "industry_crowding_monthly": (
        "processed_mainline_industry_crowding_monthly", "l2_code", "report_period"
    ),
    "etf_exposure_monthly": (
        "processed_mainline_etf_exposure_monthly", "ts_code", "as_of_trade_date"
    ),
    "leadlag_monthly": (
        "processed_mainline_leadlag_monthly",
        "leader_code",
        "month_end",
    ),
    "leadlag_score_monthly": (
        "processed_mainline_leadlag_score_monthly", "l2_code", "month_end"
    ),
    "snapshot_manifest": (
        "processed_mainline_snapshot_manifest", None, "as_of_trade_date"
    ),
    "data_status": ("processed_mainline_data_status", "dataset", "partition_date"),
}

# 主线策略的统一历史回测起点。无 --force 时仍保留最近 400 天的
# 默认增量窗口；CLI 的全量模式会显式传入该日期。
MAINLINE_HISTORY_START = "2012-01-01"
MAINLINE_FACTOR_VERSION = 4
# Monthly partitions make incremental repairs cheap, but are prohibitively
# expensive for a 10+ year rebuild because every partition re-reads its rolling
# source window.  Six months keeps individual WindowAgg operations bounded
# while removing most of that redundant I/O.
MAINLINE_LONG_RANGE_PARTITION_MONTHS = 6
MAINLINE_LONG_RANGE_THRESHOLD_DAYS = 730
MAINLINE_QUERY_WORK_MEM = "128MB"
MAINLINE_REBUILD_WARMUP_START = date(2008, 1, 1)
MAINLINE_FORMULA_HASH = sha256(
    b"mainline-pit-v4:official-sw-relative-strength,strong-stock-global-top20,"
    b"risk-adjusted-momentum,industry-own-history-crowding,hierarchical-pit-etf-mapping,"
    b"etf-tool-coverage-is-strategy-state,leadlag-shadow-only"
).hexdigest()


class MainlineDataStorage:
    """主线因子表的统一只读入口。"""

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self.db_manager = db_manager

    async def query(
        self,
        dataset: str,
        codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        eligible_only: bool = False,
        factor_version: int = MAINLINE_FACTOR_VERSION,
        usable_on_or_before: Optional[str] = None,
    ) -> pd.DataFrame:
        config = MAINLINE_TABLES.get(dataset)
        if config is None:
            raise ValueError(f"Unsupported mainline dataset: {dataset}")
        table_name, code_column, date_column = config
        clauses = ["factor_version = :factor_version"]
        params: Dict[str, Any] = {"factor_version": factor_version}
        if codes and code_column:
            clauses.append(f"{code_column} = ANY(:codes)")
            params["codes"] = codes
        if start_date:
            clauses.append(f"{date_column} >= :start_date")
            params["start_date"] = pd.Timestamp(start_date).date()
        if end_date:
            clauses.append(f"{date_column} <= :end_date")
            params["end_date"] = pd.Timestamp(end_date).date()
        if eligible_only:
            if dataset not in {"stock_daily", "etf_daily"}:
                raise ValueError(
                    "eligible_only only supports stock_daily and etf_daily"
                )
            clauses.append("is_eligible = TRUE")
        if usable_on_or_before:
            # PIT consumers pass their execution date here.  This protects a
            # backtest from reading a factor before it could have been traded.
            clauses.append("usable_from_trade_date <= :usable_on_or_before")
            params["usable_on_or_before"] = pd.Timestamp(usable_on_or_before).date()

        order = [date_column]
        if code_column:
            order.append(code_column)
        statement = text(
            f"SELECT * FROM {table_name} WHERE {' AND '.join(clauses)} "
            f"ORDER BY {', '.join(order)}"
        )
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            result = await conn.execute(statement, params)
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=result.keys())

    async def get_manifest(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        status: Optional[str] = None,
        factor_version: int = MAINLINE_FACTOR_VERSION,
    ) -> pd.DataFrame:
        """Return immutable manifest rows for one factor formula version."""
        clauses = ["factor_version = :factor_version"]
        params: Dict[str, Any] = {"factor_version": factor_version}
        if start_date:
            clauses.append("as_of_trade_date >= :start_date")
            params["start_date"] = pd.Timestamp(start_date).date()
        if end_date:
            clauses.append("as_of_trade_date <= :end_date")
            params["end_date"] = pd.Timestamp(end_date).date()
        if status:
            clauses.append("status = :status")
            params["status"] = status
        return await self._read(
            "SELECT * FROM processed_mainline_snapshot_manifest "
            f"WHERE {' AND '.join(clauses)} ORDER BY as_of_trade_date",
            params,
        )

    async def get_data_status(
        self,
        partition_date: Optional[str] = None,
        factor_version: int = MAINLINE_FACTOR_VERSION,
    ) -> pd.DataFrame:
        """Return Gate 0 evidence, defaulting to the latest completed partition."""
        params: Dict[str, Any] = {"factor_version": factor_version}
        if partition_date:
            date_clause = "AND partition_date = :partition_date"
            params["partition_date"] = pd.Timestamp(partition_date).date()
        else:
            date_clause = """AND partition_date = (
                SELECT MAX(partition_date) FROM processed_mainline_data_status
                WHERE factor_version = :factor_version
            )"""
        return await self._read(
            f"""SELECT * FROM processed_mainline_data_status
                WHERE factor_version = :factor_version {date_clause}
                ORDER BY dataset""",
            params,
        )

    async def query_asof(
        self,
        dataset: str,
        as_of_date: str,
        execution_date: str,
        codes: Optional[List[str]] = None,
        eligible_only: bool = False,
        factor_version: int = MAINLINE_FACTOR_VERSION,
    ) -> pd.DataFrame:
        """Read the latest PIT-safe row per entity as known on an observation day."""
        config = MAINLINE_TABLES.get(dataset)
        if config is None or dataset in {"snapshot_manifest", "data_status"}:
            raise ValueError(f"Unsupported as-of mainline dataset: {dataset}")
        table_name, code_column, date_column = config
        # Daily factor rows are already a complete cross-section for one
        # observation day.  Asking PostgreSQL to DISTINCT every historical
        # ETF/industry row is both unnecessary and costly after the v2 rebuild
        # (and gets worse once old chunks are compressed).  The observation
        # date is the authoritative snapshot boundary; only monthly facts
        # need an as-of carry-forward lookup.
        if dataset in {"stock_daily", "market_daily", "industry_daily", "etf_daily"}:
            clauses = [
                "factor_version = :factor_version",
                f"{date_column} = :as_of_date",
                "usable_from_trade_date <= :execution_date",
            ]
            params: Dict[str, Any] = {
                "factor_version": factor_version,
                "as_of_date": pd.Timestamp(as_of_date).date(),
                "execution_date": pd.Timestamp(execution_date).date(),
            }
            if codes and code_column:
                clauses.append(f"{code_column} = ANY(:codes)")
                params["codes"] = codes
            if eligible_only:
                if dataset not in {"stock_daily", "etf_daily"}:
                    raise ValueError("eligible_only only supports stock_daily and etf_daily")
                clauses.append("is_eligible = TRUE")
            order = [date_column]
            if code_column:
                order.append(code_column)
            return await self._read(
                f"SELECT * FROM {table_name} WHERE {' AND '.join(clauses)} "
                f"ORDER BY {', '.join(order)}",
                params,
            )
        clauses = [
            "factor_version = :factor_version",
            "as_of_trade_date <= :as_of_date",
            "usable_from_trade_date <= :execution_date",
        ]
        params: Dict[str, Any] = {
            "factor_version": factor_version,
            "as_of_date": pd.Timestamp(as_of_date).date(),
            "execution_date": pd.Timestamp(execution_date).date(),
        }
        if codes and code_column:
            clauses.append(f"{code_column} = ANY(:codes)")
            params["codes"] = codes
        if eligible_only:
            if dataset not in {"stock_daily", "etf_daily"}:
                raise ValueError("eligible_only only supports stock_daily and etf_daily")
            clauses.append("is_eligible = TRUE")
        if code_column:
            statement = (
                f"SELECT DISTINCT ON ({code_column}) * FROM {table_name} "
                f"WHERE {' AND '.join(clauses)} "
                f"ORDER BY {code_column}, as_of_trade_date DESC, {date_column} DESC"
            )
        else:
            statement = (
                f"SELECT * FROM {table_name} WHERE {' AND '.join(clauses)} "
                f"ORDER BY as_of_trade_date DESC, {date_column} DESC LIMIT 1"
            )
        return await self._read(statement, params)

    async def _read(self, statement: str, params: Dict[str, Any]) -> pd.DataFrame:
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            result = await conn.execute(text(statement), params)
            rows = result.fetchall()
            return pd.DataFrame(rows, columns=result.keys())

    async def upsert_leadlag(
        self,
        data: pd.DataFrame,
        month_end: str,
        leader_type: str = "industry",
        follower_type: str = "industry",
    ) -> int:
        """持久化调用方已限定候选关系的领先滞后因子。"""
        required = {
            "leader_code",
            "follower_code",
            "best_lag_days",
            "correlation",
            "sample_count",
        }
        missing = required - set(data.columns)
        if missing:
            raise ValueError(f"leadlag data missing columns: {sorted(missing)}")
        if data.empty:
            return 0
        records = data[list(required)].to_dict("records")
        for record in records:
            record.update(
                {
                    "factor_version": MAINLINE_FACTOR_VERSION,
                    "month_end": pd.Timestamp(month_end).date(),
                    "as_of_trade_date": pd.Timestamp(month_end).date(),
                    "usable_from_trade_date": pd.Timestamp(month_end).date()
                    + timedelta(days=1),
                    "leader_type": leader_type,
                    "follower_type": follower_type,
                }
            )
        statement = text(
            """
            INSERT INTO processed_mainline_leadlag_monthly (
              factor_version,month_end,as_of_trade_date,usable_from_trade_date,
              leader_type,leader_code,follower_type,follower_code,
              best_lag_days,correlation,sample_count,source_asof
            ) VALUES (
              :factor_version,:month_end,:as_of_trade_date,:usable_from_trade_date,
              :leader_type,:leader_code,:follower_type,:follower_code,
              :best_lag_days,:correlation,:sample_count,NOW()
            )
            ON CONFLICT(factor_version,month_end,leader_code,follower_code)
            DO UPDATE SET best_lag_days=EXCLUDED.best_lag_days,
              correlation=EXCLUDED.correlation,sample_count=EXCLUDED.sample_count,
              source_asof=EXCLUDED.source_asof,processed_at=NOW()
            """
        )
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            result = await conn.execute(statement, records)
            return int(max(result.rowcount, 0))


class MainlinePreprocessor:
    """使用数据库现有事实表生成点时正确的主线因子表。"""

    BENCHMARK_CODE = "000985.CSI"

    def __init__(self, db_manager: "DatabaseManager") -> None:
        self.db_manager = db_manager
        self._bulk_mode = False

    @staticmethod
    def _dates(start_date: Optional[str], end_date: Optional[str]) -> tuple[date, date]:
        end = pd.Timestamp(end_date).date() if end_date else date.today()
        start = (
            pd.Timestamp(start_date).date() if start_date else end - timedelta(days=400)
        )
        if start > end:
            raise ValueError("start_date must not be later than end_date")
        return start, end

    @staticmethod
    def _build_partitions(start: date, end: date) -> List[tuple[date, date]]:
        """Split long rebuilds coarsely, while keeping routine repairs monthly."""
        partition_months = (
            MAINLINE_LONG_RANGE_PARTITION_MONTHS
            if (end - start).days > MAINLINE_LONG_RANGE_THRESHOLD_DAYS
            else 1
        )
        partitions: List[tuple[date, date]] = []
        partition_start = start
        while partition_start <= end:
            if partition_months == 1:
                next_end = (
                    pd.Timestamp(partition_start) + pd.offsets.MonthEnd(0)
                ).date()
            else:
                next_end = (
                    pd.Timestamp(partition_start)
                    + pd.DateOffset(months=partition_months)
                    - pd.Timedelta(days=1)
                ).date()
            partition_end = min(
                next_end,
                end,
            )
            partitions.append((partition_start, partition_end))
            partition_start = partition_end + timedelta(days=1)
        return partitions

    async def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_monthly: bool = True,
        stages: Optional[List[str]] = None,
        source_updated_since: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """Materialize only the requested layers.

        ``pit,daily,stock,market,industry,etf,etf_premium,exposure,crowding,leadlag,publish`` is intentionally explicit: daily
        facts may be recomputed often, while disclosure-driven crowding and the
        expensive monthly Lasso model are not safe to run as a side effect.
        """
        start, end = self._dates(start_date, end_date)
        explicit_stages = stages is not None
        requested = set(stages or (["daily", "crowding"] if include_monthly else ["daily"]))
        unknown = requested - {
            "pit", "daily", "stock", "market", "industry", "etf", "etf_premium", "exposure", "crowding", "leadlag", "publish", "rebuild"
        }
        if unknown:
            raise ValueError(f"Unsupported mainline stage(s): {sorted(unknown)}")
        if "rebuild" in requested:
            if requested != {"rebuild"}:
                raise ValueError("rebuild must be requested as the only mainline stage")
            return await self._run_rebuild(start, end, progress_callback)
        if requested == {"etf_premium"}:
            return {"etf_premium":await self._enrich_etf_premium_duckdb(start,end)}
        if "etf_premium" in requested:
            raise ValueError("etf_premium must be requested as the only mainline stage")
        partitions = self._build_partitions(start, end)
        partition_months = (
            MAINLINE_LONG_RANGE_PARTITION_MONTHS
            if (end - start).days > MAINLINE_LONG_RANGE_THRESHOLD_DAYS
            else 1
        )
        logger.info(
            "Mainline preprocessing uses {}-month partitions ({} partitions)",
            partition_months,
            len(partitions),
        )

        counts: Dict[str, int] = {}
        if "pit" in requested:
            if progress_callback:
                progress_callback(0, 1, "pit_bridges")
            counts["pit_bridges"] = await self._materialize_pit_bridges(start,end)
            if progress_callback:
                progress_callback(1, 1, "pit_bridges")

        materializers: List[tuple[str, Callable[[date, date], Any]]] = []
        if "daily" in requested:
            materializers.extend([
                ("stock_daily", self._materialize_stock),
                ("market_daily", self._materialize_market),
                ("etf_daily", self._materialize_etf),
            ])
            if explicit_stages:
                materializers.append(("etf_exposure_monthly", self._materialize_etf_exposure))
        else:
            # Targeted recovery stages keep a failed materializer from forcing
            # a full rebuild of the other daily fact tables.
            if "stock" in requested:
                materializers.append(("stock_daily", self._materialize_stock))
            if "market" in requested:
                materializers.append(("market_daily", self._materialize_market))
            if "industry" in requested:
                materializers.append(("industry_daily", self._materialize_industry))
        if "etf" in requested and "daily" not in requested:
            # Recovery stage: ETF facts and their exposure summary have no
            # dependency on the stock/market/industry materializers.
            materializers.extend([
                ("etf_daily", self._materialize_etf),
                ("etf_exposure_monthly", self._materialize_etf_exposure),
            ])
        elif "exposure" in requested:
            materializers.append(("etf_exposure_monthly", self._materialize_etf_exposure))
        if "crowding" in requested:
            materializers.append((
                "fund_crowding_monthly",
                lambda partition_start, partition_end: self._materialize_crowding(
                    partition_start,
                    partition_end,
                    source_updated_since=source_updated_since,
                ),
            ))
            if explicit_stages:
                materializers.append((
                    "industry_crowding_monthly",
                    lambda partition_start, partition_end: self._materialize_industry_crowding(
                        partition_start,
                        partition_end,
                        source_updated_since=source_updated_since,
                    ),
                ))
        if "daily" in requested:
            # Industry enrichment consumes both ETF exposure and the latest
            # disclosed crowding snapshot, so it must be last in a combined run.
            materializers.append(("industry_daily", self._materialize_industry))

        counts.update({name: 0 for name, _ in materializers})
        if "leadlag" in requested:
            counts["leadlag_monthly"] = 0
            counts["leadlag_score_monthly"] = 0
        legacy_status = not explicit_stages
        total_steps = len(partitions) * len(materializers) + int("leadlag" in requested) + int("publish" in requested) + int(legacy_status)
        completed = 0
        for partition_start, partition_end in partitions:
            partition_label = (
                f"{partition_start.isoformat()} ~ {partition_end.isoformat()}"
            )
            for dataset, materialize in materializers:
                description = f"{dataset}  {partition_label}"
                if progress_callback:
                    progress_callback(completed, total_steps, description)
                counts[dataset] += await materialize(partition_start, partition_end)
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_steps, description)

        if "leadlag" in requested:
            if progress_callback:
                progress_callback(completed, total_steps, "leadlag")
            leadlag_counts = await self._materialize_leadlag(start, end)
            counts.update(leadlag_counts)
            completed += 1
        if "publish" in requested:
            if progress_callback:
                progress_callback(completed, total_steps, "publish")
            await self._refresh_status_range(start, end)
            counts["snapshot_manifest"] = await self._publish_snapshots(start, end)
            completed += 1
        elif legacy_status:
            if progress_callback:
                progress_callback(completed, total_steps, "data_status")
            await self._refresh_status(end)
            completed += 1
        if progress_callback and total_steps:
            progress_callback(completed, total_steps, "data_status" if legacy_status else "complete")
        logger.info(f"Mainline preprocessing completed: {counts}")
        return counts

    async def _execute(self, sql: str, params: Dict[str, Any]) -> int:
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            # The factor SQL has several WindowAgg sorts.  The PostgreSQL
            # default is commonly 4MB, which spills ordinary six-month A-share
            # partitions to temporary files.  SET LOCAL confines this memory
            # budget to the current statement transaction.
            await conn.execute(text(f"SET LOCAL work_mem = '{MAINLINE_QUERY_WORK_MEM}'"))
            # ETF recovery updates touch compressed TimescaleDB segments.  A
            # segment is ordered by ETF code across its history, so changing a
            # single day can legitimately decompress more than the extension's
            # conservative default of 100,000 tuples.  Scope the unlimited
            # allowance to this one preprocessing transaction.
            await conn.execute(text(
                "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0"
            ))
            if self._bulk_mode:
                await conn.execute(text("SET LOCAL synchronous_commit = off"))
            result = await conn.execute(text(sql), params)
            return int(max(result.rowcount, 0))

    async def _run_rebuild(
        self,
        formal_start: date,
        end: date,
        progress_callback: Optional[Callable[[int, int, str], None]],
    ) -> Dict[str, int]:
        """Run the dependency-ordered v2 bulk build with durable checkpoints."""
        run_id = uuid5(
            NAMESPACE_URL,
            f"mainline:{MAINLINE_FACTOR_VERSION}:{MAINLINE_FORMULA_HASH}:{formal_start}:{end}",
        )
        await self._ensure_build_run(run_id, formal_start, end)
        stages: List[tuple[str, date, Callable[[], Any]]] = [
            ("pit", MAINLINE_REBUILD_WARMUP_START, lambda: self._materialize_pit_bridges(MAINLINE_REBUILD_WARMUP_START, end)),
            ("stock", MAINLINE_REBUILD_WARMUP_START, lambda: self._materialize_stock(MAINLINE_REBUILD_WARMUP_START, end)),
            ("market", MAINLINE_REBUILD_WARMUP_START, lambda: self._materialize_market(MAINLINE_REBUILD_WARMUP_START, end)),
            ("etf", formal_start, lambda: self._materialize_etf(formal_start, end)),
            # The first 2012 trading days must be able to carry the latest
            # index-weight snapshot released before the formal backtest.  The
            # exposure table therefore uses the same 2008 warm-up as industry
            # returns even though ETF daily rows start at ``formal_start``.
            ("exposure", MAINLINE_REBUILD_WARMUP_START, lambda: self._materialize_etf_exposure(MAINLINE_REBUILD_WARMUP_START, end)),
            ("fund_crowding", formal_start, lambda: self._materialize_crowding(formal_start, end)),
            ("industry_crowding", formal_start, lambda: self._materialize_industry_crowding(formal_start, end)),
            ("industry", MAINLINE_REBUILD_WARMUP_START, lambda: self._materialize_industry(MAINLINE_REBUILD_WARMUP_START, end)),
            ("leadlag", formal_start, lambda: self._materialize_leadlag(formal_start, end)),
            ("publish", formal_start, lambda: self._rebuild_publish(formal_start, end)),
            ("finalize", formal_start, self._finalize_rebuild),
        ]
        counts: Dict[str, int] = {}
        self._bulk_mode = True
        try:
            total = len(stages)
            for completed, (stage, stage_start, callback) in enumerate(stages):
                if progress_callback:
                    progress_callback(completed, total, f"rebuild:{stage}")
                previous = await self._build_stage_status(run_id, stage, stage_start, end)
                if previous == "completed":
                    counts[stage] = 0
                    continue
                await self._mark_build_stage(run_id, stage, stage_start, end, "running")
                started = perf_counter()
                try:
                    result = await callback()
                    await self._analyze_stage_output(stage)
                    if isinstance(result, dict):
                        row_count = sum(int(value) for value in result.values())
                    else:
                        row_count = int(result or 0)
                    await self._mark_build_stage(
                        run_id,stage,stage_start,end,"completed",row_count,perf_counter()-started
                    )
                    counts[stage] = row_count
                except Exception as exc:
                    await self._mark_build_stage(
                        run_id,stage,stage_start,end,"failed",0,perf_counter()-started,str(exc)
                    )
                    await self._finish_build_run(run_id,"failed",str(exc))
                    raise
                if progress_callback:
                    progress_callback(completed + 1, total, f"rebuild:{stage}")
            await self._finish_build_run(run_id,"completed")
            return counts
        finally:
            self._bulk_mode = False

    async def _analyze_stage_output(self, stage: str) -> None:
        """Publish fresh planner statistics before a downstream bulk stage."""
        tables = {
            "stock": ("processed_mainline_stock_daily",),
            "market": ("processed_mainline_market_daily",),
            "etf": ("processed_mainline_etf_daily",),
            "exposure": (
                "processed_mainline_etf_exposure_monthly",
                "processed_mainline_etf_exposure_summary",
            ),
            "fund_crowding": ("processed_mainline_fund_crowding_monthly",),
            "industry_crowding": ("processed_mainline_industry_crowding_monthly",),
            "industry": ("processed_mainline_industry_daily",),
            "leadlag": (
                "processed_mainline_leadlag_monthly",
                "processed_mainline_leadlag_score_monthly",
            ),
        }.get(stage, ())
        for table_name in tables:
            await self._execute(f"ANALYZE {table_name}",{})

    async def _ensure_build_run(self, run_id: UUID, start: date, end: date) -> None:
        await self._execute(
            """INSERT INTO processed_mainline_build_run(
                 run_id,factor_version,formula_hash,requested_start_date,requested_end_date,status
               ) VALUES (:run_id,:factor_version,:formula_hash,:start_date,:end_date,'running')
               ON CONFLICT(factor_version,formula_hash,requested_start_date,requested_end_date)
               DO UPDATE SET status=CASE WHEN processed_mainline_build_run.status='completed'
                                         THEN 'completed' ELSE 'running' END,
                             error_message=NULL""",
            {"run_id":run_id,"factor_version":MAINLINE_FACTOR_VERSION,
             "formula_hash":MAINLINE_FORMULA_HASH,"start_date":start,"end_date":end},
        )

    async def _build_stage_status(self, run_id: UUID, stage: str, start: date, end: date) -> Optional[str]:
        frame = await self._read_dataframe(
            """SELECT status FROM processed_mainline_build_stage
               WHERE run_id=:run_id AND stage=:stage
                 AND partition_start=:start_date AND partition_end=:end_date""",
            {"run_id":run_id,"stage":stage,"start_date":start,"end_date":end},
        )
        return None if frame.empty else str(frame.iloc[0]["status"])

    async def _mark_build_stage(
        self,run_id: UUID,stage: str,start: date,end: date,status: str,
        row_count: int = 0,duration: Optional[float] = None,error: Optional[str] = None,
    ) -> None:
        await self._execute(
            """INSERT INTO processed_mainline_build_stage(
                 run_id,stage,partition_start,partition_end,status,row_count,duration_seconds,
                 started_at,completed_at,error_message
               ) VALUES (:run_id,:stage,:start_date,:end_date,CAST(:status AS varchar),:row_count,:duration,
                         NOW(),CASE WHEN CAST(:status AS varchar) IN ('completed','failed') THEN NOW() END,:error)
               ON CONFLICT(run_id,stage,partition_start,partition_end) DO UPDATE SET
                 status=EXCLUDED.status,row_count=EXCLUDED.row_count,
                 duration_seconds=EXCLUDED.duration_seconds,
                 started_at=CASE WHEN EXCLUDED.status='running' THEN NOW()
                                 ELSE processed_mainline_build_stage.started_at END,
                 completed_at=EXCLUDED.completed_at,error_message=EXCLUDED.error_message""",
            {"run_id":run_id,"stage":stage,"start_date":start,"end_date":end,
             "status":status,"row_count":row_count,"duration":duration,"error":(error or "")[:4000]},
        )

    async def _finish_build_run(self, run_id: UUID, status: str, error: Optional[str] = None) -> None:
        await self._execute(
            """UPDATE processed_mainline_build_run SET status=CAST(:status AS varchar),
                 completed_at=CASE WHEN CAST(:status AS varchar) IN ('completed','failed') THEN NOW() END,
                 error_message=:error WHERE run_id=:run_id""",
            {"run_id":run_id,"status":status,"error":(error or "")[:4000]},
        )

    async def _rebuild_publish(self, start: date, end: date) -> int:
        await self._refresh_status_range(start,end)
        count = await self._publish_snapshots(start,end)
        latest = await self._read_dataframe(
            """SELECT as_of_trade_date,status,blocker_reasons
               FROM processed_mainline_snapshot_manifest
               WHERE factor_version=:factor_version AND as_of_trade_date<=:end_date
               ORDER BY as_of_trade_date DESC LIMIT 1""",
            {"factor_version":MAINLINE_FACTOR_VERSION,"end_date":end},
        )
        if latest.empty:
            raise RuntimeError("Gate 0 failed: no snapshot manifest was generated")
        row = latest.iloc[0]
        if str(row["status"]) != "ready":
            blockers = row.get("blocker_reasons") or []
            raise RuntimeError(
                f"Gate 0 failed for {row['as_of_trade_date']}: {blockers}"
            )
        return count

    async def _materialize_pit_bridges(self, start: date, end: date) -> int:
        """Collapse disclosure/member histories into interval joins once per build."""
        financial_sql = """
        WITH fi_source AS (
          SELECT x.ts_code,x.end_date_time::date AS report_period,
                 COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD'))::date AS available_date,
                 x.roe_dt,x.roic,x.grossprofit_margin,x.q_sales_yoy,x.q_profit_yoy,
                 x.ocf_to_profit,x.debt_to_assets,
                 LAG(x.q_sales_yoy) OVER(PARTITION BY x.ts_code ORDER BY x.end_date_time) AS revenue_yoy_prev,
                 LAG(x.q_profit_yoy) OVER(PARTITION BY x.ts_code ORDER BY x.end_date_time) AS profit_yoy_prev
          FROM fina_indicator x
          WHERE COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD'))::date<=:end_date
        ), qf_source AS (
          SELECT q.ts_code,q.end_date_time::date AS report_period,
                 COALESCE(q.f_ann_date_time,q.ann_date_time)::date AS available_date,
                 q.roe_5y_avg,q.roa_ttm,q.cfo_to_ni_ttm,q.debt_ratio
          FROM processed_fundamental_quality q
          WHERE COALESCE(q.f_ann_date_time,q.ann_date_time)::date<=:end_date
        ), fi_events AS (
          SELECT DISTINCT ON(ts_code,available_date) * FROM fi_source
          WHERE available_date IS NOT NULL
          ORDER BY ts_code,available_date,report_period DESC
        ), qf_events AS (
          SELECT DISTINCT ON(ts_code,available_date) * FROM qf_source
          WHERE available_date IS NOT NULL
          ORDER BY ts_code,available_date,report_period DESC
        ), events AS (
          SELECT ts_code,available_date,TRUE AS has_fi,FALSE AS has_qf FROM fi_events
          UNION ALL
          SELECT ts_code,available_date,FALSE,TRUE FROM qf_events
        ), event_days AS (
          SELECT ts_code,available_date,BOOL_OR(has_fi) AS has_fi,BOOL_OR(has_qf) AS has_qf
          FROM events GROUP BY ts_code,available_date
        ), stamped AS (
          SELECT event_days.*,
                 MAX(available_date) FILTER(WHERE has_fi) OVER w AS latest_fi_date,
                 MAX(available_date) FILTER(WHERE has_qf) OVER w AS latest_qf_date
          FROM event_days
          WINDOW w AS(PARTITION BY ts_code ORDER BY available_date ROWS UNBOUNDED PRECEDING)
        ), snapshots AS (
          SELECT e.ts_code,e.available_date,
                 fi.report_period,
                 COALESCE(qf.roe_5y_avg,fi.roe_dt) AS roe_ttm,qf.roa_ttm,fi.roic,
                 fi.grossprofit_margin,fi.q_sales_yoy AS revenue_yoy,
                 fi.q_profit_yoy AS profit_yoy,fi.revenue_yoy_prev,fi.profit_yoy_prev,
                 fi.q_sales_yoy-fi.revenue_yoy_prev AS revenue_acceleration,
                 fi.q_profit_yoy-fi.profit_yoy_prev AS profit_acceleration,
                 COALESCE(qf.cfo_to_ni_ttm,fi.ocf_to_profit) AS ocf_to_profit,
                 COALESCE(qf.debt_ratio,fi.debt_to_assets) AS debt_to_assets
          FROM stamped e
          LEFT JOIN fi_events fi ON fi.ts_code=e.ts_code AND fi.available_date=e.latest_fi_date
          LEFT JOIN qf_events qf ON qf.ts_code=e.ts_code AND qf.available_date=e.latest_qf_date
        ), intervals AS (
          SELECT snapshots.*,
                 LEAD(available_date) OVER(PARTITION BY ts_code ORDER BY available_date) AS usable_to
          FROM snapshots
        )
        INSERT INTO processed_mainline_financial_pit(
          ts_code,usable_from_trade_date,usable_to_trade_date,report_period,
          roe_ttm,roa_ttm,roic,grossprofit_margin,revenue_yoy,profit_yoy,
          revenue_yoy_prev,profit_yoy_prev,revenue_acceleration,profit_acceleration,
          ocf_to_profit,debt_to_assets,source_watermark
        )
        SELECT ts_code,available_date,usable_to,report_period,roe_ttm,roa_ttm,roic,
          grossprofit_margin,revenue_yoy,profit_yoy,revenue_yoy_prev,profit_yoy_prev,
          revenue_acceleration,profit_acceleration,ocf_to_profit,debt_to_assets,
          jsonb_build_object('financial_available_date',available_date,'report_period',report_period)
        FROM intervals WHERE available_date IS NOT NULL
        ON CONFLICT(ts_code,usable_from_trade_date) DO UPDATE SET
          usable_to_trade_date=EXCLUDED.usable_to_trade_date,report_period=EXCLUDED.report_period,
          roe_ttm=EXCLUDED.roe_ttm,roa_ttm=EXCLUDED.roa_ttm,roic=EXCLUDED.roic,
          grossprofit_margin=EXCLUDED.grossprofit_margin,revenue_yoy=EXCLUDED.revenue_yoy,
          profit_yoy=EXCLUDED.profit_yoy,revenue_yoy_prev=EXCLUDED.revenue_yoy_prev,
          profit_yoy_prev=EXCLUDED.profit_yoy_prev,revenue_acceleration=EXCLUDED.revenue_acceleration,
          profit_acceleration=EXCLUDED.profit_acceleration,ocf_to_profit=EXCLUDED.ocf_to_profit,
          debt_to_assets=EXCLUDED.debt_to_assets,source_watermark=EXCLUDED.source_watermark
        """
        member_sql = """
        WITH source AS (
          SELECT DISTINCT ON (m.ts_code,COALESCE(m.in_date,DATE '1900-01-01'))
                 m.ts_code,COALESCE(m.in_date,DATE '1900-01-01') AS usable_from,
                 m.out_date,m.l1_code,m.l1_name,m.l2_code,m.l2_name
          FROM sw_industry_member m
          JOIN sw_industry_classify ic
            ON ic.index_code=m.l2_code AND ic.level='L2' AND ic.is_pub='1'
          WHERE COALESCE(m.in_date,DATE '1900-01-01')<=:end_date
          ORDER BY m.ts_code,COALESCE(m.in_date,DATE '1900-01-01'),
                   m.out_date DESC NULLS FIRST,m.l2_code
        ), intervals AS (
          SELECT source.*,
                 LEAD(usable_from) OVER(PARTITION BY ts_code ORDER BY usable_from) AS next_from
          FROM source
        )
        INSERT INTO processed_mainline_sw_member_pit(
          ts_code,usable_from_trade_date,usable_to_trade_date,l1_code,l1_name,l2_code,l2_name
        )
        SELECT ts_code,usable_from,
               CASE
                 WHEN out_date IS NULL THEN next_from
                 WHEN next_from IS NULL THEN out_date+1
                 ELSE LEAST(out_date+1,next_from)
               END,
               l1_code,l1_name,l2_code,l2_name
        FROM intervals
        ON CONFLICT(ts_code,usable_from_trade_date,l2_code) DO UPDATE SET
          usable_to_trade_date=EXCLUDED.usable_to_trade_date,l1_code=EXCLUDED.l1_code,
          l1_name=EXCLUDED.l1_name,l2_name=EXCLUDED.l2_name
        """
        status_sql = """
        WITH source AS (
          SELECT DISTINCT ON (n.ts_code,n.start_date)
                 n.ts_code,n.start_date AS usable_from,n.end_date,n.name,
                 COALESCE(n.name ~* '(^|\\*)ST',FALSE) AS is_st
          FROM stock_namechange n
          WHERE n.start_date IS NOT NULL AND n.start_date<=:end_date
          ORDER BY n.ts_code,n.start_date,n.end_date DESC NULLS FIRST
        ), intervals AS (
          SELECT source.*,
                 LEAD(usable_from) OVER(PARTITION BY ts_code ORDER BY usable_from) AS next_from
          FROM source
        )
        INSERT INTO processed_mainline_stock_status_pit(
          ts_code,usable_from_trade_date,usable_to_trade_date,is_st,st_source,source_name
        )
        SELECT ts_code,usable_from,
               CASE WHEN end_date IS NULL THEN next_from
                    WHEN next_from IS NULL THEN end_date+1
                    ELSE LEAST(end_date+1,next_from) END,
               is_st,'namechange_reconstructed',name
        FROM intervals
        """
        event_sql = """
        WITH event_source AS (
          SELECT ts_code,'dividend'::varchar AS event_type,ann_date::date AS event_date
          FROM stock_dividend WHERE ann_date IS NOT NULL AND ann_date<=:end_date
          UNION ALL
          SELECT ts_code,'repurchase'::varchar,ann_date::date
          FROM stock_repurchase WHERE ann_date IS NOT NULL AND ann_date<=:end_date
        ), merged AS (
          SELECT ts_code,event_type,
                 UNNEST(RANGE_AGG(DATERANGE(event_date,event_date+120,'[)'))) AS active_range
          FROM event_source GROUP BY ts_code,event_type
        )
        INSERT INTO processed_mainline_stock_event_pit(
          ts_code,event_type,usable_from_trade_date,usable_to_trade_date
        )
        SELECT ts_code,event_type,LOWER(active_range),UPPER(active_range) FROM merged
        """
        await self._execute("TRUNCATE processed_mainline_financial_pit",{})
        financial_count = await self._execute(financial_sql,{"start_date":start,"end_date":end})
        await self._execute("TRUNCATE processed_mainline_sw_member_pit",{})
        member_count = await self._execute(member_sql,{"end_date":end})
        await self._execute("TRUNCATE processed_mainline_stock_status_pit",{})
        status_count = await self._execute(status_sql,{"end_date":end})
        await self._execute("TRUNCATE processed_mainline_stock_event_pit",{})
        event_count = await self._execute(event_sql,{"end_date":end})
        return financial_count + member_count + status_count + event_count

    async def _finalize_rebuild(self) -> int:
        """Create read indexes only after the bulk rows and Gate evidence exist."""
        statements = [
          """DO $$ DECLARE t text; BEGIN
             FOREACH t IN ARRAY ARRAY[
               'processed_mainline_stock_daily','processed_mainline_market_daily',
               'processed_mainline_industry_daily','processed_mainline_etf_daily',
               'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly',
               'processed_mainline_industry_crowding_monthly','processed_mainline_leadlag_monthly',
               'processed_mainline_leadlag_score_monthly'
             ] LOOP EXECUTE format('ALTER TABLE %I RESET (autovacuum_enabled)',t); END LOOP;
             END $$""",
          "CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_date ON processed_mainline_stock_daily(factor_version,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_code_date ON processed_mainline_stock_daily(factor_version,ts_code,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_stock_usable ON processed_mainline_stock_daily(usable_from_trade_date)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_market_version_date ON processed_mainline_market_daily(factor_version,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_date ON processed_mainline_industry_daily(factor_version,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_code_date ON processed_mainline_industry_daily(factor_version,l2_code,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_date ON processed_mainline_etf_daily(factor_version,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_code_date ON processed_mainline_etf_daily(factor_version,ts_code,trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_etf_exposure_lookup ON processed_mainline_etf_exposure_monthly(factor_version,ts_code,as_of_trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_fund_crowding_usable ON processed_mainline_fund_crowding_monthly(factor_version,usable_from_trade_date,ts_code)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_industry_crowding_usable ON processed_mainline_industry_crowding_monthly(factor_version,usable_from_trade_date,l2_code)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_lookup ON processed_mainline_leadlag_monthly(factor_version,month_end DESC,follower_code)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_score_lookup ON processed_mainline_leadlag_score_monthly(factor_version,month_end DESC,l2_code)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_manifest_ready ON processed_mainline_snapshot_manifest(factor_version,status,usable_from_trade_date DESC)",
          "CREATE INDEX IF NOT EXISTS idx_mainline_status_lookup ON processed_mainline_data_status(factor_version,partition_date DESC,status)",
          "CREATE OR REPLACE VIEW v_mainline_ready_snapshot AS SELECT * FROM processed_mainline_snapshot_manifest WHERE status='ready'",
          "ANALYZE processed_mainline_stock_daily",
          "ANALYZE processed_mainline_market_daily",
          "ANALYZE processed_mainline_industry_daily",
          "ANALYZE processed_mainline_etf_daily",
          """DO $$ DECLARE item record; BEGIN
             IF EXISTS(SELECT 1 FROM pg_proc WHERE proname='add_compression_policy') THEN
               FOR item IN SELECT * FROM (VALUES
                 ('processed_mainline_stock_daily','factor_version,ts_code','trade_date DESC'),
                 ('processed_mainline_etf_daily','factor_version,ts_code','trade_date DESC'),
                 ('processed_mainline_market_daily','factor_version','trade_date DESC'),
                 ('processed_mainline_industry_daily','factor_version,l2_code','trade_date DESC'),
                 ('processed_mainline_etf_exposure_monthly','factor_version,ts_code','as_of_trade_date DESC'),
                 ('processed_mainline_fund_crowding_monthly','factor_version,ts_code','report_period DESC'),
                 ('processed_mainline_industry_crowding_monthly','factor_version,l2_code','report_period DESC'),
                 ('processed_mainline_leadlag_monthly','factor_version,follower_code','month_end DESC'),
                 ('processed_mainline_leadlag_score_monthly','factor_version,l2_code','month_end DESC')
               ) AS v(table_name,segment_by,order_by) LOOP
                 EXECUTE format('ALTER TABLE %I SET (timescaledb.compress=true,timescaledb.compress_segmentby=%L,timescaledb.compress_orderby=%L)',item.table_name,item.segment_by,item.order_by);
                 BEGIN
                   PERFORM add_compression_policy(item.table_name::regclass,INTERVAL '180 days',if_not_exists=>TRUE);
                 EXCEPTION WHEN undefined_function OR feature_not_supported THEN
                   RAISE NOTICE 'compression policy API unavailable for %',item.table_name;
                 END;
               END LOOP;
             END IF;
             END $$""",
        ]
        total = 0
        for statement in statements:
            total += await self._execute(statement,{})
        return total

    async def _materialize_stock(self, start: date, end: date) -> int:
        sql = """
        WITH purged_non_mainland AS (
            -- A previous version materialized cross-market rows.  Make each
            -- requested partition self-healing instead of retaining stale HK
            -- rows after an incremental or forced repair.
            DELETE FROM processed_mainline_stock_daily
            WHERE factor_version=:factor_version
              AND trade_date BETWEEN :start_date AND :end_date
              AND ts_code LIKE '%.HK'
            RETURNING ts_code
        ), open_days AS (
            SELECT cal_date::date AS trade_date,
                   LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
            FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), st_days AS (
            SELECT DISTINCT ts_code,trade_date FROM stock_st
            WHERE trade_date BETWEEN :start_date AND :end_date
        ), suspended_days AS (
            SELECT DISTINCT ts_code,trade_date FROM stock_suspend
            WHERE trade_date BETWEEN :start_date AND :end_date
              AND UPPER(suspend_type)='S'
        ), prices AS (
            SELECT d.time::date AS trade_date, d.symbol AS ts_code,
                   d.close, d.amount,
                   d.close / NULLIF(LAG(d.close, 1) OVER w, 0) - 1 AS daily_return,
                   d.close / NULLIF(LAG(d.close, 20) OVER w, 0) - 1 AS return_20d,
                   d.close / NULLIF(LAG(d.close, 60) OVER w, 0) - 1 AS return_60d,
                   d.close / NULLIF(LAG(d.close, 120) OVER w, 0) - 1 AS return_120d,
                   MAX(d.close) OVER (w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS max_close_120d,
                   MIN(d.amount) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS min_amount_20d,
                   MAX(d.amount) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS max_amount_20d,
                   AVG(d.amount) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_amount_20d,
                   AVG(d.amount) OVER (w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS avg_amount_60d,
                   AVG(d.close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                   AVG(d.close) OVER (w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                   AVG(d.close) OVER (w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120,
                   AVG(d.close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                   COUNT(d.close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS trades20
            FROM processed_daily_qfq d
            WHERE d.time >= (CAST(:start_date AS date) - INTERVAL '260 days')
              AND d.time < (CAST(:end_date AS date) + INTERVAL '1 day')
              -- processed_daily_qfq is cross-market; the SW2021 mainline
              -- universe is mainland A shares, not Hong Kong equities.
              AND (d.symbol LIKE '%.SH' OR d.symbol LIKE '%.SZ')
            WINDOW w AS (PARTITION BY d.symbol ORDER BY d.time)
        ), features AS (
            SELECT p.*,
                   STDDEV_SAMP(daily_return) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS volatility_20d,
                   STDDEV_SAMP(daily_return) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS volatility_60d,
                   STDDEV_SAMP(daily_return) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                   ) AS volatility_120d,
                   SUM(ABS(daily_return)) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS path_length_60d,
                   MIN(daily_return) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                   ) AS tail_return_60d
            FROM prices p
        ), basics AS (
            SELECT db.time::date AS trade_date,db.symbol,db.total_mv,db.circ_mv,
                   db.turnover_rate,db.pe_ttm,db.pb,db.dv_ttm,
                   MIN(db.turnover_rate) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS min_turnover_20d,
                   MAX(db.turnover_rate) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS max_turnover_20d,
                   AVG(db.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_mean_2y,
                   STDDEV_SAMP(db.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_std_2y,
                   COUNT(db.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_count_2y
            FROM daily_basic db
            WHERE db.time >= (CAST(:start_date AS date)-INTERVAL '800 days')
              AND db.time < (CAST(:end_date AS date)+INTERVAL '1 day')
            WINDOW w AS(PARTITION BY db.symbol ORDER BY db.time)
        ), valuations AS (
            SELECT pv.time::date AS trade_date,pv.symbol,pv.pe_ttm,pv.pb,pv.dv_ttm,
                   pv.pe_ttm_pct_1250d,pv.pb_pct_1250d
            FROM processed_valuation_pct pv
            WHERE pv.time >= CAST(:start_date AS date)
              AND pv.time < (CAST(:end_date AS date)+INTERVAL '1 day')
        ), moneyflow_features AS (
            SELECT ts_code,trade_date,
                   SUM(net_mf_amount) OVER(w ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) * 10000 AS moneyflow5,
                   SUM(net_mf_amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * 10000 AS moneyflow20
            FROM moneyflow
            WHERE trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '60 days') AND :end_date
            WINDOW w AS(PARTITION BY ts_code ORDER BY trade_date)
        ), margin_features AS (
            SELECT ts_code,trade_date,rzye,rqye,rzmre,
                   rzye-LAG(rzye,20) OVER(PARTITION BY ts_code ORDER BY trade_date)
                     AS margin_balance_change_20d
            FROM margin_detail
            WHERE trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '60 days') AND :end_date
        ), enriched AS (
            SELECT f.trade_date, f.ts_code, sw.l1_code, sw.l1_name,
                   sw.l2_code, sw.l2_name,
                   (ab.list_date IS NOT NULL AND ab.list_date <= f.trade_date
                    AND (ab.delist_date IS NULL OR ab.delist_date >= f.trade_date)) AS is_listed,
                   CASE WHEN f.trade_date >= DATE '2016-08-09'
                        THEN st.ts_code IS NOT NULL
                        ELSE COALESCE(ns.is_st,FALSE) END AS is_st,
                   CASE WHEN f.trade_date >= DATE '2016-08-09'
                        THEN 'stock_st' ELSE 'namechange_reconstructed' END AS st_source,
                   sus.ts_code IS NOT NULL AS is_suspended,
                   f.trade_date - ab.list_date AS listing_days,
                   f.close, f.amount * 1000 AS amount,
                   db.total_mv * 10000 AS total_mv, db.circ_mv * 10000 AS circ_mv,
                   db.turnover_rate,
                   COALESCE(v.pe_ttm, db.pe_ttm) AS pe_ttm,
                   COALESCE(v.pb, db.pb) AS pb, COALESCE(v.dv_ttm, db.dv_ttm) AS dv_ttm,
                   fp.roe_ttm,fp.roa_ttm,fp.roic,fp.grossprofit_margin,
                   fp.revenue_yoy,fp.profit_yoy,fp.revenue_yoy_prev,fp.profit_yoy_prev,
                   fp.revenue_acceleration,fp.profit_acceleration,
                   fp.ocf_to_profit,fp.debt_to_assets,
                   fp.usable_from_trade_date AS financial_available_date,
                   f.daily_return AS return_1d,
                   f.close/NULLIF(f.ma20,0)-1 AS ma20_gap,
                   f.return_20d, f.return_60d, f.return_120d, f.volatility_20d,
                   f.volatility_60d,f.volatility_120d,
                   f.close / NULLIF(f.max_close_120d, 0) - 1 AS drawdown_120d,
                   ABS(f.return_60d)/NULLIF(f.path_length_60d,0) AS path_efficiency_60d,
                   f.tail_return_60d,
                   f.trades20,f.avg_amount_20d*1000 AS avg_amount_20d,
                   f.avg_amount_60d*1000 AS avg_amount_60d,
                   f.close/NULLIF(f.ma60,0)-1 AS ma60_gap,
                   f.close/NULLIF(f.ma120,0)-1 AS ma120_gap,
                   f.close/NULLIF(f.ma200,0)-1 AS ma200_gap,
                   (f.amount-f.min_amount_20d) /
                     NULLIF(f.max_amount_20d-f.min_amount_20d, 0) AS amount_pct_20d,
                   (db.turnover_rate-db.min_turnover_20d) /
                     NULLIF(db.max_turnover_20d-db.min_turnover_20d,0) AS turnover_pct_20d,
                   CASE WHEN db.turnover_count_2y>=504 AND db.turnover_std_2y>0
                     THEN 1.0/(1.0+EXP(-1.702*(db.turnover_rate-db.turnover_mean_2y)/db.turnover_std_2y)) END AS turnover_pct_2y,
                   db.turnover_count_2y,
                   v.pe_ttm_pct_1250d AS pe_pct_5y, v.pb_pct_1250d AS pb_pct_5y,
                   md.rzye,md.rqye,md.rzmre,md.margin_balance_change_20d,
                   mf.net_mf_amount * 10000 AS moneyflow_net_amount,
                   mf.net_mf_amount * 10 / NULLIF(f.amount, 0) AS moneyflow_net_amount_ratio,
                   (mf.buy_lg_amount-mf.sell_lg_amount+mf.buy_elg_amount-mf.sell_elg_amount) * 10000 AS moneyflow_large_net_amount,
                   (mf.buy_lg_amount-mf.sell_lg_amount+mf.buy_elg_amount-mf.sell_elg_amount) * 10 / NULLIF(f.amount, 0) AS moneyflow_large_net_ratio,
                   (mf.ts_code IS NOT NULL) AS moneyflow_available,
                   mff.moneyflow5 AS moneyflow_net_amount_5d,
                   mff.moneyflow20 AS moneyflow_net_amount_20d,
                   div_event.ts_code IS NOT NULL AS dividend_event_120d,
                   rep_event.ts_code IS NOT NULL AS repurchase_event_120d
            FROM features f
            JOIN asset_basic ab ON ab.symbol=f.ts_code
            LEFT JOIN basics db ON db.symbol=f.ts_code AND db.trade_date=f.trade_date
            LEFT JOIN valuations v
              ON v.symbol=f.ts_code AND v.trade_date=f.trade_date
            LEFT JOIN processed_mainline_financial_pit fp
              ON fp.ts_code=f.ts_code
             AND fp.usable_from_trade_date<=f.trade_date
             AND (fp.usable_to_trade_date IS NULL OR fp.usable_to_trade_date>f.trade_date)
            LEFT JOIN margin_features md ON md.ts_code=f.ts_code AND md.trade_date=f.trade_date
            LEFT JOIN moneyflow mf ON mf.ts_code=f.ts_code AND mf.trade_date=f.trade_date
            LEFT JOIN moneyflow_features mff ON mff.ts_code=f.ts_code AND mff.trade_date=f.trade_date
            LEFT JOIN processed_mainline_sw_member_pit sw
              ON sw.ts_code=f.ts_code
             AND sw.usable_from_trade_date<=f.trade_date
             AND (sw.usable_to_trade_date IS NULL OR sw.usable_to_trade_date>f.trade_date)
            LEFT JOIN processed_mainline_stock_status_pit ns
              ON ns.ts_code=f.ts_code AND ns.usable_from_trade_date<=f.trade_date
             AND (ns.usable_to_trade_date IS NULL OR ns.usable_to_trade_date>f.trade_date)
            LEFT JOIN st_days st ON st.ts_code=f.ts_code AND st.trade_date=f.trade_date
            LEFT JOIN suspended_days sus ON sus.ts_code=f.ts_code AND sus.trade_date=f.trade_date
            LEFT JOIN processed_mainline_stock_event_pit div_event
              ON div_event.ts_code=f.ts_code AND div_event.event_type='dividend'
             AND div_event.usable_from_trade_date<=f.trade_date
             AND div_event.usable_to_trade_date>f.trade_date
            LEFT JOIN processed_mainline_stock_event_pit rep_event
              ON rep_event.ts_code=f.ts_code AND rep_event.event_type='repurchase'
             AND rep_event.usable_from_trade_date<=f.trade_date
             AND rep_event.usable_to_trade_date>f.trade_date
            WHERE f.trade_date BETWEEN :start_date AND :end_date
        ), ranked AS (
            SELECT enriched.*,
                   PERCENT_RANK() OVER(PARTITION BY trade_date ORDER BY circ_mv) AS circ_mv_pct
            FROM enriched
        )
        INSERT INTO processed_mainline_stock_daily (
            factor_version,trade_date,as_of_trade_date,usable_from_trade_date,
            ts_code,l1_code,l1_name,l2_code,l2_name,is_listed,is_st,st_source,
            is_suspended,is_market_breadth_eligible,is_industry_breadth_eligible,
            is_stock_candidate_eligible,is_eligible,exclusion_reason,exclusion_reasons,
            listing_days,trading_days_20d,close,amount,total_mv,circ_mv,circ_mv_pct,
            return_1d,ma20_gap,margin_balance_change_20d,
            turnover_rate,avg_amount_20d,avg_amount_60d,amount_ratio_20_60,amount_pct_20d,
            turnover_pct_2y,turnover_pct_20d,pe_ttm,pb,dv_ttm,roe_ttm,roa_ttm,roic,grossprofit_margin,
            revenue_yoy,profit_yoy,revenue_yoy_prev,profit_yoy_prev,
            revenue_acceleration,profit_acceleration,
            ocf_to_profit,debt_to_assets,financial_available_date,
            ma60_gap,ma120_gap,ma200_gap,return_20d,return_60d,return_120d,
            volatility_20d,volatility_60d,volatility_120d,drawdown_120d,
            path_efficiency_60d,tail_return_p05_60d,pe_pct_5y,pb_pct_5y,
            rzye,rqye,rzmre,moneyflow_net_amount,moneyflow_net_amount_ratio,
            moneyflow_large_net_amount,moneyflow_large_net_ratio,moneyflow_available,
            moneyflow_net_amount_5d,moneyflow_net_amount_20d,
            dividend_event_120d,repurchase_event_120d,data_quality,source_watermark,source_asof
        )
        SELECT :factor_version,e.trade_date,e.trade_date,
               COALESCE(nt.next_trade_date,e.trade_date + 1),
               ts_code,l1_code,l1_name,l2_code,l2_name,
               is_listed,is_st,st_source,is_suspended,
               (is_listed AND NOT is_st AND NOT is_suspended AND trades20>=20 AND close IS NOT NULL),
               (is_listed AND NOT is_st AND NOT is_suspended AND trades20>=20 AND l2_code IS NOT NULL),
               (is_listed AND NOT is_st AND NOT is_suspended AND listing_days >= 120
                AND l2_code IS NOT NULL AND trades20>=20 AND close IS NOT NULL AND COALESCE(amount,0)>0),
               (is_listed AND NOT is_st AND NOT is_suspended AND listing_days >= 120
                AND l2_code IS NOT NULL AND trades20>=20 AND close IS NOT NULL AND COALESCE(amount,0)>0),
               CASE WHEN NOT is_listed THEN 'not_listed' WHEN is_st THEN 'st'
                    WHEN is_suspended THEN 'suspended' WHEN listing_days < 120 THEN 'new_listing'
                    WHEN l2_code IS NULL THEN 'missing_sw2021_l2'
                    WHEN trades20<20 THEN 'insufficient_trading_days_20d'
                    WHEN close IS NULL OR COALESCE(amount,0)<=0 THEN 'not_tradable' END,
               ARRAY_REMOVE(ARRAY[
                 CASE WHEN NOT is_listed THEN 'not_listed' END,CASE WHEN is_st THEN 'st' END,
                 CASE WHEN is_suspended THEN 'suspended' END,CASE WHEN listing_days<120 THEN 'new_listing' END,
                 CASE WHEN l2_code IS NULL THEN 'missing_published_sw2021_l2' END,
                 CASE WHEN trades20<20 THEN 'insufficient_trading_days_20d' END,
                 CASE WHEN close IS NULL OR COALESCE(amount,0)<=0 THEN 'not_tradable' END
               ],NULL),
               listing_days,trades20,close,amount,total_mv,circ_mv,circ_mv_pct,
               return_1d,ma20_gap,margin_balance_change_20d,
               turnover_rate,avg_amount_20d,avg_amount_60d,
               avg_amount_20d/NULLIF(avg_amount_60d,0),amount_pct_20d,
               turnover_pct_2y,turnover_pct_20d,pe_ttm,pb,dv_ttm,
               roe_ttm,roa_ttm,roic,grossprofit_margin,revenue_yoy,profit_yoy,
               revenue_yoy_prev,profit_yoy_prev,
               revenue_acceleration,profit_acceleration,
               ocf_to_profit,debt_to_assets,financial_available_date,
               ma60_gap,ma120_gap,ma200_gap,return_20d,return_60d,return_120d,
               volatility_20d,volatility_60d,volatility_120d,drawdown_120d,
               path_efficiency_60d,tail_return_60d,pe_pct_5y,pb_pct_5y,
               rzye,rqye,rzmre,moneyflow_net_amount,moneyflow_net_amount_ratio,
               moneyflow_large_net_amount,moneyflow_large_net_ratio,moneyflow_available,
               moneyflow_net_amount_5d,moneyflow_net_amount_20d,
               dividend_event_120d,repurchase_event_120d,
               JSONB_BUILD_OBJECT('st_source',st_source,'tail_proxy','rolling_min_return_if_p05_unavailable',
                 'turnover_pct_2y_method','pit_504d_logistic_ecdf_proxy','turnover_pct_2y_observations',turnover_count_2y),
               JSONB_BUILD_OBJECT('prices_as_of',e.trade_date,'financial_available_date',financial_available_date,
                 'moneyflow',CASE WHEN moneyflow_available THEN e.trade_date END),NOW()
        FROM ranked e
        LEFT JOIN open_days nt ON nt.trade_date=e.trade_date
        ON CONFLICT (factor_version,ts_code,trade_date) DO UPDATE SET
            l1_code=EXCLUDED.l1_code,l1_name=EXCLUDED.l1_name,l2_code=EXCLUDED.l2_code,
            l2_name=EXCLUDED.l2_name,is_listed=EXCLUDED.is_listed,is_st=EXCLUDED.is_st,
            st_source=EXCLUDED.st_source,is_suspended=EXCLUDED.is_suspended,
            is_market_breadth_eligible=EXCLUDED.is_market_breadth_eligible,
            is_industry_breadth_eligible=EXCLUDED.is_industry_breadth_eligible,
            is_stock_candidate_eligible=EXCLUDED.is_stock_candidate_eligible,
            is_eligible=EXCLUDED.is_eligible,exclusion_reason=EXCLUDED.exclusion_reason,
            exclusion_reasons=EXCLUDED.exclusion_reasons,
            listing_days=EXCLUDED.listing_days,trading_days_20d=EXCLUDED.trading_days_20d,
            close=EXCLUDED.close,amount=EXCLUDED.amount,total_mv=EXCLUDED.total_mv,
            circ_mv=EXCLUDED.circ_mv,circ_mv_pct=EXCLUDED.circ_mv_pct,
            return_1d=EXCLUDED.return_1d,ma20_gap=EXCLUDED.ma20_gap,
            margin_balance_change_20d=EXCLUDED.margin_balance_change_20d,
            turnover_rate=EXCLUDED.turnover_rate,avg_amount_20d=EXCLUDED.avg_amount_20d,
            avg_amount_60d=EXCLUDED.avg_amount_60d,amount_ratio_20_60=EXCLUDED.amount_ratio_20_60,
            amount_pct_20d=EXCLUDED.amount_pct_20d,turnover_pct_2y=EXCLUDED.turnover_pct_2y,
            turnover_pct_20d=EXCLUDED.turnover_pct_20d,
            pe_ttm=EXCLUDED.pe_ttm,pb=EXCLUDED.pb,dv_ttm=EXCLUDED.dv_ttm,
            roe_ttm=EXCLUDED.roe_ttm,roa_ttm=EXCLUDED.roa_ttm,
            roic=EXCLUDED.roic,grossprofit_margin=EXCLUDED.grossprofit_margin,
            revenue_yoy=EXCLUDED.revenue_yoy,profit_yoy=EXCLUDED.profit_yoy,
            revenue_yoy_prev=EXCLUDED.revenue_yoy_prev,profit_yoy_prev=EXCLUDED.profit_yoy_prev,
            revenue_acceleration=EXCLUDED.revenue_acceleration,profit_acceleration=EXCLUDED.profit_acceleration,
            ocf_to_profit=EXCLUDED.ocf_to_profit,debt_to_assets=EXCLUDED.debt_to_assets,financial_available_date=EXCLUDED.financial_available_date,
            ma60_gap=EXCLUDED.ma60_gap,ma120_gap=EXCLUDED.ma120_gap,ma200_gap=EXCLUDED.ma200_gap,
            return_20d=EXCLUDED.return_20d,return_60d=EXCLUDED.return_60d,
            return_120d=EXCLUDED.return_120d,volatility_20d=EXCLUDED.volatility_20d,
            volatility_60d=EXCLUDED.volatility_60d,volatility_120d=EXCLUDED.volatility_120d,
            drawdown_120d=EXCLUDED.drawdown_120d,path_efficiency_60d=EXCLUDED.path_efficiency_60d,
            tail_return_p05_60d=EXCLUDED.tail_return_p05_60d,
            pe_pct_5y=EXCLUDED.pe_pct_5y,pb_pct_5y=EXCLUDED.pb_pct_5y,
            rzye=EXCLUDED.rzye,rqye=EXCLUDED.rqye,rzmre=EXCLUDED.rzmre,
            moneyflow_net_amount=EXCLUDED.moneyflow_net_amount,moneyflow_net_amount_ratio=EXCLUDED.moneyflow_net_amount_ratio,
            moneyflow_large_net_amount=EXCLUDED.moneyflow_large_net_amount,moneyflow_large_net_ratio=EXCLUDED.moneyflow_large_net_ratio,
            moneyflow_available=EXCLUDED.moneyflow_available,
            moneyflow_net_amount_5d=EXCLUDED.moneyflow_net_amount_5d,
            moneyflow_net_amount_20d=EXCLUDED.moneyflow_net_amount_20d,
            dividend_event_120d=EXCLUDED.dividend_event_120d,
            repurchase_event_120d=EXCLUDED.repurchase_event_120d,
            data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,
            source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        count = await self._execute(
            sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION}
        )
        return count

    async def _enrich_stock_features(self, start: date, end: date) -> int:
        """Fill PIT-safe rolling stock factors after the fact snapshot exists."""
        sql = """
        WITH raw AS (
          SELECT s.*, s.close / NULLIF(LAG(s.close) OVER w, 0) - 1 AS r1,
                 AVG(s.close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                 AVG(s.close) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120,
                 AVG(s.close) OVER(w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                 AVG(s.amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount20,
                 AVG(s.amount) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS amount60,
                 COUNT(s.close) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS trades20,
                 ABS(s.close / NULLIF(LAG(s.close,60) OVER w, 0) - 1) AS path_displacement,
                 PERCENT_RANK() OVER(PARTITION BY s.trade_date ORDER BY s.circ_mv) AS circ_rank,
                 AVG(s.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_mean_2y,
                 STDDEV_SAMP(s.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_std_2y,
                 COUNT(s.turnover_rate) OVER(w ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_count_2y
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version
            AND s.trade_date BETWEEN (CAST(:start_date AS date) - INTERVAL '800 days') AND :end_date
          WINDOW w AS (PARTITION BY s.ts_code ORDER BY s.trade_date)
        ), source AS (
          SELECT raw.*,
            STDDEV_SAMP(r1) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS vol60,
            STDDEV_SAMP(r1) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS vol120,
            SUM(ABS(r1)) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS path_length,
            MIN(r1) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS tail_p05,
            SUM(moneyflow_net_amount) OVER(w ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moneyflow5,
            SUM(moneyflow_net_amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS moneyflow20
          FROM raw WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
        )
        UPDATE processed_mainline_stock_daily target SET
          trading_days_20d=source.trades20, avg_amount_20d=source.amount20, avg_amount_60d=source.amount60,
          amount_ratio_20_60=source.amount20/NULLIF(source.amount60,0), circ_mv_pct=source.circ_rank,
          ma60_gap=source.close/NULLIF(source.ma60,0)-1, ma120_gap=source.close/NULLIF(source.ma120,0)-1,
          ma200_gap=source.close/NULLIF(source.ma200,0)-1, volatility_60d=source.vol60, volatility_120d=source.vol120,
          path_efficiency_60d=source.path_displacement/NULLIF(source.path_length,0), tail_return_p05_60d=source.tail_p05,
          turnover_pct_2y=CASE WHEN source.turnover_count_2y>=504 AND source.turnover_std_2y>0
            THEN 1.0/(1.0+EXP(-1.702*(source.turnover_rate-source.turnover_mean_2y)/source.turnover_std_2y)) END,
          moneyflow_net_amount_5d=source.moneyflow5,moneyflow_net_amount_20d=source.moneyflow20,
          is_market_breadth_eligible=(target.is_listed AND NOT target.is_st AND NOT target.is_suspended AND source.trades20 >= 20 AND target.close IS NOT NULL),
          is_industry_breadth_eligible=(target.is_listed AND NOT target.is_st AND NOT target.is_suspended AND target.l2_code IS NOT NULL AND source.trades20 >= 20),
          is_stock_candidate_eligible=(target.is_listed AND NOT target.is_st AND NOT target.is_suspended AND target.listing_days >= 120 AND target.l2_code IS NOT NULL AND source.trades20 >= 20 AND COALESCE(target.amount,0)>0),
          is_eligible=(target.is_listed AND NOT target.is_st AND NOT target.is_suspended AND target.listing_days >= 120 AND target.l2_code IS NOT NULL AND source.trades20 >= 20 AND COALESCE(target.amount,0)>0),
          exclusion_reasons=ARRAY_REMOVE(ARRAY[
            CASE WHEN NOT target.is_listed THEN 'not_listed' END, CASE WHEN target.is_st THEN 'st' END,
            CASE WHEN target.is_suspended THEN 'suspended' END, CASE WHEN target.listing_days < 120 THEN 'new_listing' END,
            CASE WHEN target.l2_code IS NULL THEN 'missing_published_sw2021_l2' END,
            CASE WHEN source.trades20 < 20 THEN 'insufficient_trading_days_20d' END, CASE WHEN COALESCE(target.amount,0)<=0 THEN 'not_tradable' END
          ], NULL),
          data_quality=jsonb_build_object(
            'st_source',target.st_source,
            'tail_proxy','rolling_min_return_if_p05_unavailable',
            'turnover_pct_2y_method','pit_504d_logistic_ecdf_proxy',
            'turnover_pct_2y_observations',source.turnover_count_2y
          ),
          source_watermark=jsonb_build_object('prices_as_of',target.trade_date,'financial_available_date',target.financial_available_date,'moneyflow',CASE WHEN target.moneyflow_available THEN target.trade_date ELSE NULL END), processed_at=NOW()
        FROM source
        WHERE target.factor_version=source.factor_version AND target.ts_code=source.ts_code AND target.trade_date=source.trade_date
          AND target.trade_date BETWEEN :start_date AND :end_date
        """
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _materialize_market(self, start: date, end: date) -> int:
        sql = """
        WITH open_days AS (
            SELECT cal_date::date AS trade_date,
                   LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
            FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), benchmark AS (
            SELECT trade_date::date AS trade_date, close,
                   close/NULLIF(LAG(close,20) OVER w,0)-1 AS r20,
                   close/NULLIF(LAG(close,60) OVER w,0)-1 AS r60,
                   close/NULLIF(LAG(close,120) OVER w,0)-1 AS r120,
                   AVG(close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                   AVG(close) OVER (w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                   AVG(close) OVER (w ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS ma200,
                   STDDEV_SAMP(pct_chg/100.0) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20
            FROM index_daily WHERE ts_code=:benchmark
              AND trade_date >= (CAST(:start_date AS date)-INTERVAL '260 days')
              AND trade_date < (CAST(:end_date AS date)+INTERVAL '1 day')
            WINDOW w AS (ORDER BY trade_date)
        ), stock_ma AS (
            SELECT s.trade_date,s.ts_code AS symbol,s.return_1d,s.ma20_gap,
                   s.ma60_gap,s.ma120_gap
            FROM processed_mainline_stock_daily s
            WHERE s.factor_version=:factor_version
              AND s.trade_date BETWEEN :start_date AND :end_date
              AND s.is_market_breadth_eligible
        ), breadth AS (
            SELECT x.trade_date,
                   AVG((x.ma20_gap>0)::int) AS above20,
                   AVG((x.ma60_gap>0)::int) AS above60,
                   AVG((x.ma120_gap>0)::int) AS above120,
                   COUNT(*) AS denominator,
                   COUNT(*) FILTER(WHERE x.ma60_gap>0) AS above60_count,
                   COUNT(*) FILTER(WHERE x.ma120_gap>0) AS above120_count,
                   SUM((x.return_1d>0)::int)::numeric /
                     NULLIF(SUM((x.return_1d<0)::int),0) AS ad_ratio
            FROM stock_ma x GROUP BY x.trade_date
        )
        INSERT INTO processed_mainline_market_daily (
            factor_version,trade_date,as_of_trade_date,usable_from_trade_date,
            benchmark_code,benchmark_close,benchmark_return_20d,
            benchmark_return_60d,benchmark_return_120d,benchmark_ma20_gap,
            benchmark_ma60_gap,benchmark_ma200_gap,benchmark_volatility_20d,breadth_above_ma20,
            breadth_above_ma60,breadth_above_ma120,breadth_denominator,breadth_above_ma60_count,breadth_above_ma120_count,effective_stock_count,advance_decline_ratio,north_money,north_money_20d,
            market_regime,northbound_available,northbound_available_from,data_quality,source_watermark,source_asof
        )
        SELECT :factor_version,b.trade_date,b.trade_date,
               COALESCE(od.next_trade_date,b.trade_date+1),
               :benchmark,b.close,b.r20,b.r60,b.r120,
               b.close/NULLIF(b.ma20,0)-1,b.close/NULLIF(b.ma60,0)-1,b.close/NULLIF(b.ma200,0)-1,b.vol20,
               x.above20,x.above60,x.above120,x.denominator,x.above60_count,x.above120_count,x.denominator,x.ad_ratio,m.north_money,
               SUM(m.north_money) OVER (ORDER BY b.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
               CASE WHEN b.close>b.ma60 AND x.above60>=0.5 THEN 'risk_on'
                    WHEN b.close<b.ma60 AND x.above60<0.5 THEN 'risk_off' ELSE 'neutral' END,
               m.north_money IS NOT NULL,(SELECT MIN(trade_date) FROM moneyflow_hsgt WHERE north_money IS NOT NULL),
               jsonb_build_object('northbound_available',m.north_money IS NOT NULL,'northbound_optional',true),
               jsonb_build_object('benchmark',b.trade_date,'moneyflow_hsgt',CASE WHEN m.north_money IS NOT NULL THEN b.trade_date ELSE NULL END),NOW()
        FROM benchmark b LEFT JOIN breadth x USING(trade_date)
        LEFT JOIN moneyflow_hsgt m USING(trade_date)
        LEFT JOIN open_days od ON od.trade_date=b.trade_date
        WHERE b.trade_date BETWEEN :start_date AND :end_date
        ON CONFLICT (factor_version,trade_date) DO UPDATE SET
            benchmark_close=EXCLUDED.benchmark_close,
            benchmark_return_20d=EXCLUDED.benchmark_return_20d,
            benchmark_return_60d=EXCLUDED.benchmark_return_60d,
            benchmark_return_120d=EXCLUDED.benchmark_return_120d,
            benchmark_ma20_gap=EXCLUDED.benchmark_ma20_gap,
            benchmark_ma60_gap=EXCLUDED.benchmark_ma60_gap,
            benchmark_ma200_gap=EXCLUDED.benchmark_ma200_gap,
            benchmark_volatility_20d=EXCLUDED.benchmark_volatility_20d,
            breadth_above_ma20=EXCLUDED.breadth_above_ma20,
            breadth_above_ma60=EXCLUDED.breadth_above_ma60,
            breadth_above_ma120=EXCLUDED.breadth_above_ma120,
            breadth_denominator=EXCLUDED.breadth_denominator,
            breadth_above_ma60_count=EXCLUDED.breadth_above_ma60_count,
            breadth_above_ma120_count=EXCLUDED.breadth_above_ma120_count,
            effective_stock_count=EXCLUDED.effective_stock_count,
            advance_decline_ratio=EXCLUDED.advance_decline_ratio,
            north_money=EXCLUDED.north_money,north_money_20d=EXCLUDED.north_money_20d,
            market_regime=EXCLUDED.market_regime,northbound_available=EXCLUDED.northbound_available,northbound_available_from=EXCLUDED.northbound_available_from,
            data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,source_asof=EXCLUDED.source_asof,
            processed_at=NOW()
        """
        return await self._execute(
            sql,
            {"start_date": start, "end_date": end, "benchmark": self.BENCHMARK_CODE, "factor_version": MAINLINE_FACTOR_VERSION},
        )

    async def _materialize_industry(self, start: date, end: date) -> int:
        sql = """
        WITH open_days AS (
          SELECT cal_date::date AS trade_date,
                 LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
          FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), industry_index AS (
          SELECT d.trade_date::date AS trade_date,d.ts_code AS l2_code,d.close,
                 d.close/NULLIF(LAG(d.close,20) OVER w,0)-1 AS r20,
                 d.close/NULLIF(LAG(d.close,60) OVER w,0)-1 AS r60,
                 d.close/NULLIF(LAG(d.close,120) OVER w,0)-1 AS r120,
                 AVG(d.close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                 AVG(d.close) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120
          FROM sw_daily d
          WHERE d.trade_date >= (CAST(:start_date AS date) - INTERVAL '260 days')
            AND d.trade_date < (CAST(:end_date AS date) + INTERVAL '1 day')
          WINDOW w AS (PARTITION BY d.ts_code ORDER BY d.trade_date)
        ), base AS (
          SELECT s.*,s.return_1d AS r1,s.margin_balance_change_20d AS rz20
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version
            AND s.trade_date BETWEEN :start_date AND :end_date
        ), ranked_base AS (
          SELECT base.*,
                 PERCENT_RANK() OVER(PARTITION BY trade_date ORDER BY return_60d) AS global_return_60d_rank
          FROM base
        )
        INSERT INTO processed_mainline_industry_daily (
          factor_version,trade_date,as_of_trade_date,usable_from_trade_date,l1_code,l1_name,l2_code,l2_name,index_code,index_close,return_20d,return_60d,return_120d,ma60_gap,ma120_gap,stock_count,equal_weight_return,
          cap_weight_return,relative_return_20d,relative_return_60d,relative_return_120d,breadth_above_ma20,
          breadth_above_ma60,breadth_above_ma120,strong_stock_count,strong_stock_ratio,return_dispersion,amount_share,median_pe_ttm,median_pb,
          median_roe_ttm,median_roic,median_grossprofit_margin,median_revenue_yoy,
          median_profit_yoy,margin_balance_change_20d,
          moneyflow_net_amount,moneyflow_net_amount_ratio,moneyflow_large_net_amount,moneyflow_large_net_ratio,
          moneyflow_net_amount_5d,moneyflow_net_amount_20d,moneyflow_positive_stock_ratio,moneyflow_top_stock_contribution,moneyflow_coverage,moneyflow_available,
          data_quality,source_watermark,source_asof
        )
        SELECT :factor_version,b.trade_date,b.trade_date,
          COALESCE(MAX(od.next_trade_date),b.trade_date+1),
          MAX(b.l1_code),MAX(b.l1_name),b.l2_code,MAX(b.l2_name),MAX(ii.l2_code),MAX(ii.close),MAX(ii.r20),MAX(ii.r60),MAX(ii.r120),MAX(ii.close/NULLIF(ii.ma60,0)-1),MAX(ii.close/NULLIF(ii.ma120,0)-1),COUNT(*),
          AVG(b.r1),SUM(b.r1*b.circ_mv)/NULLIF(SUM(b.circ_mv),0),
          MAX(ii.r20)-MAX(m.benchmark_return_20d),
          MAX(ii.r60)-MAX(m.benchmark_return_60d),
          MAX(ii.r120)-MAX(m.benchmark_return_120d),
          AVG((b.ma20_gap>0)::int),AVG((b.ma60_gap>0)::int),AVG((b.ma120_gap>0)::int),
          COUNT(*) FILTER(WHERE b.global_return_60d_rank>=0.80 AND b.amount_ratio_20_60>1.20),
          AVG((b.global_return_60d_rank>=0.80 AND b.amount_ratio_20_60>1.20)::int),STDDEV_SAMP(b.r1),
          SUM(b.amount)/NULLIF(SUM(SUM(b.amount)) OVER(PARTITION BY b.trade_date),0),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pb),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roic),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.grossprofit_margin),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.revenue_yoy),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.profit_yoy),SUM(b.rz20),
          SUM(b.moneyflow_net_amount),SUM(b.moneyflow_net_amount)/NULLIF(SUM(b.amount),0),
          SUM(b.moneyflow_large_net_amount),SUM(b.moneyflow_large_net_amount)/NULLIF(SUM(b.amount),0),
          SUM(b.moneyflow_net_amount_5d),SUM(b.moneyflow_net_amount_20d),
          COUNT(*) FILTER(WHERE b.moneyflow_net_amount>0)::numeric/NULLIF(COUNT(*) FILTER(WHERE b.moneyflow_available),0),
          MAX(GREATEST(COALESCE(b.moneyflow_net_amount,0),0))/NULLIF(SUM(GREATEST(COALESCE(b.moneyflow_net_amount,0),0)),0),
          COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0),
          COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0)>=0.95,
          jsonb_build_object('published_sw2021_l2',true,'industry_index_available',MAX(ii.close) IS NOT NULL,'moneyflow_coverage',COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0)),
          jsonb_build_object('sw_daily',MAX(ii.trade_date),'moneyflow',CASE WHEN COUNT(*) FILTER(WHERE b.moneyflow_available)>0 THEN b.trade_date ELSE NULL END),NOW()
        FROM ranked_base b
        JOIN sw_industry_classify ic ON ic.index_code=b.l2_code AND ic.level='L2' AND ic.is_pub='1'
        LEFT JOIN industry_index ii ON ii.l2_code=b.l2_code AND ii.trade_date=b.trade_date
        LEFT JOIN processed_mainline_market_daily m ON m.trade_date=b.trade_date AND m.factor_version=:factor_version
        LEFT JOIN open_days od ON od.trade_date=b.trade_date
        WHERE b.is_industry_breadth_eligible AND b.l2_code IS NOT NULL AND b.trade_date BETWEEN :start_date AND :end_date
        GROUP BY b.trade_date,b.l2_code
        ON CONFLICT (factor_version,l2_code,trade_date) DO UPDATE SET
          l1_code=EXCLUDED.l1_code,l1_name=EXCLUDED.l1_name,l2_name=EXCLUDED.l2_name,
          stock_count=EXCLUDED.stock_count,equal_weight_return=EXCLUDED.equal_weight_return,
          cap_weight_return=EXCLUDED.cap_weight_return,relative_return_20d=EXCLUDED.relative_return_20d,
          relative_return_60d=EXCLUDED.relative_return_60d,relative_return_120d=EXCLUDED.relative_return_120d,
          return_20d=EXCLUDED.return_20d,return_60d=EXCLUDED.return_60d,return_120d=EXCLUDED.return_120d,ma60_gap=EXCLUDED.ma60_gap,ma120_gap=EXCLUDED.ma120_gap,
          breadth_above_ma20=EXCLUDED.breadth_above_ma20,
          breadth_above_ma60=EXCLUDED.breadth_above_ma60,
          breadth_above_ma120=EXCLUDED.breadth_above_ma120,strong_stock_count=EXCLUDED.strong_stock_count,strong_stock_ratio=EXCLUDED.strong_stock_ratio,
          return_dispersion=EXCLUDED.return_dispersion,amount_share=EXCLUDED.amount_share,
          median_pe_ttm=EXCLUDED.median_pe_ttm,median_pb=EXCLUDED.median_pb,
          median_roe_ttm=EXCLUDED.median_roe_ttm,
          median_roic=EXCLUDED.median_roic,
          median_grossprofit_margin=EXCLUDED.median_grossprofit_margin,
          median_revenue_yoy=EXCLUDED.median_revenue_yoy,
          median_profit_yoy=EXCLUDED.median_profit_yoy,
          margin_balance_change_20d=EXCLUDED.margin_balance_change_20d,
          moneyflow_net_amount=EXCLUDED.moneyflow_net_amount,moneyflow_net_amount_ratio=EXCLUDED.moneyflow_net_amount_ratio,
          moneyflow_large_net_amount=EXCLUDED.moneyflow_large_net_amount,moneyflow_large_net_ratio=EXCLUDED.moneyflow_large_net_ratio,
          moneyflow_net_amount_5d=EXCLUDED.moneyflow_net_amount_5d,moneyflow_net_amount_20d=EXCLUDED.moneyflow_net_amount_20d,
          moneyflow_positive_stock_ratio=EXCLUDED.moneyflow_positive_stock_ratio,moneyflow_top_stock_contribution=EXCLUDED.moneyflow_top_stock_contribution,
          moneyflow_coverage=EXCLUDED.moneyflow_coverage,moneyflow_available=EXCLUDED.moneyflow_available,
          index_code=EXCLUDED.index_code,index_close=EXCLUDED.index_close,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,
          source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        count = await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})
        await self._enrich_industry_inputs(start, end)
        return count

    async def _enrich_industry_inputs(self, start: date, end: date) -> int:
        """Attach PIT-safe liquidity, financial, fund and ETF inputs to industries."""
        sql = """
        WITH ranked_stock AS (
          SELECT s.*,
                 ROW_NUMBER() OVER(
                   PARTITION BY s.trade_date,s.l2_code ORDER BY s.amount DESC NULLS LAST
                 ) AS amount_rank
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version
            AND s.is_industry_breadth_eligible
            AND s.trade_date BETWEEN :start_date AND :end_date
        ), stock_inputs AS (
          SELECT trade_date,l2_code,
                 SUM(avg_amount_20d) AS avg_amount_20d,
                 SUM(avg_amount_60d) AS avg_amount_60d,
                 SUM(amount) FILTER(WHERE amount_rank<=5)/NULLIF(SUM(amount),0) AS top5_amount_share,
                 PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY revenue_acceleration)
                   FILTER(WHERE revenue_acceleration IS NOT NULL) AS revenue_acceleration,
                 PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY profit_acceleration)
                   FILTER(WHERE profit_acceleration IS NOT NULL) AS profit_acceleration
          FROM ranked_stock GROUP BY trade_date,l2_code
        ), crowding AS (
          SELECT DISTINCT ON (i.trade_date,i.l2_code)
                 i.trade_date,i.l2_code,c.holding_value,c.fund_count,c.holding_change,
                 c.concentration,c.disclosure_coverage
          FROM processed_mainline_industry_daily i
          LEFT JOIN processed_mainline_industry_crowding_monthly c
            ON c.factor_version=i.factor_version AND c.l2_code=i.l2_code
           AND c.as_of_trade_date<=i.trade_date
           AND c.usable_from_trade_date<=i.trade_date
          WHERE i.factor_version=:factor_version
            AND i.trade_date BETWEEN :start_date AND :end_date
          ORDER BY i.trade_date,i.l2_code,c.as_of_trade_date DESC NULLS LAST
        ), etf_inputs AS (
          SELECT trade_date,primary_l2_code AS l2_code,
                 SUM(net_inflow_20d) AS etf_net_inflow_20d,
                 SUM(total_size) AS etf_aum
          FROM processed_mainline_etf_daily
          WHERE factor_version=:factor_version AND primary_l2_code IS NOT NULL
            AND trade_date BETWEEN :start_date AND :end_date
          GROUP BY trade_date,primary_l2_code
        ), inputs AS (
          SELECT i.trade_date,i.l2_code,s.avg_amount_20d,s.avg_amount_60d,
                 s.top5_amount_share,s.revenue_acceleration,s.profit_acceleration,
                 c.holding_value,c.fund_count,c.holding_change,c.concentration,c.disclosure_coverage,
                 e.etf_net_inflow_20d,e.etf_aum
          FROM processed_mainline_industry_daily i
          LEFT JOIN stock_inputs s USING(trade_date,l2_code)
          LEFT JOIN crowding c USING(trade_date,l2_code)
          LEFT JOIN etf_inputs e USING(trade_date,l2_code)
          WHERE i.factor_version=:factor_version
            AND i.trade_date BETWEEN :start_date AND :end_date
        ), industry_returns AS (
          SELECT i.trade_date,i.l2_code,i.index_close,i.relative_return_60d,i.top5_amount_share,
                 i.ma60_gap,i.amount_ratio_20_60,
                 i.index_close/NULLIF(LAG(i.index_close) OVER w,0)-1 AS index_return_1d,
                 LAG(i.relative_return_60d,20) OVER w AS relative_return_60d_lag20,
                 LAG(i.top5_amount_share,20) OVER w AS top5_amount_share_lag20
          FROM processed_mainline_industry_daily i
          WHERE i.factor_version=:factor_version
            AND i.trade_date BETWEEN (:start_date - INTERVAL '400 days') AND :end_date
          WINDOW w AS (PARTITION BY i.l2_code ORDER BY i.trade_date)
        ), advanced AS (
          SELECT *,
                 STDDEV_SAMP(index_return_1d) OVER w AS vol_60d,
                 SUM(ABS(index_return_1d)) OVER w AS path_length_60d,
                 MIN(index_return_1d) OVER w AS tail_return_p05_proxy
          FROM industry_returns
          WINDOW w AS (PARTITION BY l2_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
        )
        UPDATE processed_mainline_industry_daily target SET
          avg_amount_20d=inputs.avg_amount_20d,
          avg_amount_60d=inputs.avg_amount_60d,
          amount_ratio_20_60=inputs.avg_amount_20d/NULLIF(inputs.avg_amount_60d,0),
          top5_amount_share=inputs.top5_amount_share,
          revenue_acceleration=inputs.revenue_acceleration,
          profit_acceleration=inputs.profit_acceleration,
          fund_holding_value=inputs.holding_value,
          fund_count=inputs.fund_count,
          fund_holding_change=inputs.holding_change,
          fund_concentration=inputs.concentration,
          disclosure_coverage=inputs.disclosure_coverage,
          etf_net_inflow_20d=inputs.etf_net_inflow_20d,
          etf_aum=inputs.etf_aum,
          relative_strength_slope_20d=advanced.relative_return_60d-advanced.relative_return_60d_lag20,
          risk_adjusted_momentum_60d=target.return_60d/NULLIF(advanced.vol_60d*SQRT(252),0),
          information_ratio_60d=target.relative_return_60d/NULLIF(advanced.vol_60d*SQRT(252),0),
          path_efficiency_60d=ABS(target.return_60d)/NULLIF(advanced.path_length_60d,0),
          tail_return_p05_60d=advanced.tail_return_p05_proxy,
          top5_amount_share_change_20d=inputs.top5_amount_share-advanced.top5_amount_share_lag20,
          -- v4 crowding is an industry-own-history construction.  The exact
          -- percentile is intentionally left null until a full 504-day PIT
          -- history exists; the engine then treats it as a gate unavailable,
          -- never as a cross-sectional fund-concentration reward.
          crowding_input=inputs.concentration,
          data_quality=target.data_quality || JSONB_BUILD_OBJECT(
            'financial_acceleration_pit',inputs.revenue_acceleration IS NOT NULL,
            'fund_crowding_pit',inputs.holding_value IS NOT NULL,
            'etf_aggregate_available',inputs.etf_aum IS NOT NULL
          ),
          source_watermark=target.source_watermark || JSONB_BUILD_OBJECT(
            'fund_crowding',CASE WHEN inputs.holding_value IS NOT NULL THEN target.trade_date ELSE NULL END,
            'etf_daily',CASE WHEN inputs.etf_aum IS NOT NULL THEN target.trade_date ELSE NULL END
          ),
          processed_at=NOW()
        FROM inputs
        JOIN advanced ON advanced.trade_date=inputs.trade_date AND advanced.l2_code=inputs.l2_code
        WHERE target.factor_version=:factor_version
          AND target.trade_date=inputs.trade_date AND target.l2_code=inputs.l2_code
        """
        return await self._execute(
            sql,
            {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION},
        )

    async def _materialize_etf(self, start: date, end: date) -> int:
        """Compute all ETF rolling factors in DuckDB, then COPY one final batch."""
        source = await self._read_dataframe(
            """WITH open_days AS (
                 SELECT cal_date::date AS trade_date,
                        LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
                 FROM trade_cal WHERE exchange='SSE' AND is_open=1
               )
               SELECT d.trade_date,d.ts_code,m.benchmark_index_code AS index_code,b.list_date,
                      COALESCE(m.mapping_status,'mapping_pending') AS benchmark_mapping_status,
                      m.source_name AS benchmark_mapping_source,
                      m.confidence AS benchmark_mapping_confidence,
                      COALESCE(m.review_status,'pending') AS benchmark_mapping_review_status,
                      od.next_trade_date,d.close AS raw_close,
                      d.close*COALESCE(a.adj_factor,1) AS adj_close,
                      d.amount*1000 AS amount,s.total_share*10000 AS total_share,
                      s.total_size*10000 AS total_size,s.nav,id.close AS index_close
               FROM fund_daily d JOIN etf_basic b USING(ts_code)
               LEFT JOIN mainline_etf_benchmark_history m
                 ON m.ts_code=d.ts_code
                AND m.usable_from_trade_date<=d.trade_date
                AND (m.usable_to_trade_date IS NULL OR m.usable_to_trade_date>d.trade_date)
               -- m retains its own ts_code, so USING here would make the
               -- accumulated left join ambiguous in PostgreSQL.
               LEFT JOIN fund_adj a
                 ON a.ts_code=d.ts_code AND a.trade_date=d.trade_date
               LEFT JOIN etf_share_size s
                 ON s.ts_code=d.ts_code AND s.trade_date=d.trade_date
               LEFT JOIN index_daily id ON id.ts_code=m.benchmark_index_code AND id.trade_date::date=d.trade_date
                AND m.mapping_status='mapped' AND m.review_status='approved'
               LEFT JOIN open_days od ON od.trade_date=d.trade_date
               WHERE d.trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '260 days') AND :end_date
               """,
            {"start_date":start,"end_date":end},
        )
        if source.empty:
            return 0
        numeric_columns = (
            "raw_close","adj_close","amount","total_share","total_size","nav","index_close"
        )
        for column in numeric_columns:
            source[column] = pd.to_numeric(source[column],errors="coerce").astype("float64")
        import duckdb
        duck = duckdb.connect(database=":memory:")
        try:
            duck.register("etf_source",source)
            calculated = duck.execute(
                """WITH returns AS (
                     SELECT *,
                       adj_close/NULLIF(lag(adj_close,1) OVER w,0)-1 AS r1,
                       adj_close/NULLIF(lag(adj_close,20) OVER w,0)-1 AS r20,
                       adj_close/NULLIF(lag(adj_close,60) OVER w,0)-1 AS r60,
                       adj_close/NULLIF(lag(adj_close,120) OVER w,0)-1 AS r120,
                       index_close/NULLIF(lag(index_close,1) OVER w,0)-1 AS idx_r1,
                       total_share-lag(total_share,5) OVER w AS share5,
                       total_share-lag(total_share,20) OVER w AS share20,
                       min(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amin,
                       max(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amax,
                       avg(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount20,
                       avg(amount) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS amount60,
                       avg(adj_close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                       raw_close/NULLIF(nav,0)-1 AS premium
                     FROM etf_source WINDOW w AS(PARTITION BY ts_code ORDER BY trade_date)
                   ), factors AS (
                     SELECT *,
                       stddev_samp(r1) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20,
                       stddev_samp(r1-idx_r1) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)*sqrt(252) AS te60,
                       stddev_samp(r1-idx_r1) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW)*sqrt(252) AS te120,
                       quantile_cont(abs(premium),0.95) FILTER(WHERE premium IS NOT NULL)
                         OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS premium_p95
                     FROM returns WINDOW w AS(PARTITION BY ts_code ORDER BY trade_date)
                   )
                   SELECT trade_date,ts_code,index_code,list_date,benchmark_mapping_status,
                          benchmark_mapping_source,benchmark_mapping_confidence,
                          benchmark_mapping_review_status,next_trade_date,adj_close,amount,
                          total_share,total_size,nav,index_close,r20,r60,r120,share5,share20,
                          amin,amax,amount20,amount60,ma60,vol20,te60,te120,premium,premium_p95
                   FROM factors WHERE trade_date BETWEEN ? AND ?
                   ORDER BY ts_code,trade_date""",
                [start,end],
            ).fetch_df()
        finally:
            duck.close()
        for column in ("trade_date","list_date","next_trade_date"):
            calculated[column] = pd.to_datetime(calculated[column],errors="coerce").dt.date
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        columns = list(calculated.columns)
        async with engine.begin() as conn:
            await conn.execute(text("""CREATE TEMP TABLE mainline_etf_stage(
              trade_date date,ts_code varchar(32),index_code varchar(32),list_date date,
              benchmark_mapping_status varchar(32),benchmark_mapping_source varchar(64),
              benchmark_mapping_confidence varchar(16),benchmark_mapping_review_status varchar(16),
              next_trade_date date,adj_close double precision,amount double precision,
              total_share double precision,total_size double precision,nav double precision,
              index_close double precision,r20 double precision,r60 double precision,r120 double precision,
              share5 double precision,share20 double precision,amin double precision,amax double precision,
              amount20 double precision,amount60 double precision,ma60 double precision,vol20 double precision,
              te60 double precision,te120 double precision,premium double precision,premium_p95 double precision
            ) ON COMMIT DROP"""))
            raw = await conn.get_raw_connection()
            driver = raw.driver_connection
            for offset in range(0,len(calculated),100_000):
                batch = calculated.iloc[offset:offset+100_000]
                records = [tuple(None if pd.isna(value) else value.item() if isinstance(value,np.generic) else value for value in row)
                           for row in batch.itertuples(index=False,name=None)]
                await driver.copy_records_to_table("mainline_etf_stage",records=records,columns=columns)
            result = await conn.execute(text("""INSERT INTO processed_mainline_etf_daily(
              factor_version,trade_date,as_of_trade_date,usable_from_trade_date,ts_code,index_code,list_date,
              benchmark_mapping_status,benchmark_mapping_source,benchmark_mapping_confidence,benchmark_mapping_review_status,
              benchmark_available,data_complete,is_tradable,is_eligible,exclusion_reason,exclusion_reasons,
              adj_close,ma60_gap,return_20d,return_60d,return_120d,volatility_20d,amount,
              avg_amount_20d,avg_amount_60d,amount_ratio_20_60,amount_pct_20d,total_share,total_size,
              share_change_5d,share_change_20d,net_inflow_5d,net_inflow_20d,tracking_error_60d,
              tracking_error_120d,premium_discount,premium_discount_abs_p95_60d,
              primary_l2_code,primary_l2_weight,top5_l2_exposure,exposure_hhi,
              data_quality,source_watermark,source_asof
            ) SELECT :factor_version,trade_date,trade_date,COALESCE(next_trade_date,trade_date+1),
              ts_code,index_code,list_date,benchmark_mapping_status,benchmark_mapping_source,
              benchmark_mapping_confidence,benchmark_mapping_review_status,
              index_close IS NOT NULL,
              (adj_close IS NOT NULL AND amount20 IS NOT NULL AND total_share IS NOT NULL
                AND total_size IS NOT NULL AND nav IS NOT NULL),
              (COALESCE(amount,0)>0 AND COALESCE(list_date<=trade_date,FALSE)),
              (benchmark_mapping_status='mapped' AND index_close IS NOT NULL AND adj_close IS NOT NULL AND amount20 IS NOT NULL
                AND total_share IS NOT NULL AND total_size IS NOT NULL AND nav IS NOT NULL
                AND COALESCE(amount,0)>0 AND COALESCE(list_date<=trade_date,FALSE)
                AND exposure.primary_l2_code IS NOT NULL),
              CASE WHEN benchmark_mapping_status <> 'mapped' THEN benchmark_mapping_status
                   WHEN index_code IS NULL THEN 'missing_index_code'
                   WHEN index_close IS NULL THEN 'missing_benchmark_daily'
                   WHEN list_date IS NULL THEN 'missing_list_date' WHEN total_size IS NULL THEN 'missing_size'
                   WHEN total_share IS NULL THEN 'missing_share' WHEN nav IS NULL THEN 'missing_nav'
                   WHEN COALESCE(amount,0)<=0 THEN 'not_tradable'
                   WHEN exposure.primary_l2_code IS NULL THEN 'missing_pit_industry_exposure' END,
              ARRAY_REMOVE(ARRAY[CASE WHEN benchmark_mapping_status <> 'mapped' THEN benchmark_mapping_status END,
                CASE WHEN index_code IS NULL THEN 'missing_index_code' END,
                CASE WHEN index_close IS NULL THEN 'missing_benchmark_daily' END,
                CASE WHEN list_date IS NULL THEN 'missing_list_date' END,
                CASE WHEN total_size IS NULL THEN 'missing_size' END,
                CASE WHEN total_share IS NULL THEN 'missing_share' END,
                CASE WHEN nav IS NULL THEN 'missing_nav' END,
                CASE WHEN COALESCE(amount,0)<=0 THEN 'not_tradable' END,
                CASE WHEN exposure.primary_l2_code IS NULL THEN 'missing_pit_industry_exposure' END],NULL),
              adj_close,adj_close/NULLIF(ma60,0)-1,r20,r60,r120,vol20,amount,
              amount20,amount60,amount20/NULLIF(amount60,0),(amount-amin)/NULLIF(amax-amin,0),
              total_share,total_size,share5,share20,share5*nav,share20*nav,te60,te120,premium,premium_p95,
              exposure.primary_l2_code,exposure.primary_l2_weight,
              COALESCE(exposure.top5_l2_exposure,'[]'::jsonb),exposure.exposure_hhi,
              JSONB_BUILD_OBJECT('benchmark_available',index_close IS NOT NULL,'size_available',total_size IS NOT NULL,
                'benchmark_mapping_status',benchmark_mapping_status,
                'benchmark_mapping_source',benchmark_mapping_source,
                'benchmark_mapping_confidence',benchmark_mapping_confidence,
                'benchmark_mapping_review_status',benchmark_mapping_review_status,
                'pit_exposure_available',exposure.primary_l2_code IS NOT NULL,
                'exposure_carried_forward',exposure.primary_l2_code IS NOT NULL,
                'rolling_engine','duckdb'),JSONB_BUILD_OBJECT('fund_daily',trade_date,'share_size',trade_date,
                'index_weight',exposure.as_of_trade_date),NOW()
            FROM mainline_etf_stage stage
            LEFT JOIN (
              SELECT ts_code AS exposure_ts_code,as_of_trade_date,usable_from_trade_date,
                     usable_to_trade_date,primary_l2_code,primary_l2_weight,
                     top5_l2_exposure,exposure_hhi
              FROM processed_mainline_etf_exposure_summary
              WHERE factor_version=:factor_version
            ) exposure
              ON exposure.exposure_ts_code=stage.ts_code
             AND exposure.usable_from_trade_date<=stage.trade_date
             AND (exposure.usable_to_trade_date IS NULL OR exposure.usable_to_trade_date>stage.trade_date)
            ON CONFLICT(factor_version,ts_code,trade_date) DO UPDATE SET
              index_code=EXCLUDED.index_code,list_date=EXCLUDED.list_date,
              benchmark_mapping_status=EXCLUDED.benchmark_mapping_status,
              benchmark_mapping_source=EXCLUDED.benchmark_mapping_source,
              benchmark_mapping_confidence=EXCLUDED.benchmark_mapping_confidence,
              benchmark_mapping_review_status=EXCLUDED.benchmark_mapping_review_status,
              benchmark_available=EXCLUDED.benchmark_available,data_complete=EXCLUDED.data_complete,
              is_tradable=EXCLUDED.is_tradable,is_eligible=EXCLUDED.is_eligible,
              exclusion_reason=EXCLUDED.exclusion_reason,exclusion_reasons=EXCLUDED.exclusion_reasons,
              adj_close=EXCLUDED.adj_close,ma60_gap=EXCLUDED.ma60_gap,return_20d=EXCLUDED.return_20d,
              return_60d=EXCLUDED.return_60d,return_120d=EXCLUDED.return_120d,
              volatility_20d=EXCLUDED.volatility_20d,amount=EXCLUDED.amount,
              avg_amount_20d=EXCLUDED.avg_amount_20d,avg_amount_60d=EXCLUDED.avg_amount_60d,
              amount_ratio_20_60=EXCLUDED.amount_ratio_20_60,amount_pct_20d=EXCLUDED.amount_pct_20d,
              total_share=EXCLUDED.total_share,total_size=EXCLUDED.total_size,
              share_change_5d=EXCLUDED.share_change_5d,share_change_20d=EXCLUDED.share_change_20d,
              net_inflow_5d=EXCLUDED.net_inflow_5d,net_inflow_20d=EXCLUDED.net_inflow_20d,
              tracking_error_60d=EXCLUDED.tracking_error_60d,tracking_error_120d=EXCLUDED.tracking_error_120d,
              premium_discount=EXCLUDED.premium_discount,
              premium_discount_abs_p95_60d=EXCLUDED.premium_discount_abs_p95_60d,
              primary_l2_code=EXCLUDED.primary_l2_code,
              primary_l2_weight=EXCLUDED.primary_l2_weight,
              top5_l2_exposure=EXCLUDED.top5_l2_exposure,
              exposure_hhi=EXCLUDED.exposure_hhi,
              data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,
              source_asof=EXCLUDED.source_asof,processed_at=NOW()"""),
              {"factor_version":MAINLINE_FACTOR_VERSION})
            # Exact L2 exposure is the safe default.  Reviewed rows in the
            # v4 mapping history may replace it with an L1/theme proxy, but a
            # current ETF catalogue attribute can never manufacture a proxy.
            await conn.execute(text("""
              UPDATE processed_mainline_etf_daily SET
                mapping_level='exact_l2',target_coverage=primary_l2_weight,
                non_target_l1_exposure=0,
                exposure_vector=JSONB_BUILD_OBJECT('primary_l2_code',primary_l2_code,'primary_l2_weight',primary_l2_weight),
                mapping_evidence=JSONB_BUILD_OBJECT('reference','processed_mainline_etf_exposure_summary','mapping_level','exact_l2')
              WHERE factor_version=:factor_version AND trade_date BETWEEN :start_date AND :end_date
            """), {"factor_version": MAINLINE_FACTOR_VERSION, "start_date": start, "end_date": end})
            await conn.execute(text("""
              UPDATE processed_mainline_etf_daily e SET
                mapping_level=COALESCE(mapping.mapping_level,'exact_l2'),
                target_coverage=COALESCE(mapping.target_coverage,e.primary_l2_weight),
                non_target_l1_exposure=COALESCE(mapping.non_target_l1_exposure,0),
                exposure_vector=COALESCE(mapping.exposure_vector,
                  JSONB_BUILD_OBJECT('primary_l2_code',e.primary_l2_code,'primary_l2_weight',e.primary_l2_weight)),
                mapping_evidence=COALESCE(JSONB_BUILD_OBJECT(
                  'reference',mapping.evidence_reference,'reviewed_by',mapping.reviewed_by,
                  'reviewed_at',mapping.reviewed_at), JSONB_BUILD_OBJECT(
                  'reference','processed_mainline_etf_exposure_summary','mapping_level','exact_l2'))
              FROM mainline_etf_strategy_mapping_history mapping
              WHERE e.factor_version=:factor_version AND e.trade_date BETWEEN :start_date AND :end_date
                AND mapping.ts_code=e.ts_code AND mapping.usable_from_trade_date<=e.trade_date
                AND (mapping.usable_to_trade_date IS NULL OR mapping.usable_to_trade_date>e.trade_date)
            """), {"factor_version": MAINLINE_FACTOR_VERSION, "start_date": start, "end_date": end})
            return int(max(result.rowcount,0))

    async def _materialize_etf_postgres_legacy(self, start: date, end: date) -> int:
        sql = """
        WITH base AS (
          SELECT d.trade_date,d.ts_code,b.index_code,b.list_date,d.close AS raw_close,
                 d.close*COALESCE(a.adj_factor,1) AS adj_close,
                 d.amount * 1000 AS amount,
                 s.total_share * 10000 AS total_share,
                 s.total_size * 10000 AS total_size,s.nav,
                 id.close AS index_close,
                 id.close/NULLIF(LAG(id.close) OVER(PARTITION BY id.ts_code ORDER BY id.trade_date),0)-1 AS idx_r1
          FROM fund_daily d JOIN etf_basic b USING(ts_code)
          LEFT JOIN fund_adj a USING(ts_code,trade_date)
          LEFT JOIN etf_share_size s USING(ts_code,trade_date)
          LEFT JOIN index_daily id ON id.ts_code=b.index_code AND id.trade_date::date=d.trade_date
          WHERE d.trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '260 days') AND :end_date
        ), f AS (
          SELECT base.*,
            adj_close/NULLIF(LAG(adj_close,1) OVER w,0)-1 AS r1,
            adj_close/NULLIF(LAG(adj_close,20) OVER w,0)-1 AS r20,
            adj_close/NULLIF(LAG(adj_close,60) OVER w,0)-1 AS r60,
            adj_close/NULLIF(LAG(adj_close,120) OVER w,0)-1 AS r120,
            total_share-LAG(total_share,5) OVER w AS share5,
            total_share-LAG(total_share,20) OVER w AS share20,
            MIN(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amin,
            MAX(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amax,
            AVG(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amount20,
            AVG(amount) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS amount60,
            AVG(adj_close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
          FROM base WINDOW w AS(PARTITION BY ts_code ORDER BY trade_date)
        ), z AS (
          SELECT f.*,
            STDDEV_SAMP(r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20,
            STDDEV_SAMP(r1-idx_r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) * SQRT(252) AS te60,
            STDDEV_SAMP(r1-idx_r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) * SQRT(252) AS te120
          FROM f
        )
        INSERT INTO processed_mainline_etf_daily (
          factor_version,trade_date,as_of_trade_date,usable_from_trade_date,ts_code,index_code,list_date,benchmark_available,data_complete,is_tradable,is_eligible,exclusion_reason,exclusion_reasons,
          adj_close,ma60_gap,return_20d,return_60d,return_120d,volatility_20d,amount,avg_amount_20d,avg_amount_60d,amount_ratio_20_60,amount_pct_20d,
          total_share,total_size,share_change_5d,share_change_20d,net_inflow_5d,net_inflow_20d,tracking_error_60d,tracking_error_120d,
          premium_discount,premium_discount_abs_p95_60d,data_quality,source_watermark,source_asof
        )
        SELECT :factor_version,trade_date,trade_date,
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>z.trade_date),z.trade_date+1),
          ts_code,index_code,list_date,index_close IS NOT NULL,
          (adj_close IS NOT NULL AND amount20 IS NOT NULL AND total_share IS NOT NULL AND total_size IS NOT NULL AND nav IS NOT NULL),
          (COALESCE(amount,0)>0 AND COALESCE(list_date<=trade_date,FALSE)),
          (index_close IS NOT NULL AND adj_close IS NOT NULL AND amount20 IS NOT NULL AND total_share IS NOT NULL AND total_size IS NOT NULL AND nav IS NOT NULL AND COALESCE(amount,0)>0 AND COALESCE(list_date<=trade_date,FALSE)),
          CASE WHEN index_code IS NULL THEN 'missing_index_code' WHEN index_close IS NULL THEN 'missing_benchmark_daily'
               WHEN list_date IS NULL THEN 'missing_list_date' WHEN total_size IS NULL THEN 'missing_size' WHEN total_share IS NULL THEN 'missing_share' WHEN nav IS NULL THEN 'missing_nav' WHEN COALESCE(amount,0)<=0 THEN 'not_tradable' END,
          ARRAY_REMOVE(ARRAY[CASE WHEN index_code IS NULL THEN 'missing_index_code' END,CASE WHEN index_close IS NULL THEN 'missing_benchmark_daily' END,CASE WHEN list_date IS NULL THEN 'missing_list_date' END,CASE WHEN total_size IS NULL THEN 'missing_size' END,CASE WHEN total_share IS NULL THEN 'missing_share' END,CASE WHEN nav IS NULL THEN 'missing_nav' END,CASE WHEN COALESCE(amount,0)<=0 THEN 'not_tradable' END],NULL),
          adj_close,adj_close/NULLIF(ma60,0)-1,r20,r60,r120,vol20,amount,amount20,amount60,amount20/NULLIF(amount60,0),(amount-amin)/NULLIF(amax-amin,0),
          total_share,total_size,share5,share20,share5*nav,share20*nav,te60,te120,
          raw_close/NULLIF(nav,0)-1,NULL::double precision,
          jsonb_build_object('benchmark_available',index_close IS NOT NULL,'size_available',total_size IS NOT NULL),jsonb_build_object('fund_daily',trade_date,'share_size',trade_date),NOW()
        FROM z WHERE trade_date BETWEEN :start_date AND :end_date
        ON CONFLICT(factor_version,ts_code,trade_date) DO UPDATE SET
          index_code=EXCLUDED.index_code,benchmark_available=EXCLUDED.benchmark_available,
          list_date=EXCLUDED.list_date, data_complete=EXCLUDED.data_complete,is_tradable=EXCLUDED.is_tradable,is_eligible=EXCLUDED.is_eligible,exclusion_reason=EXCLUDED.exclusion_reason,exclusion_reasons=EXCLUDED.exclusion_reasons,
          adj_close=EXCLUDED.adj_close,return_20d=EXCLUDED.return_20d,
          ma60_gap=EXCLUDED.ma60_gap,avg_amount_20d=EXCLUDED.avg_amount_20d,avg_amount_60d=EXCLUDED.avg_amount_60d,amount_ratio_20_60=EXCLUDED.amount_ratio_20_60,
          return_60d=EXCLUDED.return_60d,return_120d=EXCLUDED.return_120d,
          volatility_20d=EXCLUDED.volatility_20d,amount=EXCLUDED.amount,
          amount_pct_20d=EXCLUDED.amount_pct_20d,total_share=EXCLUDED.total_share,
          total_size=EXCLUDED.total_size,share_change_5d=EXCLUDED.share_change_5d,
          share_change_20d=EXCLUDED.share_change_20d,
          net_inflow_5d=EXCLUDED.net_inflow_5d,net_inflow_20d=EXCLUDED.net_inflow_20d,
          tracking_error_60d=EXCLUDED.tracking_error_60d,tracking_error_120d=EXCLUDED.tracking_error_120d,
          premium_discount=EXCLUDED.premium_discount,
          premium_discount_abs_p95_60d=EXCLUDED.premium_discount_abs_p95_60d,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,
          source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        count = await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})
        await self._enrich_etf_premium_duckdb(start,end)
        return count

    async def _enrich_etf_premium_duckdb(self, start: date, end: date) -> int:
        """Compute the expensive rolling premium P95 in DuckDB and COPY it back."""
        source = await self._read_dataframe(
            """SELECT ts_code,trade_date,premium_discount
               FROM processed_mainline_etf_daily
               WHERE factor_version=:factor_version
                 AND trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '260 days') AND :end_date
               ORDER BY ts_code,trade_date""",
            {"factor_version":MAINLINE_FACTOR_VERSION,"start_date":start,"end_date":end},
        )
        if source.empty:
            return 0
        import duckdb
        connection = duckdb.connect(database=":memory:")
        try:
            connection.register("etf_source",source)
            calculated = connection.execute(
                """SELECT ts_code,trade_date,
                         quantile_cont(abs(premium_discount),0.95)
                           FILTER(WHERE premium_discount IS NOT NULL)
                           OVER(PARTITION BY ts_code ORDER BY trade_date
                                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS premium_p95
                   FROM etf_source
                   QUALIFY trade_date BETWEEN ? AND ?
                   ORDER BY ts_code,trade_date""",
                [start,end],
            ).fetch_df()
        finally:
            connection.close()
        if calculated.empty:
            return 0
        calculated["trade_date"] = pd.to_datetime(calculated["trade_date"]).dt.date
        calculated["premium_p95"] = pd.to_numeric(calculated["premium_p95"],errors="coerce")
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            await conn.execute(text("""CREATE TEMP TABLE mainline_etf_p95_stage(
              ts_code varchar(32),trade_date date,premium_p95 double precision
            ) ON COMMIT DROP"""))
            raw = await conn.get_raw_connection()
            driver = raw.driver_connection
            batch_size = 100_000
            for offset in range(0,len(calculated),batch_size):
                batch = calculated.iloc[offset:offset+batch_size]
                records = [
                    (row.ts_code,row.trade_date,None if pd.isna(row.premium_p95) else float(row.premium_p95))
                    for row in batch.itertuples(index=False)
                ]
                await driver.copy_records_to_table(
                    "mainline_etf_p95_stage",records=records,
                    columns=["ts_code","trade_date","premium_p95"],
                )
            result = await conn.execute(text("""UPDATE processed_mainline_etf_daily target SET
              premium_discount_abs_p95_60d=stage.premium_p95,processed_at=NOW()
              FROM mainline_etf_p95_stage stage
              WHERE target.factor_version=:factor_version
                AND target.ts_code=stage.ts_code AND target.trade_date=stage.trade_date"""),
              {"factor_version":MAINLINE_FACTOR_VERSION})
            return int(max(result.rowcount,0))

    async def _materialize_crowding(
        self,
        start: date,
        end: date,
        source_updated_since: Optional[str] = None,
    ) -> int:
        sql = """
        WITH open_days AS (
          SELECT cal_date::date AS trade_date,
                 LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
          FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), impacted_periods AS (
          SELECT DISTINCT end_date
          FROM fund_portfolio
          WHERE CAST(:source_updated_since AS date) IS NOT NULL
            AND updated_at >= CAST(:source_updated_since AS date)
            AND updated_at < (CAST(:end_date AS date) + INTERVAL '1 day')
        ), agg AS (
          SELECT end_date AS report_period,MAX(ann_date) AS available_date,symbol AS ts_code,
                 COUNT(DISTINCT ts_code) AS fund_count,SUM(mkv) * 10000 AS holding_value,
                 SUM(stk_float_ratio) AS holding_ratio
          FROM fund_portfolio
          WHERE end_date <= :end_date
            AND (
              (CAST(:source_updated_since AS date) IS NULL AND end_date >= :start_date)
              OR end_date IN (SELECT end_date FROM impacted_periods)
            )
          GROUP BY end_date,symbol
        ), ranked AS (
          SELECT agg.*,PERCENT_RANK() OVER(PARTITION BY report_period ORDER BY holding_value) AS pct
          FROM agg
        )
        INSERT INTO processed_mainline_fund_crowding_monthly (
          factor_version,report_period,as_of_trade_date,available_date,usable_from_trade_date,ts_code,fund_count,holding_value,holding_ratio,
          crowding_pct,data_quality,source_watermark,source_asof
        ) SELECT :factor_version,report_period,available_date,available_date,
          COALESCE(od.next_trade_date,ranked.available_date+1),
          ts_code,fund_count,holding_value,holding_ratio,pct,
          jsonb_build_object('disclosure_available_date',available_date),jsonb_build_object('fund_portfolio',available_date),NOW()
          FROM ranked LEFT JOIN open_days od ON od.trade_date=ranked.available_date
        ON CONFLICT(factor_version,ts_code,report_period) DO UPDATE SET
          available_date=EXCLUDED.available_date,fund_count=EXCLUDED.fund_count,
          holding_value=EXCLUDED.holding_value,holding_ratio=EXCLUDED.holding_ratio,
          crowding_pct=EXCLUDED.crowding_pct,data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,source_asof=EXCLUDED.source_asof,
          processed_at=NOW()
        """
        return await self._execute(sql, {
            "start_date": start,
            "end_date": end,
            "source_updated_since": source_updated_since,
            "factor_version": MAINLINE_FACTOR_VERSION,
        })

    async def _materialize_etf_exposure(self, start: date, end: date) -> int:
        """Map ETF benchmark constituent weights to published SW2021 L2 sectors.

        Index weights are the primary historical source.  ETF daily portfolio
        files are deliberately not substituted here because their availability
        history is shorter and should only be used as a later reconciliation.
        """
        sql = """
        WITH reviewed_mappings AS (
          SELECT ts_code,benchmark_index_code,usable_from_trade_date,usable_to_trade_date,
                 source_name,confidence
          FROM mainline_etf_benchmark_history
          WHERE mapping_status='mapped' AND review_status='approved'
            AND benchmark_index_code IS NOT NULL
        ), relevant_indices AS (
          SELECT DISTINCT benchmark_index_code AS index_code FROM reviewed_mappings
        ), open_days AS (
          SELECT cal_date::date AS trade_date,
                 LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
          FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), monthly_dates AS (
          SELECT iw.index_code,DATE_TRUNC('month',iw.trade_date)::date AS month_start,
                 MAX(iw.trade_date) AS weight_date
          FROM index_weight iw JOIN relevant_indices r USING(index_code)
          WHERE iw.trade_date BETWEEN :start_date AND :end_date
          GROUP BY iw.index_code,DATE_TRUNC('month',iw.trade_date)::date
        ), weights AS (
          SELECT iw.index_code,iw.trade_date AS weight_date,
                 COALESCE(od.next_trade_date,iw.trade_date+1) AS usable_from_trade_date,
                 iw.con_code,iw.weight/100.0 AS weight
          FROM monthly_dates md
          JOIN index_weight iw ON iw.index_code=md.index_code AND iw.trade_date=md.weight_date
          LEFT JOIN open_days od ON od.trade_date=iw.trade_date
        ), mapped AS (
          SELECT w.index_code,w.weight_date,w.usable_from_trade_date,m.l2_code,SUM(w.weight) AS weight
          FROM weights w
          JOIN processed_mainline_sw_member_pit m
            ON m.ts_code=w.con_code
           AND m.usable_from_trade_date<=w.weight_date
           AND (m.usable_to_trade_date IS NULL OR m.usable_to_trade_date>w.weight_date)
          GROUP BY w.index_code,w.weight_date,w.usable_from_trade_date,m.l2_code
        ), ranked AS (
          SELECT mapped.*, ROW_NUMBER() OVER(PARTITION BY index_code,weight_date ORDER BY weight DESC) AS rk,
                 SUM(POWER(weight,2)) OVER(PARTITION BY index_code,weight_date) AS hhi
          FROM mapped
        ), expanded AS (
          SELECT bm.ts_code,ranked.weight_date,ranked.usable_from_trade_date,
                 ranked.l2_code,ranked.weight,ranked.rk,ranked.hhi,
                 bm.source_name,bm.confidence
          FROM ranked JOIN reviewed_mappings bm
            ON bm.benchmark_index_code=ranked.index_code
           AND bm.usable_from_trade_date<=ranked.usable_from_trade_date
           AND (bm.usable_to_trade_date IS NULL OR bm.usable_to_trade_date>ranked.usable_from_trade_date)
        )
        INSERT INTO processed_mainline_etf_exposure_monthly(
          factor_version,as_of_trade_date,usable_from_trade_date,weight_date,ts_code,l2_code,weight,mapping_method,is_primary,top5_rank,exposure_hhi,data_quality,source_watermark,source_asof
        ) SELECT :factor_version,weight_date,usable_from_trade_date,
          weight_date,ts_code,l2_code,weight,'reviewed_index_weight',rk=1,CASE WHEN rk<=5 THEN rk END,hhi,
          jsonb_build_object('published_sw2021_l2',true,'benchmark_mapping_source',source_name,
            'benchmark_mapping_confidence',confidence),jsonb_build_object('index_weight',weight_date),NOW()
          FROM expanded
        ON CONFLICT(factor_version,ts_code,l2_code,as_of_trade_date) DO UPDATE SET
          weight=EXCLUDED.weight,mapping_method=EXCLUDED.mapping_method,is_primary=EXCLUDED.is_primary,top5_rank=EXCLUDED.top5_rank,exposure_hhi=EXCLUDED.exposure_hhi,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,processed_at=NOW()
        """
        # A reviewed benchmark can be corrected or revoked.  Remove the
        # affected v3 observations before rebuilding this window so a prior
        # mapping cannot survive in the exposure summary.
        await self._execute(
            """DELETE FROM processed_mainline_etf_exposure_monthly
               WHERE factor_version=:factor_version
                 AND as_of_trade_date BETWEEN :start_date AND :end_date""",
            {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION},
        )
        count = await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})
        summary_insert = """
        WITH grouped AS (
          SELECT factor_version,ts_code,as_of_trade_date,MAX(usable_from_trade_date) AS usable_from,
                 (ARRAY_AGG(l2_code ORDER BY weight DESC))[1] AS primary_l2_code,
                 (ARRAY_AGG(weight ORDER BY weight DESC))[1] AS primary_l2_weight,
                 MAX(exposure_hhi) AS exposure_hhi,
                 COALESCE(JSONB_AGG(JSONB_BUILD_OBJECT('l2_code',l2_code,'weight',weight)
                   ORDER BY weight DESC) FILTER(WHERE top5_rank IS NOT NULL),'[]'::jsonb) AS top5
          FROM processed_mainline_etf_exposure_monthly
          WHERE factor_version=:factor_version
            AND EXISTS (
              SELECT 1 FROM mainline_etf_benchmark_history bm
              WHERE bm.ts_code=processed_mainline_etf_exposure_monthly.ts_code
                AND bm.mapping_status='mapped' AND bm.review_status='approved'
                AND bm.usable_from_trade_date<=processed_mainline_etf_exposure_monthly.usable_from_trade_date
                AND (bm.usable_to_trade_date IS NULL OR bm.usable_to_trade_date>processed_mainline_etf_exposure_monthly.usable_from_trade_date)
            )
          GROUP BY factor_version,ts_code,as_of_trade_date
        ), intervals AS (
          SELECT grouped.*,
                 LEAD(usable_from) OVER(PARTITION BY ts_code ORDER BY usable_from) AS usable_to
          FROM grouped
        )
        INSERT INTO processed_mainline_etf_exposure_summary(
          factor_version,ts_code,as_of_trade_date,usable_from_trade_date,usable_to_trade_date,
          primary_l2_code,primary_l2_weight,top5_l2_exposure,exposure_hhi,source_watermark
        )
        SELECT factor_version,ts_code,as_of_trade_date,usable_from,usable_to,
               primary_l2_code,primary_l2_weight,top5,exposure_hhi,
               jsonb_build_object('index_weight',as_of_trade_date)
        FROM intervals
        """
        await self._execute(
            "DELETE FROM processed_mainline_etf_exposure_summary WHERE factor_version=:factor_version",
            {"factor_version": MAINLINE_FACTOR_VERSION},
        )
        await self._execute(summary_insert,{"factor_version":MAINLINE_FACTOR_VERSION})
        summary = """
        WITH exposure AS (
          SELECT d.ts_code,d.trade_date,s.primary_l2_code,s.primary_l2_weight,
                 s.exposure_hhi,s.top5_l2_exposure
          FROM processed_mainline_etf_daily d
          LEFT JOIN processed_mainline_etf_exposure_summary s
            ON s.factor_version=d.factor_version AND s.ts_code=d.ts_code
           AND s.usable_from_trade_date<=d.trade_date
           AND (s.usable_to_trade_date IS NULL OR s.usable_to_trade_date>d.trade_date)
          WHERE d.factor_version=:factor_version
            AND d.trade_date BETWEEN :start_date AND :end_date
        )
        UPDATE processed_mainline_etf_daily d SET
          primary_l2_code=exposure.primary_l2_code,
          primary_l2_weight=exposure.primary_l2_weight,
          exposure_hhi=exposure.exposure_hhi,
          top5_l2_exposure=COALESCE(exposure.top5_l2_exposure,'[]'::jsonb),
          is_eligible=(d.benchmark_mapping_status='mapped' AND d.benchmark_available AND d.data_complete AND d.is_tradable
                       AND exposure.primary_l2_code IS NOT NULL),
          exclusion_reason=CASE
            WHEN d.benchmark_mapping_status <> 'mapped' THEN d.benchmark_mapping_status
            WHEN exposure.primary_l2_code IS NULL THEN 'missing_pit_industry_exposure'
            ELSE d.exclusion_reason END,
          exclusion_reasons=CASE
            WHEN d.benchmark_mapping_status <> 'mapped'
              THEN ARRAY_APPEND(ARRAY_REMOVE(d.exclusion_reasons,'missing_pit_industry_exposure'),d.benchmark_mapping_status)
            WHEN exposure.primary_l2_code IS NULL
              THEN ARRAY_APPEND(ARRAY_REMOVE(d.exclusion_reasons,'missing_pit_industry_exposure'),'missing_pit_industry_exposure')
            ELSE ARRAY_REMOVE(ARRAY_REMOVE(d.exclusion_reasons,'missing_pit_industry_exposure'),'mapping_pending') END,
          data_quality=d.data_quality || JSONB_BUILD_OBJECT(
            'pit_exposure_available',exposure.primary_l2_code IS NOT NULL,
            'exposure_carried_forward',TRUE,
            'benchmark_mapping_status',d.benchmark_mapping_status
          ),
          processed_at=NOW()
        FROM exposure
        -- Keep the range on the target hypertable itself.  Equality to the
        -- CTE date alone does not let TimescaleDB prune historical compressed
        -- chunks, and a one-day repair can otherwise decompress ETF history.
        WHERE d.factor_version=:factor_version
          AND d.trade_date BETWEEN :start_date AND :end_date
          AND d.ts_code=exposure.ts_code
          AND d.trade_date=exposure.trade_date
        """
        await self._execute(
            summary,
            {
                "factor_version": MAINLINE_FACTOR_VERSION,
                "start_date": start,
                "end_date": end,
            },
        )
        return count

    async def _materialize_industry_crowding(
        self,
        start: date,
        end: date,
        source_updated_since: Optional[str] = None,
    ) -> int:
        sql = """
        WITH open_days AS (
          SELECT cal_date::date AS trade_date,
                 LEAD(cal_date::date) OVER(ORDER BY cal_date) AS next_trade_date
          FROM trade_cal WHERE exchange='SSE' AND is_open=1
        ), impacted_periods AS (
          SELECT DISTINCT end_date
          FROM fund_portfolio
          WHERE CAST(:source_updated_since AS date) IS NOT NULL
            AND updated_at >= CAST(:source_updated_since AS date)
            AND updated_at < (CAST(:end_date AS date) + INTERVAL '1 day')
        ), positions AS (
          SELECT fp.end_date AS report_period,MAX(fp.ann_date) OVER(PARTITION BY fp.end_date) AS available_date,
            fp.ts_code AS fund_code, sm.l2_code, fp.mkv * 10000 AS mkv
          FROM fund_portfolio fp
          JOIN processed_mainline_sw_member_pit sm
            ON sm.ts_code=fp.symbol
           AND sm.usable_from_trade_date<=fp.end_date
           AND (sm.usable_to_trade_date IS NULL OR sm.usable_to_trade_date>fp.end_date)
          WHERE fp.end_date <= :end_date
            AND (
              (CAST(:source_updated_since AS date) IS NULL AND fp.end_date >= :start_date)
              OR fp.end_date IN (SELECT end_date FROM impacted_periods)
            )
        ), agg AS (
          SELECT report_period,MAX(available_date) available_date,l2_code,COUNT(DISTINCT fund_code) fund_count,SUM(mkv) holding_value,
                 SUM(mkv)/NULLIF(SUM(SUM(mkv)) OVER(PARTITION BY report_period),0) concentration
          FROM positions GROUP BY report_period,l2_code
        ), x AS (
          SELECT agg.*,holding_value-LAG(holding_value) OVER(PARTITION BY l2_code ORDER BY report_period) holding_change,
                 fund_count::numeric/NULLIF(MAX(fund_count) OVER(PARTITION BY report_period),0) coverage FROM agg
        )
        INSERT INTO processed_mainline_industry_crowding_monthly(
          factor_version,report_period,as_of_trade_date,available_date,usable_from_trade_date,l2_code,holding_value,fund_count,holding_change,concentration,disclosure_coverage,data_quality,source_watermark,source_asof
        ) SELECT :factor_version,report_period,available_date,available_date,
          COALESCE(od.next_trade_date,x.available_date+1),
          l2_code,holding_value,fund_count,holding_change,concentration,coverage,
          jsonb_build_object('disclosure_available_date',available_date),jsonb_build_object('fund_portfolio',available_date),NOW()
          FROM x LEFT JOIN open_days od ON od.trade_date=x.available_date
        ON CONFLICT(factor_version,l2_code,report_period) DO UPDATE SET available_date=EXCLUDED.available_date,holding_value=EXCLUDED.holding_value,
          fund_count=EXCLUDED.fund_count,holding_change=EXCLUDED.holding_change,concentration=EXCLUDED.concentration,disclosure_coverage=EXCLUDED.disclosure_coverage,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,processed_at=NOW()
        """
        return await self._execute(sql, {
            "start_date": start,
            "end_date": end,
            "source_updated_since": source_updated_since,
            "factor_version": MAINLINE_FACTOR_VERSION,
        })

    async def _read_dataframe(self, sql: str, params: Dict[str, Any]) -> pd.DataFrame:
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            await conn.execute(text(f"SET LOCAL work_mem = '{MAINLINE_QUERY_WORK_MEM}'"))
            result = await conn.execute(text(sql), params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    async def _materialize_leadlag(self, start: date, end: date) -> Dict[str, int]:
        """Fit rolling 756-trading-day Lasso + post-Lasso industry signals."""
        closed_months = await self._read_dataframe(
            """SELECT MAX(cal_date)::date AS month_end
               FROM trade_cal
               WHERE exchange='SSE' AND is_open=1
                 AND cal_date>=DATE_TRUNC('month',CAST(:start_date AS date))
                 AND cal_date<DATE_TRUNC('month',CAST(:end_date AS date))+INTERVAL '1 month'
               GROUP BY DATE_TRUNC('month',cal_date)
               HAVING MAX(cal_date)::date<=CAST(:end_date AS date)
               ORDER BY month_end""",
            {"start_date":start,"end_date":end},
        )
        if closed_months.empty:
            return {"leadlag_monthly":0,"leadlag_score_monthly":0}
        valid_cutoffs = {pd.Timestamp(value).date() for value in closed_months["month_end"]}
        source = await self._read_dataframe(
            """SELECT trade_date,l2_code,relative_return_20d FROM processed_mainline_industry_daily
               WHERE factor_version=:factor_version AND trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '1200 days') AND :end_date
               ORDER BY trade_date,l2_code""",
            {"factor_version": MAINLINE_FACTOR_VERSION, "start_date": start, "end_date": end},
        )
        if source.empty:
            return {"leadlag_monthly": 0, "leadlag_score_monthly": 0}
        source["trade_date"] = pd.to_datetime(source["trade_date"])
        rows: List[Dict[str, Any]] = []
        scores: List[Dict[str, Any]] = []
        cutoffs = [
            frame["trade_date"].max()
            for _,frame in source.groupby(source["trade_date"].dt.to_period("M"))
            if start <= frame["trade_date"].max().date() <= end
            and frame["trade_date"].max().date() in valid_cutoffs
        ]
        if not cutoffs:
            return {"leadlag_monthly":0,"leadlag_score_monthly":0}

        def fit_month(cutoff: pd.Timestamp) -> tuple[pd.Timestamp,pd.DataFrame,pd.DataFrame]:
            history = source[source["trade_date"] <= cutoff].tail(756 * 124)
            relation, score = calculate_leadlag_lasso(history, cutoff)
            return cutoff,relation,score

        # sklearn's numerical kernels release the GIL. Threads share the source
        # frame and avoid serializing ~500k industry rows into spawned workers.
        with ThreadPoolExecutor(max_workers=min(4,max(1,len(cutoffs)))) as executor:
            fitted = executor.map(fit_month,cutoffs)
            for cutoff,relation,score in fitted:
                usable = cutoff.date() + timedelta(days=1)
                for record in relation.to_dict("records"):
                    record.update({"factor_version": MAINLINE_FACTOR_VERSION, "month_end": cutoff.date(), "as_of_trade_date": cutoff.date(), "usable_from_trade_date": usable})
                    rows.append(record)
                for record in score.to_dict("records"):
                    record.update({"factor_version": MAINLINE_FACTOR_VERSION, "month_end": cutoff.date(), "as_of_trade_date": cutoff.date(), "usable_from_trade_date": usable})
                    scores.append(record)
        if not rows and not scores:
            return {"leadlag_monthly": 0, "leadlag_score_monthly": 0}
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        relation_sql = text("""INSERT INTO processed_mainline_leadlag_monthly(
          factor_version,month_end,as_of_trade_date,usable_from_trade_date,leader_code,follower_code,best_lag_days,correlation,regression_coef,selected,sample_count,training_start_date,training_end_date,stability_first,stability_second,stability_third,source_asof)
          VALUES (:factor_version,:month_end,:as_of_trade_date,:usable_from_trade_date,:leader_code,:follower_code,:best_lag_days,:correlation,:regression_coef,:selected,:sample_count,:training_start_date,:training_end_date,:stability_first,:stability_second,:stability_third,NOW())
          ON CONFLICT(factor_version,month_end,leader_code,follower_code) DO UPDATE SET regression_coef=EXCLUDED.regression_coef,selected=EXCLUDED.selected,correlation=EXCLUDED.correlation,processed_at=NOW()""")
        score_sql = text("""INSERT INTO processed_mainline_leadlag_score_monthly(
          factor_version,month_end,as_of_trade_date,usable_from_trade_date,l2_code,leadlag_score,predicted_relative_return_20d,selected_feature_count,training_start_date,training_end_date,source_asof)
          VALUES (:factor_version,:month_end,:as_of_trade_date,:usable_from_trade_date,:l2_code,:leadlag_score,:predicted_relative_return_20d,:selected_feature_count,:training_start_date,:training_end_date,NOW())
          ON CONFLICT(factor_version,month_end,l2_code) DO UPDATE SET leadlag_score=EXCLUDED.leadlag_score,predicted_relative_return_20d=EXCLUDED.predicted_relative_return_20d,processed_at=NOW()""")
        async with engine.begin() as conn:
            if rows:
                await conn.execute(relation_sql, rows)
            if scores:
                await conn.execute(score_sql, scores)
        return {"leadlag_monthly": len(rows), "leadlag_score_monthly": len(scores)}

    async def _refresh_status(self, partition_date: date) -> int:
        """Compatibility wrapper used by incremental jobs and existing callers."""
        return await self._refresh_status_range(partition_date, partition_date)

    async def _refresh_status_range(self, start: date, end: date) -> int:
        """Build Gate 0 evidence for every completed market day in one SQL pass."""
        sql = """
        WITH completed_days AS (
          SELECT trade_date AS partition_date
          FROM processed_mainline_market_daily
          WHERE factor_version=:factor_version AND trade_date BETWEEN :start_date AND :end_date
        ), summaries AS (
          SELECT s.trade_date AS partition_date,'stock_daily'::varchar AS dataset,
            COUNT(*)::bigint AS row_count,
            COUNT(*) FILTER(WHERE s.is_stock_candidate_eligible)::bigint AS eligible_count,
            COUNT(*) FILTER(WHERE NOT s.is_stock_candidate_eligible)::bigint AS excluded_count,
            MAX(s.trade_date) AS max_source_date,
            COUNT(*) FILTER(WHERE s.l2_code IS NOT NULL)::numeric/NULLIF(COUNT(*),0) AS completeness,
            ARRAY_REMOVE(ARRAY[
              CASE WHEN COUNT(*)=0 THEN 'missing_stock_daily' END,
              CASE WHEN COUNT(*)>0 AND COUNT(*) FILTER(WHERE s.l2_code IS NOT NULL)::numeric/COUNT(*)<0.90
                   THEN 'stock_industry_mapping_below_90pct' END
            ],NULL)::text[] AS blockers,
            JSONB_BUILD_OBJECT(
              'industry_level','SW2021_L2','strict_point_in_time',TRUE,
              'moneyflow_coverage',COUNT(*) FILTER(WHERE s.moneyflow_available)::numeric/NULLIF(COUNT(*),0)
            ) AS details
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version AND s.trade_date BETWEEN :start_date AND :end_date
          GROUP BY s.trade_date
          UNION ALL
          SELECT m.trade_date,'market_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(m.trade_date),
            COUNT(*) FILTER(WHERE m.benchmark_close IS NOT NULL AND m.benchmark_ma200_gap IS NOT NULL
              AND m.breadth_above_ma60 IS NOT NULL AND m.breadth_above_ma120 IS NOT NULL
              AND m.breadth_denominator IS NOT NULL AND m.effective_stock_count IS NOT NULL)::numeric/NULLIF(COUNT(*),0),
            ARRAY_REMOVE(ARRAY[
              CASE WHEN COUNT(*)=0 THEN 'missing_market' END,
              CASE WHEN COUNT(*)>0 AND COUNT(*) FILTER(WHERE m.benchmark_close IS NOT NULL
                AND m.benchmark_ma200_gap IS NOT NULL AND m.breadth_above_ma60 IS NOT NULL
                AND m.breadth_above_ma120 IS NOT NULL AND m.breadth_denominator IS NOT NULL
                AND m.effective_stock_count IS NOT NULL)<>COUNT(*) THEN 'market_core_fields_incomplete' END
            ],NULL)::text[],
            JSONB_BUILD_OBJECT('benchmark',CAST(:benchmark AS text),'required_coverage',1.0)
          FROM processed_mainline_market_daily m
          WHERE m.factor_version=:factor_version AND m.trade_date BETWEEN :start_date AND :end_date
          GROUP BY m.trade_date
          UNION ALL
          SELECT i.trade_date,'industry_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(i.trade_date),
            COUNT(*) FILTER(WHERE i.index_close IS NOT NULL AND i.relative_return_20d IS NOT NULL
              AND i.relative_return_60d IS NOT NULL AND i.relative_return_120d IS NOT NULL
              AND i.breadth_above_ma60 IS NOT NULL)::numeric/124,
            ARRAY_REMOVE(ARRAY[
              CASE WHEN COUNT(*)<>124 THEN 'published_sw2021_l2_not_124' END,
              CASE WHEN COUNT(*) FILTER(WHERE i.index_close IS NOT NULL AND i.relative_return_20d IS NOT NULL
                AND i.relative_return_60d IS NOT NULL AND i.relative_return_120d IS NOT NULL
                AND i.breadth_above_ma60 IS NOT NULL)<>124 THEN 'industry_core_fields_incomplete' END,
              CASE WHEN COUNT(*) FILTER(WHERE i.revenue_acceleration IS NOT NULL)::numeric/NULLIF(COUNT(*),0)<0.90
                THEN 'industry_economic_coverage_below_90pct' END
            ],NULL)::text[],
            JSONB_BUILD_OBJECT(
              'industry_level','SW2021_L2','expected_count',124,
              'economic_coverage',COUNT(*) FILTER(WHERE i.revenue_acceleration IS NOT NULL)::numeric/NULLIF(COUNT(*),0),
              'moneyflow_optional',TRUE
            )
          FROM processed_mainline_industry_daily i
          WHERE i.factor_version=:factor_version AND i.trade_date BETWEEN :start_date AND :end_date
          GROUP BY i.trade_date
          UNION ALL
          SELECT e.trade_date,'etf_daily',COUNT(*)::bigint,
            COUNT(*) FILTER(WHERE e.data_complete AND e.is_tradable AND e.primary_l2_code IS NOT NULL
              AND e.primary_l2_weight>=0.50 AND e.avg_amount_20d>=30000000
              AND e.total_size>=500000000 AND e.premium_discount_abs_p95_60d<=0.02
              AND e.tracking_error_60d<=0.03)::bigint,
            COUNT(*) FILTER(WHERE NOT e.is_eligible)::bigint,MAX(e.trade_date),
            COUNT(*) FILTER(WHERE e.data_complete AND e.benchmark_available AND e.is_tradable)::numeric/
              NULLIF(COUNT(*) FILTER(WHERE e.benchmark_available AND e.is_tradable),0),
            ARRAY_REMOVE(ARRAY[
              CASE WHEN COUNT(*)=0 THEN 'missing_etf_daily' END,
              CASE WHEN COUNT(*) FILTER(WHERE e.data_complete AND e.benchmark_available AND e.is_tradable)::numeric/
                NULLIF(COUNT(*) FILTER(WHERE e.benchmark_available AND e.is_tradable),0)<0.95
                THEN 'etf_size_share_nav_coverage_below_95pct' END,
              -- Tool coverage is a portfolio-state signal in v4, not a data
              -- blocker.  A valid snapshot with zero tools must be published
              -- so the strategy can turn to cash instead of holding stale ETF.
              -- A debt, overseas or broad-market ETF can have full trading
              -- facts while intentionally lacking an SW L2 mapping.  It has
              -- an explicit exclusion reason and is not an ETF-mainline
              -- candidate.  Only an internally inconsistent candidate is a
              -- release blocker.
              CASE WHEN COUNT(*) FILTER(WHERE e.is_eligible AND e.primary_l2_code IS NULL)>0
                THEN 'selectable_etf_missing_pit_exposure' END
            ],NULL)::text[],
            JSONB_BUILD_OBJECT(
              'benchmark_proxy_allowed',FALSE,
              'size_share_nav_coverage',COUNT(*) FILTER(WHERE e.data_complete AND e.benchmark_available AND e.is_tradable)::numeric/
                NULLIF(COUNT(*) FILTER(WHERE e.benchmark_available AND e.is_tradable),0),
              'pit_exposure_coverage',COUNT(*) FILTER(WHERE e.primary_l2_code IS NOT NULL
                AND e.is_eligible)::numeric/NULLIF(COUNT(*) FILTER(WHERE e.is_eligible),0),
              'mapping_status_counts',JSONB_BUILD_OBJECT(
                'mapped',COUNT(*) FILTER(WHERE e.benchmark_mapping_status='mapped'),
                'mapping_pending',COUNT(*) FILTER(WHERE e.benchmark_mapping_status='mapping_pending'),
                'ambiguous_multisector',COUNT(*) FILTER(WHERE e.benchmark_mapping_status='ambiguous_multisector'),
                'not_applicable',COUNT(*) FILTER(WHERE e.benchmark_mapping_status='not_applicable')
              ),
              'unmapped_non_candidate_count',COUNT(*) FILTER(WHERE e.data_complete
                AND e.is_tradable AND e.benchmark_mapping_status='mapping_pending'),
              'executable_candidate_count',COUNT(*) FILTER(WHERE e.data_complete AND e.benchmark_available AND e.is_tradable
                AND e.primary_l2_code IS NOT NULL AND e.primary_l2_weight>=0.50
                AND e.avg_amount_20d>=30000000 AND e.total_size>=500000000
                AND e.premium_discount_abs_p95_60d<=0.02 AND e.tracking_error_60d<=0.03),
              'thresholds',JSONB_BUILD_OBJECT('amount_20d_cny',30000000,'aum_cny',500000000,
                'premium_abs_p95_60d',0.02,'tracking_error_60d',0.03,'primary_l2_weight',0.50)
            )
          FROM processed_mainline_etf_daily e
          WHERE e.factor_version=:factor_version AND e.trade_date BETWEEN :start_date AND :end_date
          GROUP BY e.trade_date
        ), complete_summaries AS (
          SELECT d.partition_date,k.dataset,
                 COALESCE(s.row_count,0) AS row_count,s.eligible_count,s.excluded_count,
                 s.max_source_date,COALESCE(s.completeness,0) AS completeness,
                 COALESCE(s.blockers,ARRAY['missing_'||k.dataset]::text[]) AS blockers,
                 COALESCE(s.details,'{}'::jsonb) AS details
          FROM completed_days d
          CROSS JOIN (VALUES ('stock_daily'),('market_daily'),('industry_daily'),('etf_daily')) k(dataset)
          LEFT JOIN summaries s ON s.partition_date=d.partition_date AND s.dataset=k.dataset
        )
        INSERT INTO processed_mainline_data_status(
          factor_version,dataset,partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,status,blocker_reasons,details,source_watermark,checked_at
        )
        SELECT :factor_version,dataset,partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,
          CASE WHEN max_source_date>=partition_date AND CARDINALITY(blockers)=0 THEN 'ready' ELSE 'blocked' END,
          blockers,details,JSONB_BUILD_OBJECT('as_of_trade_date',partition_date),NOW()
        FROM complete_summaries
        ON CONFLICT(factor_version,dataset,partition_date) DO UPDATE SET
          row_count=EXCLUDED.row_count,eligible_count=EXCLUDED.eligible_count,
          excluded_count=EXCLUDED.excluded_count,max_source_date=EXCLUDED.max_source_date,
          completeness=EXCLUDED.completeness,status=EXCLUDED.status,
          blocker_reasons=EXCLUDED.blocker_reasons,details=EXCLUDED.details,
          source_watermark=EXCLUDED.source_watermark,checked_at=NOW()
        """
        return await self._execute(sql, {
            "start_date": start,
            "end_date": end,
            "benchmark": self.BENCHMARK_CODE,
            "factor_version": MAINLINE_FACTOR_VERSION,
        })

    async def _publish_snapshot(self, requested_date: date) -> int:
        """Compatibility wrapper that publishes one observation date."""
        return await self._publish_snapshots(requested_date, requested_date)

    async def _publish_snapshots(self, start: date, end: date) -> int:
        """Publish immutable ready/blocked manifests for a historical range."""
        formula_hash = MAINLINE_FORMULA_HASH
        sql = """
        WITH days AS (
          SELECT partition_date,
                 COUNT(*) AS dataset_count,
                 BOOL_AND(status='ready') AS all_ready,
                 JSONB_OBJECT_AGG(dataset,completeness) AS coverage
          FROM processed_mainline_data_status
          WHERE factor_version=:factor_version AND partition_date BETWEEN :start_date AND :end_date
          GROUP BY partition_date
        ), manifest AS (
          SELECT d.*,
                 ARRAY(
                   SELECT DISTINCT blocker
                   FROM processed_mainline_data_status s
                   CROSS JOIN LATERAL UNNEST(s.blocker_reasons) blocker
                   WHERE s.factor_version=:factor_version AND s.partition_date=d.partition_date
                   ORDER BY blocker
                 )::text[] AS blockers
          FROM days d
        )
        INSERT INTO processed_mainline_snapshot_manifest(
          factor_version,snapshot_id,as_of_trade_date,usable_from_trade_date,status,
          formula_hash,input_watermark,coverage,blocker_reasons,published_at
        )
        SELECT :factor_version,
          MD5(CAST(:factor_version AS text)||':'||partition_date::text)::uuid,
          partition_date,
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc
                    WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>partition_date),partition_date+1),
          CASE WHEN dataset_count=4 AND all_ready AND CARDINALITY(blockers)=0 THEN 'ready' ELSE 'blocked' END,
          :formula_hash,JSONB_BUILD_OBJECT('market',partition_date),coverage,blockers,
          CASE WHEN dataset_count=4 AND all_ready AND CARDINALITY(blockers)=0 THEN NOW() END
        FROM manifest
        ON CONFLICT(factor_version,as_of_trade_date) DO UPDATE SET
          usable_from_trade_date=EXCLUDED.usable_from_trade_date,
          status=EXCLUDED.status,
          formula_hash=EXCLUDED.formula_hash,
          input_watermark=EXCLUDED.input_watermark,
          coverage=EXCLUDED.coverage,
          blocker_reasons=EXCLUDED.blocker_reasons,
          published_at=EXCLUDED.published_at
        WHERE processed_mainline_snapshot_manifest.status<>'ready'
        """
        return await self._execute(sql, {
            "factor_version": MAINLINE_FACTOR_VERSION,
            "start_date": start,
            "end_date": end,
            "formula_hash": formula_hash,
        })


def calculate_leadlag_monthly(
    leader_returns: pd.DataFrame,
    follower_returns: pd.DataFrame,
    max_lag_days: int = 20,
) -> pd.DataFrame:
    """计算月度领先/滞后关系；调用方明确传入候选资产，避免全市场笛卡尔积。"""
    required = {"trade_date", "code", "return"}
    if not required.issubset(leader_returns) or not required.issubset(follower_returns):
        raise ValueError(f"lead/follower data must contain {sorted(required)}")
    rows = []
    for leader, left in leader_returns.groupby("code"):
        left = left.set_index(pd.to_datetime(left["trade_date"]))["return"].sort_index()
        for follower, right in follower_returns.groupby("code"):
            right = right.set_index(pd.to_datetime(right["trade_date"]))[
                "return"
            ].sort_index()
            candidates = []
            for lag in range(max_lag_days + 1):
                aligned = pd.concat([left.shift(lag), right], axis=1).dropna()
                correlation = (
                    aligned.iloc[:, 0].corr(aligned.iloc[:, 1])
                    if len(aligned) >= 20
                    else None
                )
                if correlation is not None and pd.notna(correlation):
                    candidates.append(
                        (abs(correlation), correlation, lag, len(aligned))
                    )
            if candidates:
                _, correlation, lag, sample_count = max(candidates)
                rows.append(
                    {
                        "leader_code": leader,
                        "follower_code": follower,
                        "best_lag_days": lag,
                        "correlation": correlation,
                        "sample_count": sample_count,
                    }
                )
    return pd.DataFrame(rows)


def calculate_leadlag_lasso(
    industry_returns: pd.DataFrame,
    cutoff: pd.Timestamp,
    lookback_days: int = 756,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit cross-industry one-month-ahead signals with Lasso then OLS refit.

    The input contains only observations available at ``cutoff``.  Training
    pairs features at t with the target return at t+20.  The released score
    uses the cutoff feature vector and therefore predicts a future period,
    rather than accidentally re-predicting the already observed cutoff return.
    """
    required = {"trade_date", "l2_code", "relative_return_20d"}
    if not required.issubset(industry_returns.columns):
        raise ValueError(f"industry_returns must contain {sorted(required)}")
    from sklearn.linear_model import Lasso, LinearRegression

    # PostgreSQL NUMERIC values are returned by asyncpg/SQLAlchemy as Decimal.
    # Pandas keeps the pivot object-typed in that case, causing std() to mix a
    # float mean with Decimal observations.  The model has to run on a single
    # floating-point dtype anyway, so normalize at the numerical boundary.
    observations = industry_returns.copy()
    observations["relative_return_20d"] = pd.to_numeric(
        observations["relative_return_20d"], errors="coerce"
    ).astype("float64")
    pivot = observations.pivot_table(
        index="trade_date", columns="l2_code", values="relative_return_20d", aggfunc="last"
    ).astype("float64").sort_index().tail(lookback_days + 21)
    if len(pivot) < 252 or pivot.shape[1] < 2:
        return pd.DataFrame(), pd.DataFrame()
    feature_values = pivot.iloc[:-20]
    target_values = pivot.shift(-20).iloc[:-20]
    codes = list(pivot.columns)
    relation_rows: List[Dict[str, Any]] = []
    score_rows: List[Dict[str, Any]] = []
    training_start = pd.Timestamp(feature_values.index.min()).date()
    training_end = pd.Timestamp(feature_values.index.max()).date()
    for target in codes:
        frame = pd.concat([feature_values, target_values[target].rename("target")], axis=1).dropna()
        if len(frame) < 252:
            continue
        x = frame[codes].drop(columns=[target], errors="ignore")
        y = frame["target"]
        mean, std = x.mean(), x.std(ddof=0).replace(0, np.nan)
        xz = ((x - mean) / std).fillna(0.0)
        model = Lasso(alpha=0.001, max_iter=10000, random_state=0).fit(xz, y)
        selected = x.columns[np.abs(model.coef_) > 1e-12]
        coefficients = pd.Series(model.coef_, index=x.columns)
        if len(selected):
            post = LinearRegression().fit(x[selected], y)
            coefficients.loc[selected] = post.coef_
            latest_x = pivot.loc[pivot.index[-1], x.columns].fillna(x.mean())
            prediction = float(post.predict(latest_x[selected].to_frame().T)[0])
        else:
            prediction = float(y.mean())
        thirds = np.array_split(np.arange(len(frame)), 3)
        stability = [
            float(np.corrcoef(model.predict(xz.iloc[idx]), y.iloc[idx])[0, 1])
            if len(idx) > 2 and np.std(y.iloc[idx]) > 0
            else np.nan
            for idx in thirds
        ]
        for leader, coefficient in coefficients.items():
            if leader == target or not np.isfinite(coefficient):
                continue
            correlation = float(frame[leader].corr(y))
            relation_rows.append({
                "leader_code": leader, "follower_code": target, "best_lag_days": 20,
                "correlation": correlation, "regression_coef": float(coefficient), "selected": leader in selected,
                "sample_count": len(frame), "training_start_date": training_start, "training_end_date": training_end,
                "stability_first": stability[0], "stability_second": stability[1], "stability_third": stability[2],
            })
        score_rows.append({
            "l2_code": target, "leadlag_score": prediction, "predicted_relative_return_20d": prediction,
            "selected_feature_count": len(selected), "training_start_date": training_start, "training_end_date": training_end,
        })
    return pd.DataFrame(relation_rows), pd.DataFrame(score_rows)
