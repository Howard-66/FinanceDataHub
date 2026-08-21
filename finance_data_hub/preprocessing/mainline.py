"""量化主线策略事实层与因子宽表。

本模块只生产可复用的点时数据和因子，不计算策略分数、仓位或交易指令。
"""

from datetime import date, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional
from uuid import uuid4

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
MAINLINE_FACTOR_VERSION = 1


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

    @staticmethod
    def _dates(start_date: Optional[str], end_date: Optional[str]) -> tuple[date, date]:
        end = pd.Timestamp(end_date).date() if end_date else date.today()
        start = (
            pd.Timestamp(start_date).date() if start_date else end - timedelta(days=400)
        )
        if start > end:
            raise ValueError("start_date must not be later than end_date")
        return start, end

    async def run(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_monthly: bool = True,
        stages: Optional[List[str]] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        """Materialize only the requested layers.

        ``daily,crowding,leadlag,publish`` is intentionally explicit: daily
        facts may be recomputed often, while disclosure-driven crowding and the
        expensive monthly Lasso model are not safe to run as a side effect.
        """
        start, end = self._dates(start_date, end_date)
        explicit_stages = stages is not None
        requested = set(stages or (["daily", "crowding"] if include_monthly else ["daily"]))
        unknown = requested - {"daily", "crowding", "leadlag", "publish"}
        if unknown:
            raise ValueError(f"Unsupported mainline stage(s): {sorted(unknown)}")
        partitions = []
        partition_start = start
        while partition_start <= end:
            month_end = (
                pd.Timestamp(partition_start) + pd.offsets.MonthEnd(0)
            ).date()
            partition_end = min(month_end, end)
            partitions.append((partition_start, partition_end))
            partition_start = partition_end + timedelta(days=1)

        materializers: List[tuple[str, Callable[[date, date], Any]]] = []
        if "daily" in requested:
            materializers.extend([
                ("stock_daily", self._materialize_stock),
                ("market_daily", self._materialize_market),
                ("industry_daily", self._materialize_industry),
                ("etf_daily", self._materialize_etf),
            ])
            if explicit_stages:
                materializers.append(("etf_exposure_monthly", self._materialize_etf_exposure))
        if "crowding" in requested:
            materializers.append(("fund_crowding_monthly", self._materialize_crowding))
            if explicit_stages:
                materializers.append(("industry_crowding_monthly", self._materialize_industry_crowding))

        counts = {name: 0 for name, _ in materializers}
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
            await self._refresh_status(end)
            counts["snapshot_manifest"] = await self._publish_snapshot(end)
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
            result = await conn.execute(text(sql), params)
            return int(max(result.rowcount, 0))

    async def _materialize_stock(self, start: date, end: date) -> int:
        sql = """
        WITH prices AS (
            SELECT d.time::date AS trade_date, d.symbol AS ts_code,
                   d.close, d.amount,
                   d.close / NULLIF(LAG(d.close, 1) OVER w, 0) - 1 AS daily_return,
                   d.close / NULLIF(LAG(d.close, 20) OVER w, 0) - 1 AS return_20d,
                   d.close / NULLIF(LAG(d.close, 60) OVER w, 0) - 1 AS return_60d,
                   d.close / NULLIF(LAG(d.close, 120) OVER w, 0) - 1 AS return_120d,
                   MAX(d.close) OVER (w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS max_close_120d,
                   MIN(d.amount) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS min_amount_20d,
                   MAX(d.amount) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS max_amount_20d
            FROM processed_daily_qfq d
            WHERE d.time >= (CAST(:start_date AS date) - INTERVAL '260 days')
              AND d.time < (CAST(:end_date AS date) + INTERVAL '1 day')
            WINDOW w AS (PARTITION BY d.symbol ORDER BY d.time)
        ), features AS (
            SELECT p.*,
                   STDDEV_SAMP(daily_return) OVER (
                       PARTITION BY ts_code ORDER BY trade_date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ) AS volatility_20d
            FROM prices p
        ), basics AS (
            SELECT db.time::date AS trade_date,db.symbol,db.total_mv,db.circ_mv,
                   db.turnover_rate,db.pe_ttm,db.pb,db.dv_ttm,
                   MIN(db.turnover_rate) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS min_turnover_20d,
                   MAX(db.turnover_rate) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS max_turnover_20d
            FROM daily_basic db
            WHERE db.time >= (CAST(:start_date AS date)-INTERVAL '80 days')
              AND db.time < (CAST(:end_date AS date)+INTERVAL '1 day')
            WINDOW w AS(PARTITION BY db.symbol ORDER BY db.time)
        ), valuations AS (
            SELECT pv.time::date AS trade_date,pv.symbol,pv.pe_ttm,pv.pb,pv.dv_ttm,
                   pv.pe_ttm_pct_1250d,pv.pb_pct_1250d
            FROM processed_valuation_pct pv
            WHERE pv.time >= CAST(:start_date AS date)
              AND pv.time < (CAST(:end_date AS date)+INTERVAL '1 day')
        ), enriched AS (
            SELECT f.trade_date, f.ts_code, sw.l1_code, sw.l1_name,
                   sw.l2_code, sw.l2_name,
                   (ab.list_date IS NOT NULL AND ab.list_date <= f.trade_date
                    AND (ab.delist_date IS NULL OR ab.delist_date >= f.trade_date)) AS is_listed,
                   CASE WHEN f.trade_date >= DATE '2016-08-09'
                        THEN EXISTS (SELECT 1 FROM stock_st st
                                     WHERE st.ts_code=f.ts_code AND st.trade_date=f.trade_date)
                        ELSE COALESCE(nc.name ~* '(^|\\*)ST', FALSE) END AS is_st,
                   CASE WHEN f.trade_date >= DATE '2016-08-09'
                        THEN 'stock_st' ELSE 'namechange_reconstructed' END AS st_source,
                   EXISTS (SELECT 1 FROM stock_suspend ss
                           WHERE ss.ts_code=f.ts_code AND ss.trade_date=f.trade_date
                             AND UPPER(ss.suspend_type)='S') AS is_suspended,
                   f.trade_date - ab.list_date AS listing_days,
                   f.close, f.amount, db.total_mv, db.circ_mv, db.turnover_rate,
                   COALESCE(v.pe_ttm, db.pe_ttm) AS pe_ttm,
                   COALESCE(v.pb, db.pb) AS pb, COALESCE(v.dv_ttm, db.dv_ttm) AS dv_ttm,
                   COALESCE(qf.roe_5y_avg,fi.roe_dt) AS roe_ttm, qf.roa_ttm,
                   fi.roic,fi.grossprofit_margin,
                   fi.q_sales_yoy AS revenue_yoy,fi.q_profit_yoy AS profit_yoy,
                   COALESCE(qf.cfo_to_ni_ttm,fi.ocf_to_profit) AS ocf_to_profit,
                   COALESCE(qf.debt_ratio,fi.debt_to_assets) AS debt_to_assets,
                   COALESCE(qf.available_date,fi.available_date) AS financial_available_date,
                   f.return_20d, f.return_60d, f.return_120d, f.volatility_20d,
                   f.close / NULLIF(f.max_close_120d, 0) - 1 AS drawdown_120d,
                   (f.amount-f.min_amount_20d) /
                     NULLIF(f.max_amount_20d-f.min_amount_20d, 0) AS amount_pct_20d,
                   (db.turnover_rate-db.min_turnover_20d) /
                     NULLIF(db.max_turnover_20d-db.min_turnover_20d,0) AS turnover_pct_20d,
                   v.pe_ttm_pct_1250d AS pe_pct_5y, v.pb_pct_1250d AS pb_pct_5y,
                   md.rzye, md.rqye, md.rzmre,
                   mf.net_mf_amount AS moneyflow_net_amount,
                   mf.net_mf_amount * 10 / NULLIF(f.amount, 0) AS moneyflow_net_amount_ratio,
                   (mf.buy_lg_amount-mf.sell_lg_amount+mf.buy_elg_amount-mf.sell_elg_amount) AS moneyflow_large_net_amount,
                   (mf.buy_lg_amount-mf.sell_lg_amount+mf.buy_elg_amount-mf.sell_elg_amount) * 10 / NULLIF(f.amount, 0) AS moneyflow_large_net_ratio,
                   (mf.ts_code IS NOT NULL) AS moneyflow_available,
                   EXISTS (SELECT 1 FROM stock_dividend x
                           WHERE x.ts_code=f.ts_code AND x.ann_date BETWEEN f.trade_date-119 AND f.trade_date)
                       AS dividend_event_120d,
                   EXISTS (SELECT 1 FROM stock_repurchase x
                           WHERE x.ts_code=f.ts_code AND x.ann_date BETWEEN f.trade_date-119 AND f.trade_date)
                       AS repurchase_event_120d
            FROM features f
            JOIN asset_basic ab ON ab.symbol=f.ts_code
            LEFT JOIN basics db ON db.symbol=f.ts_code AND db.trade_date=f.trade_date
            LEFT JOIN valuations v
              ON v.symbol=f.ts_code AND v.trade_date=f.trade_date
            LEFT JOIN LATERAL (
                SELECT q.roe_5y_avg,q.roa_ttm,q.cfo_to_ni_ttm,q.debt_ratio,
                       COALESCE(q.f_ann_date_time,q.ann_date_time)::date AS available_date
                FROM processed_fundamental_quality q
                WHERE q.ts_code=f.ts_code
                  AND COALESCE(q.f_ann_date_time,q.ann_date_time) <= f.trade_date
                ORDER BY COALESCE(q.f_ann_date_time,q.ann_date_time) DESC,
                         q.end_date_time DESC
                LIMIT 1
            ) qf ON TRUE
            LEFT JOIN LATERAL (
                SELECT x.roe_dt,x.roic,x.grossprofit_margin,x.q_sales_yoy,x.q_profit_yoy,
                       x.ocf_to_profit,x.debt_to_assets,
                       COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD'))::date AS available_date
                FROM fina_indicator x
                WHERE x.ts_code=f.ts_code
                  AND COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD')) <= f.trade_date
                ORDER BY COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD')) DESC,
                         x.end_date_time DESC
                LIMIT 1
            ) fi ON TRUE
            LEFT JOIN margin_detail md ON md.ts_code=f.ts_code AND md.trade_date=f.trade_date
            LEFT JOIN moneyflow mf ON mf.ts_code=f.ts_code AND mf.trade_date=f.trade_date
            LEFT JOIN LATERAL (
                SELECT m.l1_code,m.l1_name,m.l2_code,m.l2_name
                FROM sw_industry_member m
                JOIN sw_industry_classify ic
                  ON ic.industry_code=m.l2_code AND ic.level='L2' AND ic.is_pub='1'
                WHERE m.ts_code=f.ts_code
                  AND COALESCE(m.in_date, DATE '1900-01-01') <= f.trade_date
                  AND (m.out_date IS NULL OR m.out_date >= f.trade_date)
                ORDER BY m.in_date DESC NULLS LAST LIMIT 1
            ) sw ON TRUE
            LEFT JOIN LATERAL (
                SELECT n.name FROM stock_namechange n
                WHERE n.ts_code=f.ts_code AND n.start_date <= f.trade_date
                  AND (n.end_date IS NULL OR n.end_date >= f.trade_date)
                ORDER BY n.start_date DESC LIMIT 1
            ) nc ON TRUE
            WHERE f.trade_date BETWEEN :start_date AND :end_date
        )
        INSERT INTO processed_mainline_stock_daily (
            factor_version,trade_date,as_of_trade_date,usable_from_trade_date,
            ts_code,l1_code,l1_name,l2_code,l2_name,is_listed,is_st,st_source,
            is_suspended,is_eligible,exclusion_reason,listing_days,close,amount,total_mv,circ_mv,
            turnover_rate,pe_ttm,pb,dv_ttm,roe_ttm,roa_ttm,roic,grossprofit_margin,
            revenue_yoy,profit_yoy,
            ocf_to_profit,debt_to_assets,financial_available_date,return_20d,return_60d,return_120d,volatility_20d,
            drawdown_120d,amount_pct_20d,turnover_pct_20d,pe_pct_5y,pb_pct_5y,
            rzye,rqye,rzmre,moneyflow_net_amount,moneyflow_net_amount_ratio,moneyflow_large_net_amount,moneyflow_large_net_ratio,moneyflow_available,dividend_event_120d,repurchase_event_120d,source_asof
        )
        SELECT :factor_version,trade_date,trade_date,
               COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc
                         WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>e.trade_date),
                        e.trade_date + 1),
               ts_code,l1_code,l1_name,l2_code,l2_name,
               is_listed,is_st,st_source,is_suspended,
               (is_listed AND NOT is_st AND NOT is_suspended AND listing_days >= 120
                AND l2_code IS NOT NULL AND close IS NOT NULL AND COALESCE(amount,0)>0),
               CASE WHEN NOT is_listed THEN 'not_listed' WHEN is_st THEN 'st'
                    WHEN is_suspended THEN 'suspended' WHEN listing_days < 120 THEN 'new_listing'
                    WHEN l2_code IS NULL THEN 'missing_sw2021_l2'
                    WHEN close IS NULL OR COALESCE(amount,0)<=0 THEN 'not_tradable' END,
               listing_days,close,amount,total_mv,circ_mv,turnover_rate,pe_ttm,pb,dv_ttm,
               roe_ttm,roa_ttm,roic,grossprofit_margin,revenue_yoy,profit_yoy,
               ocf_to_profit,debt_to_assets,financial_available_date,
               return_20d,return_60d,return_120d,volatility_20d,drawdown_120d,
               amount_pct_20d,turnover_pct_20d,pe_pct_5y,pb_pct_5y,rzye,rqye,rzmre,moneyflow_net_amount,moneyflow_net_amount_ratio,moneyflow_large_net_amount,moneyflow_large_net_ratio,moneyflow_available,
               dividend_event_120d,repurchase_event_120d,NOW()
        FROM enriched e
        ON CONFLICT (factor_version,ts_code,trade_date) DO UPDATE SET
            l1_code=EXCLUDED.l1_code,l1_name=EXCLUDED.l1_name,l2_code=EXCLUDED.l2_code,
            l2_name=EXCLUDED.l2_name,is_listed=EXCLUDED.is_listed,is_st=EXCLUDED.is_st,
            st_source=EXCLUDED.st_source,is_suspended=EXCLUDED.is_suspended,
            is_eligible=EXCLUDED.is_eligible,exclusion_reason=EXCLUDED.exclusion_reason,
            listing_days=EXCLUDED.listing_days,close=EXCLUDED.close,amount=EXCLUDED.amount,
            total_mv=EXCLUDED.total_mv,circ_mv=EXCLUDED.circ_mv,turnover_rate=EXCLUDED.turnover_rate,
            pe_ttm=EXCLUDED.pe_ttm,pb=EXCLUDED.pb,dv_ttm=EXCLUDED.dv_ttm,
            roe_ttm=EXCLUDED.roe_ttm,roa_ttm=EXCLUDED.roa_ttm,
            roic=EXCLUDED.roic,grossprofit_margin=EXCLUDED.grossprofit_margin,
            revenue_yoy=EXCLUDED.revenue_yoy,profit_yoy=EXCLUDED.profit_yoy,
            ocf_to_profit=EXCLUDED.ocf_to_profit,debt_to_assets=EXCLUDED.debt_to_assets,financial_available_date=EXCLUDED.financial_available_date,
            return_20d=EXCLUDED.return_20d,return_60d=EXCLUDED.return_60d,
            return_120d=EXCLUDED.return_120d,volatility_20d=EXCLUDED.volatility_20d,
            drawdown_120d=EXCLUDED.drawdown_120d,amount_pct_20d=EXCLUDED.amount_pct_20d,
            turnover_pct_20d=EXCLUDED.turnover_pct_20d,
            pe_pct_5y=EXCLUDED.pe_pct_5y,pb_pct_5y=EXCLUDED.pb_pct_5y,
            rzye=EXCLUDED.rzye,rqye=EXCLUDED.rqye,rzmre=EXCLUDED.rzmre,
            moneyflow_net_amount=EXCLUDED.moneyflow_net_amount,moneyflow_net_amount_ratio=EXCLUDED.moneyflow_net_amount_ratio,
            moneyflow_large_net_amount=EXCLUDED.moneyflow_large_net_amount,moneyflow_large_net_ratio=EXCLUDED.moneyflow_large_net_ratio,
            moneyflow_available=EXCLUDED.moneyflow_available,
            dividend_event_120d=EXCLUDED.dividend_event_120d,
            repurchase_event_120d=EXCLUDED.repurchase_event_120d,
            source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        count = await self._execute(
            sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION}
        )
        await self._enrich_stock_features(start, end)
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
                 PERCENT_RANK() OVER(PARTITION BY s.ts_code ORDER BY s.turnover_rate
                    ROWS BETWEEN 503 PRECEDING AND CURRENT ROW) AS turnover_rank,
                 LAG(s.revenue_yoy) OVER w AS revenue_prev, LAG(s.profit_yoy) OVER w AS profit_prev
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version
            AND s.trade_date BETWEEN (CAST(:start_date AS date) - INTERVAL '420 days') AND :end_date
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
          turnover_pct_2y=source.turnover_rank, revenue_yoy_prev=source.revenue_prev, profit_yoy_prev=source.profit_prev,
          moneyflow_net_amount_5d=source.moneyflow5,moneyflow_net_amount_20d=source.moneyflow20,
          revenue_acceleration=target.revenue_yoy-source.revenue_prev, profit_acceleration=target.profit_yoy-source.profit_prev,
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
          data_quality=jsonb_build_object('st_source',target.st_source,'tail_proxy','rolling_min_return_if_p05_unavailable'),
          source_watermark=jsonb_build_object('prices_as_of',target.trade_date,'financial_available_date',target.financial_available_date,'moneyflow',CASE WHEN target.moneyflow_available THEN target.trade_date ELSE NULL END), processed_at=NOW()
        FROM source
        WHERE target.factor_version=source.factor_version AND target.ts_code=source.ts_code AND target.trade_date=source.trade_date
          AND target.trade_date BETWEEN :start_date AND :end_date
        """
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _materialize_market(self, start: date, end: date) -> int:
        sql = """
        WITH benchmark AS (
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
            SELECT d.time::date AS trade_date,d.symbol,d.close,
                   LAG(d.close) OVER w AS previous_close,
                   AVG(d.close) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                   AVG(d.close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
                   ,AVG(d.close) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120
            FROM processed_daily_qfq d
            WHERE d.time >= (CAST(:start_date AS date)-INTERVAL '120 days')
              AND d.time < (CAST(:end_date AS date)+INTERVAL '1 day')
            WINDOW w AS(PARTITION BY d.symbol ORDER BY d.time)
        ), breadth AS (
            SELECT x.trade_date,
                   AVG((x.close>x.ma20)::int) AS above20,
                   AVG((x.close>x.ma60)::int) AS above60,
                   AVG((x.close>x.ma120)::int) AS above120,
                   COUNT(*) AS denominator,
                   COUNT(*) FILTER(WHERE x.close>x.ma60) AS above60_count,
                   COUNT(*) FILTER(WHERE x.close>x.ma120) AS above120_count,
                   SUM((x.close>x.previous_close)::int)::numeric /
                     NULLIF(SUM((x.close<x.previous_close)::int),0) AS ad_ratio
            FROM stock_ma x
            JOIN processed_mainline_stock_daily s
              ON s.ts_code=x.symbol AND s.trade_date=x.trade_date
             AND s.factor_version=:factor_version AND s.is_market_breadth_eligible
            GROUP BY x.trade_date
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
               COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>b.trade_date), b.trade_date+1),
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
        WITH industry_index AS (
          SELECT d.trade_date::date AS trade_date,d.ts_code AS l2_code,d.close,
                 d.close/NULLIF(LAG(d.close,20) OVER w,0)-1 AS r20,
                 d.close/NULLIF(LAG(d.close,60) OVER w,0)-1 AS r60,
                 d.close/NULLIF(LAG(d.close,120) OVER w,0)-1 AS r120,
                 AVG(d.close) OVER(w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
                 AVG(d.close) OVER(w ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120
          FROM sw_daily d WHERE d.trade_date BETWEEN (CAST(:start_date AS date) - INTERVAL '260 days') AND :end_date
          WINDOW w AS (PARTITION BY d.ts_code ORDER BY d.trade_date)
        ), base AS (
          SELECT s.*,
                 s.close/NULLIF(LAG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date),0)-1 AS r1,
                 s.rzye-LAG(s.rzye,20) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date) AS rz20,
                 AVG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date
                                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                 AVG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date
                                   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
          FROM processed_mainline_stock_daily s
          WHERE s.factor_version=:factor_version
            AND s.trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '150 days') AND :end_date
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
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>b.trade_date), b.trade_date+1),
          MAX(b.l1_code),MAX(b.l1_name),b.l2_code,MAX(b.l2_name),MAX(ii.l2_code),MAX(ii.close),MAX(ii.r20),MAX(ii.r60),MAX(ii.r120),MAX(ii.close/NULLIF(ii.ma60,0)-1),MAX(ii.close/NULLIF(ii.ma120,0)-1),COUNT(*),
          AVG(b.r1),SUM(b.r1*b.circ_mv)/NULLIF(SUM(b.circ_mv),0),
          AVG(b.return_20d)-MAX(m.benchmark_return_20d),
          AVG(b.return_60d)-MAX(m.benchmark_return_60d),
          AVG(b.return_120d)-MAX(m.benchmark_return_120d),
          AVG((b.close>b.ma20)::int),AVG((b.close>b.ma60)::int),AVG((b.close>b.ma60)::int),
          COUNT(*) FILTER(WHERE b.return_20d>0 AND b.close>b.ma60),AVG((b.return_20d>0 AND b.close>b.ma60)::int),STDDEV_SAMP(b.r1),
          SUM(b.amount)/NULLIF(SUM(SUM(b.amount)) OVER(PARTITION BY b.trade_date),0),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pb),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roic),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.grossprofit_margin),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.revenue_yoy),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.profit_yoy),SUM(b.rz20),
          SUM(b.moneyflow_net_amount),SUM(b.moneyflow_net_amount)*10/NULLIF(SUM(b.amount),0),
          SUM(b.moneyflow_large_net_amount),SUM(b.moneyflow_large_net_amount)*10/NULLIF(SUM(b.amount),0),
          SUM(b.moneyflow_net_amount_5d),SUM(b.moneyflow_net_amount_20d),
          COUNT(*) FILTER(WHERE b.moneyflow_net_amount>0)::numeric/NULLIF(COUNT(*) FILTER(WHERE b.moneyflow_available),0),
          MAX(GREATEST(COALESCE(b.moneyflow_net_amount,0),0))/NULLIF(SUM(GREATEST(COALESCE(b.moneyflow_net_amount,0),0)),0),
          COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0),
          COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0)>=0.95,
          jsonb_build_object('published_sw2021_l2',true,'industry_index_available',MAX(ii.close) IS NOT NULL,'moneyflow_coverage',COUNT(*) FILTER(WHERE b.moneyflow_available)::numeric/NULLIF(COUNT(*),0)),
          jsonb_build_object('sw_daily',MAX(ii.trade_date),'moneyflow',CASE WHEN COUNT(*) FILTER(WHERE b.moneyflow_available)>0 THEN b.trade_date ELSE NULL END),NOW()
        FROM base b
        JOIN sw_industry_classify ic ON ic.industry_code=b.l2_code AND ic.level='L2' AND ic.is_pub='1'
        LEFT JOIN industry_index ii ON ii.l2_code=b.l2_code AND ii.trade_date=b.trade_date
        LEFT JOIN processed_mainline_market_daily m ON m.trade_date=b.trade_date AND m.factor_version=:factor_version
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
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _materialize_etf(self, start: date, end: date) -> int:
        sql = """
        WITH base AS (
          SELECT d.trade_date,d.ts_code,b.index_code,b.list_date,d.close AS raw_close,
                 d.close*COALESCE(a.adj_factor,1) AS adj_close,
                 d.amount,s.total_share,s.total_size,s.nav,
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
            STDDEV_SAMP(r1-idx_r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS te60,
            STDDEV_SAMP(r1-idx_r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS te120
          FROM f
        )
        INSERT INTO processed_mainline_etf_daily (
          factor_version,trade_date,as_of_trade_date,usable_from_trade_date,ts_code,index_code,list_date,benchmark_available,data_complete,is_tradable,is_eligible,exclusion_reason,exclusion_reasons,
          adj_close,ma60_gap,return_20d,return_60d,return_120d,volatility_20d,amount,avg_amount_20d,avg_amount_60d,amount_ratio_20_60,amount_pct_20d,
          total_share,total_size,share_change_5d,share_change_20d,net_inflow_5d,net_inflow_20d,tracking_error_60d,tracking_error_120d,
          premium_discount,data_quality,source_watermark,source_asof
        )
        SELECT :factor_version,trade_date,trade_date,
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>z.trade_date),z.trade_date+1),
          ts_code,index_code,list_date,index_close IS NOT NULL,
          (adj_close IS NOT NULL AND amount20 IS NOT NULL AND total_share IS NOT NULL AND total_size IS NOT NULL AND nav IS NOT NULL),
          (COALESCE(amount,0)>0 AND list_date<=trade_date),
          (index_close IS NOT NULL AND adj_close IS NOT NULL AND amount20 IS NOT NULL AND total_share IS NOT NULL AND total_size IS NOT NULL AND nav IS NOT NULL AND COALESCE(amount,0)>0 AND list_date<=trade_date),
          CASE WHEN index_code IS NULL THEN 'missing_index_code' WHEN index_close IS NULL THEN 'missing_benchmark_daily'
               WHEN total_size IS NULL THEN 'missing_size' WHEN total_share IS NULL THEN 'missing_share' WHEN nav IS NULL THEN 'missing_nav' WHEN COALESCE(amount,0)<=0 THEN 'not_tradable' END,
          ARRAY_REMOVE(ARRAY[CASE WHEN index_code IS NULL THEN 'missing_index_code' END,CASE WHEN index_close IS NULL THEN 'missing_benchmark_daily' END,CASE WHEN total_size IS NULL THEN 'missing_size' END,CASE WHEN total_share IS NULL THEN 'missing_share' END,CASE WHEN nav IS NULL THEN 'missing_nav' END,CASE WHEN COALESCE(amount,0)<=0 THEN 'not_tradable' END],NULL),
          adj_close,adj_close/NULLIF(ma60,0)-1,r20,r60,r120,vol20,amount,amount20,amount60,amount20/NULLIF(amount60,0),(amount-amin)/NULLIF(amax-amin,0),
          total_share,total_size,share5,share20,share5*nav,share20*nav,te60,te120,raw_close/NULLIF(nav,0)-1,
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
          premium_discount=EXCLUDED.premium_discount,data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,
          source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _materialize_crowding(self, start: date, end: date) -> int:
        sql = """
        WITH agg AS (
          SELECT end_date AS report_period,MAX(ann_date) AS available_date,symbol AS ts_code,
                 COUNT(DISTINCT ts_code) AS fund_count,SUM(mkv) AS holding_value,
                 SUM(stk_float_ratio) AS holding_ratio
          FROM fund_portfolio WHERE end_date BETWEEN :start_date AND :end_date
          GROUP BY end_date,symbol
        ), ranked AS (
          SELECT agg.*,PERCENT_RANK() OVER(PARTITION BY report_period ORDER BY holding_value) AS pct
          FROM agg
        )
        INSERT INTO processed_mainline_fund_crowding_monthly (
          factor_version,report_period,as_of_trade_date,available_date,usable_from_trade_date,ts_code,fund_count,holding_value,holding_ratio,
          crowding_pct,data_quality,source_watermark,source_asof
        ) SELECT :factor_version,report_period,available_date,available_date,
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>ranked.available_date),ranked.available_date+1),
          ts_code,fund_count,holding_value,holding_ratio,pct,
          jsonb_build_object('disclosure_available_date',available_date),jsonb_build_object('fund_portfolio',available_date),NOW()
          FROM ranked
        ON CONFLICT(factor_version,ts_code,report_period) DO UPDATE SET
          available_date=EXCLUDED.available_date,fund_count=EXCLUDED.fund_count,
          holding_value=EXCLUDED.holding_value,holding_ratio=EXCLUDED.holding_ratio,
          crowding_pct=EXCLUDED.crowding_pct,data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,source_asof=EXCLUDED.source_asof,
          processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _materialize_etf_exposure(self, start: date, end: date) -> int:
        """Map ETF benchmark constituent weights to published SW2021 L2 sectors.

        Index weights are the primary historical source.  ETF daily portfolio
        files are deliberately not substituted here because their availability
        history is shorter and should only be used as a later reconciliation.
        """
        sql = """
        WITH weights AS (
          SELECT eb.ts_code, iw.trade_date AS weight_date, iw.con_code, iw.weight
          FROM etf_basic eb JOIN index_weight iw ON iw.index_code=eb.index_code
          WHERE iw.trade_date BETWEEN :start_date AND :end_date
        ), mapped AS (
          SELECT w.ts_code,w.weight_date,m.l2_code,SUM(w.weight) AS weight
          FROM weights w JOIN LATERAL (
             SELECT sm.l2_code FROM sw_industry_member sm
             JOIN sw_industry_classify ic ON ic.industry_code=sm.l2_code AND ic.level='L2' AND ic.is_pub='1'
             WHERE sm.ts_code=w.con_code AND COALESCE(sm.in_date,DATE '1900-01-01')<=w.weight_date
               AND (sm.out_date IS NULL OR sm.out_date>=w.weight_date)
             ORDER BY sm.in_date DESC NULLS LAST LIMIT 1
          ) m ON TRUE GROUP BY w.ts_code,w.weight_date,m.l2_code
        ), ranked AS (
          SELECT mapped.*, ROW_NUMBER() OVER(PARTITION BY ts_code,weight_date ORDER BY weight DESC) AS rk,
                 SUM(POWER(weight,2)) OVER(PARTITION BY ts_code,weight_date) AS hhi
          FROM mapped
        )
        INSERT INTO processed_mainline_etf_exposure_monthly(
          factor_version,as_of_trade_date,usable_from_trade_date,weight_date,ts_code,l2_code,weight,mapping_method,is_primary,top5_rank,exposure_hhi,data_quality,source_watermark,source_asof
        ) SELECT :factor_version,weight_date,
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>ranked.weight_date),ranked.weight_date+1),
          weight_date,ts_code,l2_code,weight,'index_weight',rk=1,CASE WHEN rk<=5 THEN rk END,hhi,
          jsonb_build_object('published_sw2021_l2',true),jsonb_build_object('index_weight',weight_date),NOW()
          FROM ranked
        ON CONFLICT(factor_version,ts_code,l2_code,as_of_trade_date) DO UPDATE SET
          weight=EXCLUDED.weight,mapping_method=EXCLUDED.mapping_method,is_primary=EXCLUDED.is_primary,top5_rank=EXCLUDED.top5_rank,exposure_hhi=EXCLUDED.exposure_hhi,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,processed_at=NOW()
        """
        count = await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})
        summary = """
        WITH latest AS (
          SELECT DISTINCT ON (e.ts_code,e.as_of_trade_date) e.ts_code,e.as_of_trade_date,e.l2_code,e.weight,e.exposure_hhi
          FROM processed_mainline_etf_exposure_monthly e WHERE e.factor_version=:factor_version
          ORDER BY e.ts_code,e.as_of_trade_date,e.weight DESC
        ) UPDATE processed_mainline_etf_daily d SET primary_l2_code=latest.l2_code,primary_l2_weight=latest.weight,
          exposure_hhi=latest.exposure_hhi,processed_at=NOW()
          FROM latest WHERE d.factor_version=:factor_version AND d.ts_code=latest.ts_code AND d.trade_date=latest.as_of_trade_date
        """
        await self._execute(summary, {"factor_version": MAINLINE_FACTOR_VERSION})
        return count

    async def _materialize_industry_crowding(self, start: date, end: date) -> int:
        sql = """
        WITH positions AS (
          SELECT fp.end_date AS report_period,MAX(fp.ann_date) OVER(PARTITION BY fp.end_date) AS available_date,
            fp.ts_code AS fund_code, sm.l2_code, fp.mkv
          FROM fund_portfolio fp JOIN LATERAL (
            SELECT m.l2_code FROM sw_industry_member m
            JOIN sw_industry_classify ic ON ic.industry_code=m.l2_code AND ic.level='L2' AND ic.is_pub='1'
            WHERE m.ts_code=fp.symbol AND COALESCE(m.in_date,DATE '1900-01-01')<=fp.end_date
              AND (m.out_date IS NULL OR m.out_date>=fp.end_date)
            ORDER BY m.in_date DESC NULLS LAST LIMIT 1
          ) sm ON TRUE WHERE fp.end_date BETWEEN :start_date AND :end_date
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
          COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>x.available_date),x.available_date+1),
          l2_code,holding_value,fund_count,holding_change,concentration,coverage,
          jsonb_build_object('disclosure_available_date',available_date),jsonb_build_object('fund_portfolio',available_date),NOW() FROM x
        ON CONFLICT(factor_version,l2_code,report_period) DO UPDATE SET available_date=EXCLUDED.available_date,holding_value=EXCLUDED.holding_value,
          fund_count=EXCLUDED.fund_count,holding_change=EXCLUDED.holding_change,concentration=EXCLUDED.concentration,disclosure_coverage=EXCLUDED.disclosure_coverage,
          data_quality=EXCLUDED.data_quality,source_watermark=EXCLUDED.source_watermark,processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end, "factor_version": MAINLINE_FACTOR_VERSION})

    async def _read_dataframe(self, sql: str, params: Dict[str, Any]) -> pd.DataFrame:
        if self.db_manager._engine is None:
            await self.db_manager.initialize()
        engine = self.db_manager._engine
        assert engine is not None
        async with engine.begin() as conn:
            result = await conn.execute(text(sql), params)
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    async def _materialize_leadlag(self, start: date, end: date) -> Dict[str, int]:
        """Fit rolling 756-trading-day Lasso + post-Lasso industry signals."""
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
        for month_end, frame in source.groupby(source["trade_date"].dt.to_period("M")):
            cutoff = frame["trade_date"].max()
            history = source[source["trade_date"] <= cutoff].tail(756 * 124)
            relation, score = calculate_leadlag_lasso(history, cutoff)
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
        sql = """
        WITH completed_day AS (
          SELECT MAX(trade_date) AS partition_date FROM processed_mainline_market_daily
          WHERE factor_version=:factor_version AND trade_date<=:requested_date
        ), summaries AS (
          SELECT 'stock_daily'::varchar AS dataset,COUNT(*)::bigint AS row_count,
            COUNT(*) FILTER(WHERE is_stock_candidate_eligible)::bigint AS eligible_count,
            COUNT(*) FILTER(WHERE NOT is_stock_candidate_eligible)::bigint AS excluded_count,
            MAX(trade_date) AS max_source_date,
            COUNT(*) FILTER(WHERE l2_code IS NOT NULL)::numeric/NULLIF(COUNT(*),0) AS completeness,
            ARRAY[]::text[] AS blockers, jsonb_build_object('industry_level','SW2021_L2','strict_point_in_time',true) AS details
          FROM processed_mainline_stock_daily,completed_day WHERE factor_version=:factor_version AND trade_date=completed_day.partition_date
          UNION ALL
          SELECT 'market_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(trade_date),
            CASE WHEN COUNT(*)>0 THEN 1::numeric ELSE 0::numeric END,
            CASE WHEN COUNT(*)=0 THEN ARRAY['missing_market']::text[] ELSE ARRAY[]::text[] END,jsonb_build_object('benchmark',CAST(:benchmark AS text))
          FROM processed_mainline_market_daily,completed_day WHERE factor_version=:factor_version AND trade_date=completed_day.partition_date
          UNION ALL
          SELECT 'industry_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(trade_date),
            COUNT(*)::numeric/124,
            CASE WHEN COUNT(*)=124 THEN ARRAY[]::text[] ELSE ARRAY['published_sw2021_l2_not_124']::text[] END,jsonb_build_object('industry_level','SW2021_L2','expected_count',124)
          FROM processed_mainline_industry_daily,completed_day WHERE factor_version=:factor_version AND trade_date=completed_day.partition_date
          UNION ALL
          SELECT 'etf_daily',COUNT(*)::bigint,
            COUNT(*) FILTER(WHERE is_eligible)::bigint,
            COUNT(*) FILTER(WHERE NOT is_eligible)::bigint,MAX(trade_date),
            COUNT(*) FILTER(WHERE benchmark_available)::numeric/NULLIF(COUNT(*),0),
            CASE WHEN COUNT(*)=0 THEN ARRAY['missing_etf_daily']::text[] ELSE ARRAY[]::text[] END,jsonb_build_object('benchmark_proxy_allowed',false)
          FROM processed_mainline_etf_daily,completed_day WHERE factor_version=:factor_version AND trade_date=completed_day.partition_date
        )
        INSERT INTO processed_mainline_data_status(
          factor_version,dataset,partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,status,blocker_reasons,details,source_watermark,checked_at
        )
        SELECT :factor_version,dataset,completed_day.partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,
          CASE WHEN max_source_date>=completed_day.partition_date AND cardinality(blockers)=0 THEN 'ready' ELSE 'blocked' END,
          blockers,details,jsonb_build_object('as_of_trade_date',completed_day.partition_date),NOW() FROM summaries CROSS JOIN completed_day
        WHERE completed_day.partition_date IS NOT NULL
        ON CONFLICT(factor_version,dataset,partition_date) DO UPDATE SET
          row_count=EXCLUDED.row_count,eligible_count=EXCLUDED.eligible_count,
          excluded_count=EXCLUDED.excluded_count,max_source_date=EXCLUDED.max_source_date,completeness=EXCLUDED.completeness,status=EXCLUDED.status,
          blocker_reasons=EXCLUDED.blocker_reasons,details=EXCLUDED.details,source_watermark=EXCLUDED.source_watermark,checked_at=NOW()
        """
        return await self._execute(
            sql, {"requested_date": partition_date, "benchmark": self.BENCHMARK_CODE, "factor_version": MAINLINE_FACTOR_VERSION}
        )

    async def _publish_snapshot(self, requested_date: date) -> int:
        """Publish only complete, auditable factors for the latest trading day."""
        statuses = await self._read_dataframe(
            """SELECT partition_date,status,blocker_reasons,details FROM processed_mainline_data_status
                WHERE factor_version=:factor_version AND partition_date<=:requested_date ORDER BY partition_date DESC""",
            {"factor_version": MAINLINE_FACTOR_VERSION, "requested_date": requested_date},
        )
        if statuses.empty:
            return 0
        as_of = pd.Timestamp(statuses.iloc[0]["partition_date"]).date()
        latest = statuses[statuses["partition_date"] == statuses.iloc[0]["partition_date"]]
        blockers = [item for value in latest["blocker_reasons"] for item in (value or [])]
        is_ready = bool((latest["status"] == "ready").all()) and not blockers
        formula_hash = sha256(b"mainline-pit-v1:daily,crowding,leadlag,publish").hexdigest()
        statement = """
          INSERT INTO processed_mainline_snapshot_manifest(
            factor_version,snapshot_id,as_of_trade_date,usable_from_trade_date,status,formula_hash,input_watermark,coverage,blocker_reasons,published_at
          ) VALUES (:factor_version,:snapshot_id,:as_of_trade_date,
            COALESCE((SELECT MIN(tc.cal_date::date) FROM trade_cal tc WHERE tc.exchange='SSE' AND tc.is_open=1 AND tc.cal_date::date>:as_of_trade_date),:as_of_trade_date+1),
            :status,:formula_hash,:watermark,:coverage,:blockers,CASE WHEN :status='ready' THEN NOW() END)
          ON CONFLICT(factor_version,as_of_trade_date) DO NOTHING
        """
        return await self._execute(statement, {
            "factor_version": MAINLINE_FACTOR_VERSION, "snapshot_id": str(uuid4()), "as_of_trade_date": as_of,
            "status": "ready" if is_ready else "blocked", "formula_hash": formula_hash,
            "watermark": {"market": as_of.isoformat()}, "coverage": {str(row.dataset): float(row.completeness or 0) for row in latest.itertuples()},
            "blockers": sorted(set(blockers)),
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

    The input contains only observations available at ``cutoff``.  Each target
    uses t-20 industry relative returns as features and t relative return as
    the target, so no future return is included in the released score.
    """
    required = {"trade_date", "l2_code", "relative_return_20d"}
    if not required.issubset(industry_returns.columns):
        raise ValueError(f"industry_returns must contain {sorted(required)}")
    from sklearn.linear_model import Lasso, LinearRegression

    pivot = industry_returns.pivot_table(
        index="trade_date", columns="l2_code", values="relative_return_20d", aggfunc="last"
    ).sort_index().tail(lookback_days + 21)
    if len(pivot) < 252 or pivot.shape[1] < 2:
        return pd.DataFrame(), pd.DataFrame()
    feature_values = pivot.shift(20).iloc[20:]
    target_values = pivot.iloc[20:]
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
            latest_x = x.iloc[-1]
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
