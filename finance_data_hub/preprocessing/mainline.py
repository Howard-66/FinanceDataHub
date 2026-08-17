"""量化主线策略事实层与因子宽表。

本模块只生产可复用的点时数据和因子，不计算策略分数、仓位或交易指令。
"""

from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

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
    "leadlag_monthly": (
        "processed_mainline_leadlag_monthly",
        "leader_code",
        "month_end",
    ),
    "data_status": ("processed_mainline_data_status", "dataset", "partition_date"),
}

# 主线策略的统一历史回测起点。无 --force 时仍保留最近 400 天的
# 默认增量窗口；CLI 的全量模式会显式传入该日期。
MAINLINE_HISTORY_START = "2012-01-01"


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
    ) -> pd.DataFrame:
        config = MAINLINE_TABLES.get(dataset)
        if config is None:
            raise ValueError(f"Unsupported mainline dataset: {dataset}")
        table_name, code_column, date_column = config
        clauses = ["1=1"]
        params: Dict[str, Any] = {}
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
                    "month_end": pd.Timestamp(month_end).date(),
                    "leader_type": leader_type,
                    "follower_type": follower_type,
                }
            )
        statement = text(
            """
            INSERT INTO processed_mainline_leadlag_monthly (
              month_end,leader_type,leader_code,follower_type,follower_code,
              best_lag_days,correlation,sample_count,source_asof
            ) VALUES (
              :month_end,:leader_type,:leader_code,:follower_type,:follower_code,
              :best_lag_days,:correlation,:sample_count,NOW()
            )
            ON CONFLICT(month_end,leader_type,leader_code,follower_type,follower_code)
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
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, int]:
        start, end = self._dates(start_date, end_date)
        partitions = []
        partition_start = start
        while partition_start <= end:
            month_end = (
                pd.Timestamp(partition_start) + pd.offsets.MonthEnd(0)
            ).date()
            partition_end = min(month_end, end)
            partitions.append((partition_start, partition_end))
            partition_start = partition_end + timedelta(days=1)

        stages = [
            ("stock_daily", self._materialize_stock),
            ("market_daily", self._materialize_market),
            ("industry_daily", self._materialize_industry),
            ("etf_daily", self._materialize_etf),
        ]
        if include_monthly:
            stages.append(("fund_crowding_monthly", self._materialize_crowding))

        counts = {name: 0 for name, _ in stages}
        total_steps = len(partitions) * len(stages) + 1
        completed = 0
        for partition_start, partition_end in partitions:
            partition_label = (
                f"{partition_start.isoformat()} ~ {partition_end.isoformat()}"
            )
            for dataset, materialize in stages:
                description = f"{dataset}  {partition_label}"
                if progress_callback:
                    progress_callback(completed, total_steps, description)
                counts[dataset] += await materialize(partition_start, partition_end)
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_steps, description)

        if progress_callback:
            progress_callback(completed, total_steps, "data_status")
        await self._refresh_status(end)
        completed += 1
        if progress_callback:
            progress_callback(completed, total_steps, "data_status")
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
                   CASE WHEN f.trade_date >= DATE '2016-01-01'
                        THEN EXISTS (SELECT 1 FROM stock_st st
                                     WHERE st.ts_code=f.ts_code AND st.trade_date=f.trade_date)
                        ELSE COALESCE(nc.name ~* '(^|\\*)ST', FALSE) END AS is_st,
                   CASE WHEN f.trade_date >= DATE '2016-01-01'
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
                   f.return_20d, f.return_60d, f.return_120d, f.volatility_20d,
                   f.close / NULLIF(f.max_close_120d, 0) - 1 AS drawdown_120d,
                   (f.amount-f.min_amount_20d) /
                     NULLIF(f.max_amount_20d-f.min_amount_20d, 0) AS amount_pct_20d,
                   (db.turnover_rate-db.min_turnover_20d) /
                     NULLIF(db.max_turnover_20d-db.min_turnover_20d,0) AS turnover_pct_20d,
                   v.pe_ttm_pct_1250d AS pe_pct_5y, v.pb_pct_1250d AS pb_pct_5y,
                   md.rzye, md.rqye, md.rzmre,
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
                SELECT q.roe_5y_avg,q.roa_ttm,q.cfo_to_ni_ttm,q.debt_ratio
                FROM processed_fundamental_quality q
                WHERE q.ts_code=f.ts_code
                  AND COALESCE(q.f_ann_date_time,q.ann_date_time) <= f.trade_date
                ORDER BY COALESCE(q.f_ann_date_time,q.ann_date_time) DESC,
                         q.end_date_time DESC
                LIMIT 1
            ) qf ON TRUE
            LEFT JOIN LATERAL (
                SELECT x.roe_dt,x.roic,x.grossprofit_margin,x.q_sales_yoy,x.q_profit_yoy,
                       x.ocf_to_profit,x.debt_to_assets
                FROM fina_indicator x
                WHERE x.ts_code=f.ts_code
                  AND COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD')) <= f.trade_date
                ORDER BY COALESCE(x.ann_date_time,TO_TIMESTAMP(x.ann_date,'YYYYMMDD')) DESC,
                         x.end_date_time DESC
                LIMIT 1
            ) fi ON TRUE
            LEFT JOIN margin_detail md ON md.ts_code=f.ts_code AND md.trade_date=f.trade_date
            LEFT JOIN LATERAL (
                SELECT m.l1_code,m.l1_name,m.l2_code,m.l2_name
                FROM sw_industry_member m
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
            trade_date,ts_code,l1_code,l1_name,l2_code,l2_name,is_listed,is_st,st_source,
            is_suspended,is_eligible,exclusion_reason,listing_days,close,amount,total_mv,circ_mv,
            turnover_rate,pe_ttm,pb,dv_ttm,roe_ttm,roa_ttm,roic,grossprofit_margin,
            revenue_yoy,profit_yoy,
            ocf_to_profit,debt_to_assets,return_20d,return_60d,return_120d,volatility_20d,
            drawdown_120d,amount_pct_20d,turnover_pct_20d,pe_pct_5y,pb_pct_5y,
            rzye,rqye,rzmre,dividend_event_120d,repurchase_event_120d,source_asof
        )
        SELECT trade_date,ts_code,l1_code,l1_name,l2_code,l2_name,
               is_listed,is_st,st_source,is_suspended,
               (is_listed AND NOT is_st AND NOT is_suspended AND listing_days >= 120
                AND l2_code IS NOT NULL AND close IS NOT NULL AND COALESCE(amount,0)>0),
               CASE WHEN NOT is_listed THEN 'not_listed' WHEN is_st THEN 'st'
                    WHEN is_suspended THEN 'suspended' WHEN listing_days < 120 THEN 'new_listing'
                    WHEN l2_code IS NULL THEN 'missing_sw2021_l2'
                    WHEN close IS NULL OR COALESCE(amount,0)<=0 THEN 'not_tradable' END,
               listing_days,close,amount,total_mv,circ_mv,turnover_rate,pe_ttm,pb,dv_ttm,
               roe_ttm,roa_ttm,roic,grossprofit_margin,revenue_yoy,profit_yoy,
               ocf_to_profit,debt_to_assets,
               return_20d,return_60d,return_120d,volatility_20d,drawdown_120d,
               amount_pct_20d,turnover_pct_20d,pe_pct_5y,pb_pct_5y,rzye,rqye,rzmre,
               dividend_event_120d,repurchase_event_120d,NOW()
        FROM enriched e
        ON CONFLICT (ts_code,trade_date) DO UPDATE SET
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
            ocf_to_profit=EXCLUDED.ocf_to_profit,debt_to_assets=EXCLUDED.debt_to_assets,
            return_20d=EXCLUDED.return_20d,return_60d=EXCLUDED.return_60d,
            return_120d=EXCLUDED.return_120d,volatility_20d=EXCLUDED.volatility_20d,
            drawdown_120d=EXCLUDED.drawdown_120d,amount_pct_20d=EXCLUDED.amount_pct_20d,
            turnover_pct_20d=EXCLUDED.turnover_pct_20d,
            pe_pct_5y=EXCLUDED.pe_pct_5y,pb_pct_5y=EXCLUDED.pb_pct_5y,
            rzye=EXCLUDED.rzye,rqye=EXCLUDED.rqye,rzmre=EXCLUDED.rzmre,
            dividend_event_120d=EXCLUDED.dividend_event_120d,
            repurchase_event_120d=EXCLUDED.repurchase_event_120d,
            source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end})

    async def _materialize_market(self, start: date, end: date) -> int:
        sql = """
        WITH benchmark AS (
            SELECT trade_date::date AS trade_date, close,
                   close/NULLIF(LAG(close,20) OVER w,0)-1 AS r20,
                   close/NULLIF(LAG(close,60) OVER w,0)-1 AS r60,
                   close/NULLIF(LAG(close,120) OVER w,0)-1 AS r120,
                   AVG(close) OVER (w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                   AVG(close) OVER (w ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60,
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
            FROM processed_daily_qfq d
            WHERE d.time >= (CAST(:start_date AS date)-INTERVAL '120 days')
              AND d.time < (CAST(:end_date AS date)+INTERVAL '1 day')
            WINDOW w AS(PARTITION BY d.symbol ORDER BY d.time)
        ), breadth AS (
            SELECT x.trade_date,
                   AVG((x.close>x.ma20)::int) AS above20,
                   AVG((x.close>x.ma60)::int) AS above60,
                   SUM((x.close>x.previous_close)::int)::numeric /
                     NULLIF(SUM((x.close<x.previous_close)::int),0) AS ad_ratio
            FROM stock_ma x
            JOIN processed_mainline_stock_daily s
              ON s.ts_code=x.symbol AND s.trade_date=x.trade_date AND s.is_eligible
            GROUP BY x.trade_date
        )
        INSERT INTO processed_mainline_market_daily (
            trade_date,benchmark_code,benchmark_close,benchmark_return_20d,
            benchmark_return_60d,benchmark_return_120d,benchmark_ma20_gap,
            benchmark_ma60_gap,benchmark_volatility_20d,breadth_above_ma20,
            breadth_above_ma60,advance_decline_ratio,north_money,north_money_20d,
            market_regime,source_asof
        )
        SELECT b.trade_date,:benchmark,b.close,b.r20,b.r60,b.r120,
               b.close/NULLIF(b.ma20,0)-1,b.close/NULLIF(b.ma60,0)-1,b.vol20,
               x.above20,x.above60,x.ad_ratio,m.north_money,
               SUM(m.north_money) OVER (ORDER BY b.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
               CASE WHEN b.close>b.ma60 AND x.above60>=0.5 THEN 'risk_on'
                    WHEN b.close<b.ma60 AND x.above60<0.5 THEN 'risk_off' ELSE 'neutral' END,
               NOW()
        FROM benchmark b LEFT JOIN breadth x USING(trade_date)
        LEFT JOIN moneyflow_hsgt m USING(trade_date)
        WHERE b.trade_date BETWEEN :start_date AND :end_date
        ON CONFLICT (trade_date) DO UPDATE SET
            benchmark_close=EXCLUDED.benchmark_close,
            benchmark_return_20d=EXCLUDED.benchmark_return_20d,
            benchmark_return_60d=EXCLUDED.benchmark_return_60d,
            benchmark_return_120d=EXCLUDED.benchmark_return_120d,
            benchmark_ma20_gap=EXCLUDED.benchmark_ma20_gap,
            benchmark_ma60_gap=EXCLUDED.benchmark_ma60_gap,
            benchmark_volatility_20d=EXCLUDED.benchmark_volatility_20d,
            breadth_above_ma20=EXCLUDED.breadth_above_ma20,
            breadth_above_ma60=EXCLUDED.breadth_above_ma60,
            advance_decline_ratio=EXCLUDED.advance_decline_ratio,
            north_money=EXCLUDED.north_money,north_money_20d=EXCLUDED.north_money_20d,
            market_regime=EXCLUDED.market_regime,source_asof=EXCLUDED.source_asof,
            processed_at=NOW()
        """
        return await self._execute(
            sql,
            {"start_date": start, "end_date": end, "benchmark": self.BENCHMARK_CODE},
        )

    async def _materialize_industry(self, start: date, end: date) -> int:
        sql = """
        WITH base AS (
          SELECT s.*,
                 s.close/NULLIF(LAG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date),0)-1 AS r1,
                 s.rzye-LAG(s.rzye,20) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date) AS rz20,
                 AVG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date
                                   ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                 AVG(s.close) OVER(PARTITION BY s.ts_code ORDER BY s.trade_date
                                   ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
          FROM processed_mainline_stock_daily s
          WHERE s.trade_date BETWEEN (CAST(:start_date AS date)-INTERVAL '150 days') AND :end_date
        )
        INSERT INTO processed_mainline_industry_daily (
          trade_date,l1_code,l1_name,l2_code,l2_name,stock_count,equal_weight_return,
          cap_weight_return,relative_return_20d,relative_return_60d,breadth_above_ma20,
          breadth_above_ma60,return_dispersion,amount_share,median_pe_ttm,median_pb,
          median_roe_ttm,median_roic,median_grossprofit_margin,median_revenue_yoy,
          median_profit_yoy,margin_balance_change_20d,source_asof
        )
        SELECT b.trade_date,MAX(b.l1_code),MAX(b.l1_name),b.l2_code,MAX(b.l2_name),COUNT(*),
          AVG(b.r1),SUM(b.r1*b.circ_mv)/NULLIF(SUM(b.circ_mv),0),
          AVG(b.return_20d)-MAX(m.benchmark_return_20d),
          AVG(b.return_60d)-MAX(m.benchmark_return_60d),
          AVG((b.close>b.ma20)::int),AVG((b.close>b.ma60)::int),STDDEV_SAMP(b.r1),
          SUM(b.amount)/NULLIF(SUM(SUM(b.amount)) OVER(PARTITION BY b.trade_date),0),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.pb),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roe_ttm),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.roic),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.grossprofit_margin),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.revenue_yoy),
          PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY b.profit_yoy),SUM(b.rz20),NOW()
        FROM base b LEFT JOIN processed_mainline_market_daily m USING(trade_date)
        WHERE b.is_eligible AND b.l2_code IS NOT NULL AND b.trade_date BETWEEN :start_date AND :end_date
        GROUP BY b.trade_date,b.l2_code
        ON CONFLICT (l2_code,trade_date) DO UPDATE SET
          l1_code=EXCLUDED.l1_code,l1_name=EXCLUDED.l1_name,l2_name=EXCLUDED.l2_name,
          stock_count=EXCLUDED.stock_count,equal_weight_return=EXCLUDED.equal_weight_return,
          cap_weight_return=EXCLUDED.cap_weight_return,relative_return_20d=EXCLUDED.relative_return_20d,
          relative_return_60d=EXCLUDED.relative_return_60d,
          breadth_above_ma20=EXCLUDED.breadth_above_ma20,
          breadth_above_ma60=EXCLUDED.breadth_above_ma60,
          return_dispersion=EXCLUDED.return_dispersion,amount_share=EXCLUDED.amount_share,
          median_pe_ttm=EXCLUDED.median_pe_ttm,median_pb=EXCLUDED.median_pb,
          median_roe_ttm=EXCLUDED.median_roe_ttm,
          median_roic=EXCLUDED.median_roic,
          median_grossprofit_margin=EXCLUDED.median_grossprofit_margin,
          median_revenue_yoy=EXCLUDED.median_revenue_yoy,
          median_profit_yoy=EXCLUDED.median_profit_yoy,
          margin_balance_change_20d=EXCLUDED.margin_balance_change_20d,
          source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end})

    async def _materialize_etf(self, start: date, end: date) -> int:
        sql = """
        WITH base AS (
          SELECT d.trade_date,d.ts_code,b.index_code,d.close AS raw_close,
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
            MAX(amount) OVER(w ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS amax
          FROM base WINDOW w AS(PARTITION BY ts_code ORDER BY trade_date)
        ), z AS (
          SELECT f.*,
            STDDEV_SAMP(r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vol20,
            STDDEV_SAMP(r1-idx_r1) OVER(PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS te60
          FROM f
        )
        INSERT INTO processed_mainline_etf_daily (
          trade_date,ts_code,index_code,benchmark_available,is_eligible,exclusion_reason,
          adj_close,return_20d,return_60d,return_120d,volatility_20d,amount,amount_pct_20d,
          total_share,total_size,share_change_5d,share_change_20d,tracking_error_60d,
          premium_proxy,source_asof
        )
        SELECT trade_date,ts_code,index_code,index_close IS NOT NULL,index_close IS NOT NULL,
          CASE WHEN index_code IS NULL THEN 'missing_index_code'
               WHEN index_close IS NULL THEN 'missing_benchmark_daily' END,
          adj_close,r20,r60,r120,vol20,amount,(amount-amin)/NULLIF(amax-amin,0),
          total_share,total_size,share5,share20,te60,raw_close/NULLIF(nav,0)-1,NOW()
        FROM z WHERE trade_date BETWEEN :start_date AND :end_date
        ON CONFLICT(ts_code,trade_date) DO UPDATE SET
          index_code=EXCLUDED.index_code,benchmark_available=EXCLUDED.benchmark_available,
          is_eligible=EXCLUDED.is_eligible,exclusion_reason=EXCLUDED.exclusion_reason,
          adj_close=EXCLUDED.adj_close,return_20d=EXCLUDED.return_20d,
          return_60d=EXCLUDED.return_60d,return_120d=EXCLUDED.return_120d,
          volatility_20d=EXCLUDED.volatility_20d,amount=EXCLUDED.amount,
          amount_pct_20d=EXCLUDED.amount_pct_20d,total_share=EXCLUDED.total_share,
          total_size=EXCLUDED.total_size,share_change_5d=EXCLUDED.share_change_5d,
          share_change_20d=EXCLUDED.share_change_20d,
          tracking_error_60d=EXCLUDED.tracking_error_60d,
          premium_proxy=EXCLUDED.premium_proxy,
          source_asof=EXCLUDED.source_asof,processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end})

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
          report_period,available_date,ts_code,fund_count,holding_value,holding_ratio,
          crowding_pct,source_asof
        ) SELECT report_period,available_date,ts_code,fund_count,holding_value,holding_ratio,pct,NOW()
          FROM ranked
        ON CONFLICT(report_period,ts_code) DO UPDATE SET
          available_date=EXCLUDED.available_date,fund_count=EXCLUDED.fund_count,
          holding_value=EXCLUDED.holding_value,holding_ratio=EXCLUDED.holding_ratio,
          crowding_pct=EXCLUDED.crowding_pct,source_asof=EXCLUDED.source_asof,
          processed_at=NOW()
        """
        return await self._execute(sql, {"start_date": start, "end_date": end})

    async def _refresh_status(self, partition_date: date) -> int:
        sql = """
        WITH summaries AS (
          SELECT 'stock_daily'::varchar AS dataset,COUNT(*)::bigint AS row_count,
            COUNT(*) FILTER(WHERE is_eligible)::bigint AS eligible_count,
            COUNT(*) FILTER(WHERE NOT is_eligible)::bigint AS excluded_count,
            MAX(trade_date) AS max_source_date,
            COUNT(*) FILTER(WHERE l2_code IS NOT NULL)::numeric/NULLIF(COUNT(*),0) AS completeness,
            jsonb_build_object('industry_level','SW2021_L2','strict_point_in_time',true) AS details
          FROM processed_mainline_stock_daily WHERE trade_date=:partition_date
          UNION ALL
          SELECT 'market_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(trade_date),
            CASE WHEN COUNT(*)>0 THEN 1::numeric ELSE 0::numeric END,
            jsonb_build_object('benchmark',CAST(:benchmark AS text))
          FROM processed_mainline_market_daily WHERE trade_date=:partition_date
          UNION ALL
          SELECT 'industry_daily',COUNT(*)::bigint,NULL::bigint,NULL::bigint,MAX(trade_date),
            CASE WHEN COUNT(*)>0 THEN 1::numeric ELSE 0::numeric END,
            jsonb_build_object('industry_level','SW2021_L2')
          FROM processed_mainline_industry_daily WHERE trade_date=:partition_date
          UNION ALL
          SELECT 'etf_daily',COUNT(*)::bigint,
            COUNT(*) FILTER(WHERE is_eligible)::bigint,
            COUNT(*) FILTER(WHERE NOT is_eligible)::bigint,MAX(trade_date),
            COUNT(*) FILTER(WHERE benchmark_available)::numeric/NULLIF(COUNT(*),0),
            jsonb_build_object('benchmark_proxy_allowed',false)
          FROM processed_mainline_etf_daily WHERE trade_date=:partition_date
        )
        INSERT INTO processed_mainline_data_status(
          dataset,partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,status,details,checked_at
        )
        SELECT dataset,:partition_date,row_count,eligible_count,excluded_count,
          max_source_date,completeness,
          CASE WHEN max_source_date>=:partition_date THEN 'ready' ELSE 'stale' END,
          details,NOW() FROM summaries
        ON CONFLICT(dataset,partition_date) DO UPDATE SET
          row_count=EXCLUDED.row_count,eligible_count=EXCLUDED.eligible_count,
          excluded_count=EXCLUDED.excluded_count,max_source_date=EXCLUDED.max_source_date,
          completeness=EXCLUDED.completeness,status=EXCLUDED.status,
          details=EXCLUDED.details,checked_at=NOW()
        """
        return await self._execute(
            sql, {"partition_date": partition_date, "benchmark": self.BENCHMARK_CODE}
        )


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
