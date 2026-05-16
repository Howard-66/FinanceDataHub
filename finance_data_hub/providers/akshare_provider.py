"""AKShare provider for futures alternative data."""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import akshare as ak
import pandas as pd
from loguru import logger

from finance_data_hub.providers.base import BaseDataProvider, ProviderDataError
from finance_data_hub.providers.registry import register_provider
from finance_data_hub.providers.schema import (
    DailyBasicSchema,
    DailyDataSchema,
    FuturesInventoryReceiptSchema,
    FuturesSpotBasisSchema,
    MinuteDataSchema,
    StockBasicSchema,
    convert_to_standard_columns,
    validate_dataframe,
)
from finance_data_hub.utils.futures import normalize_futures_exchange


def _clean_ak_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).replace("-", "")[:8]


def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


@register_provider("akshare")
class AKShareProvider(BaseDataProvider):
    """AKShare data provider for futures spot basis and inventory receipt data."""

    def __init__(
        self,
        name: str = "akshare",
        config: Optional[Dict[str, Any]] = None,
        market: str = "CN",
    ):
        super().__init__(name, config, market=market)
        self.max_retry = config.get("max_retry", 3) if config else 3
        self.retry_delay = config.get("retry_delay", 1.0) if config else 1.0

    def initialize(self) -> None:
        self._is_initialized = True
        logger.info("AKShareProvider initialized")

    def health_check(self) -> bool:
        return self._is_initialized

    def get_stock_basic(
        self,
        market: Optional[str] = None,
        list_status: Optional[str] = "L",
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=StockBasicSchema.get_required_columns())

    def get_daily_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adj: Optional[str] = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=DailyDataSchema.get_required_columns())

    def get_minute_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        freq: str = "1m",
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=MinuteDataSchema.get_required_columns())

    def get_daily_basic(
        self,
        symbol: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        return pd.DataFrame(columns=DailyBasicSchema.get_required_columns())

    async def get_latest_record(
        self, symbol: str, data_type: str, table_name: str
    ) -> Optional[pd.DataFrame]:
        return None

    def should_overwrite_latest_record(
        self,
        latest_record_time: datetime,
        current_time: datetime,
        data_type: str,
    ) -> bool:
        return False

    async def get_incremental_data(
        self,
        symbol: Optional[str],
        data_type: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        if data_type == "spot_basis":
            return self.get_futures_spot_basis(
                start_date=start_date,
                end_date=end_date,
                products=kwargs.get("products"),
            )
        if data_type == "inventory":
            return self.get_futures_inventory(
                start_date=start_date,
                end_date=end_date,
                products=kwargs.get("products"),
            )
        raise ProviderDataError(
            f"AKShare incremental data does not support {data_type}",
            provider_name=self.name,
        )

    def _call_akshare(self, func, *args, **kwargs) -> pd.DataFrame:
        """Call an AKShare function with lightweight retries for transient HTTP issues."""
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retry + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_retry:
                    break
                wait_time = self.retry_delay * (attempt + 1)
                logger.warning(
                    f"AKShare call {func.__name__} failed, retrying in {wait_time:.1f}s "
                    f"({attempt + 1}/{self.max_retry}): {exc}"
                )
                time.sleep(wait_time)
        raise ProviderDataError(
            f"AKShare call {func.__name__} failed: {last_error}",
            provider_name=self.name,
        )

    def _normalize_spot_basis(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=FuturesSpotBasisSchema.get_required_columns())

        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        column_mapping = {
            "date": "time",
            "symbol": "product_code",
            "var": "product_code",
            "sp": "spot_price",
            "spot_price": "spot_price",
            "near_symbol": "near_contract",
            "near_contract": "near_contract",
            "near_price": "near_contract_price",
            "near_contract_price": "near_contract_price",
            "dom_symbol": "dominant_contract",
            "dominant_contract": "dominant_contract",
            "dom_price": "dominant_contract_price",
            "dominant_contract_price": "dominant_contract_price",
            "near_basis": "near_basis",
            "near_basis_rate": "near_basis_rate",
            "dom_basis": "dom_basis",
            "dom_basis_rate": "dom_basis_rate",
        }
        df = convert_to_standard_columns(df, column_mapping)

        if "time" not in df.columns:
            date_col = _find_column(df, ["日期", "trade_date"])
            if date_col:
                df = df.rename(columns={date_col: "time"})
        if "product_code" not in df.columns:
            product_col = _find_column(df, ["品种", "symbol", "var"])
            if product_col:
                df = df.rename(columns={product_col: "product_code"})

        for col in [
            "spot_price",
            "near_contract_price",
            "dominant_contract_price",
            "near_basis",
            "near_basis_rate",
            "dom_basis",
            "dom_basis_rate",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["product_code"] = df["product_code"].astype(str).str.upper()
        df["exchange"] = df.get("exchange", pd.Series([None] * len(df))).map(
            normalize_futures_exchange
        )
        df["futures_price"] = df.get("dominant_contract_price")

        # AKShare historical basis signs have changed before; store a consistent
        # FinanceDataHub definition: basis = spot - futures.
        spot = pd.to_numeric(df.get("spot_price"), errors="coerce")
        near_price = pd.to_numeric(df.get("near_contract_price"), errors="coerce")
        dom_price = pd.to_numeric(df.get("dominant_contract_price"), errors="coerce")
        df["near_basis"] = spot - near_price
        df["dom_basis"] = spot - dom_price
        df["near_basis_rate"] = (df["near_basis"] / spot).where(spot != 0)
        df["dom_basis_rate"] = (df["dom_basis"] / spot).where(spot != 0)
        df["source"] = "akshare"

        return validate_dataframe(
            df,
            FuturesSpotBasisSchema,
            provider_name=self.name,
        )

    def get_futures_spot_basis(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        products: Optional[List[str]] = None,
        **_: Any,
    ) -> pd.DataFrame:
        """获取现货价格与基差数据。"""
        if trade_date:
            kwargs = {}
            if products:
                kwargs["vars_list"] = products
            df = self._call_akshare(
                ak.futures_spot_price,
                _clean_ak_date(trade_date),
                **kwargs,
            )
            return self._normalize_spot_basis(df)

        if not start_date or not end_date:
            raise ProviderDataError(
                "AKShare futures_spot_price_daily requires start_date and end_date",
                provider_name=self.name,
            )

        df = self._call_akshare(
            ak.futures_spot_price_daily,
            start_day=_clean_ak_date(start_date),
            end_day=_clean_ak_date(end_date),
            vars_list=products,
        )
        return self._normalize_spot_basis(df)

    def _normalize_receipt(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=FuturesInventoryReceiptSchema.get_required_columns())

        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        column_mapping = {
            "date": "time",
            "var": "product_code",
            "symbol": "product_code",
            "receipt": "receipt",
            "inventory": "inventory",
            "库存": "inventory",
            "warehouse": "warehouse",
            "area": "region",
            "region": "region",
        }
        df = convert_to_standard_columns(df, column_mapping)

        if "time" not in df.columns:
            date_col = _find_column(df, ["日期", "trade_date"])
            if date_col:
                df = df.rename(columns={date_col: "time"})
        if "product_code" not in df.columns:
            product_col = _find_column(df, ["品种", "var", "symbol"])
            if product_col:
                df = df.rename(columns={product_col: "product_code"})
        if "receipt" not in df.columns:
            receipt_col = _find_column(df, ["仓单", "注册仓单", "receipt"])
            if receipt_col:
                df = df.rename(columns={receipt_col: "receipt"})

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["product_code"] = df["product_code"].astype(str).str.upper()
        df["receipt"] = pd.to_numeric(df.get("receipt"), errors="coerce")
        if "inventory" in df.columns:
            df["inventory"] = pd.to_numeric(df["inventory"], errors="coerce")
        else:
            df["inventory"] = None
        df["exchange"] = df.get("exchange", pd.Series([None] * len(df))).map(
            normalize_futures_exchange
        )
        df["source"] = "akshare"

        # Store product/date aggregate rows in V1.
        grouped = (
            df.groupby(["time", "product_code"], dropna=False)
            .agg(
                exchange=("exchange", "first"),
                receipt=("receipt", "sum"),
                inventory=("inventory", "sum"),
                warehouse=("warehouse", "first") if "warehouse" in df.columns else ("product_code", "first"),
                region=("region", "first") if "region" in df.columns else ("product_code", "first"),
                source=("source", "first"),
            )
            .reset_index()
        )
        if "warehouse" in grouped.columns:
            grouped["warehouse"] = grouped["warehouse"].where(
                grouped["warehouse"].ne(grouped["product_code"])
            )
        if "region" in grouped.columns:
            grouped["region"] = grouped["region"].where(
                grouped["region"].ne(grouped["product_code"])
            )

        return validate_dataframe(
            grouped,
            FuturesInventoryReceiptSchema,
            provider_name=self.name,
        )

    def get_futures_inventory(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        products: Optional[List[str]] = None,
        product_names: Optional[Dict[str, str]] = None,
        use_history: bool = False,
        **_: Any,
    ) -> pd.DataFrame:
        """获取注册仓单数据。"""
        if use_history:
            if not product_names:
                raise ProviderDataError(
                    "futures_inventory_99 requires product_names",
                    provider_name=self.name,
                )
            frames = []
            for product, name in product_names.items():
                df = self._call_akshare(
                    ak.futures_inventory_99,
                    symbol=name,
                )
                if df is not None and not df.empty:
                    df["product_code"] = product
                    frames.append(df)
            if not frames:
                return pd.DataFrame(columns=FuturesInventoryReceiptSchema.get_required_columns())
            return self._normalize_receipt(pd.concat(frames, ignore_index=True))

        if not start_date or not end_date:
            raise ProviderDataError(
                "AKShare get_receipt requires start_date and end_date",
                provider_name=self.name,
            )

        frames = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        final = datetime.strptime(end_date, "%Y-%m-%d")
        while current <= final:
            window_end = min(current + timedelta(days=4), final)
            df = self._call_akshare(
                ak.get_receipt,
                start_date=current.strftime("%Y%m%d"),
                end_date=window_end.strftime("%Y%m%d"),
                vars_list=products,
            )
            if df is not None and not df.empty:
                frames.append(df)
            current = window_end + timedelta(days=1)

        if not frames:
            return pd.DataFrame(columns=FuturesInventoryReceiptSchema.get_required_columns())
        return self._normalize_receipt(pd.concat(frames, ignore_index=True))
