"""
数据格式模式定义和验证

定义标准化的DataFrame Schema，确保不同数据源返回的数据格式一致。
"""

from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import pandas as pd
from loguru import logger

from finance_data_hub.providers.base import ProviderDataError


# ===========================
# Schema 定义
# ===========================


@dataclass
class ColumnSchema:
    """列模式定义"""

    name: str
    dtype: str  # pandas dtype string
    nullable: bool = True
    description: str = ""


@dataclass
class DataFrameSchema:
    """DataFrame模式定义"""

    name: str
    columns: List[ColumnSchema]
    description: str = ""

    def get_required_columns(self) -> List[str]:
        """获取所有必需列名"""
        return [col.name for col in self.columns]

    def get_dtype_mapping(self) -> Dict[str, str]:
        """获取列名到数据类型的映射"""
        return {col.name: col.dtype for col in self.columns}


# ===========================
# 标准 Schema 定义
# ===========================


# 股票基本信息 Schema
StockBasicSchema = DataFrameSchema(
    name="stock_basic",
    description="股票基本信息",
    columns=[
        ColumnSchema("symbol", "object", False, "股票代码（带后缀，如600519.SH）"),
        ColumnSchema("name", "object", False, "股票名称"),
        ColumnSchema("market", "object", False, "市场代码（SH/SZ等）"),
        ColumnSchema("industry", "object", True, "所属行业"),
        ColumnSchema("area", "object", True, "地域"),
        ColumnSchema("list_status", "object", True, "上市状态（L=上市 D=退市 P=暂停）"),
        ColumnSchema("list_date", "datetime64[ns]", True, "上市日期"),
        ColumnSchema("delist_date", "datetime64[ns]", True, "退市日期"),
        ColumnSchema("is_hs", "object", True, "是否沪深港通标的（N/H/S）"),
    ],
)


# 日线行情数据 Schema
DailyDataSchema = DataFrameSchema(
    name="daily_data",
    description="日线行情数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "交易时间"),
        ColumnSchema("symbol", "object", False, "股票代码"),
        ColumnSchema("open", "float64", False, "开盘价"),
        ColumnSchema("high", "float64", False, "最高价"),
        ColumnSchema("low", "float64", False, "最低价"),
        ColumnSchema("close", "float64", False, "收盘价"),
        ColumnSchema("volume", "int64", False, "成交量"),
        ColumnSchema("amount", "float64", False, "成交额"),
        ColumnSchema("adj_factor", "float64", True, "复权因子"),
        ColumnSchema("open_interest", "float64", True, "持仓量（期货）"),
        ColumnSchema("settle", "float64", True, "结算价（期货）"),
        ColumnSchema("change_pct", "float64", True, "涨跌幅(%)"),
        ColumnSchema("change_amount", "float64", True, "涨跌额"),
    ],
)


# 分钟行情数据 Schema
MinuteDataSchema = DataFrameSchema(
    name="minute_data",
    description="分钟级行情数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "交易时间"),
        ColumnSchema("symbol", "object", False, "股票代码"),
        ColumnSchema("open", "float64", False, "开盘价"),
        ColumnSchema("high", "float64", False, "最高价"),
        ColumnSchema("low", "float64", False, "最低价"),
        ColumnSchema("close", "float64", False, "收盘价"),
        ColumnSchema("volume", "int64", False, "成交量"),
        ColumnSchema("amount", "float64", False, "成交额"),
        ColumnSchema("open_interest", "float64", True, "持仓量（期货）"),
        ColumnSchema("settle", "float64", True, "结算价（期货）"),
        ColumnSchema("change_pct", "float64", True, "涨跌幅(%)"),
        ColumnSchema("change_amount", "float64", True, "涨跌额"),
    ],
)


# 每日指标数据 Schema
DailyBasicSchema = DataFrameSchema(
    name="daily_basic",
    description="每日市场指标数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "交易日期"),
        ColumnSchema("symbol", "object", False, "股票代码"),
        ColumnSchema("turnover_rate", "float64", True, "换手率(%)"),
        ColumnSchema("volume_ratio", "float64", True, "量比"),
        ColumnSchema("pe", "float64", True, "市盈率"),
        ColumnSchema("pe_ttm", "float64", True, "市盈率TTM"),
        ColumnSchema("pb", "float64", True, "市净率"),
        ColumnSchema("ps", "float64", True, "市销率"),
        ColumnSchema("ps_ttm", "float64", True, "市销率TTM"),
        ColumnSchema("dv_ratio", "float64", True, "股息率(%)"),
        ColumnSchema("dv_ttm", "float64", True, "股息率TTM(%)"),
        ColumnSchema("total_share", "float64", True, "总股本(万股)"),
        ColumnSchema("float_share", "float64", True, "流通股本(万股)"),
        ColumnSchema("free_share", "float64", True, "自由流通股本(万股)"),
        ColumnSchema("total_mv", "float64", True, "总市值(万元)"),
        ColumnSchema("circ_mv", "float64", True, "流通市值(万元)"),
    ],
)


# 复权因子数据 Schema
AdjFactorSchema = DataFrameSchema(
    name="adj_factor",
    description="复权因子数据",
    columns=[
        ColumnSchema("symbol", "object", False, "股票代码"),
        ColumnSchema("trade_date", "datetime64[ns]", False, "交易日期"),
        ColumnSchema("adj_factor", "float64", False, "复权因子"),
    ],
)


# ===========================
# 验证函数
# ===========================


def validate_dataframe(
    df: pd.DataFrame,
    schema: DataFrameSchema,
    strict: bool = False,
    provider_name: str = "Unknown",
) -> pd.DataFrame:
    """
    验证DataFrame是否符合指定的Schema

    Args:
        df: 待验证的DataFrame
        schema: 数据模式定义
        strict: 是否严格模式（严格模式会检查额外的列）
        provider_name: 提供者名称（用于错误信息）

    Returns:
        pd.DataFrame: 验证通过的DataFrame（可能经过类型转换）

    Raises:
        ProviderDataError: 验证失败时抛出
    """
    if df is None or df.empty:
        logger.warning(f"Empty DataFrame received from {provider_name}")
        # 返回符合schema的空DataFrame
        return pd.DataFrame(columns=schema.get_required_columns())

    # 检查必需列是否存在
    required_cols = [col.name for col in schema.columns if not col.nullable]
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        raise ProviderDataError(
            f"Missing required columns: {missing_cols}",
            provider_name=provider_name,
            schema=schema.name,
            missing_columns=list(missing_cols),
        )

    # 严格模式：检查额外的列
    if strict:
        expected_cols = set(schema.get_required_columns())
        extra_cols = set(df.columns) - expected_cols
        if extra_cols:
            logger.warning(
                f"Extra columns found in {schema.name}: {extra_cols} "
                f"(provider: {provider_name})"
            )

    # 类型转换和验证
    dtype_mapping = schema.get_dtype_mapping()
    df_validated = df.copy()

    for col_name, expected_dtype in dtype_mapping.items():
        if col_name not in df_validated.columns:
            # 如果是可选列且不存在，跳过
            col_def = next((c for c in schema.columns if c.name == col_name), None)
            if col_def and col_def.nullable:
                continue
            else:
                raise ProviderDataError(
                    f"Required column '{col_name}' not found",
                    provider_name=provider_name,
                    schema=schema.name,
                )

        try:
            # 尝试类型转换
            if expected_dtype == "datetime64[ns]":
                df_validated[col_name] = pd.to_datetime(
                    df_validated[col_name], errors="coerce"
                )
            elif expected_dtype == "float64":
                df_validated[col_name] = pd.to_numeric(
                    df_validated[col_name], errors="coerce"
                )
            elif expected_dtype == "int64":
                # 处理可能的NaN值
                df_validated[col_name] = pd.to_numeric(
                    df_validated[col_name], errors="coerce"
                ).fillna(0).astype("int64")
            elif expected_dtype == "object":
                df_validated[col_name] = df_validated[col_name].astype(str)

        except Exception as e:
            logger.warning(
                f"Failed to convert column '{col_name}' to {expected_dtype}: {str(e)}"
            )
            # 类型转换失败，继续使用原始类型

    logger.debug(
        f"DataFrame validation passed: {schema.name} "
        f"(provider: {provider_name}, rows: {len(df_validated)})"
    )

    return df_validated


def standardize_symbol(symbol: str, provider_format: str = "tushare") -> str:
    """
    标准化股票代码格式

    Args:
        symbol: 原始股票代码
        provider_format: 提供者格式（"tushare", "xtquant"等）

    Returns:
        str: 标准化后的股票代码（格式：XXXXXX.XX，例如600519.SH）
    """
    symbol = symbol.strip().upper()

    if provider_format == "tushare":
        # Tushare格式已经是 XXXXXX.XX
        return symbol
    elif provider_format == "xtquant":
        # XTQuant格式可能是 XX.XXXXXX，需要反转
        if "." in symbol:
            parts = symbol.split(".")
            if len(parts[0]) == 2:  # 交易所在前
                return f"{parts[1]}.{parts[0]}"
        return symbol
    else:
        return symbol


def convert_to_standard_columns(
    df: pd.DataFrame, column_mapping: Dict[str, str]
) -> pd.DataFrame:
    """
    将DataFrame的列名转换为标准列名

    Args:
        df: 原始DataFrame
        column_mapping: 列名映射字典（原始列名 -> 标准列名）

    Returns:
        pd.DataFrame: 列名已标准化的DataFrame
    """
    if df.empty:
        return df

    # 只重命名存在的列
    existing_mapping = {
        old: new for old, new in column_mapping.items() if old in df.columns
    }

    df_renamed = df.rename(columns=existing_mapping)

    logger.debug(f"Renamed columns: {existing_mapping}")

    return df_renamed
