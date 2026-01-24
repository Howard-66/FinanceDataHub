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


# GDP数据 Schema
CNGDPSchema = DataFrameSchema(
    name="cn_gdp",
    description="中国国民经济GDP数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "季度末日期（如2025-03-31表示2025Q1）"),
        ColumnSchema("quarter", "object", True, "季度（如2019Q1）"),
        ColumnSchema("gdp", "float64", True, "GDP累计值（亿元）"),
        ColumnSchema("gdp_yoy", "float64", True, "当季同比增速（%）"),
        ColumnSchema("pi", "float64", True, "第一产业累计值（亿元）"),
        ColumnSchema("pi_yoy", "float64", True, "第一产业同比增速（%）"),
        ColumnSchema("si", "float64", True, "第二产业累计值（亿元）"),
        ColumnSchema("si_yoy", "float64", True, "第二产业同比增速（%）"),
        ColumnSchema("ti", "float64", True, "第三产业累计值（亿元）"),
        ColumnSchema("ti_yoy", "float64", True, "第三产业同比增速（%）"),
    ],
)


# PPI数据 Schema
CNPPISchema = DataFrameSchema(
    name="cn_ppi",
    description="中国PPI工业生产者出厂价格指数数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "月份末日期"),
        ColumnSchema("month", "object", True, "月份YYYYMM格式"),
        ColumnSchema("ppi_yoy", "float64", True, "全部工业品：当月同比（%）"),
        ColumnSchema("ppi_mp_yoy", "float64", True, "生产资料：当月同比（%）"),
        ColumnSchema("ppi_mp_qm_yoy", "float64", True, "生产资料-采掘业：当月同比（%）"),
        ColumnSchema("ppi_mp_rm_yoy", "float64", True, "生产资料-原料业：当月同比（%）"),
        ColumnSchema("ppi_mp_p_yoy", "float64", True, "生产资料-加工业：当月同比（%）"),
        ColumnSchema("ppi_cg_yoy", "float64", True, "生活资料：当月同比（%）"),
        ColumnSchema("ppi_cg_f_yoy", "float64", True, "生活资料-食品类：当月同比（%）"),
        ColumnSchema("ppi_cg_c_yoy", "float64", True, "生活资料-衣着类：当月同比（%）"),
        ColumnSchema("ppi_cg_adu_yoy", "float64", True, "生活资料-一般日用品类：当月同比（%）"),
        ColumnSchema("ppi_cg_dcg_yoy", "float64", True, "生活资料-耐用消费品类：当月同比（%）"),
        ColumnSchema("ppi_mom", "float64", True, "全部工业品：环比（%）"),
        ColumnSchema("ppi_mp_mom", "float64", True, "生产资料：环比（%）"),
        ColumnSchema("ppi_mp_qm_mom", "float64", True, "生产资料-采掘业：环比（%）"),
        ColumnSchema("ppi_mp_rm_mom", "float64", True, "生产资料-原料业：环比（%）"),
        ColumnSchema("ppi_mp_p_mom", "float64", True, "生产资料-加工业：环比（%）"),
        ColumnSchema("ppi_cg_mom", "float64", True, "生活资料：环比（%）"),
        ColumnSchema("ppi_cg_f_mom", "float64", True, "生活资料-食品类：环比（%）"),
        ColumnSchema("ppi_cg_c_mom", "float64", True, "生活资料-衣着类：环比（%）"),
        ColumnSchema("ppi_cg_adu_mom", "float64", True, "生活资料-一般日用品类：环比（%）"),
        ColumnSchema("ppi_cg_dcg_mom", "float64", True, "生活资料-耐用消费品类：环比（%）"),
        ColumnSchema("ppi_accu", "float64", True, "全部工业品：累计同比（%）"),
        ColumnSchema("ppi_mp_accu", "float64", True, "生产资料：累计同比（%）"),
        ColumnSchema("ppi_mp_qm_accu", "float64", True, "生产资料-采掘业：累计同比（%）"),
        ColumnSchema("ppi_mp_rm_accu", "float64", True, "生产资料-原料业：累计同比（%）"),
        ColumnSchema("ppi_mp_p_accu", "float64", True, "生产资料-加工业：累计同比（%）"),
        ColumnSchema("ppi_cg_accu", "float64", True, "生活资料：累计同比（%）"),
        ColumnSchema("ppi_cg_f_accu", "float64", True, "生活资料-食品类：累计同比（%）"),
        ColumnSchema("ppi_cg_c_accu", "float64", True, "生活资料-衣着类：累计同比（%）"),
        ColumnSchema("ppi_cg_adu_accu", "float64", True, "生活资料-一般日用品类：累计同比（%）"),
        ColumnSchema("ppi_cg_dcg_accu", "float64", True, "生活资料-耐用消费品类：累计同比（%）"),
    ],
)


# 货币供应量数据 Schema
CNMSchema = DataFrameSchema(
    name="cn_m",
    description="中国货币供应量数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "月份末日期"),
        ColumnSchema("month", "object", True, "月份YYYYMM格式"),
        ColumnSchema("m0", "float64", True, "M0货币供应量（亿元）"),
        ColumnSchema("m0_yoy", "float64", True, "M0同比（%）"),
        ColumnSchema("m0_mom", "float64", True, "M0环比（%）"),
        ColumnSchema("m1", "float64", True, "M1货币供应量（亿元）"),
        ColumnSchema("m1_yoy", "float64", True, "M1同比（%）"),
        ColumnSchema("m1_mom", "float64", True, "M1环比（%）"),
        ColumnSchema("m2", "float64", True, "M2货币供应量（亿元）"),
        ColumnSchema("m2_yoy", "float64", True, "M2同比（%）"),
        ColumnSchema("m2_mom", "float64", True, "M2环比（%）"),
    ],
)


# PMI数据 Schema
CNPMISchema = DataFrameSchema(
    name="cn_pmi",
    description="中国PMI采购经理人指数数据",
    columns=[
        ColumnSchema("time", "datetime64[ns]", False, "月份末日期"),
        ColumnSchema("month", "object", True, "月份YYYYMM格式"),
        ColumnSchema("pmi010000", "float64", True, "制造业PMI"),
        ColumnSchema("pmi010100", "float64", True, "制造业PMI:大型企业"),
        ColumnSchema("pmi010200", "float64", True, "制造业PMI:中型企业"),
        ColumnSchema("pmi010300", "float64", True, "制造业PMI:小型企业"),
        ColumnSchema("pmi010400", "float64", True, "制造业PMI:生产指数"),
        ColumnSchema("pmi010500", "float64", True, "制造业PMI:新订单指数"),
        ColumnSchema("pmi010600", "float64", True, "制造业PMI:供应商配送时间指数"),
        ColumnSchema("pmi010700", "float64", True, "制造业PMI:原材料库存指数"),
        ColumnSchema("pmi010800", "float64", True, "制造业PMI:从业人员指数"),
        ColumnSchema("pmi010900", "float64", True, "制造业PMI:新出口订单"),
        ColumnSchema("pmi011000", "float64", True, "制造业PMI:进口"),
        ColumnSchema("pmi011100", "float64", True, "制造业PMI:采购量"),
        ColumnSchema("pmi011200", "float64", True, "制造业PMI:主要原材料购进价格"),
        ColumnSchema("pmi011300", "float64", True, "制造业PMI:出厂价格"),
        ColumnSchema("pmi011400", "float64", True, "制造业PMI:产成品库存"),
        ColumnSchema("pmi011500", "float64", True, "制造业PMI:在手订单"),
        ColumnSchema("pmi011600", "float64", True, "制造业PMI:生产经营活动预期"),
        ColumnSchema("pmi011700", "float64", True, "制造业PMI:装备制造业"),
        ColumnSchema("pmi011800", "float64", True, "制造业PMI:高技术制造业"),
        ColumnSchema("pmi011900", "float64", True, "制造业PMI:基础原材料制造业"),
        ColumnSchema("pmi012000", "float64", True, "制造业PMI:消费品制造业"),
        ColumnSchema("pmi020100", "float64", True, "非制造业PMI:商务活动"),
        ColumnSchema("pmi020200", "float64", True, "非制造业PMI:新订单指数"),
        ColumnSchema("pmi020300", "float64", True, "非制造业PMI:投入品价格指数"),
        ColumnSchema("pmi020400", "float64", True, "非制造业PMI:销售价格指数"),
        ColumnSchema("pmi020500", "float64", True, "非制造业PMI:从业人员指数"),
        ColumnSchema("pmi020600", "float64", True, "非制造业PMI:业务活动预期指数"),
        ColumnSchema("pmi020700", "float64", True, "非制造业PMI:新出口订单"),
        ColumnSchema("pmi020800", "float64", True, "非制造业PMI:在手订单"),
        ColumnSchema("pmi020900", "float64", True, "非制造业PMI:存货"),
        ColumnSchema("pmi021000", "float64", True, "非制造业PMI:供应商配送时间"),
        ColumnSchema("pmi030000", "float64", True, "中国综合PMI:产出指数"),
    ],
)


# 大盘指数每日指标数据 Schema
IndexDailybasicSchema = DataFrameSchema(
    name="index_dailybasic",
    description="大盘指数每日指标数据",
    columns=[
        ColumnSchema("ts_code", "object", False, "指数代码，如000001.SH（上证综指）"),
        ColumnSchema("trade_date", "datetime64[ns]", False, "交易日期"),
        ColumnSchema("total_mv", "float64", True, "当日总市值（元）"),
        ColumnSchema("float_mv", "float64", True, "当日流通市值（元）"),
        ColumnSchema("total_share", "float64", True, "当日总股本（股）"),
        ColumnSchema("float_share", "float64", True, "当日流通股本（股）"),
        ColumnSchema("free_share", "float64", True, "当日自由流通股本（股）"),
        ColumnSchema("turnover_rate", "float64", True, "换手率"),
        ColumnSchema("turnover_rate_f", "float64", True, "换手率(基于自由流通股本)"),
        ColumnSchema("pe", "float64", True, "市盈率"),
        ColumnSchema("pe_ttm", "float64", True, "市盈率TTM"),
        ColumnSchema("pb", "float64", True, "市净率"),
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
