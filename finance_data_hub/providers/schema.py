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


# 上市公司财务指标数据 Schema
FinaIndicatorSchema = DataFrameSchema(
    name="fina_indicator",
    description="上市公司财务指标数据",
    columns=[
        ColumnSchema("ts_code", "object", False, "TS代码"),
        ColumnSchema("ann_date", "object", True, "公告日期"),
        ColumnSchema("end_date", "object", True, "报告期"),
        ColumnSchema("end_date_time", "datetime64[ns]", False, "报告期（时间序列格式）"),
        ColumnSchema("eps", "float64", True, "基本每股收益"),
        ColumnSchema("dt_eps", "float64", True, "稀释每股收益"),
        ColumnSchema("total_revenue_ps", "float64", True, "每股营业总收入"),
        ColumnSchema("revenue_ps", "float64", True, "每股营业收入"),
        ColumnSchema("capital_rese_ps", "float64", True, "每股资本公积"),
        ColumnSchema("surplus_rese_ps", "float64", True, "每股盈余公积"),
        ColumnSchema("undist_profit_ps", "float64", True, "每股未分配利润"),
        ColumnSchema("extra_item", "float64", True, "非经常性损益"),
        ColumnSchema("profit_dedt", "float64", True, "扣除非经常性损益后的净利润"),
        ColumnSchema("gross_margin", "float64", True, "毛利"),
        ColumnSchema("current_ratio", "float64", True, "流动比率"),
        ColumnSchema("quick_ratio", "float64", True, "速动比率"),
        ColumnSchema("cash_ratio", "float64", True, "保守速动比率"),
        ColumnSchema("ar_turn", "float64", True, "应收账款周转率"),
        ColumnSchema("ca_turn", "float64", True, "流动资产周转率"),
        ColumnSchema("fa_turn", "float64", True, "固定资产周转率"),
        ColumnSchema("assets_turn", "float64", True, "总资产周转率"),
        ColumnSchema("op_income", "float64", True, "经营活动净收益"),
        ColumnSchema("ebit", "float64", True, "息税前利润"),
        ColumnSchema("ebitda", "float64", True, "息税折旧摊销前利润"),
        ColumnSchema("fcff", "float64", True, "企业自由现金流量"),
        ColumnSchema("fcfe", "float64", True, "股权自由现金流量"),
        ColumnSchema("current_exint", "float64", True, "无息流动负债"),
        ColumnSchema("noncurrent_exint", "float64", True, "无息非流动负债"),
        ColumnSchema("interestdebt", "float64", True, "带息债务"),
        ColumnSchema("netdebt", "float64", True, "净债务"),
        ColumnSchema("tangible_asset", "float64", True, "有形资产"),
        ColumnSchema("working_capital", "float64", True, "营运资金"),
        ColumnSchema("networking_capital", "float64", True, "营运流动资本"),
        ColumnSchema("invest_capital", "float64", True, "全部投入资本"),
        ColumnSchema("retained_earnings", "float64", True, "留存收益"),
        ColumnSchema("diluted2_eps", "float64", True, "期末摊薄每股收益"),
        ColumnSchema("bps", "float64", True, "每股净资产"),
        ColumnSchema("ocfps", "float64", True, "每股经营活动产生的现金流量净额"),
        ColumnSchema("cfps", "float64", True, "每股现金流量净额"),
        ColumnSchema("ebit_ps", "float64", True, "每股息税前利润"),
        ColumnSchema("netprofit_margin", "float64", True, "销售净利率"),
        ColumnSchema("grossprofit_margin", "float64", True, "销售毛利率"),
        ColumnSchema("profit_to_gr", "float64", True, "净利润/营业总收入"),
        ColumnSchema("roe", "float64", True, "净资产收益率"),
        ColumnSchema("roe_waa", "float64", True, "加权平均净资产收益率"),
        ColumnSchema("roe_dt", "float64", True, "净资产收益率(扣除非经常损益)"),
        ColumnSchema("roa", "float64", True, "总资产报酬率"),
        ColumnSchema("roic", "float64", True, "投入资本回报率"),
        ColumnSchema("debt_to_assets", "float64", True, "资产负债率"),
        ColumnSchema("assets_to_eqt", "float64", True, "权益乘数"),
        ColumnSchema("ca_to_assets", "float64", True, "流动资产/总资产"),
        ColumnSchema("nca_to_assets", "float64", True, "非流动资产/总资产"),
        ColumnSchema("tbassets_to_totalassets", "float64", True, "有形资产/总资产"),
        ColumnSchema("int_to_talcap", "float64", True, "带息债务/全部投入资本"),
        ColumnSchema("eqt_to_talcapital", "float64", True, "归属于母公司的股东权益/全部投入资本"),
        ColumnSchema("currentdebt_to_debt", "float64", True, "流动负债/负债合计"),
        ColumnSchema("longdeb_to_debt", "float64", True, "非流动负债/负债合计"),
        ColumnSchema("debt_to_eqt", "float64", True, "产权比率"),
        ColumnSchema("eqt_to_debt", "float64", True, "归属于母公司的股东权益/负债合计"),
        ColumnSchema("eqt_to_interestdebt", "float64", True, "归属于母公司的股东权益/带息债务"),
        ColumnSchema("tangibleasset_to_debt", "float64", True, "有形资产/负债合计"),
        ColumnSchema("ocf_to_debt", "float64", True, "经营活动产生的现金流量净额/负债合计"),
        ColumnSchema("turn_days", "float64", True, "营业周期"),
        ColumnSchema("fixed_assets", "float64", True, "固定资产合计"),
        ColumnSchema("profit_prefin_exp", "float64", True, "扣除财务费用前营业利润"),
        ColumnSchema("non_op_profit", "float64", True, "非营业利润"),
        ColumnSchema("op_to_ebt", "float64", True, "营业利润/利润总额"),
        ColumnSchema("q_opincome", "float64", True, "经营活动单季度净收益"),
        ColumnSchema("q_dtprofit", "float64", True, "扣除非经常损益后的单季度净利润"),
        ColumnSchema("q_eps", "float64", True, "每股收益(单季度)"),
        ColumnSchema("q_netprofit_margin", "float64", True, "销售净利率(单季度)"),
        ColumnSchema("q_gsprofit_margin", "float64", True, "销售毛利率(单季度)"),
        ColumnSchema("q_profit_to_gr", "float64", True, "净利润/营业总收入(单季度)"),
        ColumnSchema("q_salescash_to_or", "float64", True, "销售商品提供劳务收到的现金/营业收入(单季度)"),
        ColumnSchema("q_ocf_to_sales", "float64", True, "经营活动产生的现金流量净额/营业收入(单季度)"),
        ColumnSchema("basic_eps_yoy", "float64", True, "基本每股收益同比增长率(%)"),
        ColumnSchema("dt_eps_yoy", "float64", True, "稀释每股收益同比增长率(%)"),
        ColumnSchema("cfps_yoy", "float64", True, "每股经营活动产生的现金流量净额同比增长率(%)"),
        ColumnSchema("op_yoy", "float64", True, "营业利润同比增长率(%)"),
        ColumnSchema("ebt_yoy", "float64", True, "利润总额同比增长率(%)"),
        ColumnSchema("netprofit_yoy", "float64", True, "归属母公司股东的净利润同比增长率(%)"),
        ColumnSchema("dt_netprofit_yoy", "float64", True, "归属母公司股东的净利润-扣除非经常损益同比增长率(%)"),
        ColumnSchema("ocf_yoy", "float64", True, "经营活动产生的现金流量净额同比增长率(%)"),
        ColumnSchema("roe_yoy", "float64", True, "净资产收益率(摊薄)同比增长率(%)"),
        ColumnSchema("bps_yoy", "float64", True, "每股净资产相对年初增长率(%)"),
        ColumnSchema("assets_yoy", "float64", True, "资产总计相对年初增长率(%)"),
        ColumnSchema("eqt_yoy", "float64", True, "归属母公司的股东权益相对年初增长率(%)"),
        ColumnSchema("tr_yoy", "float64", True, "营业总收入同比增长率(%)"),
        ColumnSchema("or_yoy", "float64", True, "营业收入同比增长率(%)"),
        ColumnSchema("q_gr_yoy", "float64", True, "营业总收入同比增长率(%)(单季度)"),
        ColumnSchema("q_sales_yoy", "float64", True, "营业收入同比增长率(%)(单季度)"),
        ColumnSchema("q_op_yoy", "float64", True, "营业利润同比增长率(%)(单季度)"),
        ColumnSchema("q_op_qoq", "float64", True, "营业利润环比增长率(%)(单季度)"),
        ColumnSchema("q_profit_yoy", "float64", True, "净利润同比增长率(%)(单季度)"),
        ColumnSchema("q_profit_qoq", "float64", True, "净利润环比增长率(%)(单季度)"),
        ColumnSchema("q_netprofit_yoy", "float64", True, "归属母公司股东的净利润同比增长率(%)(单季度)"),
        ColumnSchema("q_netprofit_qoq", "float64", True, "归属母公司股东的净利润环比增长率(%)(单季度)"),
        ColumnSchema("equity_yoy", "float64", True, "净资产同比增长率"),
    ],
)

# 上市公司现金流量表数据 Schema
CashflowSchema = DataFrameSchema(
    name="cashflow",
    description="上市公司现金流量表数据",
    columns=[
        ColumnSchema("ts_code", "object", False, "TS代码"),
        ColumnSchema("ann_date", "object", True, "公告日期"),
        ColumnSchema("f_ann_date", "object", True, "实际公告日期"),
        ColumnSchema("end_date", "object", True, "报告期"),
        ColumnSchema("end_date_time", "datetime64[ns]", False, "报告期（时间序列格式）"),
        ColumnSchema("comp_type", "object", True, "公司类型"),
        ColumnSchema("report_type", "object", True, "报表类型"),
        ColumnSchema("end_type", "object", True, "报告期类型"),
        # 经营活动产生的现金流量
        ColumnSchema("net_profit", "float64", True, "净利润"),
        ColumnSchema("finan_exp", "float64", True, "财务费用"),
        ColumnSchema("c_fr_sale_sg", "float64", True, "销售商品、提供劳务收到的现金"),
        ColumnSchema("recp_tax_rends", "float64", True, "收到的税费返还"),
        ColumnSchema("n_depos_incr_fi", "float64", True, "客户存款和同业存放款项净增加额"),
        ColumnSchema("n_incr_loans_cb", "float64", True, "向中央银行借款净增加额"),
        ColumnSchema("n_inc_borr_oth_fi", "float64", True, "向其他金融机构拆入资金净增加额"),
        ColumnSchema("prem_fr_orig_contr", "float64", True, "收到原保险合同保费取得的现金"),
        ColumnSchema("n_incr_insured_dep", "float64", True, "保户储金净增加额"),
        ColumnSchema("n_reinsur_prem", "float64", True, "收到再保业务现金净额"),
        ColumnSchema("n_incr_disp_tfa", "float64", True, "处置交易性金融资产净增加额"),
        ColumnSchema("ifc_cash_incr", "float64", True, "收取利息和手续费净增加额"),
        ColumnSchema("n_incr_disp_faas", "float64", True, "处置可供出售金融资产净增加额"),
        ColumnSchema("n_incr_loans_oth_bank", "float64", True, "拆入资金净增加额"),
        ColumnSchema("n_cap_incr_repur", "float64", True, "回购业务资金净增加额"),
        ColumnSchema("c_fr_oth_operate_a", "float64", True, "收到其他与经营活动有关的现金"),
        ColumnSchema("c_inf_fr_operate_a", "float64", True, "经营活动现金流入小计"),
        ColumnSchema("c_paid_goods_s", "float64", True, "购买商品、接受劳务支付的现金"),
        ColumnSchema("c_paid_to_for_empl", "float64", True, "支付给职工以及为职工支付的现金"),
        ColumnSchema("c_paid_for_taxes", "float64", True, "支付的各项税费"),
        ColumnSchema("n_incr_clt_loan_adv", "float64", True, "客户贷款及垫款净增加额"),
        ColumnSchema("n_incr_dep_cbob", "float64", True, "存放央行和同业款项净增加额"),
        ColumnSchema("c_pay_claims_orig_inco", "float64", True, "支付原保险合同赔付款项的现金"),
        ColumnSchema("pay_handling_chrg", "float64", True, "支付手续费的现金"),
        ColumnSchema("pay_comm_insur_plcy", "float64", True, "支付保单红利的现金"),
        ColumnSchema("oth_cash_pay_oper_act", "float64", True, "支付其他与经营活动有关的现金"),
        ColumnSchema("st_cash_out_act", "float64", True, "经营活动现金流出小计"),
        ColumnSchema("n_cashflow_act", "float64", True, "经营活动产生的现金流量净额"),
        # 投资活动产生的现金流量
        ColumnSchema("oth_recp_ral_inv_act", "float64", True, "收到其他与投资活动有关的现金"),
        ColumnSchema("c_disp_withdrwl_invest", "float64", True, "收回投资收到的现金"),
        ColumnSchema("c_recp_return_invest", "float64", True, "取得投资收益收到的现金"),
        ColumnSchema("n_recp_disp_fiolta", "float64", True, "处置固定资产、无形资产和其他长期资产收回的现金净额"),
        ColumnSchema("n_recp_disp_sobu", "float64", True, "处置子公司及其他营业单位收到的现金净额"),
        ColumnSchema("stot_inflows_inv_act", "float64", True, "投资活动现金流入小计"),
        ColumnSchema("c_pay_acq_const_fiolta", "float64", True, "购建固定资产、无形资产和其他长期资产支付的现金"),
        ColumnSchema("c_paid_invest", "float64", True, "投资支付的现金"),
        ColumnSchema("n_disp_subs_oth_biz", "float64", True, "取得子公司及其他营业单位支付的现金净额"),
        ColumnSchema("oth_pay_ral_inv_act", "float64", True, "支付其他与投资活动有关的现金"),
        ColumnSchema("n_incr_pledge_loan", "float64", True, "质押贷款净增加额"),
        ColumnSchema("stot_out_inv_act", "float64", True, "投资活动现金流出小计"),
        ColumnSchema("n_cashflow_inv_act", "float64", True, "投资活动产生的现金流量净额"),
        # 筹资活动产生的现金流量
        ColumnSchema("c_recp_borrow", "float64", True, "取得借款收到的现金"),
        ColumnSchema("proc_issue_bonds", "float64", True, "发行债券收到的现金"),
        ColumnSchema("oth_cash_recp_ral_fnc_act", "float64", True, "收到其他与筹资活动有关的现金"),
        ColumnSchema("stot_cash_in_fnc_act", "float64", True, "筹资活动现金流入小计"),
        ColumnSchema("free_cashflow", "float64", True, "企业自由现金流量"),
        ColumnSchema("c_prepay_amt_borr", "float64", True, "偿还债务支付的现金"),
        ColumnSchema("c_pay_dist_dpcp_int_exp", "float64", True, "分配股利、利润或偿付利息支付的现金"),
        ColumnSchema("incl_dvd_profit_paid_sc_ms", "float64", True, "其中:子公司支付给少数股东的股利、利润"),
        ColumnSchema("oth_cashpay_ral_fnc_act", "float64", True, "支付其他与筹资活动有关的现金"),
        ColumnSchema("stot_cashout_fnc_act", "float64", True, "筹资活动现金流出小计"),
        ColumnSchema("n_cash_flows_fnc_act", "float64", True, "筹资活动产生的现金流量净额"),
        # 汇率变动对现金的影响
        ColumnSchema("eff_fx_flu_cash", "float64", True, "汇率变动对现金的影响"),
        ColumnSchema("n_incr_cash_cash_equ", "float64", True, "现金及现金等价物净增加额"),
        ColumnSchema("c_cash_equ_beg_period", "float64", True, "期初现金及现金等价物余额"),
        ColumnSchema("c_cash_equ_end_period", "float64", True, "期末现金及现金等价物余额"),
        # 补充资料
        ColumnSchema("c_recp_cap_contrib", "float64", True, "吸收投资收到的现金"),
        ColumnSchema("incl_cash_rec_saims", "float64", True, "其中:子公司吸收少数股东投资收到的现金"),
        ColumnSchema("uncon_invest_loss", "float64", True, "未确认投资损失"),
        ColumnSchema("prov_depr_assets", "float64", True, "加:资产减值准备"),
        ColumnSchema("depr_fa_coga_dpba", "float64", True, "固定资产折旧、油气资产折耗、生产性生物资产折旧"),
        ColumnSchema("amort_intang_assets", "float64", True, "无形资产摊销"),
        ColumnSchema("lt_amort_deferred_exp", "float64", True, "长期待摊费用摊销"),
        ColumnSchema("decr_deferred_exp", "float64", True, "待摊费用减少"),
        ColumnSchema("incr_acc_exp", "float64", True, "预提费用增加"),
        ColumnSchema("loss_disp_fiolta", "float64", True, "处置固定、无形资产和其他长期资产的损失"),
        ColumnSchema("loss_scr_fa", "float64", True, "固定资产报废损失"),
        ColumnSchema("loss_fv_chg", "float64", True, "公允价值变动损失"),
        ColumnSchema("invest_loss", "float64", True, "投资损失"),
        ColumnSchema("decr_def_inc_tax_assets", "float64", True, "递延所得税资产减少"),
        ColumnSchema("incr_def_inc_tax_liab", "float64", True, "递延所得税负债增加"),
        ColumnSchema("decr_inventories", "float64", True, "存货的减少"),
        ColumnSchema("decr_oper_payable", "float64", True, "经营性应收项目的减少"),
        ColumnSchema("incr_oper_payable", "float64", True, "经营性应付项目的增加"),
        ColumnSchema("others", "float64", True, "其他"),
        ColumnSchema("im_net_cashflow_oper_act", "float64", True, "经营活动产生的现金流量净额(间接法)"),
        ColumnSchema("conv_debt_into_cap", "float64", True, "债务转为资本"),
        ColumnSchema("conv_copbonds_due_within_1y", "float64", True, "一年内到期的可转换公司债券"),
        ColumnSchema("fa_fnc_leases", "float64", True, "融资租入固定资产"),
        ColumnSchema("im_n_incr_cash_equ", "float64", True, "现金及现金等价物净增加额(间接法)"),
        ColumnSchema("net_dism_capital_add", "float64", True, "拆出资金净增加额"),
        ColumnSchema("net_cash_rece_sec", "float64", True, "代理买卖证券收到的现金净额(元)"),
        ColumnSchema("credit_impa_loss", "float64", True, "信用减值损失"),
        ColumnSchema("use_right_asset_dep", "float64", True, "使用权资产折旧"),
        ColumnSchema("oth_loss_asset", "float64", True, "其他资产减值损失"),
        ColumnSchema("end_bal_cash", "float64", True, "现金的期末余额"),
        ColumnSchema("beg_bal_cash", "float64", True, "减:现金的期初余额"),
        ColumnSchema("end_bal_cash_equ", "float64", True, "加:现金等价物的期末余额"),
        ColumnSchema("beg_bal_cash_equ", "float64", True, "减:现金等价物的期初余额"),
        ColumnSchema("update_flag", "object", True, "更新标志"),
    ],
)


# 上市公司资产负债表数据 Schema
BalancesheetSchema = DataFrameSchema(
    name="balancesheet",
    description="上市公司资产负债表数据",
    columns=[
        ColumnSchema("ts_code", "object", False, "TS代码"),
        ColumnSchema("ann_date", "object", True, "公告日期"),
        ColumnSchema("f_ann_date", "object", True, "实际公告日期"),
        ColumnSchema("end_date", "object", True, "报告期"),
        ColumnSchema("end_date_time", "datetime64[ns]", False, "报告期（时间序列格式）"),
        ColumnSchema("comp_type", "object", True, "公司类型"),
        ColumnSchema("report_type", "object", True, "报表类型"),
        ColumnSchema("end_type", "object", True, "报告期类型"),
        # 流动资产
        ColumnSchema("total_share", "float64", True, "期末总股本"),
        ColumnSchema("cap_rese", "float64", True, "资本公积金"),
        ColumnSchema("undistr_porfit", "float64", True, "未分配利润"),
        ColumnSchema("surplus_rese", "float64", True, "盈余公积金"),
        ColumnSchema("special_rese", "float64", True, "专项储备"),
        ColumnSchema("money_cap", "float64", True, "货币资金"),
        ColumnSchema("trad_asset", "float64", True, "交易性金融资产"),
        ColumnSchema("notes_receiv", "float64", True, "应收票据"),
        ColumnSchema("accounts_receiv", "float64", True, "应收账款"),
        ColumnSchema("oth_receiv", "float64", True, "其他应收款"),
        ColumnSchema("prepayment", "float64", True, "预付款项"),
        ColumnSchema("div_receiv", "float64", True, "应收股利"),
        ColumnSchema("int_receiv", "float64", True, "应收利息"),
        ColumnSchema("inventories", "float64", True, "存货"),
        ColumnSchema("amor_exp", "float64", True, "待摊费用"),
        ColumnSchema("nca_within_1y", "float64", True, "一年内到期的非流动资产"),
        ColumnSchema("sett_rsrv", "float64", True, "结算备付金"),
        ColumnSchema("loanto_oth_bank_fi", "float64", True, "拆出资金"),
        ColumnSchema("premium_receiv", "float64", True, "应收保费"),
        ColumnSchema("reinsur_receiv", "float64", True, "应收分保账款"),
        ColumnSchema("reinsur_res_receiv", "float64", True, "应收分保合同准备金"),
        ColumnSchema("pur_resale_fa", "float64", True, "买入返售金融资产"),
        ColumnSchema("oth_cur_assets", "float64", True, "其他流动资产"),
        ColumnSchema("total_cur_assets", "float64", True, "流动资产合计"),
        # 非流动资产
        ColumnSchema("fa_avail_for_sale", "float64", True, "可供出售金融资产"),
        ColumnSchema("htm_invest", "float64", True, "持有至到期投资"),
        ColumnSchema("lt_eqt_invest", "float64", True, "长期股权投资"),
        ColumnSchema("invest_real_estate", "float64", True, "投资性房地产"),
        ColumnSchema("time_deposits", "float64", True, "定期存款"),
        ColumnSchema("oth_assets", "float64", True, "其他资产"),
        ColumnSchema("lt_rec", "float64", True, "长期应收款"),
        ColumnSchema("fix_assets", "float64", True, "固定资产"),
        ColumnSchema("cip", "float64", True, "在建工程"),
        ColumnSchema("const_materials", "float64", True, "工程物资"),
        ColumnSchema("fixed_assets_disp", "float64", True, "固定资产清理"),
        ColumnSchema("produc_bio_assets", "float64", True, "生产性生物资产"),
        ColumnSchema("oil_and_gas_assets", "float64", True, "油气资产"),
        ColumnSchema("intan_assets", "float64", True, "无形资产"),
        ColumnSchema("r_and_d", "float64", True, "研发支出"),
        ColumnSchema("goodwill", "float64", True, "商誉"),
        ColumnSchema("lt_amor_exp", "float64", True, "长期待摊费用"),
        ColumnSchema("defer_tax_assets", "float64", True, "递延所得税资产"),
        ColumnSchema("decr_in_disbur", "float64", True, "发放贷款及垫款"),
        ColumnSchema("oth_nca", "float64", True, "其他非流动资产"),
        ColumnSchema("total_nca", "float64", True, "非流动资产合计"),
        # 银行/保险特有资产
        ColumnSchema("cash_reser_cb", "float64", True, "现金及存放中央银行款项"),
        ColumnSchema("depos_in_oth_bfi", "float64", True, "存放同业和其它金融机构款项"),
        ColumnSchema("prec_metals", "float64", True, "贵金属"),
        ColumnSchema("deriv_assets", "float64", True, "衍生金融资产"),
        ColumnSchema("rr_reins_une_prem", "float64", True, "应收分保未到期责任准备金"),
        ColumnSchema("rr_reins_outstd_cla", "float64", True, "应收分保未决赔款准备金"),
        ColumnSchema("rr_reins_lins_liab", "float64", True, "应收分保寿险责任准备金"),
        ColumnSchema("rr_reins_lthins_liab", "float64", True, "应收分保长期健康险责任准备金"),
        ColumnSchema("refund_depos", "float64", True, "存出保证金"),
        ColumnSchema("ph_pledge_loans", "float64", True, "保户质押贷款"),
        ColumnSchema("refund_cap_depos", "float64", True, "存出资本保证金"),
        ColumnSchema("indept_acct_assets", "float64", True, "独立账户资产"),
        ColumnSchema("client_depos", "float64", True, "其中：客户资金存款"),
        ColumnSchema("client_prov", "float64", True, "其中：客户备付金"),
        ColumnSchema("transac_seat_fee", "float64", True, "其中:交易席位费"),
        ColumnSchema("invest_as_receiv", "float64", True, "应收款项类投资"),
        # 资产总计
        ColumnSchema("total_assets", "float64", True, "资产总计"),
        # 流动负债
        ColumnSchema("lt_borr", "float64", True, "长期借款"),
        ColumnSchema("st_borr", "float64", True, "短期借款"),
        ColumnSchema("cb_borr", "float64", True, "向中央银行借款"),
        ColumnSchema("depos_ib_deposits", "float64", True, "吸收存款及同业存放"),
        ColumnSchema("loan_oth_bank", "float64", True, "拆入资金"),
        ColumnSchema("trading_fl", "float64", True, "交易性金融负债"),
        ColumnSchema("notes_payable", "float64", True, "应付票据"),
        ColumnSchema("acct_payable", "float64", True, "应付账款"),
        ColumnSchema("adv_receipts", "float64", True, "预收款项"),
        ColumnSchema("sold_for_repur_fa", "float64", True, "卖出回购金融资产款"),
        ColumnSchema("comm_payable", "float64", True, "应付手续费及佣金"),
        ColumnSchema("payroll_payable", "float64", True, "应付职工薪酬"),
        ColumnSchema("taxes_payable", "float64", True, "应交税费"),
        ColumnSchema("int_payable", "float64", True, "应付利息"),
        ColumnSchema("div_payable", "float64", True, "应付股利"),
        ColumnSchema("oth_payable", "float64", True, "其他应付款"),
        ColumnSchema("acc_exp", "float64", True, "预提费用"),
        ColumnSchema("deferred_inc", "float64", True, "递延收益"),
        ColumnSchema("st_bonds_payable", "float64", True, "应付短期债券"),
        ColumnSchema("payable_to_reinsurer", "float64", True, "应付分保账款"),
        ColumnSchema("rsrv_insur_cont", "float64", True, "保险合同准备金"),
        ColumnSchema("acting_trading_sec", "float64", True, "代理买卖证券款"),
        ColumnSchema("acting_uw_sec", "float64", True, "代理承销证券款"),
        ColumnSchema("non_cur_liab_due_1y", "float64", True, "一年内到期的非流动负债"),
        ColumnSchema("oth_cur_liab", "float64", True, "其他流动负债"),
        ColumnSchema("total_cur_liab", "float64", True, "流动负债合计"),
        # 非流动负债
        ColumnSchema("bond_payable", "float64", True, "应付债券"),
        ColumnSchema("lt_payable", "float64", True, "长期应付款"),
        ColumnSchema("specific_payables", "float64", True, "专项应付款"),
        ColumnSchema("estimated_liab", "float64", True, "预计负债"),
        ColumnSchema("defer_tax_liab", "float64", True, "递延所得税负债"),
        ColumnSchema("defer_inc_non_cur_liab", "float64", True, "递延收益-非流动负债"),
        ColumnSchema("oth_ncl", "float64", True, "其他非流动负债"),
        ColumnSchema("total_ncl", "float64", True, "非流动负债合计"),
        # 银行/保险特有负债
        ColumnSchema("depos_oth_bfi", "float64", True, "同业和其它金融机构存放款项"),
        ColumnSchema("deriv_liab", "float64", True, "衍生金融负债"),
        ColumnSchema("depos", "float64", True, "吸收存款"),
        ColumnSchema("agency_bus_liab", "float64", True, "代理业务负债"),
        ColumnSchema("oth_liab", "float64", True, "其他负债"),
        ColumnSchema("prem_receiv_adva", "float64", True, "预收保费"),
        ColumnSchema("depos_received", "float64", True, "存入保证金"),
        ColumnSchema("ph_invest", "float64", True, "保户储金及投资款"),
        ColumnSchema("reser_une_prem", "float64", True, "未到期责任准备金"),
        ColumnSchema("reser_outstd_claims", "float64", True, "未决赔款准备金"),
        ColumnSchema("reser_lins_liab", "float64", True, "寿险责任准备金"),
        ColumnSchema("reser_lthins_liab", "float64", True, "长期健康险责任准备金"),
        ColumnSchema("indept_acc_liab", "float64", True, "独立账户负债"),
        ColumnSchema("pledge_borr", "float64", True, "其中:质押借款"),
        ColumnSchema("indem_payable", "float64", True, "应付赔付款"),
        ColumnSchema("policy_div_payable", "float64", True, "应付保单红利"),
        # 负债合计
        ColumnSchema("total_liab", "float64", True, "负债合计"),
        # 股东权益
        ColumnSchema("treasury_share", "float64", True, "减:库存股"),
        ColumnSchema("ordin_risk_reser", "float64", True, "一般风险准备"),
        ColumnSchema("forex_differ", "float64", True, "外币报表折算差额"),
        ColumnSchema("invest_loss_unconf", "float64", True, "未确认的投资损失"),
        ColumnSchema("minority_int", "float64", True, "少数股东权益"),
        ColumnSchema("total_hldr_eqy_exc_min_int", "float64", True, "股东权益合计(不含少数股东权益)"),
        ColumnSchema("total_hldr_eqy_inc_min_int", "float64", True, "股东权益合计(含少数股东权益)"),
        ColumnSchema("total_liab_hldr_eqy", "float64", True, "负债及股东权益总计"),
        # 新增字段
        ColumnSchema("lt_payroll_payable", "float64", True, "长期应付职工薪酬"),
        ColumnSchema("oth_comp_income", "float64", True, "其他综合收益"),
        ColumnSchema("oth_eqt_tools", "float64", True, "其他权益工具"),
        ColumnSchema("oth_eqt_tools_p_shr", "float64", True, "其他权益工具(优先股)"),
        ColumnSchema("lending_funds", "float64", True, "融出资金"),
        ColumnSchema("acc_receivable", "float64", True, "应收款项"),
        ColumnSchema("st_fin_payable", "float64", True, "应付短期融资款"),
        ColumnSchema("payables", "float64", True, "应付款项"),
        ColumnSchema("hfs_assets", "float64", True, "持有待售的资产"),
        ColumnSchema("hfs_sales", "float64", True, "持有待售的负债"),
        ColumnSchema("cost_fin_assets", "float64", True, "以摊余成本计量的金融资产"),
        ColumnSchema("fair_value_fin_assets", "float64", True, "以公允价值计量且其变动计入其他综合收益的金融资产"),
        ColumnSchema("cip_total", "float64", True, "在建工程(合计)(元)"),
        ColumnSchema("oth_pay_total", "float64", True, "其他应付款(合计)(元)"),
        ColumnSchema("long_pay_total", "float64", True, "长期应付款(合计)(元)"),
        ColumnSchema("debt_invest", "float64", True, "债权投资(元)"),
        ColumnSchema("oth_debt_invest", "float64", True, "其他债权投资(元)"),
        ColumnSchema("oth_eq_invest", "float64", True, "其他权益工具投资(元)"),
        ColumnSchema("oth_illiq_fin_assets", "float64", True, "其他非流动金融资产(元)"),
        ColumnSchema("oth_eq_ppbond", "float64", True, "其他权益工具:永续债(元)"),
        ColumnSchema("receiv_financing", "float64", True, "应收款项融资"),
        ColumnSchema("use_right_assets", "float64", True, "使用权资产"),
        ColumnSchema("lease_liab", "float64", True, "租赁负债"),
        ColumnSchema("contract_assets", "float64", True, "合同资产"),
        ColumnSchema("contract_liab", "float64", True, "合同负债"),
        ColumnSchema("accounts_receiv_bill", "float64", True, "应收票据及应收账款"),
        ColumnSchema("accounts_pay", "float64", True, "应付票据及应付账款"),
        ColumnSchema("oth_rcv_total", "float64", True, "其他应收款(合计)（元）"),
        ColumnSchema("fix_assets_total", "float64", True, "固定资产(合计)(元)"),
        ColumnSchema("update_flag", "object", True, "更新标志"),
    ],
)


# 上市公司利润表数据 Schema
IncomeSchema = DataFrameSchema(
    name="income",
    description="上市公司利润表数据",
    columns=[
        ColumnSchema("ts_code", "object", False, "TS代码"),
        ColumnSchema("ann_date", "object", True, "公告日期"),
        ColumnSchema("f_ann_date", "object", True, "实际公告日期"),
        ColumnSchema("end_date", "object", True, "报告期"),
        ColumnSchema("end_date_time", "datetime64[ns]", False, "报告期（时间序列格式）"),
        ColumnSchema("comp_type", "object", True, "公司类型"),
        ColumnSchema("report_type", "object", True, "报表类型"),
        ColumnSchema("end_type", "object", True, "报告期类型"),
        # 每股收益
        ColumnSchema("basic_eps", "float64", True, "基本每股收益"),
        ColumnSchema("diluted_eps", "float64", True, "稀释每股收益"),
        # 收入类字段
        ColumnSchema("total_revenue", "float64", True, "营业总收入"),
        ColumnSchema("revenue", "float64", True, "营业收入"),
        ColumnSchema("int_income", "float64", True, "利息收入"),
        ColumnSchema("prem_earned", "float64", True, "已赚保费"),
        ColumnSchema("comm_income", "float64", True, "手续费及佣金收入"),
        ColumnSchema("n_commis_income", "float64", True, "手续费及佣金净收入"),
        ColumnSchema("n_oth_income", "float64", True, "其他经营净收益"),
        ColumnSchema("n_oth_b_income", "float64", True, "加:其他业务净收益"),
        ColumnSchema("prem_income", "float64", True, "保险业务收入"),
        ColumnSchema("out_prem", "float64", True, "减:分出保费"),
        ColumnSchema("une_prem_reser", "float64", True, "提取未到期责任准备金"),
        ColumnSchema("reins_income", "float64", True, "其中:分保费收入"),
        # 证券业务
        ColumnSchema("n_sec_tb_income", "float64", True, "代理买卖证券业务净收入"),
        ColumnSchema("n_sec_uw_income", "float64", True, "证券承销业务净收入"),
        ColumnSchema("n_asset_mg_income", "float64", True, "受托客户资产管理业务净收入"),
        ColumnSchema("oth_b_income", "float64", True, "其他业务收入"),
        # 投资收益
        ColumnSchema("fv_value_chg_gain", "float64", True, "加:公允价值变动净收益"),
        ColumnSchema("invest_income", "float64", True, "加:投资净收益"),
        ColumnSchema("ass_invest_income", "float64", True, "其中:对联营企业和合营企业的投资收益"),
        ColumnSchema("forex_gain", "float64", True, "加:汇兑净收益"),
        # 成本费用
        ColumnSchema("total_cogs", "float64", True, "营业总成本"),
        ColumnSchema("oper_cost", "float64", True, "减:营业成本"),
        ColumnSchema("int_exp", "float64", True, "减:利息支出"),
        ColumnSchema("comm_exp", "float64", True, "减:手续费及佣金支出"),
        ColumnSchema("biz_tax_surchg", "float64", True, "减:营业税金及附加"),
        ColumnSchema("sell_exp", "float64", True, "减:销售费用"),
        ColumnSchema("admin_exp", "float64", True, "减:管理费用"),
        ColumnSchema("fin_exp", "float64", True, "减:财务费用"),
        ColumnSchema("assets_impair_loss", "float64", True, "减:资产减值损失"),
        # 保险业务
        ColumnSchema("prem_refund", "float64", True, "退保金"),
        ColumnSchema("compens_payout", "float64", True, "赔付总支出"),
        ColumnSchema("reser_insur_liab", "float64", True, "提取保险责任准备金"),
        ColumnSchema("div_payt", "float64", True, "保户红利支出"),
        ColumnSchema("reins_exp", "float64", True, "分保费用"),
        ColumnSchema("oper_exp", "float64", True, "营业支出"),
        ColumnSchema("compens_payout_refu", "float64", True, "减:摊回赔付支出"),
        ColumnSchema("insur_reser_refu", "float64", True, "减:摊回保险责任准备金"),
        ColumnSchema("reins_cost_refund", "float64", True, "减:摊回分保费用"),
        ColumnSchema("other_bus_cost", "float64", True, "其他业务成本"),
        # 利润
        ColumnSchema("operate_profit", "float64", True, "营业利润"),
        ColumnSchema("non_oper_income", "float64", True, "加:营业外收入"),
        ColumnSchema("non_oper_exp", "float64", True, "减:营业外支出"),
        ColumnSchema("nca_disploss", "float64", True, "其中:减:非流动资产处置净损失"),
        ColumnSchema("total_profit", "float64", True, "利润总额"),
        ColumnSchema("income_tax", "float64", True, "所得税费用"),
        ColumnSchema("n_income", "float64", True, "净利润(含少数股东损益)"),
        ColumnSchema("n_income_attr_p", "float64", True, "净利润(不含少数股东损益)"),
        ColumnSchema("minority_gain", "float64", True, "少数股东损益"),
        # 综合收益
        ColumnSchema("oth_compr_income", "float64", True, "其他综合收益"),
        ColumnSchema("t_compr_income", "float64", True, "综合收益总额"),
        ColumnSchema("compr_inc_attr_p", "float64", True, "归属于母公司(或股东)的综合收益总额"),
        ColumnSchema("compr_inc_attr_m_s", "float64", True, "归属于少数股东的综合收益总额"),
        # 关键指标
        ColumnSchema("ebit", "float64", True, "息税前利润"),
        ColumnSchema("ebitda", "float64", True, "息税折旧摊销前利润"),
        # 保险
        ColumnSchema("insurance_exp", "float64", True, "保险业务支出"),
        # 利润分配
        ColumnSchema("undist_profit", "float64", True, "年初未分配利润"),
        ColumnSchema("distable_profit", "float64", True, "可分配利润"),
        # 费用
        ColumnSchema("rd_exp", "float64", True, "研发费用"),
        ColumnSchema("fin_exp_int_exp", "float64", True, "财务费用:利息费用"),
        ColumnSchema("fin_exp_int_inc", "float64", True, "财务费用:利息收入"),
        # 盈余公积转入
        ColumnSchema("transfer_surplus_rese", "float64", True, "盈余公积转入"),
        ColumnSchema("transfer_housing_imprest", "float64", True, "住房周转金转入"),
        ColumnSchema("transfer_oth", "float64", True, "其他转入"),
        ColumnSchema("adj_lossgain", "float64", True, "调整以前年度损益"),
        # 提取
        ColumnSchema("withdra_legal_surplus", "float64", True, "提取法定盈余公积"),
        ColumnSchema("withdra_legal_pubfund", "float64", True, "提取法定公益金"),
        ColumnSchema("withdra_biz_devfund", "float64", True, "提取企业发展基金"),
        ColumnSchema("withdra_rese_fund", "float64", True, "提取储备基金"),
        ColumnSchema("withdra_oth_ersu", "float64", True, "提取任意盈余公积金"),
        ColumnSchema("workers_welfare", "float64", True, "职工奖金福利"),
        ColumnSchema("distr_profit_shrhder", "float64", True, "可供股东分配的利润"),
        # 应付股利
        ColumnSchema("prfshare_payable_dvd", "float64", True, "应付优先股股利"),
        ColumnSchema("comshare_payable_dvd", "float64", True, "应付普通股股利"),
        ColumnSchema("capit_comstock_div", "float64", True, "转作股本的普通股股利"),
        # 新增字段
        ColumnSchema("net_after_nr_lp_correct", "float64", True, "扣除非经常性损益后的净利润（更正前）"),
        ColumnSchema("credit_impa_loss", "float64", True, "信用减值损失"),
        ColumnSchema("net_expo_hedging_benefits", "float64", True, "净敞口套期收益"),
        ColumnSchema("oth_impair_loss_assets", "float64", True, "其他资产减值损失"),
        ColumnSchema("total_opcost", "float64", True, "营业总成本（二）"),
        ColumnSchema("amodcost_fin_assets", "float64", True, "以摊余成本计量的金融资产终止确认收益"),
        ColumnSchema("oth_income", "float64", True, "其他收益"),
        ColumnSchema("asset_disp_income", "float64", True, "资产处置收益"),
        ColumnSchema("continued_net_profit", "float64", True, "持续经营净利润"),
        ColumnSchema("end_net_profit", "float64", True, "终止经营净利润"),
        ColumnSchema("update_flag", "object", True, "更新标志"),
    ],
)


# ===========================
# 申万行业分类 Schema
# ===========================


# 申万行业分类 Schema
SwIndustryClassifySchema = DataFrameSchema(
    name="sw_industry_classify",
    description="申万行业分类",
    columns=[
        ColumnSchema("index_code", "object", False, "指数代码"),
        ColumnSchema("industry_name", "object", False, "行业名称"),
        ColumnSchema("parent_code", "object", True, "父级代码"),
        ColumnSchema("level", "object", False, "行业层级 (L1/L2/L3)"),
        ColumnSchema("industry_code", "object", False, "行业代码"),
        ColumnSchema("is_pub", "object", True, "是否发布指数"),
        ColumnSchema("src", "object", True, "行业分类来源"),
    ],
)


# 申万行业成分股 Schema
SwIndustryMemberSchema = DataFrameSchema(
    name="sw_industry_member",
    description="申万行业成分股",
    columns=[
        ColumnSchema("index_code", "object", False, "指数代码"),
        ColumnSchema("l1_code", "object", False, "一级行业代码"),
        ColumnSchema("l1_name", "object", False, "一级行业名称"),
        ColumnSchema("l2_code", "object", False, "二级行业代码"),
        ColumnSchema("l2_name", "object", False, "二级行业名称"),
        ColumnSchema("l3_code", "object", False, "三级行业代码"),
        ColumnSchema("l3_name", "object", False, "三级行业名称"),
        ColumnSchema("ts_code", "object", False, "成分股票代码"),
        ColumnSchema("name", "object", True, "成分股票名称"),
        ColumnSchema("in_date", "datetime64[ns]", True, "纳入日期"),
        ColumnSchema("out_date", "datetime64[ns]", True, "剔除日期"),
        ColumnSchema("is_new", "object", True, "是否最新Y/N"),
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
