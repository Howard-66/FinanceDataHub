Ready for review
Select text to add comments on the plan
行业差异化估值指标预处理方案
背景与目标
问题
ValueInvesting项目的智能选股模块统一使用PE作为估值指标，但PE并不适用于所有行业：

银行股更适合用PB
成长股更适合用PEG
亏损企业需要用PS
某些行业有指标豁免规则（如lithium_check豁免负债检查）
目标
在FinanceDataHub中增加行业-aware的估值预处理，为ValueInvesting提供：

每只股票的核心估值指标（根据行业配置自动选择）
历史分位数（自身历史 + 行业内相对）
统一的空值处理（NULL表示无效）
日频更新，与现有预处理数据保持一致
数据库设计
新增表: processed_industry_valuation
CREATE TABLE IF NOT EXISTS processed_industry_valuation (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    -- 行业信息
    l1_name VARCHAR(100),           -- 一级行业名称
    l2_name VARCHAR(100),           -- 二级行业名称（关联industry_config的key）
    l3_name VARCHAR(100),           -- 三级行业名称

    -- 核心指标（根据行业配置动态选择）
    core_indicator_type VARCHAR(10) NOT NULL,  -- PE/PB/PS/PEG
    core_indicator_value NUMERIC(18, 6),       -- 指标值，无效时为NULL
    core_indicator_pct_1250d NUMERIC(8, 4),    -- 自身历史5年分位 (0-100)
    core_indicator_industry_pct NUMERIC(8, 4), -- 行业内相对分位 (0-100)

    -- 参考指标
    ref_indicator_type VARCHAR(10),            -- PE/PB/PS/PEG
    ref_indicator_value NUMERIC(18, 6),        -- 参考指标值
    ref_indicator_pct_1250d NUMERIC(8, 4),     -- 参考指标自身历史分位
    ref_indicator_industry_pct NUMERIC(8, 4),  -- 参考指标行业内分位

    -- 其他常用估值指标（便于对比）
    pe_ttm NUMERIC(18, 6),            -- PE_TTM（原始值）
    pb NUMERIC(18, 6),                -- PB
    ps_ttm NUMERIC(18, 6),            -- PS_TTM
    peg NUMERIC(18, 6),               -- PEG
    dv_ttm NUMERIC(18, 6),            -- 股息率

    -- 豁免标记
    is_exempted BOOLEAN DEFAULT FALSE,         -- 是否有指标豁免
    exemption_reason VARCHAR(100),             -- 豁免原因

    -- 元数据
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, time)
);

-- 创建超表
SELECT create_hypertable('processed_industry_valuation', 'time',
    chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);

-- 索引
CREATE INDEX idx_industry_val_symbol ON processed_industry_valuation (symbol);
CREATE INDEX idx_industry_val_time ON processed_industry_valuation (time DESC);
CREATE INDEX idx_industry_val_l2 ON processed_industry_valuation (l2_name, time);
实现方案
Phase 1: 核心计算模块
1.1 行业配置加载器 (preprocessing/fundamental/industry_config.py)
class IndustryConfigLoader:
    """行业配置加载器，管理industry_config.json的访问"""

    def __init__(self, config_path: str = "industry_config.json"):
        self.config = self._load_config(config_path)
        self._build_index()

    def get_industry_config(self, l2_name: str) -> Dict:
        """获取行业配置，未配置返回默认值"""
        return self.config.get(l2_name, {
            "core_indicator": "PE",
            "ref_indicator": "PB",
            "exemptions": []
        })

    def get_core_indicator(self, l2_name: str) -> str:
        """获取核心指标类型"""
        return self.get_industry_config(l2_name).get("core_indicator", "PE")

    def get_ref_indicator(self, l2_name: str) -> str:
        """获取参考指标类型"""
        return self.get_industry_config(l2_name).get("ref_indicator", "PB")
1.2 行业分位计算器 (preprocessing/fundamental/industry_valuation.py)
class IndustryValuationCalculator:
    """
    行业差异化估值计算器

    功能：
    1. 根据行业配置选择核心/参考指标
    2. 计算自身历史分位和行业相对分位
    3. 处理空值和豁免情况
    """

    INDICATOR_MAP = {
        "PE": "pe_ttm",
        "PB": "pb",
        "PS": "ps_ttm",
        "PEG": "peg"
    }

    def __init__(self, industry_config_path: str = None):
        self.config_loader = IndustryConfigLoader(industry_config_path)

    def calculate(
        self,
        valuation_df: pd.DataFrame,           # processed_valuation_pct数据
        industry_members_df: pd.DataFrame     # sw_industry_member数据
    ) -> pd.DataFrame:
        """
        计算行业差异化估值

        Args:
            valuation_df: 包含pe_ttm, pb, ps_ttm, peg的日频数据
            industry_members_df: 包含ts_code, l2_name的行业分类数据

        Returns:
            包含core_indicator_*和industry_pct列的DataFrame
        """
        # 1. 合并行业分类
        merged = valuation_df.merge(
            industry_members_df[["ts_code", "l1_name", "l2_name", "l3_name"]],
            left_on="symbol",
            right_on="ts_code",
            how="left"
        )

        # 2. 为每行确定core_indicator和ref_indicator
        merged["core_indicator_type"] = merged["l2_name"].map(
            lambda x: self.config_loader.get_core_indicator(x)
        )
        merged["ref_indicator_type"] = merged["l2_name"].map(
            lambda x: self.config_loader.get_ref_indicator(x)
        )

        # 3. 提取核心指标值
        merged["core_indicator_value"] = merged.apply(
            lambda row: self._get_indicator_value(row, "core"), axis=1
        )
        merged["ref_indicator_value"] = merged.apply(
            lambda row: self._get_indicator_value(row, "ref"), axis=1
        )

        # 4. 计算自身历史分位（复用ValuationPercentile逻辑）
        merged["core_indicator_pct_1250d"] = self._calc_self_percentile(
            merged, "core_indicator_value"
        )
        merged["ref_indicator_pct_1250d"] = self._calc_self_percentile(
            merged, "ref_indicator_value"
        )

        # 5. 计算行业内相对分位
        merged["core_indicator_industry_pct"] = self._calc_industry_percentile(
            merged, "core_indicator_value", "l2_name"
        )
        merged["ref_indicator_industry_pct"] = self._calc_industry_percentile(
            merged, "ref_indicator_value", "l2_name"
        )

        # 6. 标记豁免情况
        merged["is_exempted"] = merged["core_indicator_value"].isna()
        merged["exemption_reason"] = merged.apply(self._get_exemption_reason, axis=1)

        return merged

    def _get_indicator_value(self, row: pd.Series, indicator_type: str) -> Optional[float]:
        """获取指标值，处理空值和无效值"""
        indicator_name = row[f"{indicator_type}_indicator_type"]
        col_name = self.INDICATOR_MAP.get(indicator_name)

        if not col_name or col_name not in row:
            return None

        value = row[col_name]

        # PE/PB/PS必须>0才有效，PEG可以为负但要有意义
        if indicator_name in ["PE", "PB", "PS"] and (pd.isna(value) or value <= 0):
            return None
        if indicator_name == "PEG" and pd.isna(value):
            return None

        return value

    def _calc_industry_percentile(
        self,
        df: pd.DataFrame,
        value_col: str,
        industry_col: str
    ) -> pd.Series:
        """计算行业内相对分位（按l2_name分组）"""
        def calc_group_percentile(group):
            values = group[value_col]
            valid = values.dropna()

            if len(valid) < 2:
                return pd.Series([np.nan] * len(group), index=group.index)

            # 计算每个值在组内的分位
            ranks = valid.rank(pct=True) * 100
            return ranks.reindex(group.index)

        return df.groupby(industry_col, group_keys=False).apply(calc_group_percentile)
Phase 2: 存储层扩展
2.1 修改 preprocessing/storage.py
新增 IndustryValuationStorage 类：

class IndustryValuationStorage:
    """行业差异化估值数据存储"""

    TABLE_NAME = "processed_industry_valuation"

    COLUMNS = [
        "time", "symbol", "l1_name", "l2_name", "l3_name",
        "core_indicator_type", "core_indicator_value",
        "core_indicator_pct_1250d", "core_indicator_industry_pct",
        "ref_indicator_type", "ref_indicator_value",
        "ref_indicator_pct_1250d", "ref_indicator_industry_pct",
        "pe_ttm", "pb", "ps_ttm", "peg", "dv_ttm",
        "is_exempted", "exemption_reason", "processed_at"
    ]

    async def upsert(self, df: pd.DataFrame, batch_size: int = 10000) -> int:
        """批量插入/更新"""
        # UPSERT逻辑类似FundamentalDataStorage
        pass

    async def query(
        self,
        symbols: Optional[List[str]] = None,
        l2_names: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """查询行业估值数据"""
        pass
Phase 3: CLI扩展
3.1 修改 cli/preprocess.py
新增 industry_valuation 预处理类别：

# 执行行业估值预处理
fdh-cli preprocess run --category industry_valuation --all

# 指定日期范围
fdh-cli preprocess run --category industry_valuation --start-date 2024-01-01
预处理类别映射更新：

industry_valuation: 行业差异化估值计算
Phase 4: SDK扩展
4.1 修改 sdk.py
新增接口：

async def get_industry_valuation_async(
    self,
    symbols: Optional[List[str]] = None,
    l2_names: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    include_all_indicators: bool = False
) -> pd.DataFrame:
    """
    获取行业差异化估值数据

    Args:
        symbols: 股票代码列表
        l2_names: 二级行业名称列表（可选，用于筛选特定行业）
        start_date: 开始日期
        end_date: 结束日期
        include_all_indicators: 是否包含所有估值指标（PE/PB/PS/PEG）

    Returns:
        DataFrame包含:
        - symbol, time: 股票代码和日期
        - l2_name: 二级行业名称
        - core_indicator_type: 核心指标类型(PE/PB/PS/PEG)
        - core_indicator_value: 核心指标值
        - core_indicator_pct_1250d: 自身历史5年分位
        - core_indicator_industry_pct: 行业内相对分位
        - ref_indicator_*: 参考指标相关列
        - is_exempted: 是否有豁免
        - exemption_reason: 豁免原因
    """
同步版本：

def get_industry_valuation(self, ...) -> pd.DataFrame:
    return self._run_sync(self.get_industry_valuation_async(...))
关键文件清单
新建文件
finance_data_hub/preprocessing/fundamental/industry_config.py - 行业配置加载器
finance_data_hub/preprocessing/fundamental/industry_valuation.py - 行业估值计算器
sql/init/017_create_industry_valuation.sql - 数据库表结构
修改文件
finance_data_hub/preprocessing/storage.py - 新增IndustryValuationStorage类
finance_data_hub/preprocessing/pipeline.py - 集成行业估值预处理
finance_data_hub/cli/preprocess.py - 新增industry_valuation类别支持
finance_data_hub/sdk.py - 新增get_industry_valuation接口
CLAUDE.md - 更新文档
验证方案
单元测试
def test_industry_valuation_calculator():
    calc = IndustryValuationCalculator()

    # 测试银行股使用PB
    bank_data = create_test_data(l2_name="银行", pb=1.0, pe_ttm=5.0)
    result = calc.calculate(bank_data, industry_df)
    assert result.iloc[0]["core_indicator_type"] == "PB"

    # 测试亏损股PE为NULL
    loss_data = create_test_data(l2_name="半导体", pe_ttm=-10.0, ps_ttm=5.0)
    result = calc.calculate(loss_data, industry_df)
    assert result.iloc[0]["core_indicator_type"] == "PE"
    assert pd.isna(result.iloc[0]["core_indicator_value"])
    assert result.iloc[0]["is_exempted"] == True
集成测试
# 1. 执行预处理
fdh-cli preprocess run --category industry_valuation --symbols 600519.SH,000001.SZ

# 2. 查询数据验证
python -c "
from finance_data_hub import FinanceDataHub
fdh = FinanceDataHub()
df = fdh.get_industry_valuation(['600519.SH', '000001.SZ'])
print(df[['symbol', 'l2_name', 'core_indicator_type', 'core_indicator_value', 'core_indicator_pct_1250d']])
"
ValueInvesting项目使用示例
from finance_data_hub import FinanceDataHub

fdh = FinanceDataHub()

# 获取所有股票的适配估值数据
valuation = await fdh.get_industry_valuation_async(
    start_date='2024-01-01',
    end_date='2024-12-31'
)

# 筛选低估股票（核心指标分位<20）
undervalued = valuation[
    (valuation['core_indicator_pct_1250d'] < 20) &
    (~valuation['is_exempted'])  # 排除豁免数据
]

# 按行业分组查看
for l2_name, group in undervalued.groupby('l2_name'):
    print(f"{l2_name}: 核心指标={group.iloc[0]['core_indicator_type']}, "
          f"低估股票数={len(group)}")
Add Comment