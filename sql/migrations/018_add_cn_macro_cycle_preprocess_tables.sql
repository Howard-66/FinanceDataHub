-- =====================================================
-- 新增中国宏观周期预处理表
-- =====================================================

CREATE TABLE IF NOT EXISTS processed_cn_macro_cycle_phase (
    time TIMESTAMPTZ NOT NULL,
    observation_time TIMESTAMPTZ NOT NULL,
    m2_yoy DECIMAL(10,4),
    gdp_yoy DECIMAL(10,4),
    ppi_yoy DECIMAL(10,4),
    pmi DECIMAL(10,4),
    credit_impulse DECIMAL(10,4),
    raw_phase VARCHAR(20) NOT NULL,
    stable_phase VARCHAR(20) NOT NULL,
    raw_phase_changed BOOLEAN DEFAULT FALSE,
    stable_phase_changed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time)
);

SELECT create_hypertable('processed_cn_macro_cycle_phase', 'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 year'
);

CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_phase_observation
    ON processed_cn_macro_cycle_phase (observation_time DESC);
CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_phase_raw
    ON processed_cn_macro_cycle_phase (raw_phase, time DESC);
CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_phase_stable
    ON processed_cn_macro_cycle_phase (stable_phase, time DESC);

COMMENT ON TABLE processed_cn_macro_cycle_phase IS '中国宏观周期月度主表，保存观测月份、生效月份、信用脉冲与 raw/stable 阶段';


CREATE TABLE IF NOT EXISTS processed_cn_macro_cycle_industry (
    time TIMESTAMPTZ NOT NULL,
    observation_time TIMESTAMPTZ NOT NULL,
    l1_code VARCHAR(20),
    l1_name VARCHAR(100),
    l2_code VARCHAR(20),
    l2_name VARCHAR(100),
    l3_code VARCHAR(20),
    l3_name VARCHAR(100) NOT NULL,
    config_macro_cycle VARCHAR(20),
    core_indicator VARCHAR(10),
    ref_indicator VARCHAR(10),
    logic TEXT,
    fscore_exemptions JSONB,
    is_present_in_sw_member BOOLEAN DEFAULT FALSE,
    matches_raw_phase BOOLEAN DEFAULT FALSE,
    matches_stable_phase BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, l3_name)
);

SELECT create_hypertable('processed_cn_macro_cycle_industry', 'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 year'
);

CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_industry_l3
    ON processed_cn_macro_cycle_industry (l3_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_industry_config_cycle
    ON processed_cn_macro_cycle_industry (config_macro_cycle, time DESC);
CREATE INDEX IF NOT EXISTS idx_cn_macro_cycle_industry_stable_match
    ON processed_cn_macro_cycle_industry (matches_stable_phase, time DESC);

COMMENT ON TABLE processed_cn_macro_cycle_industry IS '中国宏观周期月度行业快照表，按三级行业保存配置周期与当月匹配结果';
