-- Migration: 032_create_fund_company_and_fund_manager
-- Description: Persist all Tushare fund_company and fund_manager output fields.

CREATE TABLE IF NOT EXISTS fund_company (
    name VARCHAR(300) PRIMARY KEY,
    shortname VARCHAR(200),
    short_enname VARCHAR(200),
    province VARCHAR(100),
    city VARCHAR(100),
    address TEXT,
    phone VARCHAR(100),
    office TEXT,
    website TEXT,
    chairman VARCHAR(200),
    manager VARCHAR(200),
    reg_capital DECIMAL(20,6),
    setup_date DATE,
    end_date DATE,
    employees DECIMAL(20,6),
    main_business TEXT,
    org_code VARCHAR(100),
    credit_code VARCHAR(100),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fund_company_province ON fund_company(province);
CREATE INDEX IF NOT EXISTS idx_fund_company_city ON fund_company(city);
CREATE INDEX IF NOT EXISTS idx_fund_company_shortname ON fund_company(shortname);
COMMENT ON TABLE fund_company IS 'Tushare 公募基金管理人信息表';

CREATE TABLE IF NOT EXISTS fund_manager (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    name VARCHAR(200) NOT NULL,
    gender VARCHAR(10),
    birth_year VARCHAR(20),
    edu VARCHAR(100),
    nationality VARCHAR(100),
    begin_date DATE NOT NULL,
    end_date DATE,
    resume TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, ann_date, name, begin_date)
);

CREATE INDEX IF NOT EXISTS idx_fund_manager_name ON fund_manager(name);
CREATE INDEX IF NOT EXISTS idx_fund_manager_ann_date ON fund_manager(ann_date);
COMMENT ON TABLE fund_manager IS 'Tushare 公募基金经理任职与简历信息表';
