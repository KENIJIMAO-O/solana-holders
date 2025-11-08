-- ===== token_accounts: 每个 account 的当前快照 =====
CREATE TABLE token_accounts (
    acct_pubkey VARCHAR(44) PRIMARY KEY,     -- 若固定长度建议 VARCHAR(n)
    mint_pubkey VARCHAR(44) NOT NULL,
    owner_pubkey VARCHAR(44) NOT NULL,
    balance NUMERIC(38,12) NOT NULL,          -- 若可用 BIGINT 建议改为 BIGINT
    baseline_slot BIGINT,                     -- 允许 NULL，只有 baseline 创建的记录才有此值
    last_updated_slot BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 仅当业务频繁按 mint_pubkey 或 owner_pubkey 查询时创建（会增加写入成本）
CREATE INDEX idx_token_accounts_mint_pubkey ON token_accounts (mint_pubkey);
CREATE INDEX idx_token_accounts_owner_pubkey ON token_accounts (owner_pubkey);


-- ===== holders: (mint, owner) 级别聚合快照 =====
CREATE TABLE holders (
    mint_pubkey VARCHAR(44) NOT NULL,
    owner_pubkey VARCHAR(44) NOT NULL,
    balance NUMERIC(38,12) NOT NULL,
    last_updated_slot BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (mint_pubkey, owner_pubkey)
);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_holders_mint ON holders (mint_pubkey);

-- ===== mint_stats: 每个 mint 的聚合统计 =====
CREATE TABLE mint_stats (
    mint_pubkey TEXT PRIMARY KEY,
    holder_count BIGINT NOT NULL,
    last_updated_slot BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ===== tracked_mints: 统计哪些已经build_baselined的代币 =====
CREATE TABLE tracked_mints (
    mint_pubkey VARCHAR(44) PRIMARY KEY,
    baseline_slot BIGINT,
    status TEXT CHECK(status IN ('baseline_building', 'catching_up', 'synced')),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 对账计划表：
CREATE TABLE reconciliation_schedule (
    mint_pubkey VARCHAR(44) PRIMARY KEY,
    last_reconciliation_time TIMESTAMPTZ NOT NULL,
    last_holder_count BIGINT NOT NULL,
    next_reconciliation_time TIMESTAMPTZ NOT NULL,  -- 计算出的下次对账时间
    current_interval_hours INT NOT NULL,             -- 当前使用的对账间隔（小时）
    -- 统计信息
    total_reconciliations INT DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ===== mint_stats_audit_log: 审计日志，记录 mint_stats 每次变化 =====
CREATE TABLE mint_stats_audit_log (
    id BIGSERIAL PRIMARY KEY,
    mint_pubkey VARCHAR(44) NOT NULL,
    holder_count BIGINT NOT NULL,
    last_updated_slot BIGINT NOT NULL,
    total_balance NUMERIC(78, 0),
    source VARCHAR(20),
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_mint_pubkey ON mint_stats_audit_log(mint_pubkey, recorded_at DESC);
CREATE INDEX idx_audit_slot ON mint_stats_audit_log(last_updated_slot);