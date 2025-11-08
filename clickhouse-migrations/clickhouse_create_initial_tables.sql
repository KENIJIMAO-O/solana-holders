CREATE TABLE events
(
    -- 1. ClickHouse 的 slot 应该是 UInt64 (如果你确定它非负)
    `slot` UInt64,

    -- 2. TEXT 和 VARCHAR(44) 都转换为 String
    `tx_sig` String,
    `mint_pubkey` String,
    `account_pubkey` String,
    `owner_pubkey` String,

    -- 3. NUMERIC(38,12) 完美对应 Decimal(38, 12)
    `delta` Decimal(38, 12),

    -- 4. BOOLEAN 转换为 UInt8, DEFAULT FALSE 变为 DEFAULT 0
    `confirmed` UInt8 DEFAULT 0,

    -- 5. 我们需要一个版本字段 (或时间字段) 来让 ReplacingMergeTree 工作
    `_timestamp` DateTime DEFAULT now()
)
    ENGINE = ReplacingMergeTree(`_timestamp`) -- 关键点 1
        PARTITION BY intDiv(slot, 1000000) -- 关键点 2
        ORDER BY (tx_sig, account_pubkey) -- 关键点 3
-- 关键点 4: 你的 "id" 和 "CONSTRAINT" 都被移除了
;
