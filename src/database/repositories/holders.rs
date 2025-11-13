use rust_decimal::Decimal;
use std::collections::{HashMap};
use std::str::FromStr;
use tracing::warn;
use crate::baseline::get_program_accounts::TokenHolder;
// 或者你使用的其他日志库
use crate::clickhouse::clickhouse::Event;
use crate::clickhouse::helper::ClickhouseDecimal;


// 这是我们准备写入 holders 表的聚合后数据的结构体
#[derive(Debug)]
pub struct HolderUpsertData {
    pub mint_pubkey: String,
    pub owner_pubkey: String,
    pub balance: ClickhouseDecimal,
    pub last_updated_slot: i64,
}

/// establish_holders_baseline 函数聚合需要，将同一个 holder 对于 同一种代币且不同token_account中的余额聚合
/// 将 TokenHolder 列表按 (mint, owner) 分组，并聚合余额和 slot
pub fn aggregate_token_holders(token_accounts: &[TokenHolder]) -> Vec<HolderUpsertData> {
    // 使用 HashMap 来进行聚合
    // Key: (mint_pubkey, owner_pubkey)
    // Value: (aggregated_balance, max_slot)
    let mut aggregation_map: HashMap<&str, ClickhouseDecimal> = HashMap::new();

    for account in token_accounts {
        let balance = match ClickhouseDecimal::from_str(&account.balance) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    "Failed to parse balance '{}' for account {}. Skipping. Error: {}",
                    account.balance, account.pubkey, e
                );
                continue; // 跳过这条错误数据
            }
        };

        // 使用 HashMap 的 entry API，这是最优雅和高效的方式
        let entry = aggregation_map
            .entry(&account.owner)
            .or_insert(ClickhouseDecimal::from_int(0)); // 如果 key 不存在，则插入一个默认值

        // 累加余额
        *entry = *entry + balance;
    }

    // 将聚合后的 HashMap 转换为 Vec<HolderUpsertData>
    let results: Vec<HolderUpsertData> = aggregation_map
        .into_iter()
        .map(|(owner_pubkey, balance)| HolderUpsertData {
            mint_pubkey: token_accounts[0].mint.clone(),
            owner_pubkey: owner_pubkey.to_string(),
            balance,
            last_updated_slot: token_accounts[0].slot,
        })
        .collect();

    results
}

pub fn aggregate_events(events: &[Event]) -> Vec<HolderUpsertData> {
    let mut aggregation_map: HashMap<(String, String), (ClickhouseDecimal, u64)> = HashMap::new();

    for event in events {
        let delta = event.delta;

        let mut entry = aggregation_map
            .entry((event.mint_pubkey.clone(), event.owner_pubkey.clone()))
            .or_insert((ClickhouseDecimal::from_decimal(Decimal::from(0)), 0));
        
        // todo!: 注意
        entry.0 = entry.0 + delta;
        entry.1 = entry.1.max(event.slot);
    }

    let results: Vec<HolderUpsertData> = aggregation_map
        .into_iter()
        .map(|((mint, owner), (delta, lastest_slot))| HolderUpsertData {
            mint_pubkey: mint,
            owner_pubkey: owner,
            balance: delta,
            last_updated_slot: lastest_slot as i64,
        })
        .collect();

    results
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use super::*;
    #[test]
    fn test_aggregate_token_holders() {
        let token_holders = vec![
            TokenHolder {
                slot: 1,
                mint: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                owner: "J6dSfXD8fq4CRZDYht2nCA6KkE4vt6mxFSs7aRA39zmS".to_string(),
                pubkey: "A7hfL4RJVSJEa1GQ43ek9wnfv77jthGmhBBFwNEA9gQQ".to_string(),
                balance: "23.5".to_string(),
                decimals: 0,
            },
            TokenHolder {
                slot: 1,
                mint: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                owner: "J6dSfXD8fq4CRZDYht2nCA6KkE4vt6mxFSs7aRA39zmS".to_string(),
                pubkey: "2kaeYMG4v6Vpv7j4ryRMUUX5axnMuZ4J16BvbcFQPiyj".to_string(),
                balance: "23.5".to_string(),
                decimals: 0,
            },
            TokenHolder {
                slot: 1,
                mint: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                owner: "G7yFPLBVcToFpz5cgmWCNjcAygVzT4m9VnX1FwCa3zqY".to_string(),
                pubkey: "F3nV5qfyKJjgVg1vnnkDWwxkr9W1C2MpTTJwzcqCi53k".to_string(),
                balance: "234545.545454".to_string(),
                decimals: 0,
            },
        ];
        let holders_upsert_data = aggregate_token_holders(&token_holders);
        println!("{:?}", holders_upsert_data);
    }

    #[test]
    fn test_aggregate_events() {
        let events = vec![
            Event {
                slot: 1,
                tx_sig: "tx1".to_string(),
                mint_pubkey: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                account_pubkey: "F3nV5qfyKJjgVg1vnnkDWwxkr9W1C2MpTTJwzcqCi53k".to_string(),
                owner_pubkey: "G7yFPLBVcToFpz5cgmWCNjcAygVzT4m9VnX1FwCa3zqY".to_string(),
                delta: ClickhouseDecimal::from_f64(1.2),
                confirmed: 0,
                _timestamp: Utc::now()
            },
            Event {
                slot: 5,
                tx_sig: "tx2".to_string(),
                mint_pubkey: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                account_pubkey: "".to_string(),
                owner_pubkey: "G7yFPLBVcToFpz5cgmWCNjcAygVzT4m9VnX1FwCa3zqY".to_string(),
                delta: ClickhouseDecimal::from_f64(5.2),
                confirmed: 0,
                _timestamp: Utc::now()
            },
        ];
        let res = aggregate_events(&events);
        println!("res: {:?}", res);
    }
}
