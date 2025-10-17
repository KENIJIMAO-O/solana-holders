use crate::baseline::getProgramAccounts::TokenHolder;
use crate::database::postgresql::DatabaseConnection;
use crate::repositories::events::Event;
use crate::utils::timer::TaskLogger;
use anyhow::Error;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use tracing::warn; // 或者你使用的其他日志库
use yellowstone_grpc_proto::tonic::async_trait;

// 这是我们准备写入 holders 表的聚合后数据的结构体
#[derive(Debug)]
pub struct HolderUpsertData {
    pub mint_pubkey: String,
    pub owner_pubkey: String,
    pub balance: Decimal,
    pub last_updated_slot: i64,
}

/// establish_holders_baseline 函数聚合需要，将同一个 holder 对于 同一种代币且不同token_account中的余额聚合
/// 将 TokenHolder 列表按 (mint, owner) 分组，并聚合余额和 slot
pub fn aggregate_token_holders(token_accounts: &[TokenHolder]) -> Vec<HolderUpsertData> {
    // 使用 HashMap 来进行聚合
    // Key: (mint_pubkey, owner_pubkey)
    // Value: (aggregated_balance, max_slot)
    let mut aggregation_map: HashMap<String, Decimal> = HashMap::new();

    for account in token_accounts {
        let balance = match Decimal::from_str(&account.balance) {
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
        let mut entry = aggregation_map
            .entry(account.owner.clone())
            .or_insert(Decimal::ZERO); // 如果 key 不存在，则插入一个默认值

        // 累加余额
        entry += balance;
    }

    // 将聚合后的 HashMap 转换为 Vec<HolderUpsertData>
    let results: Vec<HolderUpsertData> = aggregation_map
        .into_iter()
        .map(|(owner_pubkey, balance)| HolderUpsertData {
            mint_pubkey: token_accounts[0].mint.clone(),
            owner_pubkey,
            balance,
            last_updated_slot: token_accounts[0].slot,
        })
        .collect();

    results
}

pub fn aggregate_events(events: &[Event]) -> Vec<HolderUpsertData> {
    let mut aggregation_map: HashMap<(String, String), (Decimal, i64)> = HashMap::new();

    for event in events {
        let delta = event.delta;

        let mut entry = aggregation_map
            .entry((event.mint_pubkey.clone(), event.owner_pubkey.clone()))
            .or_insert((Decimal::ZERO, 0));

        entry.0 += delta;
        entry.1 = entry.1.max(event.slot);
    };

    let results: Vec<HolderUpsertData> = aggregation_map
        .into_iter()
        .map(|((mint, owner), (delta, lastest_slot))| HolderUpsertData{
            mint_pubkey: mint,
            owner_pubkey: owner,
            balance: delta,
            last_updated_slot:lastest_slot
        })
    .collect();

    results
}

#[async_trait]
pub trait HoldersRepository {
    /// baseline时使用
    async fn establish_holders_baseline(
        &self,
        token_accounts: &[TokenHolder],
        logger: &mut TaskLogger,
    ) -> Result<usize, Error>;

    /// 监听时更新用户余额
    /// 返回：每个 mint 的 balance > 0 的 holder 数量 HashMap<mint_pubkey, holder_count>
    async fn upsert_holder_batch(&self, events: &[Event]) -> Result<Vec<(String, i64)>, Error>;
}

#[async_trait]
impl HoldersRepository for DatabaseConnection {
    async fn establish_holders_baseline(
        &self,
        token_accounts: &[TokenHolder],
        logger: &mut TaskLogger,
    ) -> Result<usize, Error> {
        // 1. 调用聚合函数，得到处理好的数据
        let aggregated_holders = aggregate_token_holders(token_accounts);
        if aggregated_holders.is_empty() {
            return Err(anyhow::anyhow!("empty token accounts"));
        }

        let mut tx = self.pool.begin().await?;

        let mint_pubkeys = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.mint_pubkey.clone())
            .collect::<Vec<_>>();

        let owner_pubkeys = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.owner_pubkey.clone())
            .collect::<Vec<_>>();

        let balances: Vec<String> = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.balance.to_string())
            .collect::<Vec<_>>();

        let last_updated_slots: Vec<i64> = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.last_updated_slot as i64)
            .collect();

        sqlx::query!(
            r#"
            INSERT INTO holders
                (mint_pubkey, owner_pubkey, balance, last_updated_slot)
            SELECT mint_pubkey, owner_pubkey, balance::numeric, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::text[], $4::bigint[])
                AS t(mint_pubkey, owner_pubkey, balance, last_updated_slot)
            ON CONFLICT(mint_pubkey, owner_pubkey)
            DO UPDATE SET
               balance = EXCLUDED.balance,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            "#,
            &mint_pubkeys,
            &owner_pubkeys,
            &balances,
            &last_updated_slots,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let holder_account = owner_pubkeys.len();
        Ok(holder_account)
    }

    // 在monitor阶段，接受不断产生的events，去更新数据库中数据
    // 返回每个 mint 的 balance > 0 的 holder 数量
    async fn upsert_holder_batch(&self, events: &[Event]) -> Result<Vec<(String, i64)>, Error> {
        if events.is_empty() {
            return Ok(vec![]);
        }

        // 聚合数据
        let aggregate_datas = aggregate_events(events);

        // 收集所有需要的字段，包括 mint_pubkey
        let mint_pubkeys: Vec<String> = aggregate_datas
            .iter()
            .map(|event| event.mint_pubkey.clone())
            .collect();
        let owner_pubkeys: Vec<String> = aggregate_datas
            .iter()
            .map(|event| event.owner_pubkey.clone())
            .collect();
        let deltas: Vec<String> = aggregate_datas.iter().map(|event| event.balance.to_string()).collect();
        let last_updated_slots: Vec<i64> = aggregate_datas.iter().map(|event| event.last_updated_slot).collect();

        let mut tx = self.pool.begin().await?;

        // 1. 执行 UPSERT 更新余额
        sqlx::query!(
            r#"
            INSERT INTO holders (mint_pubkey, owner_pubkey, balance, last_updated_slot)
            SELECT mint_pubkey, owner_pubkey, delta::numeric, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::text[], $4::bigint[])
                AS t(mint_pubkey, owner_pubkey, delta, last_updated_slot)
            ON CONFLICT (mint_pubkey, owner_pubkey)
            DO UPDATE SET
                balance = holders.balance + EXCLUDED.balance,  -- 累加余额
                last_updated_slot = EXCLUDED.last_updated_slot,
                updated_at = now()
            WHERE holders.last_updated_slot < EXCLUDED.last_updated_slot  -- 时效性检查
            "#,
            &mint_pubkeys,
            &owner_pubkeys,
            &deltas,
            &last_updated_slots,
        )
        .execute(&mut *tx)
        .await?;

        // 2.删除余额为0的holder数据
        let delete_result = sqlx::query!(
            r#"
      DELETE FROM holders AS h
      USING UNNEST($1::varchar[], $2::varchar[]) AS t(mint_pubkey, owner_pubkey)
      WHERE h.mint_pubkey = t.mint_pubkey
        AND h.owner_pubkey = t.owner_pubkey
        AND h.balance = 0
      "#,
            &mint_pubkeys,
            &owner_pubkeys
        )
        .execute(&mut *tx)
        .await?;

        // 3. 统计每个 mint 的 holder 数量（balance > 0）
        let unique_mints: HashSet<String> = events
            .iter()
            .map(|event| event.mint_pubkey.clone())
            .collect();

        let mut holder_counts: Vec<(String, i64)> = Vec::new();

        for mint in unique_mints {
            let count = sqlx::query_scalar!(
                r#"
                SELECT COUNT(*) as "count!"
                FROM holders
                WHERE mint_pubkey = $1 AND balance > 0
                "#,
                mint
            )
            .fetch_one(&mut *tx)
            .await?;

            holder_counts.push((mint, count));
        }

        tx.commit().await?; // 提交事务

        Ok(holder_counts)
    }
}

#[cfg(test)]
mod tests {
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
            Event{
                slot: 1,
                tx_sig: "tx1".to_string(),
                mint_pubkey: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                account_pubkey: "F3nV5qfyKJjgVg1vnnkDWwxkr9W1C2MpTTJwzcqCi53k".to_string(),
                owner_pubkey: "G7yFPLBVcToFpz5cgmWCNjcAygVzT4m9VnX1FwCa3zqY".to_string(),
                delta: Decimal::from_str("1").unwrap(),
                confirmed: false,
            },
            Event{
                slot: 5,
                tx_sig: "tx2".to_string(),
                mint_pubkey: "DrZ26cKJDksVRWib3DVVsjo9eeXccc7hKhDJviiYEEZY".to_string(),
                account_pubkey: "".to_string(),
                owner_pubkey: "G7yFPLBVcToFpz5cgmWCNjcAygVzT4m9VnX1FwCa3zqY".to_string(),
                delta: Decimal::from_str("-2").unwrap(),
                confirmed: false,
            },
        ];
        let res = aggregate_events(&events);
        println!("res: {:?}", res);
    }
}
