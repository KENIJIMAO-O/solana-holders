use std::collections::HashSet;
use yellowstone_grpc_proto::tonic::async_trait;
use crate::baseline::get_program_accounts::TokenHolder;
use crate::clickhouse::clickhouse::{ClickHouse, Event};
use crate::clickhouse::helper::ClickhouseDecimal;
use crate::database::postgresql::DatabaseConnection;
use crate::database::repositories::token_accounts::aggregate_token_account_events;
use crate::error::{DatabaseError, Result};
use crate::{app_info, app_debug};

pub mod holders;
pub mod mint_stats;
pub mod reconciliation_schedule;
pub mod token_accounts;
pub mod tracked_mints;

/// token_accounts 聚合数据结构
#[derive(Debug, Clone)]
pub struct TokenAccountAggregateData {
    pub acct_pubkey: String,
    pub mint_pubkey: String,
    pub owner_pubkey: String,
    pub delta: ClickhouseDecimal,  // 聚合后的 delta
    pub last_updated_slot: i64,    // 最大 slot
}

/// 该特征的作用是保证，在对token_accounts, holders, mint_stats这三张表更新时，必须保证更新的原子性
#[async_trait]
pub trait AtomicityData {
    /// 原子性更新三张表（用于实时监听的 synced mint）
    async fn upsert_synced_mints_atomic(&self, synced_events: &[Event], clickhouse: &ClickHouse) -> Result<()>;

    /// 原子性建立 baseline（用于初次构建 baseline）
    /// 返回 baseline_slot
    async fn establish_baseline_atomic(
        &self,
        mint: &str,
        token_accounts: Vec<TokenHolder>,
    ) -> Result<i64>;
}

/// 受内存爆炸问题困扰，下面需要对两个函数进行分块处理（流式处理暂不可用，只有getProgramAccountV2支持，但是我们不可能全部使用helius节点）
/// 对于upsert_synced_mints_atomic函数，在上游已经进行了分块处理
#[async_trait]
impl AtomicityData for DatabaseConnection {
    async fn upsert_synced_mints_atomic(
        &self,
        synced_events: &[Event],
        clickhouse: &ClickHouse
    ) -> Result<()> {
        if synced_events.is_empty() {
            return Ok(());
        }

        app_info!("start upserting synced mints atomic");
        let mut tx = self.pool.begin().await
            .map_err(|e| DatabaseError::TransactionFailed(
                format!("upsert_synced_mints_atomic: {:?}", e)
            ))?;

        // 1.0 聚合 token_accounts 数据（避免同一个 account 多次更新）
        let aggregated_accounts = aggregate_token_account_events(synced_events);

        // 优化：单次迭代构建所有 Vec，避免多次遍历
        let capacity = aggregated_accounts.len();
        let mut account_pubkeys = Vec::with_capacity(capacity);
        let mut mint_pubkeys = Vec::with_capacity(capacity);
        let mut owner_pubkeys = Vec::with_capacity(capacity);
        let mut balances = Vec::with_capacity(capacity);
        let mut slots = Vec::with_capacity(capacity);

        for account in aggregated_accounts {
            account_pubkeys.push(account.acct_pubkey);
            mint_pubkeys.push(account.mint_pubkey);
            owner_pubkeys.push(account.owner_pubkey);
            balances.push(account.delta.to_string());
            slots.push(account.last_updated_slot);
        }

        app_info!("start upserting token_accounts");
        // 1.1 upsert token_accounts（实时事件的 baseline_slot 为 NULL）
        sqlx::query!(
            r#"
            INSERT INTO token_accounts
                (acct_pubkey, mint_pubkey, owner_pubkey, balance, baseline_slot, last_updated_slot)
            SELECT acct_pubkey, mint_pubkey, owner_pubkey, balance::numeric, NULL, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::varchar[], $4::text[], $5::bigint[])
                AS t(acct_pubkey, mint_pubkey, owner_pubkey, balance, last_updated_slot)
            ON CONFLICT(acct_pubkey)
            DO UPDATE SET
               balance = token_accounts.balance + EXCLUDED.balance,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE token_accounts.last_updated_slot < EXCLUDED.last_updated_slot
            "#,
            &account_pubkeys,
            &mint_pubkeys,
            &owner_pubkeys,
            &balances,
            &slots,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: upsert token_accounts".to_string(),
            source: e,
        })?;

        // 1.2 删除 token_accounts中 balance 为 0 的数据
        sqlx::query!(
            r#"
        DELETE FROM token_accounts
        WHERE balance = 0 AND acct_pubkey = ANY($1)
        "#,
            &account_pubkeys,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: deleting token_account".to_string(),
            source: e,
        })?;

        // 2.0 聚合 synced_events（合并同一个 (mint, owner) 的多个事件）
        let aggregate_datas = holders::aggregate_events(synced_events);

        // 优化：单次迭代构建所有 Vec，避免多次遍历
        let holder_capacity = aggregate_datas.len();
        let mut holder_mint_pubkeys = Vec::with_capacity(holder_capacity);
        let mut holder_owner_pubkeys = Vec::with_capacity(holder_capacity);
        let mut holder_balances = Vec::with_capacity(holder_capacity);
        let mut holder_slots = Vec::with_capacity(holder_capacity);
        let mut total_balances: HashMap<String, ClickhouseDecimal> = HashMap::new();

        for data in aggregate_datas {
            holder_mint_pubkeys.push(data.mint_pubkey.clone());
            holder_owner_pubkeys.push(data.owner_pubkey);
            holder_balances.push(data.balance.to_string());
            holder_slots.push(data.last_updated_slot);
            // 合并第二个循环的逻辑
            let entry = total_balances
                .entry(data.mint_pubkey)
                .or_insert(ClickhouseDecimal::from_int(0));
            *entry = *entry + data.balance;
        }

        // 2.1 upsert holders
        app_info!("start upserting holders");
        sqlx::query!(
            r#"
            INSERT INTO holders
                (mint_pubkey, owner_pubkey, balance, last_updated_slot)
            SELECT mint_pubkey, owner_pubkey, balance::numeric, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::text[], $4::bigint[])
                AS t(mint_pubkey, owner_pubkey, balance, last_updated_slot)
            ON CONFLICT(mint_pubkey, owner_pubkey)
            DO UPDATE SET
               balance = holders.balance + EXCLUDED.balance,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE holders.last_updated_slot < EXCLUDED.last_updated_slot
            "#,
            &holder_mint_pubkeys,
            &holder_owner_pubkeys,
            &holder_balances,
            &holder_slots,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: upsert holders".to_string(),
            source: e,
        })?;

        // 2.2 删除 balance 为 0 的数据
        app_info!("start deleting holder in holders who's balance equals 0");
        sqlx::query!(
            r#"
        DELETE FROM holders
        WHERE balance = 0 AND owner_pubkey = ANY($1)
        "#,
            &holder_owner_pubkeys,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: delete holders with zero balance".to_string(),
            source: e,
        })?;

        // 3. 为每个 mint 计算其最大 slot
        use std::collections::HashMap;
        let mut mint_max_slots: HashMap<String, i64> = HashMap::new();
        for event in synced_events {
            mint_max_slots
                .entry(event.mint_pubkey.clone())
                .and_modify(|slot| *slot = (*slot).max(event.slot as i64))
                .or_insert(event.slot as i64);
        }

        // 4. 批量统计 holder_count（优化：一次查询代替 N 次循环）
        let unique_mints: Vec<String> = synced_events
            .iter()
            .map(|e| e.mint_pubkey.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        // 一次性查询所有 mint 的 holder_count
        let holder_counts_result = sqlx::query!(
            r#"
            SELECT mint_pubkey, COUNT(*) as "count!"
            FROM holders
            WHERE mint_pubkey = ANY($1) AND balance > 0
            GROUP BY mint_pubkey
            "#,
            &unique_mints,
        )
        .fetch_all(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: batch count holders".to_string(),
            source: e,
        })?;

        // 构建 HashMap 方便查找
        let holder_counts_map: HashMap<String, i64> = holder_counts_result
            .into_iter()
            .map(|row| (row.mint_pubkey, row.count))
            .collect();

        // 处理可能没有 holder 的 mint（count = 0）
        let holder_counts: Vec<(String, i64)> = unique_mints
            .iter()
            .map(|mint| {
                let count = holder_counts_map.get(mint).copied().unwrap_or(0);
                (mint.clone(), count)
            })
            .collect();

        // 5. Upsert mint_stats（使用重新统计的值，每个 mint 使用自己的最大 slot）
        app_info!("start upserting mint_stats");

        let mut mint_pubkeys = Vec::new();
        let mut holder_counts_vec = Vec::new();
        let mut max_slots = Vec::new();

        for (mint, count) in holder_counts {
            let max_slot = mint_max_slots.get(&mint).copied().unwrap_or(0);
            mint_pubkeys.push(mint);
            holder_counts_vec.push(count);
            max_slots.push(max_slot);
        }

        sqlx::query!(
            r#"
          INSERT INTO mint_stats (mint_pubkey, holder_count, last_updated_slot)
          SELECT mint_pubkey, holder_count, last_updated_slot
          FROM UNNEST($1::varchar[], $2::bigint[], $3::bigint[])
              AS t(mint_pubkey, holder_count, last_updated_slot)
          ON CONFLICT (mint_pubkey)
          DO UPDATE SET
              holder_count = EXCLUDED.holder_count,
              last_updated_slot = EXCLUDED.last_updated_slot,
              updated_at = now()
          WHERE mint_stats.last_updated_slot < EXCLUDED.last_updated_slot
          "#,
            &mint_pubkeys,
            &holder_counts_vec,
            &max_slots,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "upsert_synced_mints_atomic: upsert mint_stats".to_string(),
            source: e,
        })?;

        // todo!: 因为使用的两个数据库就是不一样，所以导致现有的框架无法保证两个数据库的数据更新呈现原子性
        tx.commit().await.map_err(|e| DatabaseError::TransactionFailed(
            format!("upsert_synced_mints_atomic: commit failed: {:?}", e)
        ))?;
        clickhouse.confirm_events(synced_events).await?;

        Ok(())
    }

    async fn establish_baseline_atomic(
        &self,
        mint: &str,
        token_accounts: Vec<TokenHolder>,
    ) -> Result<i64> {
        if token_accounts.is_empty() {
            return Err(crate::error::ValidationError::EmptyData(
                "establish_baseline_atomic: token_accounts is empty".to_string()
            ).into());
        }

        let mut tx = self.pool.begin().await
            .map_err(|e| DatabaseError::TransactionFailed(
                format!("establish_baseline_atomic: begin transaction failed: {:?}", e)
            ))?;

        // 1. 聚合数据并插入 holders (提前操作，因为 token_accounts 后续会被 move)
        let aggregated_holders = holders::aggregate_token_holders(&token_accounts);

        // 优化：预分配容量，单次迭代构建所有 Vec
        let holder_capacity = aggregated_holders.len();
        let mut holder_mint_pubkeys = Vec::with_capacity(holder_capacity);
        let mut holder_owner_pubkeys = Vec::with_capacity(holder_capacity);
        let mut holder_balances = Vec::with_capacity(holder_capacity);
        let mut holder_slots = Vec::with_capacity(holder_capacity);

        for aggregated_holder in aggregated_holders {
            holder_mint_pubkeys.push(aggregated_holder.mint_pubkey);
            holder_owner_pubkeys.push(aggregated_holder.owner_pubkey);
            holder_balances.push(aggregated_holder.balance.to_string());
            holder_slots.push(aggregated_holder.last_updated_slot);
        }

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
            WHERE holders.last_updated_slot <= EXCLUDED.last_updated_slot
            "#,
            &holder_mint_pubkeys,
            &holder_owner_pubkeys,
            &holder_balances,
            &holder_slots,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "establish_baseline_atomic: insert holders".to_string(),
            source: e,
        })?;

        // 2. 插入 token_accounts
        let capacity = token_accounts.len();
        let mut account_pubkeys = Vec::with_capacity(capacity);
        let mut mint_pubkeys = Vec::with_capacity(capacity);
        let mut owner_pubkeys = Vec::with_capacity(capacity);
        let mut balances = Vec::with_capacity(capacity);
        let mut last_updated_slots = Vec::with_capacity(capacity);

        // 提取 baseline_slot
        let baseline_slot = token_accounts.first().map_or(0, |th| th.slot);

        for token_holder in token_accounts {
            account_pubkeys.push(token_holder.pubkey);
            mint_pubkeys.push(token_holder.mint);
            owner_pubkeys.push(token_holder.owner);
            balances.push(token_holder.balance);
            last_updated_slots.push(token_holder.slot);
        }

        sqlx::query!(
            r#"
            INSERT INTO token_accounts
                (acct_pubkey, mint_pubkey, owner_pubkey, balance, baseline_slot, last_updated_slot)
            SELECT acct_pubkey, mint_pubkey, owner_pubkey, balance::numeric, baseline_slot, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::varchar[], $4::text[], $5::bigint[], $6::bigint[])
                AS t(acct_pubkey, mint_pubkey, owner_pubkey, balance, baseline_slot, last_updated_slot)
            ON CONFLICT(acct_pubkey)
            DO UPDATE SET
               balance = EXCLUDED.balance,
               baseline_slot = EXCLUDED.baseline_slot,
                last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE token_accounts.last_updated_slot < EXCLUDED.last_updated_slot
            "#,
            &account_pubkeys,
            &mint_pubkeys,
            &owner_pubkeys,
            &balances,
            &vec![baseline_slot; capacity], // Use the extracted baseline_slot
            &last_updated_slots,
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "establish_baseline_atomic: insert token_accounts".to_string(),
            source: e,
        })?;


        // 3. 插入 mint_stats
        let holder_count = holder_owner_pubkeys.len() as i64;

        sqlx::query!(
            r#"
            INSERT INTO mint_stats
                (mint_pubkey, holder_count, baseline_slot, last_updated_slot)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT(mint_pubkey)
            DO UPDATE SET
               holder_count = EXCLUDED.holder_count,
                baseline_slot = EXCLUDED.baseline_slot,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE mint_stats.last_updated_slot <= EXCLUDED.last_updated_slot
            "#,
            mint,
            holder_count,
            baseline_slot,
            baseline_slot
        )
        .execute(&mut *tx)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "establish_baseline_atomic: insert mint_stats".to_string(),
            source: e,
        })?;

        tx.commit().await.map_err(|e| DatabaseError::TransactionFailed(
            format!("establish_baseline_atomic: commit failed: {:?}", e)
        ))?;
        Ok(baseline_slot)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tracing::{Level, event, span};
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn test_span() {
        tracing_subscriber::registry().with(fmt::layer()).init();

        let span = span!(Level::TRACE, "my_span");
        let enter = span.enter();
        println!("wuxizhizhenshuai1");
        event!(Level::DEBUG, "something happened inside my_span");
        app_info!("something happened inside my_span");
        app_debug!("Alex wu");
    }
}