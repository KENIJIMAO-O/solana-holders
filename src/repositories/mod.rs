use crate::baseline::getProgramAccounts::TokenHolder;
use crate::database::postgresql::DatabaseConnection;
use crate::repositories::events::Event;
use crate::repositories::token_accounts::aggregate_token_account_events;
use anyhow::Error;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use tracing::info;
use yellowstone_grpc_proto::tonic::async_trait;

pub mod audit;
pub mod events;
pub mod holders;
pub mod mint_stats;
pub mod reconciliation_schedule;
pub mod token_accounts;
pub mod tracked_mints;

/// token_accounts 聚合数据结构
#[derive(Debug, Clone)]
struct TokenAccountAggregateData {
    acct_pubkey: String,
    mint_pubkey: String,
    owner_pubkey: String,
    delta: Decimal,         // 聚合后的 delta
    last_updated_slot: i64, // 最大 slot
}

/// 该特征的作用是保证，在对token_accounts, holders, mint_stats这三张表更新时，必须保证更新的原子性
#[async_trait]
pub trait AtomicityData {
    /// 原子性更新三张表（用于实时监听的 synced mint）
    async fn upsert_synced_mints_atomic(&self, synced_events: &[Event]) -> anyhow::Result<()>;

    /// 原子性建立 baseline（用于初次构建 baseline）
    /// 返回 baseline_slot
    async fn establish_baseline_atomic(
        &self,
        mint: &str,
        token_accounts: &[TokenHolder],
    ) -> anyhow::Result<i64>;
}

/// 受内存爆炸问题困扰，下面需要对两个函数进行分块处理（流式处理暂不可用，只有getProgramAccountV2支持，但是我们不可能全部使用helius节点）
/// 对于upsert_synced_mints_atomic函数，在上游已经进行了分块处理
#[async_trait]
impl AtomicityData for DatabaseConnection {
    async fn upsert_synced_mints_atomic(&self, synced_events: &[Event]) -> Result<(), Error> {
        if synced_events.is_empty() {
            return Ok(());
        }

        info!("start upserting synced mints atomic");
        let mut tx = self.pool.begin().await?;

        // 1.0 聚合 token_accounts 数据（避免同一个 account 多次更新）
        let aggregated_accounts = aggregate_token_account_events(synced_events);

        let account_pubkeys: Vec<String> = aggregated_accounts
            .iter()
            .map(|a| a.acct_pubkey.clone())
            .collect();
        let mint_pubkeys: Vec<String> = aggregated_accounts
            .iter()
            .map(|a| a.mint_pubkey.clone())
            .collect();
        let owner_pubkeys: Vec<String> = aggregated_accounts
            .iter()
            .map(|a| a.owner_pubkey.clone())
            .collect();
        let balances: Vec<String> = aggregated_accounts
            .iter()
            .map(|a| a.delta.to_string())
            .collect();
        let slots: Vec<i64> = aggregated_accounts
            .iter()
            .map(|a| a.last_updated_slot)
            .collect();

        info!("start upserting token_accounts");
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
        .await?;
        // 1.2 删除 token_accounts中 balance 为 0 的数据
        info!("start deleting token_account in token_accounts who's balance equals 0");
        sqlx::query!(
            r#"
        DELETE FROM token_accounts
        WHERE balance = 0 AND acct_pubkey = ANY($1)
        "#,
            &account_pubkeys,
        )
        .execute(&mut *tx)
        .await?;

        // 2.0 聚合 synced_events（合并同一个 (mint, owner) 的多个事件）
        let aggregate_datas = holders::aggregate_events(synced_events);

        // 从聚合后的数据提取字段
        let holder_mint_pubkeys: Vec<String> = aggregate_datas
            .iter()
            .map(|d| d.mint_pubkey.clone())
            .collect();
        let holder_owner_pubkeys: Vec<String> = aggregate_datas
            .iter()
            .map(|d| d.owner_pubkey.clone())
            .collect();
        let holder_balances: Vec<String> = aggregate_datas
            .iter()
            .map(|d| d.balance.to_string())
            .collect();
        let holder_slots: Vec<i64> = aggregate_datas
            .iter()
            .map(|d| d.last_updated_slot)
            .collect();

        // 2.1 upsert holders
        info!("start upserting holders");
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
        .await?;
        // 2.2 删除 balance 为 0 的数据
        info!("start deleting holder in holders who's balance equals 0");
        sqlx::query!(
            r#"
        DELETE FROM holders
        WHERE balance = 0 AND owner_pubkey = ANY($1)
        "#,
            &holder_owner_pubkeys,
        )
        .execute(&mut *tx)
        .await?;

        // 3. 为每个 mint 计算其最大 slot
        use std::collections::HashMap;
        let mut mint_max_slots: HashMap<String, i64> = HashMap::new();
        for event in synced_events {
            mint_max_slots
                .entry(event.mint_pubkey.clone())
                .and_modify(|slot| *slot = (*slot).max(event.slot))
                .or_insert(event.slot);
        }

        // 4. 重新统计 holder_count
        let unique_mints: HashSet<String> = synced_events
            .iter()
            .map(|e| e.mint_pubkey.clone())
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
            .fetch_one(&mut *tx) // 关键：在同一事务中
            .await?;

            holder_counts.push((mint, count));
        }

        // 5. Upsert mint_stats（使用重新统计的值，每个 mint 使用自己的最大 slot）
        info!("start upserting mint_stats");

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
        .await?;

        // 6. 审计：记录 mint_stats 变化历史（如果在审计列表中）
        let mut total_balances: HashMap<String, Decimal> = HashMap::new();
        for data in &aggregate_datas {
            total_balances
                .entry(data.mint_pubkey.clone())
                .and_modify(|b| *b += data.balance)
                .or_insert(data.balance);
        }

        audit::insert_audit_records(
            &mut tx,
            &mint_pubkeys,
            &holder_counts_vec,
            &max_slots,
            &total_balances,
            "realtime",
        )
        .await?;

        // 7. 确认所有处理完成的 events（将 confirmed 设置为 true）
        info!("start confirming events");
        let event_tx_sigs: Vec<String> = synced_events
            .iter()
            .map(|event| event.tx_sig.clone())
            .collect();
        let event_account_pubkeys: Vec<String> = synced_events
            .iter()
            .map(|event| event.account_pubkey.clone())
            .collect();

        let rows_affected = sqlx::query!(
            r#"
            UPDATE events
            SET confirmed = true
            FROM UNNEST($1::varchar[], $2::varchar[]) AS t(tx_sig, account_pubkey)
            WHERE
                events.tx_sig = t.tx_sig
                AND events.account_pubkey = t.account_pubkey
                AND events.confirmed = false
            "#,
            &event_tx_sigs,
            &event_account_pubkeys,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if rows_affected > 0 {
            info!("Confirmed {} events in atomic transaction", rows_affected);
        }

        tx.commit().await?;
        Ok(())
    }

    async fn establish_baseline_atomic(
        &self,
        mint: &str,
        token_accounts: &[TokenHolder],
    ) -> Result<i64, Error> {
        if token_accounts.is_empty() {
            return Err(anyhow::anyhow!("empty token accounts"));
        }

        const CHUNK_SIZE: usize = 2000;
        let mut tx = self.pool.begin().await?;

        // 1. 插入 token_accounts（分块处理以减少内存峰值）
        for chunk in token_accounts.chunks(CHUNK_SIZE) {
            let mut account_pubkeys = Vec::with_capacity(chunk.len());
            let mut mint_pubkeys = Vec::with_capacity(chunk.len());
            let mut owner_pubkeys = Vec::with_capacity(chunk.len());
            let mut balances = Vec::with_capacity(chunk.len());
            let mut last_updated_slots = Vec::with_capacity(chunk.len());

            for token_holder in chunk {
                account_pubkeys.push(token_holder.pubkey.clone());
                mint_pubkeys.push(token_holder.mint.clone());
                owner_pubkeys.push(token_holder.owner.clone());
                balances.push(token_holder.balance.clone());
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
                &last_updated_slots,
                &last_updated_slots,
            )
            .execute(&mut *tx)
            .await?;
        }

        // 2. 聚合数据并插入 holders
        let aggregated_holders = holders::aggregate_token_holders(token_accounts);

        let mut holder_mint_pubkeys = Vec::with_capacity(aggregated_holders.len());
        let mut holder_owner_pubkeys = Vec::with_capacity(aggregated_holders.len());
        let mut holder_balances = Vec::with_capacity(aggregated_holders.len());
        let mut holder_slots = Vec::with_capacity(aggregated_holders.len());

        for aggregated_holder in &aggregated_holders {
            holder_mint_pubkeys.push(aggregated_holder.mint_pubkey.clone());
            holder_owner_pubkeys.push(aggregated_holder.owner_pubkey.clone());
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
        .await?;

        // 3. 插入 mint_stats
        let holder_count = holder_owner_pubkeys.len() as i64;
        let baseline_slot = token_accounts[0].slot;

        sqlx::query!(
            r#"
            INSERT INTO mint_stats
                (mint_pubkey, holder_count, last_updated_slot)
            VALUES ($1, $2, $3)
            ON CONFLICT(mint_pubkey)
            DO UPDATE SET
               holder_count = EXCLUDED.holder_count,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE mint_stats.last_updated_slot <= EXCLUDED.last_updated_slot
            "#,
            mint,
            holder_count,
            baseline_slot,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(baseline_slot)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tracing::{Level, debug, event, span};
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn test_span() {
        tracing_subscriber::registry().with(fmt::layer()).init();

        let span = span!(Level::TRACE, "my_span");
        let enter = span.enter();
        println!("wuxizhizhenshuai1");
        event!(Level::DEBUG, "something happened inside my_span");
        info!("something happened inside my_span");
        debug!("Alex wu");
    }
}
