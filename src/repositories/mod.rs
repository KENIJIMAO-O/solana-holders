use std::collections::HashSet;
use anyhow::Error;
use yellowstone_grpc_proto::tonic::async_trait;
use crate::database::postgresql::DatabaseConnection;
use crate::repositories::events::Event;
use crate::baseline::getProgramAccounts::TokenHolder;

pub mod events;
pub mod holders;
pub mod mint_stats;
pub mod token_accounts;
pub mod tracked_mints;

/// 该特征的作用是保证，在对token_accounts, holders, mint_stats这三张表更新时，必须保证更新的原子性
#[async_trait]
pub trait AtomicityData{
    /// 原子性更新三张表（用于实时监听的 synced mint）
    async fn upsert_synced_mints_atomic(&self, synced_events: &[Event]) -> anyhow::Result<()>;

    /// 原子性建立 baseline（用于初次构建 baseline）
    /// 返回 baseline_slot
    async fn establish_baseline_atomic(&self, mint: &str, token_accounts: &[TokenHolder]) -> anyhow::Result<i64>;
}

#[async_trait]
impl AtomicityData for DatabaseConnection{
    async fn upsert_synced_mints_atomic(&self, synced_events: &[Event]) -> Result<(), Error> {
        if synced_events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        let account_pubkeys: Vec<String> = synced_events.iter()
            .map(|e| e.account_pubkey.clone())
            .collect();
        let owner_pubkeys: Vec<String> = synced_events.iter()
            .map(|e| e.owner_pubkey.clone())
            .collect();
        let mint_pubkeys: Vec<String> = synced_events.iter()
            .map(|e| e.mint_pubkey.clone())
            .collect();
        let balances: Vec<String> = synced_events.iter()
            .map(|e| e.delta.to_string())
            .collect();
        let slots: Vec<i64> = synced_events.iter()
            .map(|e| e.slot)
            .collect();

        // 1.1 upsert token_accounts
        sqlx::query!(
            r#"
            INSERT INTO token_accounts
                (acct_pubkey, mint_pubkey, owner_pubkey, balance, last_updated_slot)
            SELECT acct_pubkey, mint_pubkey, owner_pubkey, balance::numeric, last_updated_slot
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
        let holder_mint_pubkeys: Vec<String> = aggregate_datas.iter()
            .map(|d| d.mint_pubkey.clone())
            .collect();
        let holder_owner_pubkeys: Vec<String> = aggregate_datas.iter()
            .map(|d| d.owner_pubkey.clone())
            .collect();
        let holder_balances: Vec<String> = aggregate_datas.iter()
            .map(|d| d.balance.to_string())
            .collect();
        let holder_slots: Vec<i64> = aggregate_datas.iter()
            .map(|d| d.last_updated_slot)
            .collect();

        // 2.1 upsert holders
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
        sqlx::query!(
            r#"
        DELETE FROM holders
        WHERE balance = 0 AND owner_pubkey = ANY($1)
        "#,
            &holder_owner_pubkeys,
        )
            .execute(&mut *tx)
            .await?;

        // 3. 重新统计 holder_count
        let unique_mints: HashSet<String> = synced_events.iter()
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
                .fetch_one(&mut *tx)  // 关键：在同一事务中
                .await?;

            holder_counts.push((mint, count));
        }

        // 4. Upsert mint_stats（使用重新统计的值）
        let (mint_pubkeys, counts): (Vec<String>, Vec<i64>) =
            holder_counts.into_iter().unzip();

        // 找到这批事件的最大 slot
        let max_slot = synced_events.iter()
            .map(|e| e.slot)
            .max()
            .unwrap_or(0);

        sqlx::query!(
          r#"
          INSERT INTO mint_stats (mint_pubkey, holder_count, last_updated_slot)
          SELECT mint_pubkey, holder_count, $3
          FROM UNNEST($1::varchar[], $2::bigint[])
              AS t(mint_pubkey, holder_count)
          ON CONFLICT (mint_pubkey)
          DO UPDATE SET
              holder_count = EXCLUDED.holder_count,
              last_updated_slot = EXCLUDED.last_updated_slot,
              updated_at = now()
          WHERE mint_stats.last_updated_slot < EXCLUDED.last_updated_slot
          "#,
          &mint_pubkeys,
          &counts,
          &max_slot,
      )
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn establish_baseline_atomic(&self, mint: &str, token_accounts: &[TokenHolder]) -> Result<i64, Error> {
        if token_accounts.is_empty() {
            return Err(anyhow::anyhow!("empty token accounts"));
        }

        let mut tx = self.pool.begin().await?;

        // 1. 插入 token_accounts
        let account_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.pubkey.clone())
            .collect::<Vec<_>>();
        let mint_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.mint.clone())
            .collect::<Vec<_>>();
        let owner_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.owner.clone())
            .collect::<Vec<_>>();
        let balances = token_accounts
            .iter()
            .map(|token_holder| token_holder.balance.clone())
            .collect::<Vec<_>>();
        let last_updated_slots: Vec<i64> = token_accounts
            .iter()
            .map(|token_holder| token_holder.slot as i64)
            .collect();

        sqlx::query!(
            r#"
            INSERT INTO token_accounts
                (acct_pubkey, mint_pubkey, owner_pubkey, balance, last_updated_slot)
            SELECT acct_pubkey, mint_pubkey, owner_pubkey, balance::numeric, last_updated_slot
            FROM UNNEST($1::varchar[], $2::varchar[], $3::varchar[], $4::text[], $5::bigint[])
                AS t(acct_pubkey, mint_pubkey, owner_pubkey, balance, last_updated_slot)
            ON CONFLICT(acct_pubkey)
            DO UPDATE SET
               balance = EXCLUDED.balance,
               last_updated_slot = EXCLUDED.last_updated_slot,
               updated_at = now()
            WHERE token_accounts.last_updated_slot < EXCLUDED.last_updated_slot
            "#,
            &account_pubkeys,
            &mint_pubkeys,
            &owner_pubkeys,
            &balances,
            &last_updated_slots,
        )
        .execute(&mut *tx)
        .await?;

        // 2. 聚合数据并插入 holders
        let aggregated_holders = holders::aggregate_token_holders(token_accounts);

        let holder_mint_pubkeys = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.mint_pubkey.clone())
            .collect::<Vec<_>>();
        let holder_owner_pubkeys = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.owner_pubkey.clone())
            .collect::<Vec<_>>();
        let holder_balances: Vec<String> = aggregated_holders
            .iter()
            .map(|aggregated_holder| aggregated_holder.balance.to_string())
            .collect::<Vec<_>>();
        let holder_slots: Vec<i64> = aggregated_holders
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