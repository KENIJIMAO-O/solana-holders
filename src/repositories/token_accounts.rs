use std::collections::HashMap;
use crate::baseline::getProgramAccounts::TokenHolder;
use crate::database::postgresql::DatabaseConnection;
use crate::repositories::events::Event;
use crate::utils::timer::TaskLogger;
use anyhow::Error;
use rust_decimal::Decimal;
use yellowstone_grpc_proto::tonic::async_trait;
use crate::repositories::TokenAccountAggregateData;

/// 聚合相同 account_pubkey 的 events，避免 ON CONFLICT 多次更新同一行
pub fn aggregate_token_account_events(events: &[Event]) -> Vec<TokenAccountAggregateData> {
    // Key: account_pubkey, Value: (mint, owner, aggregated_delta, max_slot)
    let mut aggregation_map: HashMap<String, (String, String, Decimal, i64)> = HashMap::new();

    for event in events {
        aggregation_map
            .entry(event.account_pubkey.clone())
            .and_modify(|(_, _, delta, slot)| {
                *delta += event.delta;
                *slot = (*slot).max(event.slot);
            })
            .or_insert((
                event.mint_pubkey.clone(),
                event.owner_pubkey.clone(),
                event.delta,
                event.slot,
            ));
    }

    aggregation_map
        .into_iter()
        .map(|(acct, (mint, owner, delta, slot))| TokenAccountAggregateData {
            acct_pubkey: acct,
            mint_pubkey: mint,
            owner_pubkey: owner,
            delta,
            last_updated_slot: slot,
        })
        .collect()
}


#[async_trait]
pub trait TokenAccountsRepository {
    async fn establish_token_accounts_baseline(
        &self,
        token_accounts: &[TokenHolder],
        logger: &mut TaskLogger,
    ) -> Result<(), Error>;

    async fn upsert_token_accounts_batch(&self, token_accounts: &[Event]) -> Result<(), Error>;
}

#[async_trait]
impl TokenAccountsRepository for DatabaseConnection {
    async fn establish_token_accounts_baseline(
        &self,
        token_accounts: &[TokenHolder],
        logger: &mut TaskLogger,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        let last_updated_slots: Vec<i64> = token_accounts
            .iter()
            .map(|token_holder| token_holder.slot as i64)
            .collect();
        let mint_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.mint.clone())
            .collect::<Vec<_>>();
        let account_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.pubkey.clone())
            .collect::<Vec<_>>();
        let owner_pubkeys = token_accounts
            .iter()
            .map(|token_holder| token_holder.owner.clone())
            .collect::<Vec<_>>();
        let balances = token_accounts
            .iter()
            .map(|token_holder| token_holder.balance.clone())
            .collect::<Vec<_>>();

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
        tx.commit().await?;

        Ok(())
    }

    async fn upsert_token_accounts_batch(&self, events: &[Event]) -> Result<(), Error> {
        // 1.empty check
        if events.is_empty() {
            return Ok(());
        }

        // 2.get pool connection
        let mut tx = self.pool.begin().await?;

        // 3.parse data
        let account_pubkeys: Vec<String> = events
            .iter()
            .map(|event| event.account_pubkey.clone())
            .collect();
        // 将 Decimal 变化量转换为字符串以便传递
        let deltas: Vec<String> = events.iter().map(|event| event.delta.to_string()).collect();
        let last_updated_slots: Vec<i64> = events.iter().map(|event| event.slot as i64).collect();

        // todo!: 其实这里还有一个地方有难搞，就是如果在baseline之后出现新的token_account咋办
        // 4.这个地方目前只是update，但是感觉肯定是会变成upsert
        let rows_affected = sqlx::query!(
            r#"
        UPDATE token_accounts AS ta
        SET
            balance = ta.balance + t.delta::numeric, -- 核心逻辑：累加余额
            last_updated_slot = t.last_updated_slot,
            updated_at = now()
        FROM UNNEST($1::varchar[], $2::text[], $3::bigint[])
            AS t(acct_pubkey, delta, last_updated_slot)
        WHERE
            ta.acct_pubkey = t.acct_pubkey
            AND ta.last_updated_slot < t.last_updated_slot -- 同样保留时效性检查
        "#,
            &account_pubkeys,
            &deltas,
            &last_updated_slots,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected(); // 获取受影响的行数

        // 删除上述账户中余额为0的数据
        let delete_result = sqlx::query!(
            r#"
        DELETE FROM token_accounts
        WHERE balance = 0 AND acct_pubkey = ANY($1)
        "#,
            &account_pubkeys,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }
}
