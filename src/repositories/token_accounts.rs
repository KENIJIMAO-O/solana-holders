use crate::baseline::getProgramAccounts::TokenHolder;
use crate::database::postgresql::DatabaseConnection;
use crate::repositories::events::Event;
use crate::utils::timer::TaskLogger;
use anyhow::Error;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait TokenAccountsRepository {
    async fn upsert_token_accounts_batch(
        &self,
        token_accounts: &[TokenHolder],
        logger: &mut TaskLogger,
    ) -> Result<(), Error>;

    async fn update_token_accounts_batch(&self, token_accounts: &[Event]) -> Result<(), Error>;
}

#[async_trait]
impl TokenAccountsRepository for DatabaseConnection {
    async fn upsert_token_accounts_batch(
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

    async fn update_token_accounts_batch(&self, events: &[Event]) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        let account_pubkeys: Vec<String> = events
            .iter()
            .map(|event| event.account_pubkey.clone())
            .collect();
        // 将 Decimal 变化量转换为字符串以便传递
        let deltas: Vec<String> = events.iter().map(|event| event.delta.to_string()).collect();
        let last_updated_slots: Vec<i64> = events.iter().map(|event| event.slot as i64).collect();

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

        tx.commit().await?;
        Ok(())
    }
}
