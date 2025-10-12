use crate::database::postgresql::DatabaseConnection;
use crate::utils::timer::TaskLogger;
use anyhow::Error;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait MintStatsRepository {
    /// baseline时使用
    async fn upsert_mint_stats(
        &self,
        mint_pubkey: &str,
        holder_count: i64,
        last_updated_slot: i64,
    ) -> Result<(), Error>;

    /// 监听时更新用户余额
    async fn update_holder_count();

    /// 获取用户holder account数量
    async fn get_holder_account(&self, mint_pubkey: &str) -> Result<i64, Error>;
}

#[async_trait]
impl MintStatsRepository for DatabaseConnection {
    async fn upsert_mint_stats(
        &self,
        mint_pubkey: &str,
        holder_count: i64,
        last_updated_slot: i64,
    ) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;

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
            mint_pubkey,
            holder_count,
            last_updated_slot,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(())
    }

    async fn update_holder_count() {
        todo!()
    }

    async fn get_holder_account(&self, mint_pubkey: &str) -> Result<i64, Error> {
        // 使用 query_scalar! 来获取单个值
        let holder_count: i64 = sqlx::query_scalar!(
            r#"
            SELECT holder_count
            FROM mint_stats
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey
        )
        // 使用 .fetch_one() 来执行查询并获取期望的一行结果
        .fetch_one(&self.pool)
        .await?;

        Ok(holder_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
