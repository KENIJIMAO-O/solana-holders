use crate::database::postgresql::DatabaseConnection;
use anyhow::Error;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait TrackedMintsRepository {
    /// 开始 baseline 构建（插入新 mint，状态为 baseline_building）
    async fn start_baseline(&self, mint_pubkey: &str, baseline_slot: i64) -> Result<(), Error>;

    /// 完成 baseline 构建（更新状态为 catching_up）
    async fn finish_baseline(&self, mint_pubkey: &str) -> Result<(), Error>;

    /// 完成 catch_up（更新状态为 synced）
    async fn finish_catch_up(&self, mint_pubkey: &str) -> Result<(), Error>;

    /// 检查 mint 是否已被追踪
    async fn is_tracked(&self, mint_pubkey: &str) -> Result<bool, Error>;

    /// 获取 mint 的当前状态
    async fn get_status(&self, mint_pubkey: &str) -> Result<Option<String>, Error>;

    /// 获取所有已同步的 mints
    async fn get_synced_mints(&self) -> Result<Vec<String>, Error>;
}

#[async_trait]
impl TrackedMintsRepository for DatabaseConnection {
    async fn start_baseline(&self, mint_pubkey: &str, baseline_slot: i64) -> Result<(), Error> {
        sqlx::query!(
            r#"
            INSERT INTO tracked_mints
                (mint_pubkey, baseline_slot, status)
            VALUES ($1, $2, 'baseline_building')
            ON CONFLICT(mint_pubkey)
            DO UPDATE SET
               baseline_slot = EXCLUDED.baseline_slot,
               status = 'baseline_building',
               updated_at = now()
            "#,
            mint_pubkey,
            baseline_slot,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn finish_baseline(&self, mint_pubkey: &str) -> Result<(), Error> {
        sqlx::query!(
            r#"
            UPDATE tracked_mints
            SET status = 'catching_up',
                updated_at = now()
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn finish_catch_up(&self, mint_pubkey: &str) -> Result<(), Error> {
        sqlx::query!(
            r#"
            UPDATE tracked_mints
            SET status = 'synced',
                updated_at = now()
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn is_tracked(&self, mint_pubkey: &str) -> Result<bool, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM tracked_mints WHERE mint_pubkey = $1
            ) as "exists!"
            "#,
            mint_pubkey,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(result)
    }

    async fn get_status(&self, mint_pubkey: &str) -> Result<Option<String>, Error> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT status
            FROM tracked_mints
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey,
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.flatten())
    }

    async fn get_synced_mints(&self) -> Result<Vec<String>, Error> {
        let mints = sqlx::query_scalar!(
            r#"
            SELECT mint_pubkey
            FROM tracked_mints
            WHERE status = 'synced'
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(mints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
