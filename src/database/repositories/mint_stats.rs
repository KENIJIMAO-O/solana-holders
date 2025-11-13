use crate::database::postgresql::DatabaseConnection;
use crate::error::{DatabaseError, Result};
use crate::utils::timer::TaskLogger;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait MintStatsRepository {
    /// 获取用户holder account数量
    async fn get_holder_account(&self, mint_pubkey: &str) -> Result<i64>;

    /// 批量获取多个 mint 的 holder_count
    async fn get_holder_counts_batch(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<Vec<(String, i64)>>;
}

#[async_trait]
impl MintStatsRepository for DatabaseConnection {
    async fn get_holder_account(&self, mint_pubkey: &str) -> Result<i64> {
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
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: format!("get_holder_account: mint={}", mint_pubkey),
            source: e,
        })?;

        Ok(holder_count)
    }

    async fn get_holder_counts_batch(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<Vec<(String, i64)>> {
        if mint_pubkeys.is_empty() {
            return Ok(vec![]);
        }

        let records = sqlx::query!(
            r#"
            SELECT mint_pubkey, holder_count
            FROM mint_stats
            WHERE mint_pubkey = ANY($1)
            "#,
            mint_pubkeys
        )
        .fetch_all(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: format!("get_holder_counts_batch: {} mints", mint_pubkeys.len()),
            source: e,
        })?;

        let result = records
            .into_iter()
            .map(|record| (record.mint_pubkey, record.holder_count))
            .collect();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
