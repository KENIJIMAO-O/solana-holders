use crate::database::postgresql::DatabaseConnection;
use crate::utils::timer::TaskLogger;
use anyhow::Error;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait MintStatsRepository {
    /// baseline时使用
    async fn establish_mint_stats_baseline(
        &self,
        mint_pubkey: &str,
        holder_count: i64,
        last_updated_slot: i64,
    ) -> Result<(), Error>;

    /// 监听时更新用户余额
    async fn update_mint_stats_holder_count(
        &self,
        holder_counts: &[(String, i64)],
        last_updated_slot: i64,
    ) -> Result<(), Error>;

    /// 获取用户holder account数量
    async fn get_holder_account(&self, mint_pubkey: &str) -> Result<i64, Error>;

    /// 批量获取多个 mint 的 holder_count
    async fn get_holder_counts_batch(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<Vec<(String, i64)>, Error>;
}

#[async_trait]
impl MintStatsRepository for DatabaseConnection {
    async fn establish_mint_stats_baseline(
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

    //
    async fn update_mint_stats_holder_count(
        &self,
        holder_counts: &[(String, i64)], // 我们要处理全部的数据，而不关注某一个键对应的值时，Vec<()>性能 > HashMap，因为数据在内存中是连续存放的
        last_updated_slot: i64,
    ) -> Result<(), Error> {
        if holder_counts.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        let (mint_pubkeys, holder_counts): (Vec<String>, Vec<i64>) =
            holder_counts.iter().cloned().unzip();

        sqlx::query!(
            r#"
        UPDATE mint_stats AS ta
        SET
            holder_count = t.holder_count,
            last_updated_slot = $3,
            updated_at = now()
        FROM UNNEST($1::varchar[], $2::bigint[])
            AS t(mint_pubkey, holder_count)
        WHERE
            ta.mint_pubkey = t.mint_pubkey
            AND ta.last_updated_slot < $3 -- 同样保留时效性检查
        "#,
            &mint_pubkeys,
            &holder_counts,
            &last_updated_slot,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(())
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

    async fn get_holder_counts_batch(
        &self,
        mint_pubkeys: &[String],
    ) -> Result<Vec<(String, i64)>, Error> {
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
        .await?;

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
