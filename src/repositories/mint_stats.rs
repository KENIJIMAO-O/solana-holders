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
        let mut tx = self.pool.begin().await?;

        let (owners_pubkeys, deltas_i64): (Vec<String>, Vec<i64>) =
            holder_counts.iter().cloned().unzip();
        let deltas: Vec<String> = deltas_i64.iter().map(|d| d.to_string()).collect();

        sqlx::query!(
            r#"
        UPDATE holders AS ta
        SET
            balance = ta.balance + t.delta::numeric, -- 核心逻辑：累加余额
            last_updated_slot = $3,
            updated_at = now()
        FROM UNNEST($1::varchar[], $2::text[])
            AS t(owner_pubkey, delta)
        WHERE
            ta.owner_pubkey = t.owner_pubkey
            AND ta.last_updated_slot < $3 -- 同样保留时效性检查
        "#,
            &owners_pubkeys,
            &deltas,
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
}

#[cfg(test)]
mod tests {
    use super::*;
}
