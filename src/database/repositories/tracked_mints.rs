use crate::database::postgresql::DatabaseConnection;
use crate::error::{DatabaseError, ValidationError, Result};
use std::collections::HashSet;
use yellowstone_grpc_proto::tonic::async_trait;

#[async_trait]
pub trait TrackedMintsRepository {
    /// 批量开始 baseline 构建（插入新 mints，状态为 baseline_building）
    async fn start_baseline_batch(
        &self,
        mint_pubkeys: &[String],
        baseline_slots: &[i64],
    ) -> Result<()>;

    /// 批量完成 baseline 构建（更新状态为 catching_up）
    async fn finish_baseline_batch(&self, mint_pubkeys: &[String]) -> Result<()>;

    /// 批量完成 catch_up（更新状态为 synced）
    async fn finish_catch_up_batch(&self, mint_pubkeys: &[String]) -> Result<()>;

    /// 批量检查 mints 是否已被追踪
    /// 返回未被追踪的 mint_pubkeys 数组
    async fn is_tracked_batch(&self, mint_pubkeys: &[String]) -> Result<Vec<String>>;

    /// 过滤出已经完成同步的 mints (status = 'synced')
    /// 返回输入列表中状态为 synced 的 mint_pubkeys
    async fn filter_synced_mints(&self, mint_pubkeys: &[String]) -> Result<Vec<String>>;

    /// 获取 mint 的当前状态（单个查询保留，用于特殊场景）
    async fn get_status(&self, mint_pubkey: &str) -> Result<Option<String>>;

    /// 获取所有已同步的 mints
    async fn get_synced_mints(&self) -> Result<Vec<String>>;
}

#[async_trait]
impl TrackedMintsRepository for DatabaseConnection {
    async fn start_baseline_batch(
        &self,
        mint_pubkeys: &[String],
        baseline_slots: &[i64],
    ) -> Result<()> {
        if mint_pubkeys.is_empty() {
            return Ok(());
        }

        // 确保两个数组长度一致
        if mint_pubkeys.len() != baseline_slots.len() {
            return Err(ValidationError::InvalidParameter {
                field: "mint_pubkeys and baseline_slots".to_string(),
                reason: "length mismatch".to_string(),
            }.into());
        }

        sqlx::query!(
            r#"
            INSERT INTO tracked_mints (mint_pubkey, baseline_slot, status)
            SELECT mint_pubkey, baseline_slot, 'baseline_building'
            FROM UNNEST($1::varchar[], $2::bigint[])
                AS t(mint_pubkey, baseline_slot)
            ON CONFLICT(mint_pubkey)
            DO UPDATE SET
               baseline_slot = EXCLUDED.baseline_slot,
               status = 'baseline_building',
               updated_at = now()
            "#,
            mint_pubkeys,
            baseline_slots,
        )
        .execute(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "start_baseline_batch: insert tracked_mints".to_string(),
            source: e,
        })?;

        Ok(())
    }

    async fn finish_baseline_batch(&self, mint_pubkeys: &[String]) -> Result<()> {
        if mint_pubkeys.is_empty() {
            return Ok(());
        }

        sqlx::query!(
            r#"
            UPDATE tracked_mints
            SET status = 'catching_up',
                updated_at = now()
            WHERE mint_pubkey = ANY($1)
            "#,
            mint_pubkeys,
        )
        .execute(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "finish_baseline_batch: update tracked_mints status to catching_up".to_string(),
            source: e,
        })?;

        Ok(())
    }

    async fn finish_catch_up_batch(&self, mint_pubkeys: &[String]) -> Result<()> {
        if mint_pubkeys.is_empty() {
            return Ok(());
        }

        sqlx::query!(
            r#"
            UPDATE tracked_mints
            SET status = 'synced',
                updated_at = now()
            WHERE mint_pubkey = ANY($1)
            "#,
            mint_pubkeys,
        )
        .execute(&self.pool) // execute用于不需要返回数据的场景
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "finish_catch_up_batch: update tracked_mints status to synced".to_string(),
            source: e,
        })?;

        Ok(())
    }

    // 筛选出已经存在于tracked_mint表中的所有mint
    async fn is_tracked_batch(&self, mint_pubkeys: &[String]) -> Result<Vec<String>> {
        if mint_pubkeys.is_empty() {
            return Ok(vec![]);
        }

        // 查询所有存在于 tracked_mints 表中的 mint_pubkeys
        let tracked_mints = sqlx::query_scalar!(
            r#"
            SELECT mint_pubkey
            FROM tracked_mints
            WHERE mint_pubkey = ANY($1)
            "#,
            mint_pubkeys,
        )
        .fetch_all(&self.pool) // query_scalar!查询单个字段，fetch_all用于返回多行数据
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "is_tracked_batch: query tracked_mints".to_string(),
            source: e,
        })?;

        // 将查询结果转换为 HashSet，用于 O(1) 查找
        let tracked_set: HashSet<String> = tracked_mints.into_iter().collect();

        // 过滤出不在 tracked_set 中的 mint_pubkeys
        let untracked_mints: Vec<String> = mint_pubkeys
            .iter()
            .filter(|mint| !tracked_set.contains(*mint))
            .cloned()
            .collect();

        Ok(untracked_mints)
    }

    // 批量将代币状态置为synced
    async fn filter_synced_mints(&self, mint_pubkeys: &[String]) -> Result<Vec<String>> {
        if mint_pubkeys.is_empty() {
            return Ok(vec![]);
        }

        let synced_mints = sqlx::query_scalar!(
            r#"
            SELECT mint_pubkey
            FROM tracked_mints
            WHERE mint_pubkey = ANY($1)
              AND status = 'synced'
            "#,
            mint_pubkeys,
        )
        .fetch_all(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "filter_synced_mints: query synced mints".to_string(),
            source: e,
        })?;

        Ok(synced_mints)
    }

    async fn get_status(&self, mint_pubkey: &str) -> Result<Option<String>> {
        let result = sqlx::query_scalar!(
            r#"
            SELECT status
            FROM tracked_mints
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey,
        )
        .fetch_optional(&self.pool) // fetch_option返回一个可能单行的数据，还可能是None
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: format!("get_status: mint={}", mint_pubkey),
            source: e,
        })?;

        Ok(result.flatten())
    }

    async fn get_synced_mints(&self) -> Result<Vec<String>> {
        let mints = sqlx::query_scalar!(
            r#"
            SELECT mint_pubkey
            FROM tracked_mints
            WHERE status = 'synced'
            "#,
        )
        .fetch_all(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "get_synced_mints: query all synced mints".to_string(),
            source: e,
        })?;

        Ok(mints)
    }
}
