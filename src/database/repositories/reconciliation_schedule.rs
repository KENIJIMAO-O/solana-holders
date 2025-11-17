use crate::database::postgresql::DatabaseConnection;
use crate::error::{DatabaseError, ValidationError, Result};
use sqlx::types::chrono::{DateTime, Utc};
use yellowstone_grpc_proto::tonic::async_trait;

#[derive(Debug, Clone)]
pub struct ReconciliationSchedule {
    pub mint_pubkey: String,
    pub last_reconciliation_time: DateTime<Utc>,
    pub last_holder_count: i64,
    pub next_reconciliation_time: DateTime<Utc>,
    pub current_interval_hours: i32,             // 当前对账间隔
    pub total_reconciliations: i32,
}

#[async_trait]
pub trait ReconciliationScheduleRepository {
    /// 批量初始化对账调度（mint 进入 synced 状态时调用）
    async fn initialize_schedule_batch(
        &self,
        mint_pubkeys: &[String],
        holder_counts: &[i64],
    ) -> Result<()>;

    /// 查询到期需要对账的 mints
    async fn get_due_mints(&self, limit: i64) -> Result<Vec<ReconciliationSchedule>>;

    /// 对账后更新调度信息
    async fn update_schedule_after_reconciliation(
        &self,
        mint_pubkey: &str,
        current_holder_count: i64,
        next_interval_hours: i32,
    ) -> Result<()>;
}

#[async_trait]
impl ReconciliationScheduleRepository for DatabaseConnection {
    async fn initialize_schedule_batch(
        &self,
        mint_pubkeys: &[String],
        holder_counts: &[i64],
    ) -> Result<()> {
        if mint_pubkeys.is_empty() {
            return Ok(());
        }

        // 确保两个数组长度一致
        if mint_pubkeys.len() != holder_counts.len() {
            return Err(ValidationError::InvalidParameter {
                field: "mint_pubkeys and holder_counts".to_string(),
                reason: "length mismatch".to_string(),
            }.into());
        }

        sqlx::query!(
            r#"
            INSERT INTO reconciliation_schedule
                (mint_pubkey, last_reconciliation_time, last_holder_count,
                 next_reconciliation_time, current_interval_hours, total_reconciliations)
            SELECT
                mint_pubkey,
                now() as last_reconciliation_time,
                holder_count as last_holder_count,
                now() + INTERVAL '24 hours' as next_reconciliation_time,
                24 as current_interval_hours,
                0 as total_reconciliations
            FROM UNNEST($1::varchar[], $2::bigint[])
                AS t(mint_pubkey, holder_count)
            ON CONFLICT(mint_pubkey) DO NOTHING
            "#,
            mint_pubkeys,
            holder_counts,
        )
        .execute(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "initialize_schedule_batch: insert reconciliation_schedule".to_string(),
            source: e,
        })?;

        Ok(())
    }

    // todo！：感觉这里一下子查所有，有点太多了，对内存来说又是一个不小的压力
    async fn get_due_mints(&self, limit: i64) -> Result<Vec<ReconciliationSchedule>> {
        let records = sqlx::query!(
            r#"
            SELECT
                mint_pubkey,
                last_reconciliation_time,
                last_holder_count,
                next_reconciliation_time,
                current_interval_hours,
                total_reconciliations
            FROM reconciliation_schedule
            WHERE next_reconciliation_time <= now()
            ORDER BY next_reconciliation_time ASC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: "get_due_mints: query reconciliation_schedule".to_string(),
            source: e,
        })?;

        let schedules = records
            .into_iter()
            .map(|record| ReconciliationSchedule {
                mint_pubkey: record.mint_pubkey,
                last_reconciliation_time: record.last_reconciliation_time,
                last_holder_count: record.last_holder_count,
                next_reconciliation_time: record.next_reconciliation_time,
                current_interval_hours: record.current_interval_hours,
                total_reconciliations: record.total_reconciliations.unwrap_or(0),
            })
            .collect();

        Ok(schedules)
    }

    async fn update_schedule_after_reconciliation(
        &self,
        mint_pubkey: &str,
        current_holder_count: i64,
        next_interval_hours: i32,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE reconciliation_schedule
            SET
                last_reconciliation_time = now(),
                last_holder_count = $2,
                next_reconciliation_time = now() + make_interval(hours => $3),
                current_interval_hours = $3,
                total_reconciliations = total_reconciliations + 1,
                updated_at = now()
            WHERE mint_pubkey = $1
            "#,
            mint_pubkey,
            current_holder_count,
            next_interval_hours
        )
        .execute(&self.pool)
        .await.map_err(|e| DatabaseError::QueryFailed {
            query: format!("update_schedule_after_reconciliation: mint={}", mint_pubkey),
            source: e,
        })?;

        Ok(())
    }
}
