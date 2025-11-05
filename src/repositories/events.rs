use crate::EVENT_LOG_TARGET;
use crate::database::postgresql::DatabaseConnection;
use crate::utils::timer::TaskLogger;
use anyhow::{Error, Result, anyhow};
use rust_decimal::Decimal;
use sqlx::FromRow;
use tracing::info;
use yellowstone_grpc_proto::tonic::async_trait;

pub struct Event {
    pub slot: i64,
    pub tx_sig: String,
    pub mint_pubkey: String,
    pub account_pubkey: String,
    pub owner_pubkey: String,
    pub delta: Decimal,
    pub confirmed: bool,
}

#[derive(FromRow)]
struct EventFromDb {
    pub id: i64,
    pub slot: i64,
    pub tx_sig: String,
    pub mint_pubkey: String,
    pub account_pubkey: String,
    pub owner_pubkey: String,
    pub delta: Decimal,
    pub confirmed: bool,
}

impl Event {
    pub fn new(
        slot: i64,
        tx_sig: String,
        mint_pubkey: String,
        account_pubkey: String,
        owner_pubkey: String,
        delta: Decimal,
        confirmed: bool,
    ) -> Self {
        Self {
            slot,
            tx_sig,
            mint_pubkey,
            account_pubkey,
            owner_pubkey,
            delta,
            confirmed,
        }
    }
}

#[async_trait]
pub trait EventsRepository {
    /// 批量插入或者更新
    async fn upsert_events_batch(
        &self,
        events: &[Event],
        logger: &mut TaskLogger,
    ) -> Result<(), Error>;

    /// 更新confirmed
    async fn confirm_events(&self, events: &[Event]) -> Result<(), Error>;

    /// 将 baseline_slot 之前的所有未确认事件标记为 confirmed
    /// 因为 baseline 已经代表了那个时刻的完整状态，这些事件不需要再处理
    async fn confirm_events_before_baseline(
        &self,
        mint: &str,
        baseline_slot: i64,
    ) -> Result<u64, Error>;

    /// 从指定的游标 (last_slot, last_id) 开始，获取下一批最新的 events
    async fn get_next_events_batch(
        &self,
        cursor: (i64, i64), // 游标现在是一个元组
        mint: &str,
        limit: i64,
    ) -> Result<(Vec<Event>, Option<(i64, i64)>), Error>;

    async fn get_latest_event_slot(&self) -> Result<i64>;
}

#[async_trait]
impl EventsRepository for DatabaseConnection {
    async fn upsert_events_batch(
        &self,
        events: &[Event],
        logger: &mut TaskLogger,
    ) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        // 在这个连接上开启事务
        logger.log("start to pool begin");
        let mut tx = self.pool.begin().await?;

        let slots: Vec<i64> = events
            .iter()
            .map(|event| event.slot as i64)
            .collect::<Vec<_>>();
        let tx_sigs = events
            .iter()
            .map(|event| {
                info!(target: EVENT_LOG_TARGET, "Upserted event sig:{:?}", event.tx_sig);
                event.tx_sig.clone()
            })
            .collect::<Vec<_>>();
        let mint_pubkeys = events
            .iter()
            .map(|event| event.mint_pubkey.clone())
            .collect::<Vec<_>>();
        let account_pubkeys = events
            .iter()
            .map(|event| event.account_pubkey.clone())
            .collect::<Vec<_>>();
        let owner_pubkeys = events
            .iter()
            .map(|event| event.owner_pubkey.clone())
            .collect::<Vec<_>>();
        let deltas: Vec<String> = events
            .iter()
            .map(|event| event.delta.to_string())
            .collect::<Vec<_>>();

        logger.log("start to upsert events");
        sqlx::query!(
            r#"
            INSERT INTO events
                (slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta)
            SELECT slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta::numeric
            FROM UNNEST($1::bigint[], $2::varchar[], $3::varchar[], $4::varchar[], $5::varchar[], $6::text[])
                AS t(slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta)
            ON CONFLICT (tx_sig, account_pubkey)
            DO UPDATE SET
                slot = EXCLUDED.slot,
                mint_pubkey = EXCLUDED.mint_pubkey,
                owner_pubkey = EXCLUDED.owner_pubkey,
                delta = EXCLUDED.delta
            "#,
            &slots,
            &tx_sigs,
            &mint_pubkeys,
            &account_pubkeys,
            &owner_pubkeys,
            &deltas,
        ).execute(&mut *tx).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn confirm_events(&self, events: &[Event]) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;

        // 提取 tx_sig 和 account_pubkey
        let tx_sigs: Vec<String> = events.iter().map(|event| event.tx_sig.clone()).collect();

        let account_pubkeys: Vec<String> = events
            .iter()
            .map(|event| event.account_pubkey.clone())
            .collect();

        // 批量更新 confirmed 字段
        let rows_affected = sqlx::query!(
            r#"
            UPDATE events
            SET confirmed = true
            FROM UNNEST($1::varchar[], $2::varchar[]) AS t(tx_sig, account_pubkey)
            WHERE
                events.tx_sig = t.tx_sig
                AND events.account_pubkey = t.account_pubkey
                AND events.confirmed = false
            "#,
            &tx_sigs,
            &account_pubkeys,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        tx.commit().await?;

        if rows_affected > 0 {
            info!("Confirmed {} events", rows_affected);
        }

        Ok(())
    }

    /// 将 baseline_slot 之前的所有未确认事件标记为 confirmed
    async fn confirm_events_before_baseline(
        &self,
        mint: &str,
        baseline_slot: i64,
    ) -> Result<u64, Error> {
        let rows_affected = sqlx::query!(
            r#"
            UPDATE events
            SET confirmed = true
            WHERE mint_pubkey = $1
              AND slot < $2
              AND confirmed = false
            "#,
            mint,
            baseline_slot,
        )
        .execute(&self.pool)
        .await?
        .rows_affected();

        if rows_affected > 0 {
            info!(
                "Marked {} events before baseline (slot < {}) as confirmed for mint {}",
                rows_affected, baseline_slot, mint
            );
        }

        Ok(rows_affected)
    }

    /// 从指定的游标 (last_slot, last_id) 开始，获取下一批最新的 events
    async fn get_next_events_batch(
        &self,
        cursor: (i64, i64), // 游标现在是一个元组
        mint: &str,
        limit: i64,
    ) -> Result<(Vec<Event>, Option<(i64, i64)>), Error> {
        let (last_slot, last_id) = cursor;

        // 1. 查询数据库时，使用内部的 EventFromDb 结构体来接收数据
        let events_from_db: Vec<EventFromDb> = sqlx::query_as!(
            EventFromDb, // <--- 使用内部专用的结构体
            r#"
        SELECT id, slot, tx_sig, mint_pubkey, account_pubkey, owner_pubkey, delta, confirmed
        FROM events
        WHERE (slot, id) > ($1, $2) AND mint_pubkey = $3 AND confirmed = false
        ORDER BY slot ASC, id ASC
        LIMIT $4
        "#,
            last_slot,
            last_id,
            mint,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        // 2. 确定下一次查询的新游标
        // 如果 next_cursor 返回Null，则意味着 数据库中已经没有符合条件的数据要取出来了，说明取完了
        let next_cursor = events_from_db
            .last() // 如果数据不为空，返回最后一个元素，否则返回Null
            .map(|last_event| (last_event.slot, last_event.id));

        // 3. 将包含 id 的数据库模型转换为不含 id 的通用业务模型
        let events: Vec<Event> = events_from_db
            .into_iter()
            .map(|db_event| Event {
                // 手动转换
                slot: db_event.slot,
                tx_sig: db_event.tx_sig,
                mint_pubkey: db_event.mint_pubkey,
                account_pubkey: db_event.account_pubkey,
                owner_pubkey: db_event.owner_pubkey,
                delta: db_event.delta,
                confirmed: db_event.confirmed,
            })
            .collect();

        // 4. 返回包含两个元素的元组
        Ok((events, next_cursor))
    }

    async fn get_latest_event_slot(&self) -> Result<i64> {
        Ok(1)
    }
}
