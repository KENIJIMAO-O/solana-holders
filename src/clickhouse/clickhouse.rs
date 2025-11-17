use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use fixnum::{typenum, FixedPoint};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::clickhouse::helper::ClickhouseDecimal;
use crate::{app_info};

use crate::error::{ClickHouseError, Result};

type Money = FixedPoint<i128, typenum::U12>;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct Event {
    pub slot: u64,
    pub tx_sig: String,
    pub mint_pubkey: String,
    pub account_pubkey: String,
    pub owner_pubkey: String,
    pub delta: ClickhouseDecimal,
    pub confirmed: u8,
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub _timestamp: DateTime<Utc>
}

impl Event {
    pub fn new(
        slot: u64,
        tx_sig: String,
        mint_pubkey: String,
        account_pubkey: String,
        owner_pubkey: String,
        delta: Decimal,
        confirmed: u8
    ) -> Self {
        Self {
            slot,
            tx_sig,
            mint_pubkey,
            account_pubkey,
            owner_pubkey,
            delta: ClickhouseDecimal::from_decimal(delta),
            confirmed,
            _timestamp: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct ClickHouse {
    pub client: Client
}

impl ClickHouse {
    pub fn new(url: &str, user: &str, password: &str) -> Self {
        let client = Client::default()
            .with_password(password)
            .with_user(user)
            .with_url(url);
        Self {
            client
        }
    }

    pub async fn upsert_events_batch(
        &self,
        events: &[Event]
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        app_info!("start to upsert events");

        // 批量插入events（ReplacingMergeTree会自动处理upsert逻辑）
        let mut insert = self.client.insert::<Event>("events").await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "upsert_events_batch: create insert".to_string(),
                source: e,
            })?;

        for event in events {
            let new_event = Event {
                slot: event.slot,
                tx_sig: event.tx_sig.clone(),
                mint_pubkey: event.mint_pubkey.clone(),
                account_pubkey: event.account_pubkey.clone(),
                owner_pubkey: event.owner_pubkey.clone(),
                delta: event.delta,
                confirmed: 0,                // 新事件默认未确认
                _timestamp: Utc::now(),      // 设置时间戳
            };
            insert.write(&new_event).await
                .map_err(|e| ClickHouseError::InsertFailed {
                    operation: format!("upsert_events_batch: write event {}", event.tx_sig),
                    source: e,
                })?;
        }

        insert.end().await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "upsert_events_batch: end insert".to_string(),
                source: e,
            })?;
        Ok(())
    }

    pub async fn confirm_events(&self, events: &[Event]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // 批量插入新版本的events（confirmed=1, timestamp=now）
        let mut insert = self.client.insert::<Event>("events").await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "confirm_events: create insert".to_string(),
                source: e,
            })?;

        for event in events {
            let updated_event = Event {
                slot: event.slot,
                tx_sig: event.tx_sig.clone(),
                mint_pubkey: event.mint_pubkey.clone(),
                account_pubkey: event.account_pubkey.clone(),
                owner_pubkey: event.owner_pubkey.clone(),
                delta: event.delta,
                confirmed: 1,                // 标记为已确认
                _timestamp: Utc::now(),      // 更新时间戳，确保是最新版本
            };
            insert.write(&updated_event).await
                .map_err(|e| ClickHouseError::InsertFailed {
                    operation: format!("confirm_events: write event {}", event.tx_sig),
                    source: e,
                })?;
        }

        insert.end().await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "confirm_events: end insert".to_string(),
                source: e,
            })?;
        Ok(())
    }

    pub async fn confirm_events_before_baseline(
        &self,
        mint: &str,
        baseline_slot: i64,
    ) -> Result<u64> {
        // 1. 查询符合条件的events
        let query_template = "
            SELECT * FROM events
            WHERE mint_pubkey = ?
            AND slot < ?
            AND confirmed = 0
        ";

        // 2. 使用 .bind() 方法安全地绑定变量
        let mut cursor = self.client
            .query(query_template)
            .bind(mint)                 // 绑定第一个 '?' (mint_pubkey)
            .bind(baseline_slot as u64) // 绑定第二个 '?' (slot)
            .fetch::<Event>()
            .map_err(|e| ClickHouseError::QueryFailed {
                query: format!("confirm_events_before_baseline: query mint={} slot={}", mint, baseline_slot),
                source: e,
            })?;

        let mut count = 0u64;
        let mut insert = self.client.insert::<Event>("events").await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "confirm_events_before_baseline: create insert".to_string(),
                source: e,
            })?;

        while let Some(mut event) = cursor.next().await
            .map_err(|e| ClickHouseError::QueryFailed {
                query: format!("confirm_events_before_baseline: fetch next event for mint={}", mint),
                source: e,
            })? {
            // ... 处理 event
            event.confirmed = 1;
            event._timestamp = Utc::now();
            insert.write(&event).await
                .map_err(|e| ClickHouseError::InsertFailed {
                    operation: format!("confirm_events_before_baseline: write event {}", event.tx_sig),
                    source: e,
                })?;

            count += 1;
        }

        insert.end().await
            .map_err(|e| ClickHouseError::InsertFailed {
                operation: "confirm_events_before_baseline: end insert".to_string(),
                source: e,
            })?;

        println!(
                "Marked {} events before baseline (slot < {}) as confirmed for mint {}",
                count, baseline_slot, mint
            );

        Ok(count)
    }

    pub async fn get_next_events_batch(
        &self,
        cursor: (i64, i64),
        mint: &str,
        limit: i64,
    ) -> Result<(Vec<Event>, Option<(i64, i64)>)> {
        let (last_slot, last_timestamp) = cursor;

        // 构造查询：使用(slot, _timestamp)作为游标
        // last_timestamp是Unix时间戳秒数，需要转换为DateTime比较
        let query = format!(
            "SELECT * FROM events
             WHERE mint_pubkey = '{}'
             AND confirmed = 0
             AND (slot > {} OR (slot = {} AND toUnixTimestamp(_timestamp) > {}))
             ORDER BY slot ASC, _timestamp ASC
             LIMIT {}",
            mint,
            last_slot as u64,
            last_slot as u64,
            last_timestamp,
            limit
        );

        let events = self.client
            .query(&query)
            .fetch_all::<Event>()
            .await
            .map_err(|e| ClickHouseError::QueryFailed {
                query: format!("get_next_events_batch: mint={} cursor=({},{}) limit={}",
                    mint, last_slot, last_timestamp, limit),
                source: e,
            })?;

        // 确定下一个游标
        let next_cursor = events.last().map(|last_event| {
            (last_event.slot as i64, last_event._timestamp.timestamp())
        });

        Ok((events, next_cursor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use crate::utils::task_logger::TaskLogger;

    fn init_clickhouse() -> ClickHouse {
        ClickHouse::new("http://localhost:8123", "default", "12345")
    }

    async fn cleanup_events_table() {
        let ch = init_clickhouse();
        let _ = ch.client.query("TRUNCATE TABLE events").execute().await;
    }

    #[tokio::test]
    async fn test_cleanup_events_table() {
        cleanup_events_table().await
    }

    #[tokio::test]
    async fn test_upsert_events_batch() {
        cleanup_events_table().await;

        let ch = init_clickhouse();
        // 1. 插入3条events
        let events = vec![
            Event {
                slot: 100,
                tx_sig: "sig1".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account1".to_string(),
                owner_pubkey: "owner1".to_string(),
                delta: ClickhouseDecimal::from_f64(-1.12112120),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
            Event {
                slot: 200,
                tx_sig: "sig2".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account2".to_string(),
                owner_pubkey: "owner2".to_string(),
                delta: ClickhouseDecimal::from_f64(1.0),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
            Event {
                slot: 300,
                tx_sig: "sig3".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account3".to_string(),
                owner_pubkey: "owner3".to_string(),
                delta: ClickhouseDecimal::from_f64(1.5),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
        ];

        ch.upsert_events_batch(&events).await.unwrap();

        // 2. 查询验证
        let count: u64 = ch.client
            .query("SELECT count() FROM events")
            .fetch_one()
            .await
            .unwrap();

        println!("✅ 插入后count: {}", count);
        assert!(count >= 3, "应该至少有3条记录");

        // 3. 再次插入相同的(tx_sig, account_pubkey)但delta不同 - 测试upsert
        let updated_events = vec![
            Event {
                slot: 150,
                tx_sig: "sig1".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account1".to_string(),
                owner_pubkey: "owner1_updated".to_string(),
                delta: ClickhouseDecimal::from_f64(555.0121),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
        ];

        ch.upsert_events_batch(&updated_events).await.unwrap();

        println!("✅ test_upsert_events_batch 测试通过");
    }

    #[tokio::test]
    async fn test_confirm_events() {
        cleanup_events_table().await;

        let ch = init_clickhouse();

        // 1. 先插入一些未确认的events
        let events = vec![
            Event {
                slot: 100,
                tx_sig: "sig1".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account1".to_string(),
                owner_pubkey: "owner1".to_string(),
                delta: ClickhouseDecimal::from_f64(80.0212),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
            Event {
                slot: 200,
                tx_sig: "sig2".to_string(),
                mint_pubkey: "mint1".to_string(),
                account_pubkey: "account2".to_string(),
                owner_pubkey: "owner2".to_string(),
                delta: ClickhouseDecimal::from_f64(454.0),
                confirmed: 0,
                _timestamp: Utc::now(),
            },
        ];

        ch.upsert_events_batch(&events).await.unwrap();

        // 2. 调用confirm_events
        ch.confirm_events(&events).await.unwrap();

        // 3. 稍等片刻让ClickHouse merge（可选）
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        println!("✅ test_confirm_events 测试通过");
    }

    #[tokio::test]
    async fn test_confirm_events_before_baseline() {
        let ch = init_clickhouse();
        let count = ch.confirm_events_before_baseline("mint1", 300).await.unwrap();

        println!("✅ 确认了 {} 条baseline前的events", count);
        assert!(count >= 2, "应该确认至少2条记录（slot=100和200）");

        println!("✅ test_confirm_events_before_baseline 测试通过");
    }

    #[tokio::test]
    async fn test_get_next_events_batch() {

        let ch = init_clickhouse();
        let mut logger = TaskLogger::new("test_pagination", "1");

        // 2. 第一页：从(0, 0)开始，limit=2
        let (page1, cursor1) = ch.get_next_events_batch((0, 0), "mint1", 2).await.unwrap();

        println!("✅ 第一页返回 {} 条记录", page1.len());
        println!("✅ 第一页内容: {:?}", page1);
        assert!(page1.len() <= 2, "第一页应该最多2条记录");

        if let Some(next_cursor) = cursor1 {
            println!("✅ 第一页游标: ({}, {})", next_cursor.0, next_cursor.1);

            // 3. 第二页：使用next_cursor继续查询
            let (page2, cursor2) = ch.get_next_events_batch(next_cursor, "mint1", 2).await.unwrap();
            println!("✅ 第二页返回 {} 条记录", page2.len());
            println!("✅ 第二页内容: {:?}", page2);

            if let Some(next_cursor2) = cursor2 {
                println!("✅ 第二页游标: ({}, {})", next_cursor2.0, next_cursor2.1);
            }
        }

        println!("✅ test_get_next_events_batch 测试通过");
    }
}