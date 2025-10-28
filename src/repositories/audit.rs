use anyhow::Error;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;

/// todo!: 当前crate，仅用于数据验证，到生产环境就删除，审计记录结构
#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub id: i64,
    pub mint_pubkey: String,
    pub holder_count: i64,
    pub last_updated_slot: i64,
    pub total_balance: Option<Decimal>,
    pub source: Option<String>,
    pub recorded_at: Option<DateTime<Utc>>,
}

pub async fn insert_audit_records(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    mint_pubkeys: &[String],
    holder_counts: &[i64],
    max_slots: &[i64],
    total_balances: &HashMap<String, Decimal>,
    source: &str,
) -> Result<(), Error> {
    let audit_mints = std::env::var("AUDIT_MINTS").unwrap_or_default();
    let audit_list: Vec<&str> = audit_mints.split(',').filter(|s| !s.is_empty()).collect();

    if audit_list.is_empty() {
        return Ok(());
    }

    for (i, mint) in mint_pubkeys.iter().enumerate() {
        if audit_list.contains(&mint.as_str()) {
            let total_balance = total_balances.get(mint).copied().unwrap_or(Decimal::ZERO);
            sqlx::query!(
                r#"
                INSERT INTO mint_stats_audit_log
                    (mint_pubkey, holder_count, last_updated_slot, total_balance, source)
                VALUES ($1, $2, $3, $4, $5)
                "#,
                mint,
                holder_counts[i],
                max_slots[i],
                total_balance,
                source
            )
            .execute(&mut **tx)
            .await?;
        }
    }

    Ok(())
}

pub async fn get_mint_audit_history(
    pool: &PgPool,
    mint: &str,
    limit: i64,
) -> Result<Vec<AuditRecord>, Error> {
    let records = sqlx::query_as!(
        AuditRecord,
        r#"
        SELECT id, mint_pubkey, holder_count, last_updated_slot,
               total_balance, source, recorded_at
        FROM mint_stats_audit_log
        WHERE mint_pubkey = $1
        ORDER BY recorded_at DESC
        LIMIT $2
        "#,
        mint,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::postgresql::{DatabaseConfig, DatabaseConnection};
    use std::sync::Arc;
    use tokio::time::Duration;

    #[test]
    fn test_read_from_env() {
        dotenv::dotenv().ok();

        let audit_mints = std::env::var("AUDIT_MINTS").unwrap_or_default();
        println!("audit_mints: {}", audit_mints);
    }

    #[tokio::test]
    #[ignore]
    async fn test_mint_stats_audit_tracking() {
        dotenv::dotenv().ok();

        let test_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

        let db_url = std::env::var("DATABASE_URL").unwrap();
        let database_config = DatabaseConfig::new_optimized(db_url);
        let database = Arc::new(DatabaseConnection::new(database_config).await.unwrap());

        println!("Starting audit tracking test for mint: {}", test_mint);
        println!("Running for 5 minutes...");

        tokio::time::sleep(Duration::from_secs(300)).await;

        let audit_records = get_mint_audit_history(&database.pool, &test_mint, 100)
            .await
            .expect("Failed to get audit history");

        println!("\n=== Audit Records for {} ===", test_mint);
        println!("Total records: {}", audit_records.len());
        println!(
            "\n{:<6} {:<15} {:<15} {:<25} {:<20}",
            "ID", "Holder Count", "Slot", "Total Balance", "Time"
        );
        println!("{}", "-".repeat(85));

        for record in &audit_records {
            println!(
                "{:<6} {:<15} {:<15} {:<25} {:<20}",
                record.id,
                record.holder_count,
                record.last_updated_slot,
                record
                    .total_balance
                    .map(|b| b.to_string())
                    .unwrap_or_else(|| "N/A".to_string()),
                record
                    .recorded_at
                    .map(|t| t.to_rfc3339())
                    .unwrap_or_else(|| "N/A".to_string())
            );
        }

        assert!(!audit_records.is_empty(), "Should have audit records");
    }
}
