use crate::error::DatabaseError;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;
use crate::app_info;

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    // 连接相关
    pub database_url: String,

    // 连接池大小配置
    pub max_connections: u32,

    // 超时配置（单位：秒）
    pub acquire_timeout: u64, // 当连接池满时，新请求等待空闲连接的最长时间，超过该时间报错(事务处理的速度)
    pub idle_timeout: u64, // 当连接池需要创建数据库连接时，建立TCP连接+PostgreSQL握手的最长时间(连接建立的速度)
    pub max_lifetime: u64, // 当连接被归还到连接池时，会测试连接是否仍然有效的最长时间(如果超时则说明当前与数据库建立连接丢失，同时改连接会被丢弃)
}

impl DatabaseConfig {
    pub fn new_with_param(
        database_url: String,
        max_connections: u32,
        acquire_timeout: u64,
        idle_timeout: u64,
        max_lifetime: u64,
    ) -> Self {
        Self {
            database_url,
            max_connections,
            acquire_timeout,
            idle_timeout,
            max_lifetime,
        }
    }

    /// 根据系统资源和环境变量创建优化的数据库配置
    pub fn new_optimized(database_url: String) -> Self {
        use std::env;

        // 数据库连接池配置常量
        const SYSTEM_OVERHEAD_CONNECTIONS: u32 = 20; // 健康检查、监控、管理任务等
        const MIN_CONNECTIONS_DIVISOR: u64 = 4; // min_connections = max_connections / 4

        // 数据库超时配置常量（秒）
        const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 15;
        const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600; // 10分钟
        const DEFAULT_MAX_LIFETIME_SECS: u64 = 3600; // 1小时

        // 智能配置辅助函数
        let get_env_or_default = |key: &str, default: u64| {
            env::var(key)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default)
        };

        // 智能计算连接池大小 - 基于实际并发需求而非CPU核心数
        let max_connections = get_env_or_default("DB_MAX_CONNECTIONS", 0) as u32;
        let max_connections = if max_connections == 0 {
            // 分析系统实际并发需求:
            // - SyncController max_concurrent_updates: SYNC_CONTROLLER_MAX_CONCURRENT_UPDATES
            // - Storage task limit (STORAGE_TASK_LIMIT)
            // - Heavy RPC semaphore
            // - 预留给健康检查、监控等: SYSTEM_OVERHEAD_CONNECTIONS
            let storage_tasks = get_env_or_default("STORAGE_TASK_LIMIT", 100) as u32;
            let heavy_rpc_limit = get_env_or_default("HEAVY_RPC_CONCURRENCY", 15) as u32;

            let calculated = (storage_tasks + heavy_rpc_limit + SYSTEM_OVERHEAD_CONNECTIONS)
                .max(30) // 最小连接数
                .min(300); // 最大连接数，避免过度占用数据库资源

            app_info!(
                "Auto-calculated max_connections: {} (based on storage_tasks: {}, heavy_rpc: {}, overhead: {})",
                calculated, storage_tasks, heavy_rpc_limit, SYSTEM_OVERHEAD_CONNECTIONS
            );
            calculated
        } else {
            max_connections
        };

        let acquire_timeout = DEFAULT_ACQUIRE_TIMEOUT_SECS;
        let idle_timeout = DEFAULT_IDLE_TIMEOUT_SECS;
        let max_lifetime = DEFAULT_MAX_LIFETIME_SECS;

        Self {
            database_url,
            max_connections,
            acquire_timeout,
            idle_timeout,
            max_lifetime,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatabaseConnection {
    pub pool: PgPool,
}

impl DatabaseConnection {
    pub async fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout))
            .idle_timeout(Duration::from_secs(config.idle_timeout))
            .max_lifetime(Duration::from_secs(config.max_lifetime))
            // .connect_timeout() 也是一个有用的选项，用于设置建立连接本身的超时
            .connect(&config.database_url)
            .await?;

        app_info!("Database pool initialized successfully with sqlx.");

        Ok(Self { pool })
    }

    pub async fn get_pool(&self) -> PgPool {
        self.pool.clone()
    }
}