use thiserror::Error;

pub type Result<T> = std::result::Result<T, SolanaHoldersError>;

#[derive(Error, Debug)]
pub enum SolanaHoldersError{
    #[error("gRPC 错误: {0}")]
    Grpc(#[from] GrpcError),

    #[error("数据库错误: {0}")]
    Database(#[from] DatabaseError),

    #[error("ClickHouse 错误: {0}")]
    ClickHouse(#[from] ClickHouseError),

    #[error("Kafka 错误: {0}")]
    Kafka(#[from] KafkaError),

    #[error("Baseline 错误: {0}")]
    Baseline(#[from] BaselineError),

    #[error("Reconciliation 错误: {0}")]
    Reconciliation(#[from] ReconciliationError),

    #[error("解析错误: {0}")]
    Parse(#[from] ParseError),

    #[error("配置错误: {0}")]
    Config(#[from] ConfigError),

    #[error("验证错误: {0}")]
    Validation(#[from] ValidationError),
}


// ========== gRPC 模块错误 ==========
#[derive(Error, Debug)]
pub enum GrpcError {
    #[error("gRPC 连接失败: {url}, 原因: {source}")]
    ConnectionFailed {
        url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("gRPC 订阅失败: {0}")]
    SubscriptionFailed(String),

    #[error("gRPC 流错误: {0}")]
    StreamError(String),

    #[error("gRPC 流意外结束")]
    StreamEnded,
}

// ========== 数据库模块错误 ==========
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("数据库连接失败: {0}")]
    ConnectionFailed(#[from] sqlx::Error), // #[from] 这个属性宏可以自动的将sqlx::Error转换成DatabaseError

    #[error("连接池获取超时")]
    PoolTimeout,

    #[error("SQL 执行失败: {query}, 原因: {source}")]
    QueryFailed {
        query: String,
        source: sqlx::Error,
    },

    #[error("事务失败: {0}")]
    TransactionFailed(String),
}


// ========== ClickHouse 模块错误 ==========
#[derive(Error, Debug)]
pub enum ClickHouseError {
    #[error("ClickHouse 连接失败: {0}")]
    ConnectionFailed(String),

    #[error("ClickHouse 插入失败: {operation}, 原因: {source}")]
    InsertFailed {
        operation: String,
        source: clickhouse::error::Error,
    },

    #[error("ClickHouse 查询失败: {query}, 原因: {source}")]
    QueryFailed {
        query: String,
        source: clickhouse::error::Error,
    },
}

// ========== Kafka 模块错误 ==========
#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Kafka Producer 创建失败: {0}")]
    ProducerCreationFailed(String),

    #[error("Kafka Consumer 创建失败: {0}")]
    ConsumerCreationFailed(String),

    #[error("Kafka 消息发送失败: topic={topic}, 原因: {reason}")]
    SendFailed { topic: String, reason: String },

    #[error("Kafka 消息接收失败: {0}")]
    ReceiveFailed(String),

    #[error("Kafka 序列化失败: {0}")]
    SerializationFailed(String),
}

// ========== Baseline/RPC 模块错误 ==========
#[derive(Error, Debug)]
pub enum BaselineError {
    #[error("HTTP 客户端创建失败: {0}")]
    ClientCreationFailed(String),

    #[error("RPC 调用失败: {method}, 原因: {source}")]
    RpcCallFailed {
        method: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("RPC 调用超时: {0}")]
    RpcTimeout(String),

    #[error("解析失败: {operation}, 原因: {source}")]
    ParseFailed {
        operation: String,
        source: serde_json::Error,
    },

    #[error("无效的 Token 程序: {program_id}")]
    InvalidTokenProgram {
        program_id: String,
    },

    #[error("Solana RPC 错误: {0}")]
    SolanaRpc(#[from] solana_client::client_error::ClientError),
}


// ========== Reconciliation 对账 模块错误 ==========
#[derive(Error, Debug)]
pub enum ReconciliationError {
    #[error("HTTP 客户端创建失败: {0}")]
    ServerCreationFailed(String),

    #[error("数据库连接失败: {0}")]
    ConfigError(#[from] sqlx::Error),

    #[error("RPC 调用失败: {method}, 原因: {source}")]
    ReconcileManualFailed {
        method: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

// ========== 解析错误 ==========
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("交易缺少 Meta 数据: slot={slot}, sig={sig}")]
    MissingMeta { slot: u64, sig: String },

    #[error("交易解码失败: slot={slot}, sig={sig}")]
    TransactionDecodeFailed { slot: u64, sig: String },

    #[error("账户索引越界: index={index}, sig={sig}")]
    AccountIndexOutOfBounds { index: usize, sig: String },

    #[error("签名转换失败")]
    SignatureConversionFailed,

    #[error("数据格式错误: {0}")]
    InvalidFormat(String),

    #[error("JSON 解析失败: {0}")]
    JsonParse(#[from] serde_json::Error),
}

// ========== 配置错误 ==========
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("环境变量缺失: {0}")]
    MissingEnvVar(String),

    #[error("配置值无效: {key}={value}")]
    InvalidValue { key: String, value: String },
}


#[derive(Error, Debug)]
pub enum ValidationError {
    #[error("参数验证失败: {field}, 原因: {reason}")]
    InvalidParameter { field: String, reason: String },

    #[error("数据为空: {0}")]
    EmptyData(String),
}

// ========== 辅助方法 ==========
impl SolanaHoldersError {
    /// 判断错误是否可重试
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Grpc(e) => e.is_retryable(),
            Self::Database(e) => e.is_retryable(),
            Self::Kafka(e) => e.is_retryable(),
            Self::Baseline(e) => e.is_retryable(),
            _ => false,
        }
    }

    /// 判断是否为连接错误
    pub fn is_connection_error(&self) -> bool {
        matches!(
              self,
              Self::Grpc(GrpcError::ConnectionFailed { .. })
                  | Self::Database(DatabaseError::ConnectionFailed(_))
                  | Self::Database(DatabaseError::PoolTimeout)
          )
    }

    /// 判断是否为致命错误（不可恢复）
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Config(_))
    }
}

impl GrpcError {
    pub fn is_retryable(&self) -> bool {
        matches!(
              self,
              Self::ConnectionFailed { .. } | Self::StreamError(_) | Self::StreamEnded
          )
    }
}

impl DatabaseError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::PoolTimeout | Self::ConnectionFailed(_))
    }
}

impl KafkaError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::SendFailed { .. } | Self::ReceiveFailed(_))
    }
}

impl BaselineError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::RpcTimeout(_) | Self::RpcCallFailed { .. })
    }
}
