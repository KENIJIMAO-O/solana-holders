# ================================
# Build stage - 编译 Rust 程序
# ================================
FROM rust:latest AS builder

# 安装编译依赖
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ⚡ 优化：先复制依赖文件，利用 Docker 缓存层
COPY Cargo.toml Cargo.lock ./

# ⚡ 优化：构建依赖（这一层会被缓存）
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# 复制源代码
COPY src ./src

# 复制 SQLX 离线缓存文件
COPY .sqlx ./.sqlx

# 设置 SQLX 离线模式
ENV SQLX_OFFLINE=true

# 构建应用程序
RUN cargo build --release

# ================================
# Runtime stage - 运行环境
# ================================
FROM ubuntu:24.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libssl3 \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 创建非 root 用户
RUN groupadd -r solana && useradd -r -g solana solana

WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /app/target/release/solana_holders ./

# 修改所有权
RUN chown -R solana:solana /app

# 切换到非 root 用户
USER solana

# 健康检查：检查进程是否存在
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD pgrep -f solana_holders > /dev/null || exit 1

# 运行程序
CMD ["./solana_holders"]