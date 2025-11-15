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

# 复制源代码和配置文件
COPY src ./src
COPY migrations ./migrations

# 复制 SQLX 离线缓存文件
COPY .sqlx ./.sqlx

# 设置 SQLX 离线模式
ENV SQLX_OFFLINE=true

# 构建应用程序
RUN cargo build --release

# 安装 sqlx-cli 用于运行迁移
RUN cargo install sqlx-cli --no-default-features --features postgres

# ================================
# Runtime stage - 运行环境
# ================================
FROM ubuntu:24.04

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    bash \
    libssl3 \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 创建非 root 用户
RUN groupadd -r solana && useradd -r -g solana solana

WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /app/target/release/solana_holders ./
COPY --from=builder /root/.cargo/bin/sqlx /usr/local/bin/sqlx

# 复制迁移文件
COPY migrations ./migrations

# 创建启动脚本
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Running database migrations..."\n\
sqlx migrate run --database-url "$DATABASE_URL"\n\
echo "Starting application..."\n\
exec ./solana_holders' > /app/start.sh && \
    chmod +x /app/start.sh

# 修改所有权
RUN chown -R solana:solana /app

# 切换到非 root 用户
USER solana

# 健康检查：检查进程是否存在
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep -f solana_holders > /dev/null || exit 1

# 运行启动脚本
CMD ["/app/start.sh"]