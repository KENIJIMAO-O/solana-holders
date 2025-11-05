use std::time::Instant;
use tracing::{Level, debug, error, info, warn};

/// TaskLogger - 任务级别的日志收集器
/// 用于收集单个异步任务的所有日志信息，避免多线程交错问题
pub struct TaskLogger {
    task_id: String,
    task_type: String,
    logs: Vec<(u64, Level, String)>, // (elapsed_ms, level, message)
    start_time: Instant,
    flushed: bool, // 防止重复输出
}

impl TaskLogger {
    /// 创建新的任务日志收集器
    pub fn new(task_type: &str, task_id: &str) -> Self {
        Self {
            task_id: task_id.to_string(),
            task_type: task_type.to_string(),
            logs: Vec::new(),
            start_time: Instant::now(),
            flushed: false,
        }
    }

    /// 根据任务类型获取相应的图标
    pub fn get_task_icon(&self) -> &'static str {
        if self.task_type.contains("NewToken") || self.task_type.contains("Token Creation") {
            "🪙" // 代币创建图标
        } else if self.task_type.contains("Info Update")
            || self.task_type.contains("Update")
            || self.task_type.contains("Sync")
        {
            "🔄" // 更新/同步图标
        } else {
            "📋" // 默认任务图标
        }
    }

    /// 添加日志条目（默认 INFO 级别，保持向后兼容）
    pub fn log(&mut self, message: &str) {
        self.info(message);
    }

    /// 添加格式化日志条目（默认 INFO 级别，保持向后兼容）
    pub fn log_fmt(&mut self, message: String) {
        self.info_fmt(message);
    }

    /// 添加 DEBUG 级别日志
    pub fn debug(&mut self, message: &str) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs
            .push((elapsed_ms, Level::DEBUG, message.to_string()));
    }

    /// 添加格式化的 DEBUG 级别日志
    pub fn debug_fmt(&mut self, message: String) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, Level::DEBUG, message));
    }

    /// 添加 INFO 级别日志
    pub fn info(&mut self, message: &str) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs
            .push((elapsed_ms, Level::INFO, message.to_string()));
    }

    /// 添加格式化的 INFO 级别日志
    pub fn info_fmt(&mut self, message: String) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, Level::INFO, message));
    }

    /// 添加 WARN 级别日志
    pub fn warn(&mut self, message: &str) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs
            .push((elapsed_ms, Level::WARN, message.to_string()));
    }

    /// 添加格式化的 WARN 级别日志
    pub fn warn_fmt(&mut self, message: String) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, Level::WARN, message));
    }

    /// 添加 ERROR 级别日志
    pub fn error(&mut self, message: &str) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs
            .push((elapsed_ms, Level::ERROR, message.to_string()));
    }

    /// 添加格式化的 ERROR 级别日志
    pub fn error_fmt(&mut self, message: String) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, Level::ERROR, message));
    }

    /// 获取任务总耗时
    pub fn total_duration_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }

    /// 一次性输出所有收集的日志
    pub fn flush(mut self) {
        if self.flushed {
            return; // 已经输出过，避免重复
        }

        let total_ms = self.total_duration_ms();
        let icon = self.get_task_icon();

        debug!(
            "=== {} {} Processing: {} ===",
            icon, self.task_type, self.task_id
        );

        // 输出所有收集的日志，根据等级调用相应的宏
        for (elapsed_ms, level, message) in &self.logs {
            match *level {
                Level::ERROR => error!("  [{}ms] {}", elapsed_ms, message),
                Level::WARN => warn!("  [{}ms] {}", elapsed_ms, message),
                Level::INFO => info!("  [{}ms] {}", elapsed_ms, message),
                Level::DEBUG => debug!("  [{}ms] {}", elapsed_ms, message),
                Level::TRACE => debug!("  [{}ms] {}", elapsed_ms, message),
            }
        }

        // 输出任务完成标记
        debug!("=== Task Completed: {}ms total ===", total_ms);

        self.flushed = true;
    }

    /// 获取任务ID（用于调试）
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// 获取任务类型（用于调试）
    pub fn task_type(&self) -> &str {
        &self.task_type
    }
}

impl Drop for TaskLogger {
    fn drop(&mut self) {
        if !self.flushed && !self.logs.is_empty() {
            let total_ms = self.total_duration_ms();
            let icon = self.get_task_icon();
            info!(
                "=== {} {} Processing: {} ===",
                icon, self.task_type, self.task_id
            );
            for (elapsed_ms, level, message) in &self.logs {
                match *level {
                    Level::ERROR => error!("  [{}ms] {}", elapsed_ms, message),
                    Level::WARN => warn!("  [{}ms] {}", elapsed_ms, message),
                    Level::INFO => info!("  [{}ms] {}", elapsed_ms, message),
                    Level::DEBUG => debug!("  [{}ms] {}", elapsed_ms, message),
                    Level::TRACE => debug!("  [{}ms] {}", elapsed_ms, message),
                }
            }
            debug!("=== Task Completed: {}ms total ===", total_ms);
            self.flushed = true;
        }
    }
}
