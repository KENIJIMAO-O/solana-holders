use std::time::Instant;
use tracing::{debug, info};

/// TaskLogger - 任务级别的日志收集器
/// 用于收集单个异步任务的所有日志信息，避免多线程交错问题
pub struct TaskLogger {
    task_id: String,
    task_type: String,
    logs: Vec<(u64, String)>, // (elapsed_ms, message)
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

    /// 添加日志条目
    pub fn log(&mut self, message: &str) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, message.to_string()));
    }

    /// 添加格式化日志条目
    pub fn log_fmt(&mut self, message: String) {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        self.logs.push((elapsed_ms, message));
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

        info!(
            "=== {} {} Processing: {} ===",
            icon, self.task_type, self.task_id
        );

        // 输出所有收集的日志
        for (elapsed_ms, message) in &self.logs {
            info!("  [{}ms] {}", elapsed_ms, message);
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
            for (elapsed_ms, message) in &self.logs {
                info!("  [{}ms] {}", elapsed_ms, message);
            }
            debug!("=== Task Completed: {}ms total ===", total_ms);
            self.flushed = true;
        }
    }
}
