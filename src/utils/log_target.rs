/// app log - 自动使用 APP_LOG_TARGET
#[macro_export]
macro_rules! app_info {
    ($($arg:tt)*) => {
        tracing::info!(target: $crate::APP_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! app_debug {
    ($($arg:tt)*) => {
        tracing::debug!(target: $crate::APP_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! app_warn {
    ($($arg:tt)*) => {
        tracing::warn!(target: $crate::APP_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! app_error {
    ($($arg:tt)*) => {
        tracing::error!(target: $crate::APP_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! app_trace {
    ($($arg:tt)*) => {
        tracing::trace!(target: $crate::APP_LOG_TARGET, $($arg)*)
    };
}


/// request log - 自动使用 REQUEST_LOG_TARGET
#[macro_export]
macro_rules! request_info {
    ($($arg:tt)*) => {
        tracing::info!(target: $crate::REQUEST_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! request_debug {
    ($($arg:tt)*) => {
        tracing::debug!(target: $crate::REQUEST_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! request_warn {
    ($($arg:tt)*) => {
        tracing::warn!(target: $crate::REQUEST_LOG_TARGET, $($arg)*)
    };
}

#[macro_export]
macro_rules! request_error {
    ($($arg:tt)*) => {
        tracing::error!(target: $crate::REQUEST_LOG_TARGET, $($arg)*)
    };
}
