// Stub for Zed's telemetry crate

pub mod event {
    pub trait TelemetryEvent: Send + Sync {
        fn to_json(&self) -> serde_json::Value;
    }
}

#[macro_export]
macro_rules! event {
    ($($tt:tt)*) => {{
        // No-op stub
    }};
}
