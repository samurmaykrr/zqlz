// Stub for Zed's dap (Debug Adapter Protocol) crate

#[derive(Clone, Copy, Debug)]
pub enum TelemetrySpawnLocation {
    Editor,
}

pub fn send_telemetry(_location: TelemetrySpawnLocation) {
    // No-op stub
}
