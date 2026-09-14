mod network;
mod file_scanner;
mod protocol;

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Sets up two log destinations at once:
/// - stdout: human-readable, colored, respects RUST_LOG (defaults to "info").
/// - logs/server.log.<date>: structured JSON, one file per day, so a sync
///   session can still be reconstructed after the terminal is gone (e.g.
///   `jq 'select(.span.conn_id == 7)' logs/server.log.2026-09-14`).
///
/// The returned guard must stay alive for the whole program - dropping it
/// flushes and shuts down the background file-writer thread. Bind it in
/// main() with a name (`_log_guard`), never `let _ = init_tracing()`.
fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let _ = std::fs::create_dir_all("logs");
    let file_appender = tracing_appender::rolling::daily("logs", "server.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer()) // stdout, human-readable
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false).json()) // file, JSON
        .init();

    guard
}

#[tokio::main]
async fn main() {
    let _log_guard = init_tracing();

    if let Err(e) = network::run_server().await {
        tracing::error!(error = %e, "server exited with error");
    }
}