mod network;
mod file_scanner;
mod protocol;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    if let Err(e) = network::run_server().await {
        tracing::error!(error = %e, "server exited with error");
    }
}