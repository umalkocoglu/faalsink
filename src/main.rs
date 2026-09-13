mod network;
mod file_scanner;
mod protocol;

#[tokio::main]
async fn main() {
    if let Err(e) = network::run_server().await {
        eprintln!("Server exited with error: {}", e);
    }
}