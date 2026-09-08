mod network;
mod file_scanner;
mod protocol;

#[tokio::main]
async fn main() {
    network::run_server().await;
}   