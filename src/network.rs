use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use crate::protocol::SyncMessage;

use std::path::Path;
use crate::file_scanner::build_manifest;

pub async fn run_server() {
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("Server listening on: 127.0.0.1:8080");

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        println!("New connection from: {}", addr);

        tokio::spawn(async move {
            let (read_half, mut write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);

            let manifest = build_manifest("received_files");
            let manifest_message = SyncMessage::Manifest(manifest);
            let json = serde_json::to_string(&manifest_message).unwrap();

            write_half.write_all(json.as_bytes()).await.unwrap();
            write_half.write_all(b"\n").await.unwrap();
            println!("Sent manifest to {}: {}", addr, json); 

            let mut line = String::new();
            loop {
                line.clear();

                let n = reader.read_line(&mut line).await.unwrap();

                if n == 0 {
                    println!("Connection closed by client: {}", addr);
                    break;
                }

                match serde_json::from_str::<SyncMessage>(line.trim()) {
                    Ok(message) => {
                        match message {
                            SyncMessage::FileInfo(file_info) => {
                                println!("Received FileInfo from {}: {:?}", addr, file_info);
                            }
                            SyncMessage::SyncComplete => {
                                println!("Received SyncComplete from {}", addr );
                            }
                            SyncMessage::FileContent { path, content } => {
                                println!("Received FileContent from {}: path: {}, content length: {}", addr, path, content.len());
                            
                                let safe_path = Path::new("received_files").join(&path);
                                if let Some(parent) = safe_path.parent() {
                                    std::fs::create_dir_all(parent).unwrap();
                                }

                                std::fs::write(&safe_path, &content).unwrap();
                            
                                println!("Saved file content to: {}", safe_path.display());
                            }
                            SyncMessage::Manifest(_) => {
                                println!("Unexpected Manifest message from {}. ", addr);
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("Failed to parse JSON from {}: {}", addr, err);
                    }
                }
            }
        });   
    }
}