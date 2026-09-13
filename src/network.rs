use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use anyhow::{Context, Result};
use crate::protocol::SyncMessage;

use std::path::{Path, PathBuf, Component};
use crate::file_scanner::build_manifest;

fn build_safe_path(base: &str, user_path: &str) -> Result<PathBuf, &'static str> {
    let path = Path::new(user_path);

    for component in path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err("Safety breach: Invalid file path component.");
            }
            _ => {}
        }
    }

    Ok(Path::new(base).join(path))
}



pub async fn run_server() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await.context("Could not bind the server")?;
    println!("Server listening on: 127.0.0.1:8080");

    if let Err(e) = std::fs::create_dir_all("received_files") {
        eprintln!("Warning! Could not create the directory received_files: {}", e);
    }

    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                eprintln!("Connection could not be accepted: {}", e);
                continue;
            }
        };

        println!("New connection from: {}", addr);

        tokio::spawn(async move {
            let (read_half, mut write_half) = socket.into_split();
            let mut reader = BufReader::new(read_half);

            let manifest = build_manifest("received_files");
            let manifest_message = SyncMessage::Manifest(manifest);

            let Ok(json) = serde_json::to_string(&manifest_message) else { return; };

            if write_half.write_all(json.as_bytes()).await.is_err() { return; }
            if write_half.write_all(b"\n").await.is_err() { return; }
            println!("Sent manifest to {}: {}", addr, json); 

            let mut line = String::new();
            loop {
                line.clear();

                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        println!("Connection closed by client: {}", addr);
                        break;
                    }
                    Ok(_) => {
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
                                        match build_safe_path("received_files", &path) {
                                            Ok(safe_path) => {
                                                if let Some(parent) = safe_path.parent() {
                                                    let _ = std::fs::create_dir_all(parent);
                                                }
                                                match std::fs::write(&safe_path, &content) {
                                                    Ok(_) => println!("Saved FileContent to: {}", safe_path.display()),
                                                    Err(e) => eprintln!("Error writing file: {}", e),
                                                }
                                            }
                                            Err(e) => eprintln!("Rejected (FileContent) {}: {}", addr, e),
                                        }
                                    }
                                    SyncMessage::DeleteFile {path} => {
                                        match build_safe_path("received_files", &path) {
                                            Ok(safe_path) => {
                                                match std::fs::remove_file(&safe_path) {
                                                    Ok(_) => println!("Deleted File: {}", safe_path.display()),
                                                    Err(e) => eprintln!("Could not delete file {}: {}", safe_path.display(), e),
                                                }
                                            }
                                            Err(e) => eprintln!("Rejected (DeleteFile) {}: {}", addr, e),
                                        }

                                    }
                                    SyncMessage::CreateDir { path } => {
                                        match build_safe_path("received_files", &path) {
                                            Ok(safe_path) => {
                                                match std::fs::create_dir_all(&safe_path) {
                                                    Ok(_) => println!("Created directory: {}", safe_path.display()),
                                                    Err(e) => eprintln!("Could not create directory {}: {}", safe_path.display(), e),
                                                }
                                            }
                                            Err(e) => eprintln!("Rejected (CreateDir) {}: {}", addr, e)
                                        }
                                    }
                                    SyncMessage::RemoveDir { path } => {
                                        match build_safe_path("received_files", &path) {
                                            Ok(safe_path) => {
                                                match std::fs::remove_dir_all(&safe_path) {
                                                    Ok(_) => println!("Removed directory: {}", safe_path.display()),
                                                    Err(e) => eprintln!("Could not remove directory {}: {}", safe_path.display(), e),
                                                }
                                            }
                                            Err(e) => eprintln!("Rejected (RemoveDir) {}: {}", addr, e)
                                        }
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
                    Err(err) => {
                        eprintln!("Network error while reading from {}: {}", addr, err);
                        break;
                    }
                }
            }
        });   
    }
}