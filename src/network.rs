use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use anyhow::{Context, Result};
use crate::protocol::SyncMessage;

use std::net::SocketAddr;
use std::path::{Path, PathBuf, Component};
use std::sync::Arc;
use crate::file_scanner::{build_manifest, FileInfo};

/// Chunks are always <= 64 KB before compression, and LZ4 never expands
/// data by much. Anything claiming to be bigger than this is either a
/// corrupt stream or a hostile length field - reject it instead of
/// allocating whatever size it asks for.
const MAX_CHUNK_LEN: u32 = 1024 * 1024; // 1 MB

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

/// Consumes and discards a chunked byte stream (used when we can't accept the
/// incoming file) so the reader stays aligned on the next JSON line.
/// Bails out instead of draining if a chunk claims to exceed MAX_CHUNK_LEN,
/// since at that point we can no longer trust the stream at all.
async fn drain_stream(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Result<()> {
    loop {
        let chunk_len = reader.read_u32().await?;
        if chunk_len == 0 {
            break;
        }
        if chunk_len > MAX_CHUNK_LEN {
            anyhow::bail!("Chunk length {} exceeds the {}-byte limit", chunk_len, MAX_CHUNK_LEN);
        }
        let mut trash = vec![0u8; chunk_len as usize];
        reader.read_exact(&mut trash).await?;
    }
    Ok(())
}

async fn handle_connection(socket: TcpStream, addr: SocketAddr, expected_token: Arc<String>) -> Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);

    // First message on the wire must be Auth with a matching token.
    let mut auth_line = String::new();
    let bytes_read = reader.read_line(&mut auth_line).await?;
    if bytes_read == 0 {
        println!("Connection closed by {} before authenticating", addr);
        return Ok(());
    }

    let authenticated = matches!(
        serde_json::from_str::<SyncMessage>(auth_line.trim()),
        Ok(SyncMessage::Auth { token }) if token == *expected_token
    );

    if !authenticated {
        eprintln!("Rejected connection from {}: authentication failed", addr);
        return Ok(());
    }

    println!("Authenticated connection from {}", addr);

    let manifest = build_manifest("received_files");
    let manifest_message = SyncMessage::Manifest(manifest);
    let json = serde_json::to_string(&manifest_message)?;

    write_half.write_all(json.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    println!("Sent manifest to {}: {}", addr, json);

    // Set by FileInfo, consumed by the FileContentStream that immediately
    // follows it - this pairing order is guaranteed by client.rs.
    let mut pending_file: Option<FileInfo> = None;

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
                            SyncMessage::Auth { .. } => {
                                println!("Unexpected second Auth message from {}, ignoring.", addr);
                            }
                            SyncMessage::FileInfo(file_info) => {
                                println!("Received FileInfo from {}: {:?}", addr, file_info);
                                pending_file = Some(file_info);
                            }
                            SyncMessage::SyncComplete => {
                                println!("Received SyncComplete from {}", addr);
                            }
                            SyncMessage::FileContentStream { path } => {
                                let expected_hash = pending_file.take().and_then(|info| info.hash);

                                match build_safe_path("received_files", &path) {
                                    Ok(safe_path) => {
                                        if let Some(parent) = safe_path.parent() {
                                            let _ = std::fs::create_dir_all(parent);
                                        }

                                        match tokio::fs::File::create(&safe_path).await {
                                            Ok(mut file) => {
                                                let mut hasher = blake3::Hasher::new();

                                                loop {
                                                    let chunk_len = reader.read_u32().await?;
                                                    if chunk_len == 0 {
                                                        let actual_hash = hasher.finalize().to_string();
                                                        match &expected_hash {
                                                            Some(expected) if *expected == actual_hash => {
                                                                println!("Saved (LZ4, hash verified) to: {}", safe_path.display());
                                                            }
                                                            Some(expected) => {
                                                                eprintln!(
                                                                    "Hash mismatch for {}: expected {}, got {} - removing corrupted file",
                                                                    safe_path.display(), expected, actual_hash
                                                                );
                                                                let _ = tokio::fs::remove_file(&safe_path).await;
                                                            }
                                                            None => {
                                                                println!("Saved (LZ4, no hash to verify) to: {}", safe_path.display());
                                                            }
                                                        }
                                                        break;
                                                    }

                                                    if chunk_len > MAX_CHUNK_LEN {
                                                        eprintln!(
                                                            "Chunk length {} from {} exceeds the {}-byte limit, closing connection",
                                                            chunk_len, addr, MAX_CHUNK_LEN
                                                        );
                                                        let _ = tokio::fs::remove_file(&safe_path).await;
                                                        anyhow::bail!("Chunk length exceeded limit");
                                                    }

                                                    let mut comp_buf = vec![0u8; chunk_len as usize];
                                                    reader.read_exact(&mut comp_buf).await?;

                                                    match lz4_flex::decompress_size_prepended(&comp_buf) {
                                                        Ok(decompressed) => {
                                                            hasher.update(&decompressed);
                                                            file.write_all(&decompressed).await?;
                                                        }
                                                        Err(e) => {
                                                            eprintln!("LZ4 decode error for {}: {}", path, e);
                                                            // Data is corrupt; drain the rest of this
                                                            // file's stream, remove the partial file, and
                                                            // move on to the next message.
                                                            drain_stream(&mut reader).await?;
                                                            let _ = tokio::fs::remove_file(&safe_path).await;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!("Could not create file {}: {}", safe_path.display(), e);
                                                drain_stream(&mut reader).await?;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Rejected (Path Traversal) {}: {}", addr, e);
                                        drain_stream(&mut reader).await?;
                                    }
                                }
                            }
                            SyncMessage::DeleteFile { path } => {
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
                                    Err(e) => eprintln!("Rejected (CreateDir) {}: {}", addr, e),
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
                                    Err(e) => eprintln!("Rejected (RemoveDir) {}: {}", addr, e),
                                }
                            }
                            SyncMessage::Manifest(_) => {
                                println!("Unexpected Manifest message from {}.", addr);
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

    Ok(())
}

pub async fn run_server() -> Result<()> {
    let shared_secret = Arc::new(
        std::env::var("SYNC_SHARED_SECRET")
            .context("SYNC_SHARED_SECRET environment variable must be set")?,
    );

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
        let secret = shared_secret.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, addr, secret).await {
                eprintln!("Connection with {} ended with error: {}", addr, e);
            }
        });
    }
}