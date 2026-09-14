use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use anyhow::{Context, Result};
use crate::protocol::SyncMessage;

use std::path::{Path, PathBuf, Component};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, info, warn, Instrument};
use crate::file_scanner::{build_manifest, FileInfo};

/// Chunks are always <= 64 KB before compression, and LZ4 never expands
/// data by much. Anything claiming to be bigger than this is either a
/// corrupt stream or a hostile length field - reject it instead of
/// allocating whatever size it asks for.
const MAX_CHUNK_LEN: u32 = 1024 * 1024; // 1 MB

/// Monotonically increasing id handed out per accepted connection, purely
/// for log correlation - lets us grep all lines for one sync session
/// (`conn_id=7`) even while other clients are connected concurrently.
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

type ConnReader = BufReader<tokio::net::tcp::OwnedReadHalf>;

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
async fn drain_stream(reader: &mut ConnReader) -> Result<()> {
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

/// Receives one chunked, LZ4-compressed file body and writes it to disk,
/// verifying it against `expected_hash` once the stream ends. Runs inside
/// its own `file_transfer` span (see call site), so every log line here
/// already carries the file path - no need to repeat it in each message.
async fn handle_file_stream(reader: &mut ConnReader, path: &str, expected_hash: Option<String>) -> Result<()> {
    let safe_path = match build_safe_path("received_files", path) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "rejected path (possible path traversal)");
            return drain_stream(reader).await;
        }
    };

    if let Some(parent) = safe_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let mut file = match tokio::fs::File::create(&safe_path).await {
        Ok(f) => f,
        Err(e) => {
            error!(error = %e, "could not create file");
            return drain_stream(reader).await;
        }
    };

    let mut hasher = blake3::Hasher::new();

    loop {
        let chunk_len = reader.read_u32().await?;
        if chunk_len == 0 {
            let actual_hash = hasher.finalize().to_string();
            match &expected_hash {
                Some(expected) if *expected == actual_hash => {
                    info!("file saved, hash verified");
                }
                Some(expected) => {
                    error!(expected = %expected, actual = %actual_hash, "hash mismatch, removing corrupted file");
                    let _ = tokio::fs::remove_file(&safe_path).await;
                }
                None => {
                    info!("file saved (no hash to verify)");
                }
            }
            return Ok(());
        }

        if chunk_len > MAX_CHUNK_LEN {
            error!(chunk_len, max = MAX_CHUNK_LEN, "chunk length exceeds limit, closing connection");
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
                error!(error = %e, "lz4 decode error");
                // Data is corrupt; drain the rest of this file's stream,
                // remove the partial file, and move on to the next message.
                drain_stream(reader).await?;
                let _ = tokio::fs::remove_file(&safe_path).await;
                return Ok(());
            }
        }
    }
}

async fn handle_connection(socket: TcpStream, expected_token: Arc<String>) -> Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);

    // First message on the wire must be Auth with a matching token.
    let mut auth_line = String::new();
    let bytes_read = reader.read_line(&mut auth_line).await?;
    if bytes_read == 0 {
        info!("connection closed before authenticating");
        return Ok(());
    }

    let authenticated = matches!(
        serde_json::from_str::<SyncMessage>(auth_line.trim()),
        Ok(SyncMessage::Auth { token }) if token == *expected_token
    );

    if !authenticated {
        warn!("authentication failed, closing connection");
        return Ok(());
    }

    info!("authenticated");

    let manifest = build_manifest("received_files");
    let manifest_message = SyncMessage::Manifest(manifest);
    let json = serde_json::to_string(&manifest_message)?;

    write_half.write_all(json.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    debug!(%json, "sent manifest");

    // Set by FileInfo, consumed by the FileContentStream that immediately
    // follows it - this pairing order is guaranteed by client.rs.
    let mut pending_file: Option<FileInfo> = None;

    let mut line = String::new();
    loop {
        line.clear();

        match reader.read_line(&mut line).await {
            Ok(0) => {
                info!("connection closed by client");
                break;
            }
            Ok(_) => {
                match serde_json::from_str::<SyncMessage>(line.trim()) {
                    Ok(message) => {
                        match message {
                            SyncMessage::Auth { .. } => {
                                warn!("duplicate auth message, ignoring");
                            }
                            SyncMessage::FileInfo(file_info) => {
                                debug!(?file_info, "received file info");
                                pending_file = Some(file_info);
                            }
                            SyncMessage::SyncComplete => {
                                info!("sync complete");
                            }
                            SyncMessage::FileContentStream { path } => {
                                let expected_hash = pending_file.take().and_then(|info| info.hash);
                                let file_span = tracing::info_span!("file_transfer", path = %path);
                                handle_file_stream(&mut reader, &path, expected_hash)
                                    .instrument(file_span)
                                    .await?;
                            }
                            SyncMessage::DeleteFile { path } => {
                                match build_safe_path("received_files", &path) {
                                    Ok(safe_path) => {
                                        match std::fs::remove_file(&safe_path) {
                                            Ok(_) => info!(path = %safe_path.display(), "deleted file"),
                                            Err(e) => error!(path = %safe_path.display(), error = %e, "could not delete file"),
                                        }
                                    }
                                    Err(e) => warn!(error = %e, "rejected delete (possible path traversal)"),
                                }
                            }
                            SyncMessage::CreateDir { path } => {
                                match build_safe_path("received_files", &path) {
                                    Ok(safe_path) => {
                                        match std::fs::create_dir_all(&safe_path) {
                                            Ok(_) => info!(path = %safe_path.display(), "created directory"),
                                            Err(e) => error!(path = %safe_path.display(), error = %e, "could not create directory"),
                                        }
                                    }
                                    Err(e) => warn!(error = %e, "rejected create-dir (possible path traversal)"),
                                }
                            }
                            SyncMessage::RemoveDir { path } => {
                                match build_safe_path("received_files", &path) {
                                    Ok(safe_path) => {
                                        match std::fs::remove_dir_all(&safe_path) {
                                            Ok(_) => info!(path = %safe_path.display(), "removed directory"),
                                            Err(e) => error!(path = %safe_path.display(), error = %e, "could not remove directory"),
                                        }
                                    }
                                    Err(e) => warn!(error = %e, "rejected remove-dir (possible path traversal)"),
                                }
                            }
                            SyncMessage::Manifest(_) => {
                                warn!("unexpected manifest message from client");
                            }
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, raw_line = %line.trim(), "failed to parse JSON");
                    }
                }
            }
            Err(err) => {
                error!(error = %err, "network error while reading");
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
    info!("server listening on 127.0.0.1:8080");

    if let Err(e) = std::fs::create_dir_all("received_files") {
        warn!(error = %e, "could not create received_files directory");
    }

    loop {
        let (socket, addr) = match listener.accept().await {
            Ok(res) => res,
            Err(e) => {
                error!(error = %e, "connection could not be accepted");
                continue;
            }
        };

        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let span = tracing::info_span!("sync_session", conn_id, %addr);
        span.in_scope(|| info!("new connection accepted"));

        let secret = shared_secret.clone();

        tokio::spawn(
            async move {
                if let Err(e) = handle_connection(socket, secret).await {
                    error!(error = %e, "connection ended with error");
                }
            }
                .instrument(span),
        );
    }
}