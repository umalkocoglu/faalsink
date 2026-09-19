use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use std::collections::HashMap;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use faalsink::protocol::SyncMessage;
use faalsink::file_scanner::{scan_directory, normalize_path};

/// Same dual stdout+JSON-file setup as the server (see main.rs's
/// init_tracing doc comment) - logs to logs/client.log.<date>.
fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let _ = std::fs::create_dir_all("logs");
    let file_appender = tracing_appender::rolling::daily("logs", "client.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false).json())
        .init();

    guard
}

#[tokio::main]
async fn main() -> Result<()>{
    let _log_guard = init_tracing();

    // Shared secret used to authenticate against the server. Plaintext over
    // the wire (no TLS yet), so this guards against stray/accidental
    // connections rather than a determined network attacker.
    let token = std::env::var("SYNC_SHARED_SECRET")
        .context("SYNC_SHARED_SECRET environment variable must be set")?;

    let stream = TcpStream::connect("127.0.0.1:8080")
        .await
        .context("Could not connect to server, make sure the server is running.")?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let auth_message = SyncMessage::Auth { token };
    let json = serde_json::to_string(&auth_message)?;
    write_half.write_all(json.as_bytes()).await?;
    write_half.write_all(b"\n").await?;

    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)
        .await
        .context("Could not read line from server")?;

    if bytes_read == 0 {
        anyhow::bail!("Server closed the connection - authentication was likely rejected");
    }

    let mut server_manifest: HashMap<String, Option<String>> = match serde_json::from_str::<SyncMessage>(line.trim()) {
        Ok(SyncMessage::Manifest(manifest)) => manifest,
        _ => {
            warn!("expected Manifest message, got something else");
            HashMap::new()
        }
    };

    info!(file_count = server_manifest.len(), "received manifest from server");

    let files = scan_directory(".");


    for file in &files {
        let path_str = normalize_path(&file.path, ".");

        if file.is_dir {
            if server_manifest.contains_key(&path_str) {
                server_manifest.remove(&path_str);
            } else {
                let message = SyncMessage::CreateDir { path: path_str.clone() };
                let json = serde_json::to_string(&message)?;
                write_half.write_all(json.as_bytes()).await?;
                write_half.write_all(b"\n").await?;
                info!(path = %path_str, "sent create-directory");
            }

            continue;
        }

        let needs_sync = match server_manifest.get(&path_str) {
            Some(server_hash) => server_hash != &file.hash,
            None => true,
        };

        server_manifest.remove(&path_str);

        if !needs_sync {
            debug!(path = %path_str, "up to date, skipping");
            continue;
        }

        // Open the file BEFORE announcing anything to the server. If this fails
        // we skip the file silently instead of leaving the server waiting for a
        // FileContentStream that never arrives (protocol desync).
        let mut f = match tokio::fs::File::open(&file.path).await {
            Ok(f) => f,
            Err(e) => {
                warn!(path = %path_str, error = %e, "could not open file, skipping");
                continue;
            }
        };

        let message = SyncMessage::FileInfo(file.clone());
        let json = serde_json::to_string(&message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

        // Tell the server a chunked, LZ4-compressed byte stream follows.
        let stream_msg = SyncMessage::FileContentStream { path: path_str.clone() };
        let json = serde_json::to_string(&stream_msg)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

        // Read the file in 64 KB chunks, LZ4-compress each one, and stream it:
        // [4-byte compressed length][compressed bytes], terminated by a 0 length.
        let mut buffer = [0u8; 65536];

        loop {
            let n = f.read(&mut buffer).await?;
            if n == 0 {
                write_half.write_u32(0).await?; // EOF signal
                break;
            }

            let compressed = lz4_flex::compress_prepend_size(&buffer[..n]);
            write_half.write_u32(compressed.len() as u32).await?;
            write_half.write_all(&compressed).await?;
        }

        info!(path = %path_str, "sent (LZ4 chunked)");
    }

    for (deleted_path, value) in server_manifest {
        let message = match value {
            Some(_) => SyncMessage::DeleteFile { path: deleted_path.clone() },
            None => SyncMessage::RemoveDir { path: deleted_path.clone() },
        };

        let json = serde_json::to_string(&message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        info!(path = %deleted_path, "sent delete/remove");
    }

    let complete_message = SyncMessage::SyncComplete;
    let json = serde_json::to_string(&complete_message)?;
    write_half.write_all(json.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    info!("sent SyncComplete");

    Ok(())
}