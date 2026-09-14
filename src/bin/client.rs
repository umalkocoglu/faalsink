use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt, AsyncBufReadExt, BufReader};
use std::collections::HashMap;
use anyhow::{Context, Result};

#[path = "../protocol.rs"]
mod protocol;
use protocol::SyncMessage;

#[path = "../file_scanner.rs"]
mod file_scanner;
use file_scanner::{scan_directory, normalize_path};


#[tokio::main]
async fn main() -> Result<()>{
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
            eprintln!("Expected Manifest message, got something else.");
            HashMap::new()
        }
    };

    println!("Received manifest from server: {} files", server_manifest.len());

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
                println!("Sent (created directory): {}", path_str);
            }

            continue;
        }

        let needs_sync = match server_manifest.get(&path_str) {
            Some(server_hash) => server_hash != &file.hash,
            None => true,
        };

        server_manifest.remove(&path_str);

        if !needs_sync {
            println!("File {} is up to date, skipping.", path_str);
            continue;
        }

        let mut f = match tokio::fs::File::open(&file.path).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Warning: {} could not be opened, skipping... Error: {}", path_str, e);
                continue;
            }
        };

        let message = SyncMessage::FileInfo(file.clone());
        let json = serde_json::to_string(&message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

        let stream_msg = SyncMessage::FileContentStream { path: path_str.clone() };
        let json = serde_json::to_string(&stream_msg)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

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

        println!("Sent: {}", path_str);
    }

    for (deleted_path, value) in server_manifest {
        let message = match value {
            Some(_) => SyncMessage::DeleteFile { path: deleted_path.clone() },
            None => SyncMessage::RemoveDir { path: deleted_path.clone() },
        };

        let json = serde_json::to_string(&message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        println!("Sent(delete/remove): {}", deleted_path);
    }

    let complete_message = SyncMessage::SyncComplete;
    let json = serde_json::to_string(&complete_message)?;
    write_half.write_all(json.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    println!("Sent: SyncComplete");

    Ok(())
}