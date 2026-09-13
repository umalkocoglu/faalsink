use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
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
    let mut stream = TcpStream::connect("127.0.0.1:8080")
        .await
        .context("Could not connect to server, make sure the server is running.")?;
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);


    let mut line = String::new();
    reader.read_line(&mut line)
        .await
        .context("Could not read line from server")?;

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

        let content = match std::fs::read(&file.path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Warning: {} could not be read, skipping... Error: {}", path_str, e);
                continue;
            }
        };

        let message = SyncMessage::FileInfo(file.clone());
        let json = serde_json::to_string(&message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

        let content_message = SyncMessage::FileContent {
            path: path_str.clone(),
            content,
        };

        let json = serde_json::to_string(&content_message)?;
        write_half.write_all(json.as_bytes()).await?;
        write_half.write_all(b"\n").await?;

        println!("Sent(changed/new): {}", path_str);   
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
    
    println!("Sent: SyncComplete");
    Ok(())
}