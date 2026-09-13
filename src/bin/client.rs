use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use std::collections::HashMap;

#[path = "../protocol.rs"]
mod protocol;
use protocol::SyncMessage;

#[path = "../file_scanner.rs"]
mod file_scanner;
use file_scanner::{scan_directory, normalize_path};


#[tokio::main]
async fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);


    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();

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
                let json = serde_json::to_string(&message).unwrap();
                write_half.write_all(json.as_bytes()).await.unwrap();
                write_half.write_all(b"\n").await.unwrap();
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


        let message = SyncMessage::FileInfo(file.clone());
        let json = serde_json::to_string(&message).unwrap();
        write_half.write_all(json.as_bytes()).await.unwrap();
        write_half.write_all(b"\n").await.unwrap();

        let content = std::fs::read(&file.path).unwrap();
        let content_message = SyncMessage::FileContent { 
            path: path_str.clone(),
            content,
        };

        let json = serde_json::to_string(&content_message).unwrap();
        write_half.write_all(json.as_bytes()).await.unwrap();
        write_half.write_all(b"\n").await.unwrap();

        println!("Sent(changed/new): {}", path_str);   
    }

    for (deleted_path, value) in server_manifest {
        match value {
            Some(_) => {
                let delete_message = SyncMessage::DeleteFile {path: deleted_path.clone()};
                let json = serde_json::to_string(&delete_message).unwrap();
                write_half.write_all(json.as_bytes()).await.unwrap();
                write_half.write_all(b"\n").await.unwrap();
                println!("Sent(deleted file): {}", deleted_path);
            }
            None => {
                let remove_message = SyncMessage::RemoveDir {path: deleted_path.clone()};
                let json = serde_json::to_string(&remove_message).unwrap();
                write_half.write_all(json.as_bytes()).await.unwrap();
                write_half.write_all(b"\n").await.unwrap();
                println!("Sent(removed directory): {}", deleted_path);
            }
        }
    }

    let complete_message = SyncMessage::SyncComplete;
    let json = serde_json::to_string(&complete_message).unwrap();
    write_half.write_all(json.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    
    println!("Sent: SyncComplete");
}