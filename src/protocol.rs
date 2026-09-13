use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::file_scanner::FileInfo;

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncMessage {
    // Sent by client.
    FileInfo(FileInfo),
    FileContentStream { path: String }, // chunked LZ4 byte stream follows on the wire
    DeleteFile{path: String},
    CreateDir{path: String},
    RemoveDir{path: String},
    SyncComplete,
    // Sent by server.
    Manifest(HashMap<String, Option<String>>), // Some(hash) for files and None for directories
}