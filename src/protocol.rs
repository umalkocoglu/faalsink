use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::file_scanner::FileInfo;

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncMessage {
    FileInfo(FileInfo),
    FileContent{ path: String, content: Vec<u8> },
    SyncComplete,
    Manifest(HashMap<String, String>),
}