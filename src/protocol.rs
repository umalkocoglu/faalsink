use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::file_scanner::FileInfo;

#[derive(Serialize, Deserialize, Debug)]
pub enum SyncMessage {
    // Sent by client, first message on the wire.
    Auth { token: String },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_message_roundtrips_through_json() {
        let msg = SyncMessage::Auth { token: "abc".to_string() };
        let json = serde_json::to_string(&msg).unwrap();
        let back: SyncMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, SyncMessage::Auth { token } if token == "abc"));
    }

    #[test]
    fn file_content_stream_roundtrips_through_json() {
        let msg = SyncMessage::FileContentStream { path: "a/b.txt".to_string() };
        let json = serde_json::to_string(&msg).unwrap();
        let back: SyncMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, SyncMessage::FileContentStream { path } if path == "a/b.txt"));
    }

    #[test]
    fn sync_complete_is_a_bare_json_string() {
        // Unit variants serialize as a plain string under serde's default
        // (externally tagged) representation - pin that down explicitly
        // since client.rs and network.rs both rely on it implicitly.
        let json = serde_json::to_string(&SyncMessage::SyncComplete).unwrap();
        assert_eq!(json, "\"SyncComplete\"");
    }
}