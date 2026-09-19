use std::path::PathBuf;
use std::fs::File;
use std::io::{BufReader, Read};
use walkdir::WalkDir;
use std::collections::HashMap;
use tracing::warn;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub size: u64,
    pub is_dir: bool,
    pub hash: Option<String>,
}

pub fn scan_directory(root: &str) -> Vec<FileInfo> {
    let mut files: Vec<FileInfo> = Vec::new();
    let skip_received_files = root != "received_files";

    for entry in WalkDir::new(root) {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                warn!(error = %err, "error reading directory entry");
                continue;
            }
        };

        let path = entry.path();

        let should_skip = path.components().any(|comp| {
            comp.as_os_str() == "target"
                || comp.as_os_str() == ".git"
                || (skip_received_files && comp.as_os_str() == "received_files")
        });

        if should_skip {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "error reading metadata");
                continue;
            }
        };

        let is_dir = metadata.is_dir();

        if is_dir {
            files.push(FileInfo {
                path: path.to_path_buf(),
                size: metadata.len(),
                is_dir,
                hash: None,
            });
            continue;
        }

        let hash = match hash_file(path) {
            Ok(h) => h,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "error hashing file");
                continue;
            }
        };

        files.push(FileInfo {
            path: path.to_path_buf(),
            size: metadata.len(),
            is_dir,
            hash: Some(hash),
        });
    }

    files
}

/// Hashes a file in fixed-size chunks instead of loading it fully into memory,
/// so scanning stays cheap even for very large files. yay!
fn hash_file(path: &std::path::Path) -> std::io::Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 65536];

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(hasher.finalize().to_string())
}

pub fn build_manifest(root: &str) -> HashMap<String, Option<String>> {
    let files = scan_directory(root);
    let mut manifest: HashMap<String, Option<String>> = HashMap::new();

    for file in files {
        let path_str = normalize_path(&file.path, root);
        manifest.insert(path_str, file.hash);
    }
    manifest
}

pub fn normalize_path(path: &PathBuf, root: &str) -> String {
    let path_str = match path.strip_prefix(root) {
        Ok(stripped) => stripped.to_string_lossy().into_owned(),
        Err(_) => path.to_string_lossy().into_owned(),
    };
    path_str.replace("\\", "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn hash_file_matches_blake3_of_its_contents() {
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        tmp.write_all(b"hello, sync tool").expect("write temp file");
        tmp.flush().expect("flush temp file");

        let expected = blake3::hash(b"hello, sync tool").to_string();
        let actual = hash_file(tmp.path()).expect("hash file");

        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_file_is_correct_across_multiple_read_buffer_chunks() {
        // Bigger than the 64 KB buffer hash_file reads with internally, to
        // make sure chunked reading doesn't change the resulting hash.
        let data = vec![0xABu8; 200_000];
        let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
        tmp.write_all(&data).expect("write temp file");
        tmp.flush().expect("flush temp file");

        let expected = blake3::hash(&data).to_string();
        let actual = hash_file(tmp.path()).expect("hash file");

        assert_eq!(actual, expected);
    }

    #[test]
    fn hash_file_errors_on_a_missing_file() {
        let missing = std::path::Path::new("this/path/does/not/exist.bin");
        assert!(hash_file(missing).is_err());
    }

    #[test]
    fn normalize_path_strips_root_and_uses_forward_slashes() {
        let full = PathBuf::from(".").join("sub").join("dir").join("file.txt");
        assert_eq!(normalize_path(&full, "."), "sub/dir/file.txt");
    }

    #[test]
    fn normalize_path_falls_back_to_full_path_when_root_does_not_match() {
        let full = PathBuf::from("unrelated/file.txt");
        assert_eq!(normalize_path(&full, "received_files"), "unrelated/file.txt");
    }
}