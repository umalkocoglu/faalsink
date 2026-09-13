use std::path::PathBuf;
use std::fs;
use walkdir::WalkDir;
use std::collections::HashMap;


use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub size: u64,
    pub is_dir: bool,
    pub hash: Option<String>,
}

pub fn scan_directory (root: &str) -> Vec<FileInfo> {
    let mut files: Vec<FileInfo> = Vec::new();
    let skip_received_files = root != "received_files";

    for entry in WalkDir::new(root) {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                eprintln!("Error reading entry: {}", err);
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
                eprintln!("Error reading metadata for {}: {}", path.display(), err);
                continue;
            }
        };

        let is_dir = metadata.is_dir();
        
        if is_dir {
            let file_info = FileInfo {
                path: path.to_path_buf(),
                size: metadata.len(),
                is_dir,
                hash: None,
            };
            files.push(file_info);
            continue;
        }
        

        let content = match fs::read(&path) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("Error reading file {}: {}", path.display(), err);
                continue;
            }
        };


        let hash = blake3::hash(&content);
        
        let file_info = FileInfo {
            path: path.to_path_buf(),
            size: metadata.len(),
            is_dir,
            hash: Some(hash.to_string()),
        };

        files.push(file_info);
    }

    files

}

pub fn build_manifest(root: &str) -> HashMap<String, Option<String>> {
    let files = scan_directory(root);
    let mut manifest : HashMap<String, Option<String>> = HashMap::new();

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