//! File-based storage.

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;

/// File-based storage for persistent data.
///
/// Stores data as JSON files in a specified directory.
///
/// # Example
/// ```rust,no_run
/// use sdktrader_storage::FileStorage;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Config {
///     api_key: String,
/// }
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let storage = FileStorage::new("./data");
///
///     let config = Config { api_key: "test".to_string() };
///     storage.save("config", &config).await?;
///
///     let loaded: Config = storage.load("config").await?.unwrap();
///     Ok(())
/// }
/// ```
pub struct FileStorage {
    base_path: PathBuf,
}

impl FileStorage {
    /// Create a new file storage at the specified path.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Self {
        Self {
            base_path: base_path.as_ref().to_path_buf(),
        }
    }

    /// Ensure the storage directory exists.
    async fn ensure_dir(&self) -> Result<()> {
        if !self.base_path.exists() {
            fs::create_dir_all(&self.base_path)
                .await
                .context("Failed to create storage directory")?;
        }
        Ok(())
    }

    /// Get the file path for a key.
    fn file_path(&self, key: &str) -> PathBuf {
        self.base_path.join(format!("{}.json", key))
    }

    /// Save data to a file.
    pub async fn save<T: Serialize>(&self, key: &str, data: &T) -> Result<()> {
        self.ensure_dir().await?;

        let path = self.file_path(key);
        let json = serde_json::to_string_pretty(data)?;

        fs::write(&path, json)
            .await
            .context(format!("Failed to write file: {:?}", path))?;

        Ok(())
    }

    /// Load data from a file.
    pub async fn load<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        let path = self.file_path(key);

        if !path.exists() {
            return Ok(None);
        }

        let json = fs::read_to_string(&path)
            .await
            .context(format!("Failed to read file: {:?}", path))?;

        let data: T = serde_json::from_str(&json)?;
        Ok(Some(data))
    }

    /// Delete a file.
    pub async fn delete(&self, key: &str) -> Result<bool> {
        let path = self.file_path(key);

        if !path.exists() {
            return Ok(false);
        }

        fs::remove_file(&path)
            .await
            .context(format!("Failed to delete file: {:?}", path))?;

        Ok(true)
    }

    /// Check if a key exists.
    pub async fn exists(&self, key: &str) -> bool {
        self.file_path(key).exists()
    }

    /// List all keys in storage.
    pub async fn list_keys(&self) -> Result<Vec<String>> {
        if !self.base_path.exists() {
            return Ok(vec![]);
        }

        let mut keys = Vec::new();
        let mut entries = fs::read_dir(&self.base_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(stem) = path.file_stem() {
                    if let Some(key) = stem.to_str() {
                        keys.push(key.to_string());
                    }
                }
            }
        }

        Ok(keys)
    }

    /// Clear all data in storage.
    pub async fn clear(&self) -> Result<()> {
        if !self.base_path.exists() {
            return Ok(());
        }

        let keys = self.list_keys().await?;
        for key in keys {
            self.delete(&key).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use tempfile::tempdir;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        value: i32,
        name: String,
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path());

        let data = TestData {
            value: 42,
            name: "test".to_string(),
        };

        storage.save("test_key", &data).await.unwrap();

        let loaded: TestData = storage.load("test_key").await.unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path());

        let data = TestData {
            value: 42,
            name: "test".to_string(),
        };

        storage.save("to_delete", &data).await.unwrap();
        assert!(storage.exists("to_delete").await);

        storage.delete("to_delete").await.unwrap();
        assert!(!storage.exists("to_delete").await);
    }
}
