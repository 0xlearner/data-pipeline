use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use crate::config::MinioConfig;
use crate::storage::MinioStorage;

/// Storage abstraction trait
#[async_trait]
pub trait Storage: Send + Sync {
    /// Store raw JSON data
    async fn store_raw_data(&self, source_name: &str, data: &[u8]) -> Result<String>;
    
    /// Store processed Parquet data
    async fn store_processed_data(&self, source_name: &str, data: &[u8]) -> Result<String>;
    
    /// Load raw data for a source
    async fn load_raw_data(&self, source_name: &str) -> Result<Vec<u8>>;
    
    /// Load processed data for a source
    async fn load_processed_data(&self, source_name: &str) -> Result<Vec<u8>>;
    
    /// Check if raw data exists for a source
    async fn raw_data_exists(&self, source_name: &str) -> Result<bool>;
    
    /// Check if processed data exists for a source
    async fn processed_data_exists(&self, source_name: &str) -> Result<bool>;
    
    /// List all available sources
    async fn list_sources(&self) -> Result<Vec<String>>;
    
    /// Get storage statistics
    async fn get_statistics(&self) -> Result<StorageStatistics>;
    
    /// Ensure storage is ready (buckets exist, etc.)
    async fn ensure_ready(&self) -> Result<()>;
    
    /// Clean up old data (optional)
    async fn cleanup_old_data(&self, retention_days: u32) -> Result<usize>;
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStatistics {
    pub total_sources: usize,
    pub total_raw_files: usize,
    pub total_processed_files: usize,
    pub total_size_bytes: u64,
    pub oldest_file_age_days: Option<u32>,
    pub newest_file_age_days: Option<u32>,
}

/// Storage manager that provides a unified interface to different storage backends
pub struct StorageManager {
    backend: Arc<dyn Storage>,
}

impl StorageManager {
    /// Create a new storage manager with MinIO backend
    pub fn new_minio(config: &MinioConfig) -> Result<Self> {
        let minio_storage = MinioStorage::from_config(config)?;
        let backend = Arc::new(MinioStorageAdapter::new(minio_storage));
        Ok(Self { backend })
    }
    
    /// Create a new storage manager with a custom backend
    pub fn new_with_backend(backend: Arc<dyn Storage>) -> Self {
        Self { backend }
    }
    
    /// Get the underlying storage backend
    pub fn backend(&self) -> &Arc<dyn Storage> {
        &self.backend
    }
}

// Delegate all methods to the backend
#[async_trait]
impl Storage for StorageManager {
    async fn store_raw_data(&self, source_name: &str, data: &[u8]) -> Result<String> {
        self.backend.store_raw_data(source_name, data).await
    }
    
    async fn store_processed_data(&self, source_name: &str, data: &[u8]) -> Result<String> {
        self.backend.store_processed_data(source_name, data).await
    }
    
    async fn load_raw_data(&self, source_name: &str) -> Result<Vec<u8>> {
        self.backend.load_raw_data(source_name).await
    }
    
    async fn load_processed_data(&self, source_name: &str) -> Result<Vec<u8>> {
        self.backend.load_processed_data(source_name).await
    }
    
    async fn raw_data_exists(&self, source_name: &str) -> Result<bool> {
        self.backend.raw_data_exists(source_name).await
    }
    
    async fn processed_data_exists(&self, source_name: &str) -> Result<bool> {
        self.backend.processed_data_exists(source_name).await
    }
    
    async fn list_sources(&self) -> Result<Vec<String>> {
        self.backend.list_sources().await
    }
    
    async fn get_statistics(&self) -> Result<StorageStatistics> {
        self.backend.get_statistics().await
    }
    
    async fn ensure_ready(&self) -> Result<()> {
        self.backend.ensure_ready().await
    }
    
    async fn cleanup_old_data(&self, retention_days: u32) -> Result<usize> {
        self.backend.cleanup_old_data(retention_days).await
    }
}

/// Adapter to make MinioStorage compatible with the Storage trait
pub struct MinioStorageAdapter {
    minio: MinioStorage,
}

impl MinioStorageAdapter {
    pub fn new(minio: MinioStorage) -> Self {
        Self { minio }
    }
}

#[async_trait]
impl Storage for MinioStorageAdapter {
    async fn store_raw_data(&self, source_name: &str, data: &[u8]) -> Result<String> {
        let data_str = String::from_utf8(data.to_vec())?;
        self.minio.store_raw_json(source_name, &data_str).await
    }
    
    async fn store_processed_data(&self, source_name: &str, data: &[u8]) -> Result<String> {
        self.minio.store_parquet(source_name, data).await
    }
    
    async fn load_raw_data(&self, source_name: &str) -> Result<Vec<u8>> {
        let json_data = self.minio.load_latest_raw_data(source_name).await?;
        let json_str = serde_json::to_string(&json_data)?;
        Ok(json_str.into_bytes())
    }
    
    async fn load_processed_data(&self, _source_name: &str) -> Result<Vec<u8>> {
        // MinioStorage doesn't have a direct method for this yet
        // This would need to be implemented in MinioStorage
        Err(anyhow::anyhow!("Loading processed data not yet implemented"))
    }
    
    async fn raw_data_exists(&self, source_name: &str) -> Result<bool> {
        // Check if we can load raw data without error
        match self.minio.load_latest_raw_data(source_name).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    
    async fn processed_data_exists(&self, _source_name: &str) -> Result<bool> {
        // This would need to be implemented based on MinioStorage capabilities
        Ok(false) // Placeholder
    }
    
    async fn list_sources(&self) -> Result<Vec<String>> {
        // This would need to be implemented in MinioStorage
        // For now, return empty list
        Ok(Vec::new())
    }
    
    async fn get_statistics(&self) -> Result<StorageStatistics> {
        // This would need to be implemented based on MinioStorage capabilities
        Ok(StorageStatistics {
            total_sources: 0,
            total_raw_files: 0,
            total_processed_files: 0,
            total_size_bytes: 0,
            oldest_file_age_days: None,
            newest_file_age_days: None,
        })
    }
    
    async fn ensure_ready(&self) -> Result<()> {
        self.minio.ensure_bucket().await
    }
    
    async fn cleanup_old_data(&self, _retention_days: u32) -> Result<usize> {
        // This would need to be implemented in MinioStorage
        Ok(0) // Placeholder
    }
}

impl StorageStatistics {
    /// Calculate storage efficiency (processed vs raw data ratio)
    pub fn efficiency_ratio(&self) -> f64 {
        if self.total_raw_files == 0 {
            0.0
        } else {
            self.total_processed_files as f64 / self.total_raw_files as f64
        }
    }
    
    /// Check if storage is healthy (has both raw and processed data)
    pub fn is_healthy(&self) -> bool {
        self.total_sources > 0 && self.total_processed_files > 0
    }
    
    /// Get human-readable size
    pub fn human_readable_size(&self) -> String {
        let size = self.total_size_bytes as f64;
        if size < 1024.0 {
            format!("{} B", size)
        } else if size < 1024.0 * 1024.0 {
            format!("{:.2} KB", size / 1024.0)
        } else if size < 1024.0 * 1024.0 * 1024.0 {
            format!("{:.2} MB", size / (1024.0 * 1024.0))
        } else {
            format!("{:.2} GB", size / (1024.0 * 1024.0 * 1024.0))
        }
    }
}
