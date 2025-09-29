use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

use crate::storage::MinioStorage;
use crate::traits::storage::{
    CleanupResult, ConfigurableStorage, HealthCheck, HealthStatus, RetentionPolicy, Storage,
    StorageData, StorageHealth, StorageLocation, StorageMetadata, StorageStatistics, StorageType,
};

/// Adapter that makes MinioStorage compatible with enhanced Storage trait
pub struct MinioStorageAdapter {
    storage: MinioStorage,
}

impl MinioStorageAdapter {
    pub fn new(storage: MinioStorage) -> Self {
        Self { storage }
    }

    pub async fn from_config(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket_name: &str,
    ) -> Result<Self> {
        let storage = MinioStorage::new(endpoint, access_key, secret_key, bucket_name)?;
        Ok(Self::new(storage))
    }
}

#[async_trait]
impl Storage for MinioStorageAdapter {
    fn name(&self) -> &str {
        "minio_storage"
    }

    fn storage_type(&self) -> StorageType {
        StorageType::ObjectStorage
    }

    async fn store_raw(
        &self,
        key: &str,
        data: &[u8],
        metadata: Option<StorageMetadata>,
    ) -> Result<StorageLocation> {
        // Convert bytes to string for JSON storage
        let data_str = std::str::from_utf8(data)?;
        self.storage.store_raw_json(key, data_str).await?;

        Ok(StorageLocation {
            key: key.to_string(),
            uri: format!("s3://{}/{}", self.storage.get_bucket_name(), key),
            size_bytes: data.len() as u64,
            checksum: None, // TODO: Calculate checksum
            created_at: chrono::Utc::now(),
            metadata: metadata.map(|m| m.custom_metadata).unwrap_or_default(),
        })
    }

    async fn store_processed(
        &self,
        key: &str,
        data: &[u8],
        metadata: Option<StorageMetadata>,
    ) -> Result<StorageLocation> {
        // Convert bytes to string for JSON storage
        let data_str = std::str::from_utf8(data)?;
        self.storage.store_raw_json(key, data_str).await?; // Use raw storage for now

        Ok(StorageLocation {
            key: key.to_string(),
            uri: format!("s3://{}/{}", self.storage.get_bucket_name(), key),
            size_bytes: data.len() as u64,
            checksum: None, // TODO: Calculate checksum
            created_at: chrono::Utc::now(),
            metadata: metadata.map(|m| m.custom_metadata).unwrap_or_default(),
        })
    }

    async fn load_raw(&self, key: &str) -> Result<StorageData> {
        let data_str = self.storage.load_latest_raw_data(key).await?;
        let data = serde_json::to_vec(&data_str)?;

        Ok(StorageData {
            data,
            metadata: StorageMetadata::default(),
            location: StorageLocation {
                key: key.to_string(),
                uri: format!("s3://{}/{}", self.storage.get_bucket_name(), key),
                size_bytes: 0, // TODO: Get actual size
                checksum: None,
                created_at: chrono::Utc::now(),
                metadata: HashMap::new(),
            },
        })
    }

    async fn load_processed(&self, key: &str) -> Result<StorageData> {
        let data_str = self.storage.load_latest_raw_data(key).await?; // Use raw storage for now
        let data = serde_json::to_vec(&data_str)?;

        Ok(StorageData {
            data,
            metadata: StorageMetadata::default(),
            location: StorageLocation {
                key: key.to_string(),
                uri: format!("s3://{}/{}", self.storage.get_bucket_name(), key),
                size_bytes: 0, // TODO: Get actual size
                checksum: None,
                created_at: chrono::Utc::now(),
                metadata: HashMap::new(),
            },
        })
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        // This is a simplified implementation
        // In practice, we'd use MinIO's head_object or similar
        match self.storage.load_latest_raw_data(key).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list_keys(&self, _prefix: Option<&str>) -> Result<Vec<String>> {
        // This would need to be implemented in MinioStorage
        // For now, return empty list
        Ok(Vec::new())
    }

    async fn delete(&self, _key: &str) -> Result<()> {
        // This would need to be implemented in MinioStorage
        // For now, return success
        Ok(())
    }

    async fn get_statistics(&self) -> Result<StorageStatistics> {
        // This would need to be implemented by querying MinIO
        Ok(StorageStatistics {
            total_objects: 0,
            total_size_bytes: 0,
            raw_data_count: 0,
            processed_data_count: 0,
            oldest_object_age: None,
            newest_object_age: None,
            average_object_size_bytes: 0.0,
            storage_utilization_percent: 0.0,
        })
    }

    async fn ensure_ready(&self) -> Result<()> {
        // Use existing ensure_bucket_exists method
        self.storage.ensure_bucket().await
    }

    async fn health_check(&self) -> Result<StorageHealth> {
        let start_time = std::time::Instant::now();

        // Try to perform a simple operation to check health
        match self.storage.ensure_bucket().await {
            Ok(_) => {
                let response_time = start_time.elapsed().as_millis() as u64;
                Ok(StorageHealth {
                    is_healthy: true,
                    status: HealthStatus::Healthy,
                    response_time_ms: response_time,
                    available_space_bytes: None, // TODO: Get from MinIO
                    error_rate: 0.0,
                    last_successful_operation: Some(chrono::Utc::now()),
                    checks: vec![HealthCheck {
                        name: "bucket_access".to_string(),
                        status: HealthStatus::Healthy,
                        message: Some("Bucket accessible".to_string()),
                        duration_ms: response_time,
                    }],
                })
            }
            Err(e) => Ok(StorageHealth {
                is_healthy: false,
                status: HealthStatus::Unhealthy,
                response_time_ms: start_time.elapsed().as_millis() as u64,
                available_space_bytes: None,
                error_rate: 1.0,
                last_successful_operation: None,
                checks: vec![HealthCheck {
                    name: "bucket_access".to_string(),
                    status: HealthStatus::Unhealthy,
                    message: Some(e.to_string()),
                    duration_ms: start_time.elapsed().as_millis() as u64,
                }],
            }),
        }
    }

    async fn cleanup(&self, _retention_policy: &RetentionPolicy) -> Result<CleanupResult> {
        // This would need to be implemented with proper MinIO operations
        Ok(CleanupResult {
            objects_deleted: 0,
            bytes_freed: 0,
            duration: Duration::from_secs(0),
            errors: Vec::new(),
        })
    }
}

/// Configuration for MinIO storage
#[derive(Debug, Clone)]
pub struct MinioStorageConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub bucket_name: String,
    pub region: Option<String>,
    pub use_ssl: bool,
}

impl ConfigurableStorage for MinioStorageAdapter {
    type Config = MinioStorageConfig;

    fn from_config(_config: Self::Config) -> Result<Self> {
        // This would need to be async, but trait doesn't support it
        // In practice, you'd use a factory method
        Err(anyhow::anyhow!(
            "Use MinioStorageAdapter::from_config_async instead"
        ))
    }

    fn update_config(&mut self, _config: Self::Config) -> Result<()> {
        // This would require recreating the MinioStorage instance
        Err(anyhow::anyhow!(
            "Configuration update not supported - create new instance"
        ))
    }

    fn get_config(&self) -> &Self::Config {
        // This would require storing config in the adapter
        unimplemented!("Config storage not implemented")
    }

    fn validate_config(config: &Self::Config) -> Result<()> {
        if config.endpoint.is_empty() {
            return Err(anyhow::anyhow!("Endpoint cannot be empty"));
        }
        if config.access_key.is_empty() {
            return Err(anyhow::anyhow!("Access key cannot be empty"));
        }
        if config.secret_key.is_empty() {
            return Err(anyhow::anyhow!("Secret key cannot be empty"));
        }
        if config.bucket_name.is_empty() {
            return Err(anyhow::anyhow!("Bucket name cannot be empty"));
        }
        Ok(())
    }
}

impl MinioStorageAdapter {
    /// Async factory method for creating from configuration
    pub async fn from_config_async(config: MinioStorageConfig) -> Result<Self> {
        <MinioStorageAdapter as ConfigurableStorage>::validate_config(&config)?;

        let storage = MinioStorage::new(
            &config.endpoint,
            &config.access_key,
            &config.secret_key,
            &config.bucket_name,
        )?;

        Ok(Self::new(storage))
    }
}

/// Factory for creating storage adapters
pub struct StorageAdapterFactory;

impl StorageAdapterFactory {
    pub async fn create_minio_storage(config: MinioStorageConfig) -> Result<Box<dyn Storage>> {
        let adapter = MinioStorageAdapter::from_config_async(config).await?;
        Ok(Box::new(adapter))
    }

    pub async fn create_storage_from_env() -> Result<Box<dyn Storage>> {
        let config = MinioStorageConfig {
            endpoint: std::env::var("MINIO_ENDPOINT")
                .unwrap_or_else(|_| "localhost:9000".to_string()),
            access_key: std::env::var("MINIO_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            secret_key: std::env::var("MINIO_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".to_string()),
            bucket_name: std::env::var("MINIO_BUCKET")
                .unwrap_or_else(|_| "data-pipeline".to_string()),
            region: std::env::var("MINIO_REGION").ok(),
            use_ssl: std::env::var("MINIO_USE_SSL")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
        };

        Self::create_minio_storage(config).await
    }
}
