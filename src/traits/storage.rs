use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;

/// Enhanced storage abstraction trait
/// 
/// This trait provides a comprehensive interface for different storage backends
/// including object storage, databases, file systems, and cloud storage.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get storage backend name
    fn name(&self) -> &str;
    
    /// Get storage type
    fn storage_type(&self) -> StorageType;
    
    /// Store raw data
    async fn store_raw(&self, key: &str, data: &[u8], metadata: Option<StorageMetadata>) -> Result<StorageLocation>;
    
    /// Store processed data
    async fn store_processed(&self, key: &str, data: &[u8], metadata: Option<StorageMetadata>) -> Result<StorageLocation>;
    
    /// Load raw data
    async fn load_raw(&self, key: &str) -> Result<StorageData>;
    
    /// Load processed data
    async fn load_processed(&self, key: &str) -> Result<StorageData>;
    
    /// Check if data exists
    async fn exists(&self, key: &str) -> Result<bool>;
    
    /// List available keys with optional prefix filter
    async fn list_keys(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    
    /// Delete data
    async fn delete(&self, key: &str) -> Result<()>;
    
    /// Get storage statistics
    async fn get_statistics(&self) -> Result<StorageStatistics>;
    
    /// Ensure storage is ready (create buckets, tables, etc.)
    async fn ensure_ready(&self) -> Result<()>;
    
    /// Get storage health status
    async fn health_check(&self) -> Result<StorageHealth>;
    
    /// Clean up old data based on retention policy
    async fn cleanup(&self, retention_policy: &RetentionPolicy) -> Result<CleanupResult>;
}

/// Storage backend types
#[derive(Debug, Clone, PartialEq)]
pub enum StorageType {
    ObjectStorage,  // S3, MinIO, GCS, Azure Blob
    Database,       // PostgreSQL, MySQL, MongoDB
    FileSystem,     // Local file system, NFS
    Memory,         // In-memory storage
    Cache,          // Redis, Memcached
    Queue,          // Kafka, RabbitMQ, SQS
}

/// Storage location information
#[derive(Debug, Clone)]
pub struct StorageLocation {
    pub key: String,
    pub uri: String,
    pub size_bytes: u64,
    pub checksum: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub metadata: HashMap<String, String>,
}

/// Storage data with metadata
#[derive(Debug)]
pub struct StorageData {
    pub data: Vec<u8>,
    pub metadata: StorageMetadata,
    pub location: StorageLocation,
}

/// Storage metadata
#[derive(Debug, Clone)]
pub struct StorageMetadata {
    pub content_type: Option<String>,
    pub encoding: Option<String>,
    pub compression: Option<CompressionType>,
    pub tags: HashMap<String, String>,
    pub custom_metadata: HashMap<String, String>,
    pub ttl: Option<Duration>,
}

/// Compression types
#[derive(Debug, Clone, PartialEq)]
pub enum CompressionType {
    Gzip,
    Snappy,
    Lz4,
    Zstd,
    Brotli,
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStatistics {
    pub total_objects: u64,
    pub total_size_bytes: u64,
    pub raw_data_count: u64,
    pub processed_data_count: u64,
    pub oldest_object_age: Option<Duration>,
    pub newest_object_age: Option<Duration>,
    pub average_object_size_bytes: f64,
    pub storage_utilization_percent: f64,
}

/// Storage health status
#[derive(Debug, Clone)]
pub struct StorageHealth {
    pub is_healthy: bool,
    pub status: HealthStatus,
    pub response_time_ms: u64,
    pub available_space_bytes: Option<u64>,
    pub error_rate: f64,
    pub last_successful_operation: Option<chrono::DateTime<chrono::Utc>>,
    pub checks: Vec<HealthCheck>,
}

/// Health status levels
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Maintenance,
}

/// Individual health check
#[derive(Debug, Clone)]
pub struct HealthCheck {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub duration_ms: u64,
}

/// Retention policy for data cleanup
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_age: Option<Duration>,
    pub max_count: Option<u64>,
    pub max_size_bytes: Option<u64>,
    pub keep_latest: u32,
    pub patterns: Vec<RetentionPattern>,
}

/// Retention pattern for specific data types
#[derive(Debug, Clone)]
pub struct RetentionPattern {
    pub key_pattern: String,
    pub max_age: Duration,
    pub priority: u32,
}

/// Cleanup operation result
#[derive(Debug, Clone)]
pub struct CleanupResult {
    pub objects_deleted: u64,
    pub bytes_freed: u64,
    pub duration: Duration,
    pub errors: Vec<String>,
}

/// Trait for transactional storage operations
#[async_trait]
pub trait TransactionalStorage: Storage {
    /// Begin a transaction
    async fn begin_transaction(&self) -> Result<Box<dyn StorageTransaction>>;
    
    /// Check if transactions are supported
    fn supports_transactions(&self) -> bool;
}

/// Storage transaction interface
#[async_trait]
pub trait StorageTransaction: Send + Sync {
    /// Store data within transaction
    async fn store(&mut self, key: &str, data: &[u8], metadata: Option<StorageMetadata>) -> Result<StorageLocation>;
    
    /// Delete data within transaction
    async fn delete(&mut self, key: &str) -> Result<()>;
    
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<()>;
    
    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Trait for versioned storage
#[async_trait]
pub trait VersionedStorage: Storage {
    /// Store data with version
    async fn store_version(&self, key: &str, version: &str, data: &[u8], metadata: Option<StorageMetadata>) -> Result<StorageLocation>;
    
    /// Load specific version
    async fn load_version(&self, key: &str, version: &str) -> Result<StorageData>;
    
    /// List all versions for a key
    async fn list_versions(&self, key: &str) -> Result<Vec<VersionInfo>>;
    
    /// Get latest version
    async fn get_latest_version(&self, key: &str) -> Result<Option<String>>;
    
    /// Delete specific version
    async fn delete_version(&self, key: &str, version: &str) -> Result<()>;
}

/// Version information
#[derive(Debug, Clone)]
pub struct VersionInfo {
    pub version: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub size_bytes: u64,
    pub checksum: Option<String>,
    pub is_latest: bool,
}

/// Trait for encrypted storage
#[async_trait]
pub trait EncryptedStorage: Storage {
    /// Store encrypted data
    async fn store_encrypted(&self, key: &str, data: &[u8], encryption_key: &[u8], metadata: Option<StorageMetadata>) -> Result<StorageLocation>;
    
    /// Load and decrypt data
    async fn load_decrypted(&self, key: &str, encryption_key: &[u8]) -> Result<StorageData>;
    
    /// Get encryption information
    fn get_encryption_info(&self) -> EncryptionInfo;
}

/// Encryption information
#[derive(Debug, Clone)]
pub struct EncryptionInfo {
    pub algorithm: String,
    pub key_size_bits: u32,
    pub supports_key_rotation: bool,
}

/// Trait for storage with search capabilities
#[async_trait]
pub trait SearchableStorage: Storage {
    /// Search for data using query
    async fn search(&self, query: &SearchQuery) -> Result<SearchResult>;
    
    /// Index data for search
    async fn index_data(&self, key: &str, searchable_fields: HashMap<String, serde_json::Value>) -> Result<()>;
    
    /// Get search capabilities
    fn get_search_capabilities(&self) -> SearchCapabilities;
}

/// Search query
#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub query_string: String,
    pub filters: HashMap<String, serde_json::Value>,
    pub sort_by: Option<String>,
    pub sort_order: SortOrder,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
}

/// Sort order
#[derive(Debug, Clone)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Search result
#[derive(Debug)]
pub struct SearchResult {
    pub matches: Vec<SearchMatch>,
    pub total_count: u64,
    pub query_time_ms: u64,
}

/// Search match
#[derive(Debug)]
pub struct SearchMatch {
    pub key: String,
    pub score: f64,
    pub highlights: Vec<String>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Search capabilities
#[derive(Debug, Clone)]
pub struct SearchCapabilities {
    pub supports_full_text: bool,
    pub supports_faceted_search: bool,
    pub supports_fuzzy_search: bool,
    pub supports_range_queries: bool,
    pub max_query_size: Option<usize>,
}

/// Storage configuration trait
pub trait ConfigurableStorage {
    type Config;
    
    /// Create storage from configuration
    fn from_config(config: Self::Config) -> Result<Self>
    where
        Self: Sized;
    
    /// Update storage configuration
    fn update_config(&mut self, config: Self::Config) -> Result<()>;
    
    /// Get current configuration
    fn get_config(&self) -> &Self::Config;
    
    /// Validate configuration
    fn validate_config(config: &Self::Config) -> Result<()>;
}

/// Storage factory for creating different storage backends
pub trait StorageFactory {
    /// Create storage backend
    fn create_storage(&self, storage_type: StorageType, config: serde_json::Value) -> Result<Box<dyn Storage>>;
    
    /// List supported storage types
    fn supported_types(&self) -> Vec<StorageType>;
    
    /// Validate storage configuration
    fn validate_config(&self, storage_type: StorageType, config: &serde_json::Value) -> Result<()>;
}

/// Default implementations
impl Default for StorageMetadata {
    fn default() -> Self {
        Self {
            content_type: None,
            encoding: None,
            compression: None,
            tags: HashMap::new(),
            custom_metadata: HashMap::new(),
            ttl: None,
        }
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(30 * 24 * 60 * 60)), // 30 days
            max_count: None,
            max_size_bytes: None,
            keep_latest: 10,
            patterns: Vec::new(),
        }
    }
}

impl Default for SearchQuery {
    fn default() -> Self {
        Self {
            query_string: "*".to_string(),
            filters: HashMap::new(),
            sort_by: None,
            sort_order: SortOrder::Descending,
            limit: Some(100),
            offset: None,
        }
    }
}
