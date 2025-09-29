use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

use crate::extractor::ScrapedProduct;

/// Core trait for all data sources in the pipeline
/// 
/// This trait abstracts over different types of data sources (API, HTML, Storage)
/// and provides a unified interface for fetching data.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Get the source name/identifier
    fn name(&self) -> &str;
    
    /// Get the source type
    fn source_type(&self) -> SourceType;
    
    /// Fetch all data from this source
    async fn fetch_all(&self) -> Result<RawSourceData>;
    
    /// Fetch data for a specific category (if applicable)
    async fn fetch_category(&self, category: &str) -> Result<RawSourceData>;
    
    /// Get available categories for this source
    async fn get_categories(&self) -> Result<Vec<String>>;
    
    /// Check if the source is available/reachable
    async fn health_check(&self) -> Result<SourceHealth>;
    
    /// Get source metadata
    fn metadata(&self) -> SourceMetadata;
}

/// Source type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum SourceType {
    Api,
    Html,
    Storage,
    File,
    Database,
}

/// Raw data returned by data sources
#[derive(Debug)]
pub enum RawSourceData {
    Json(Vec<Value>),
    Html(Vec<ScrapedProduct>),
    Binary(Vec<u8>),
}

/// Source health status
#[derive(Debug, Clone)]
pub struct SourceHealth {
    pub is_healthy: bool,
    pub response_time_ms: Option<u64>,
    pub error_message: Option<String>,
    pub last_successful_fetch: Option<chrono::DateTime<chrono::Utc>>,
}

/// Source metadata
#[derive(Debug, Clone)]
pub struct SourceMetadata {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub tags: Vec<String>,
    pub rate_limit: Option<RateLimit>,
    pub authentication_required: bool,
    pub supported_operations: Vec<SourceOperation>,
}

/// Rate limiting information
#[derive(Debug, Clone)]
pub struct RateLimit {
    pub requests_per_second: f64,
    pub burst_size: u32,
    pub retry_after_seconds: Option<u64>,
}

/// Supported operations for a source
#[derive(Debug, Clone, PartialEq)]
pub enum SourceOperation {
    FetchAll,
    FetchCategory,
    ListCategories,
    HealthCheck,
    Pagination,
    Filtering,
    Sorting,
}

/// Trait for configurable data sources
pub trait ConfigurableSource {
    type Config;
    
    /// Create a new source from configuration
    fn from_config(config: Self::Config) -> Result<Self>
    where
        Self: Sized;
    
    /// Update source configuration
    fn update_config(&mut self, config: Self::Config) -> Result<()>;
    
    /// Get current configuration
    fn get_config(&self) -> &Self::Config;
}

/// Trait for sources that support pagination
#[async_trait]
pub trait PaginatedSource: DataSource {
    /// Fetch a specific page of data
    async fn fetch_page(&self, page: u32, page_size: Option<u32>) -> Result<PagedData>;
    
    /// Get total number of pages (if known)
    async fn get_total_pages(&self) -> Result<Option<u32>>;
    
    /// Get default page size
    fn default_page_size(&self) -> u32;
}

/// Paged data result
#[derive(Debug)]
pub struct PagedData {
    pub data: RawSourceData,
    pub page: u32,
    pub page_size: u32,
    pub total_items: Option<u64>,
    pub has_next_page: bool,
}

/// Trait for sources that support filtering
#[async_trait]
pub trait FilterableSource: DataSource {
    /// Fetch data with filters applied
    async fn fetch_with_filters(&self, filters: &HashMap<String, FilterValue>) -> Result<RawSourceData>;
    
    /// Get available filter fields
    fn get_filter_fields(&self) -> Vec<FilterField>;
}

/// Filter value types
#[derive(Debug, Clone)]
pub enum FilterValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<String>),
    Range { min: f64, max: f64 },
}

/// Filter field definition
#[derive(Debug, Clone)]
pub struct FilterField {
    pub name: String,
    pub field_type: FilterFieldType,
    pub description: Option<String>,
    pub required: bool,
    pub default_value: Option<FilterValue>,
}

/// Filter field types
#[derive(Debug, Clone)]
pub enum FilterFieldType {
    Text,
    Number,
    Boolean,
    Select(Vec<String>),
    MultiSelect(Vec<String>),
    DateRange,
    NumberRange,
}

/// Trait for sources that can be cached
#[async_trait]
pub trait CacheableSource: DataSource {
    /// Get cache key for this source
    fn cache_key(&self) -> String;
    
    /// Get cache TTL in seconds
    fn cache_ttl(&self) -> u64;
    
    /// Check if cached data is still valid
    async fn is_cache_valid(&self, cached_at: chrono::DateTime<chrono::Utc>) -> bool;
}

/// Trait for sources that support real-time updates
#[async_trait]
pub trait StreamingSource: DataSource {
    /// Start streaming data updates
    async fn start_stream(&self) -> Result<Box<dyn DataStream>>;
    
    /// Check if streaming is supported
    fn supports_streaming(&self) -> bool;
}

/// Data stream interface
#[async_trait]
pub trait DataStream: Send + Sync {
    /// Get the next data update
    async fn next(&mut self) -> Result<Option<RawSourceData>>;
    
    /// Close the stream
    async fn close(&mut self) -> Result<()>;
}

/// Factory trait for creating data sources
pub trait DataSourceFactory {
    /// Create a data source from configuration
    fn create_source(&self, source_type: SourceType, config: Value) -> Result<Box<dyn DataSource>>;
    
    /// List supported source types
    fn supported_types(&self) -> Vec<SourceType>;
    
    /// Validate source configuration
    fn validate_config(&self, source_type: SourceType, config: &Value) -> Result<()>;
}

/// Registry for managing multiple data sources
pub struct DataSourceRegistry {
    sources: HashMap<String, Box<dyn DataSource>>,
    factory: Box<dyn DataSourceFactory>,
}

impl DataSourceRegistry {
    /// Create a new registry with a factory
    pub fn new(factory: Box<dyn DataSourceFactory>) -> Self {
        Self {
            sources: HashMap::new(),
            factory,
        }
    }
    
    /// Register a data source
    pub fn register(&mut self, name: String, source: Box<dyn DataSource>) {
        self.sources.insert(name, source);
    }
    
    /// Get a data source by name
    pub fn get(&self, name: &str) -> Option<&dyn DataSource> {
        self.sources.get(name).map(|s| s.as_ref())
    }
    
    /// List all registered sources
    pub fn list_sources(&self) -> Vec<&str> {
        self.sources.keys().map(|s| s.as_str()).collect()
    }
    
    /// Create and register a source from configuration
    pub fn create_and_register(&mut self, name: String, source_type: SourceType, config: Value) -> Result<()> {
        let source = self.factory.create_source(source_type, config)?;
        self.register(name, source);
        Ok(())
    }
    
    /// Remove a source
    pub fn remove(&mut self, name: &str) -> Option<Box<dyn DataSource>> {
        self.sources.remove(name)
    }
    
    /// Check health of all sources
    pub async fn health_check_all(&self) -> HashMap<String, SourceHealth> {
        let mut results = HashMap::new();
        
        for (name, source) in &self.sources {
            match source.health_check().await {
                Ok(health) => {
                    results.insert(name.clone(), health);
                }
                Err(e) => {
                    results.insert(name.clone(), SourceHealth {
                        is_healthy: false,
                        response_time_ms: None,
                        error_message: Some(e.to_string()),
                        last_successful_fetch: None,
                    });
                }
            }
        }
        
        results
    }
}
