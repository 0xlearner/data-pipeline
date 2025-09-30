use anyhow::Result;
use async_trait::async_trait;

use crate::config::{ApiConfig, HtmlConfig};
use crate::fetcher::{ApiFetcher, HtmlFetcher};
use crate::traits::data_source::{
    ConfigurableSource, DataSource, RateLimit, RawSourceData, SourceHealth, SourceMetadata,
    SourceOperation, SourceType,
};

/// Adapter that makes ApiFetcher compatible with DataSource trait
pub struct ApiSourceAdapter {
    fetcher: ApiFetcher,
    config: ApiConfig,
    storage: Option<std::sync::Arc<crate::storage::MinioStorage>>,
}

impl ApiSourceAdapter {
    pub async fn new(config: ApiConfig) -> Result<Self> {
        let fetcher = ApiFetcher::new_async(config.clone()).await?;
        Ok(Self {
            fetcher,
            config,
            storage: None,
        })
    }

    pub async fn new_with_storage(
        config: ApiConfig,
        storage: std::sync::Arc<crate::storage::MinioStorage>
    ) -> Result<Self> {
        let fetcher = ApiFetcher::new_with_storage(config.clone(), storage.clone()).await?;
        Ok(Self {
            fetcher,
            config,
            storage: Some(storage),
        })
    }
}

#[async_trait]
impl DataSource for ApiSourceAdapter {
    fn name(&self) -> &str {
        &self.config.api.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Api
    }

    async fn fetch_all(&self) -> Result<RawSourceData> {
        let data = self.fetcher.fetch_all_categories().await?;
        Ok(RawSourceData::Json(data))
    }

    async fn fetch_category(&self, _category: &str) -> Result<RawSourceData> {
        // For now, we'll fetch all and filter
        // TODO: Implement category-specific fetching in ApiFetcher
        let all_data = self.fetcher.fetch_all_categories().await?;

        // Filter data for specific category if possible
        // This is a simplified implementation
        Ok(RawSourceData::Json(all_data))
    }

    async fn get_categories(&self) -> Result<Vec<String>> {
        let categories = self.config.categories.keys().cloned().collect();
        Ok(categories)
    }

    async fn health_check(&self) -> Result<SourceHealth> {
        let start_time = std::time::Instant::now();

        // Try to fetch a small amount of data to check health
        match self.fetcher.fetch_all_categories().await {
            Ok(_) => {
                let response_time = start_time.elapsed().as_millis() as u64;
                Ok(SourceHealth {
                    is_healthy: true,
                    response_time_ms: Some(response_time),
                    error_message: None,
                    last_successful_fetch: Some(chrono::Utc::now()),
                })
            }
            Err(e) => Ok(SourceHealth {
                is_healthy: false,
                response_time_ms: None,
                error_message: Some(e.to_string()),
                last_successful_fetch: None,
            }),
        }
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            name: self.config.api.name.clone(),
            description: Some(format!("API source for {}", self.config.api.name)),
            version: Some("1.0.0".to_string()),
            tags: vec!["api".to_string(), "json".to_string()],
            rate_limit: Some(RateLimit {
                requests_per_second: 10.0, // Default conservative rate
                burst_size: 5,
                retry_after_seconds: Some(60),
            }),
            authentication_required: !self.config.api.auth_token.is_empty(),
            supported_operations: vec![
                SourceOperation::FetchAll,
                SourceOperation::FetchCategory,
                SourceOperation::ListCategories,
                SourceOperation::HealthCheck,
                SourceOperation::Pagination,
            ],
        }
    }
}

impl ConfigurableSource for ApiSourceAdapter {
    type Config = ApiConfig;

    fn from_config(config: Self::Config) -> Result<Self> {
        // For synchronous compatibility, we'll use the blocking version
        // This should only be used in non-async contexts
        let fetcher = ApiFetcher::new(config.clone())?;
        Ok(Self {
            fetcher,
            config,
            storage: None,
        })
    }

    fn update_config(&mut self, config: Self::Config) -> Result<()> {
        self.fetcher = ApiFetcher::new(config.clone())?;
        self.config = config;
        Ok(())
    }

    fn get_config(&self) -> &Self::Config {
        &self.config
    }
}

/// Adapter that makes HtmlFetcher compatible with DataSource trait
pub struct HtmlSourceAdapter {
    fetcher: HtmlFetcher,
    config: HtmlConfig,
    storage: Option<std::sync::Arc<crate::storage::MinioStorage>>,
}

impl HtmlSourceAdapter {
    pub async fn new(config: HtmlConfig) -> Result<Self> {
        let fetcher = HtmlFetcher::new_async(config.clone()).await?;
        Ok(Self {
            fetcher,
            config,
            storage: None,
        })
    }

    pub async fn new_with_storage(
        config: HtmlConfig,
        storage: std::sync::Arc<crate::storage::MinioStorage>
    ) -> Result<Self> {
        let fetcher = HtmlFetcher::new_with_storage(config.clone(), storage.clone()).await?;
        Ok(Self {
            fetcher,
            config,
            storage: Some(storage),
        })
    }

    /// Fetch data from storage instead of live scraping
    pub async fn fetch_from_storage(&self) -> Result<RawSourceData> {
        use crate::fetcher::HtmlPageProcessor;

        let storage = self.storage.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Storage not configured for HTML source adapter"))?;

        let processor = HtmlPageProcessor::new(self.config.clone(), storage.clone())?;
        let products = processor.process_all_stored_pages().await?;

        Ok(RawSourceData::Html(products))
    }
}

#[async_trait]
impl DataSource for HtmlSourceAdapter {
    fn name(&self) -> &str {
        &self.config.site.name
    }

    fn source_type(&self) -> SourceType {
        SourceType::Html
    }

    async fn fetch_all(&self) -> Result<RawSourceData> {
        if self.storage.is_some() {
            // Storage mode: fetch HTML, store, then scrape from storage
            let data = self.fetcher.fetch_all_categories().await?;
            Ok(RawSourceData::Html(data))
        } else {
            // Direct mode: fetch and scrape immediately
            let data = self.fetcher.fetch_all_categories().await?;
            Ok(RawSourceData::Html(data))
        }
    }

    async fn fetch_category(&self, category: &str) -> Result<RawSourceData> {
        // Check if category exists in config
        if let Some(_category_config) = self.config.categories.get(category) {
            // For now, we'll fetch all and filter
            // TODO: Implement category-specific fetching in HtmlFetcher
            let all_data = self.fetcher.fetch_all_categories().await?;

            // Filter data for specific category
            let filtered_data = all_data
                .into_iter()
                .filter(|product| product.category == category)
                .collect();

            Ok(RawSourceData::Html(filtered_data))
        } else {
            Err(anyhow::anyhow!("Category '{}' not found", category))
        }
    }

    async fn get_categories(&self) -> Result<Vec<String>> {
        let categories = self.config.categories.keys().cloned().collect();
        Ok(categories)
    }

    async fn health_check(&self) -> Result<SourceHealth> {
        let start_time = std::time::Instant::now();

        // Try to fetch a small amount of data to check health
        match self.fetcher.fetch_all_categories().await {
            Ok(_) => {
                let response_time = start_time.elapsed().as_millis() as u64;
                Ok(SourceHealth {
                    is_healthy: true,
                    response_time_ms: Some(response_time),
                    error_message: None,
                    last_successful_fetch: Some(chrono::Utc::now()),
                })
            }
            Err(e) => Ok(SourceHealth {
                is_healthy: false,
                response_time_ms: None,
                error_message: Some(e.to_string()),
                last_successful_fetch: None,
            }),
        }
    }

    fn metadata(&self) -> SourceMetadata {
        SourceMetadata {
            name: self.config.site.name.clone(),
            description: Some(format!(
                "HTML scraping source for {}",
                self.config.site.name
            )),
            version: Some("1.0.0".to_string()),
            tags: vec!["html".to_string(), "scraping".to_string()],
            rate_limit: Some(RateLimit {
                requests_per_second: 1.0, // Conservative for web scraping
                burst_size: 1,
                retry_after_seconds: Some(self.config.scraping.delay_between_requests_ms / 1000),
            }),
            authentication_required: false,
            supported_operations: vec![
                SourceOperation::FetchAll,
                SourceOperation::FetchCategory,
                SourceOperation::ListCategories,
                SourceOperation::HealthCheck,
                SourceOperation::Pagination,
            ],
        }
    }
}

impl ConfigurableSource for HtmlSourceAdapter {
    type Config = HtmlConfig;

    fn from_config(config: Self::Config) -> Result<Self> {
        // For synchronous compatibility, we'll use the blocking version
        // This should only be used in non-async contexts
        let fetcher = HtmlFetcher::new(config.clone())?;
        Ok(Self {
            fetcher,
            config,
            storage: None,
        })
    }

    fn update_config(&mut self, config: Self::Config) -> Result<()> {
        self.fetcher = HtmlFetcher::new(config.clone())?;
        self.config = config;
        // Keep existing storage reference
        Ok(())
    }

    fn get_config(&self) -> &Self::Config {
        &self.config
    }
}

/// Factory for creating data source adapters
pub struct DataSourceAdapterFactory;

impl DataSourceAdapterFactory {
    pub async fn create_api_source(config: ApiConfig) -> Result<Box<dyn DataSource>> {
        let adapter = ApiSourceAdapter::new(config).await?;
        Ok(Box::new(adapter))
    }

    pub async fn create_html_source(config: HtmlConfig) -> Result<Box<dyn DataSource>> {
        let adapter = HtmlSourceAdapter::new(config).await?;
        Ok(Box::new(adapter))
    }

    pub async fn create_html_source_with_storage(
        config: HtmlConfig,
        storage: std::sync::Arc<crate::storage::MinioStorage>
    ) -> Result<Box<dyn DataSource>> {
        let adapter = HtmlSourceAdapter::new_with_storage(config, storage).await?;
        Ok(Box::new(adapter))
    }

    pub async fn create_source_from_config(
        source_type: SourceType,
        config_path: &str,
    ) -> Result<Box<dyn DataSource>> {
        match source_type {
            SourceType::Api => {
                let config = ApiConfig::from_file(config_path)?;
                Self::create_api_source(config).await
            }
            SourceType::Html => {
                let config = HtmlConfig::from_file(config_path)?;
                Self::create_html_source(config).await
            }
            _ => Err(anyhow::anyhow!(
                "Unsupported source type: {:?}",
                source_type
            )),
        }
    }
}
