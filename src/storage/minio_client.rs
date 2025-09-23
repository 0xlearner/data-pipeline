use crate::config::MinioConfig;
use anyhow::{Result, anyhow};
use chrono::Utc;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use tracing::info;

pub struct MinioStorage {
    bucket: Bucket,
}

impl MinioStorage {
    pub fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket_name: &str,
    ) -> Result<Self> {
        // Parse the endpoint - the Region::Custom handles the full endpoint

        // Create custom region for MinIO endpoint
        let region = Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: endpoint.to_owned(),
        };

        // Create credentials
        let credentials = Credentials::new(
            Some(access_key),
            Some(secret_key),
            None, // security_token
            None, // session_token
            None, // expiration
        )?;

        // Create bucket instance
        let bucket = Bucket::new(bucket_name, region, credentials)?;

        // Configure for path-style access (required for MinIO)
        let bucket = *bucket.with_path_style();

        Ok(MinioStorage { bucket })
    }

    pub fn from_config(config: &MinioConfig) -> Result<Self> {
        // Validate configuration
        config.validate()?;

        // Create custom region for MinIO endpoint
        let region = Region::Custom {
            region: config.get_region().to_owned(),
            endpoint: config.endpoint.clone(),
        };

        // Create credentials from config
        let credentials = Credentials::new(
            Some(config.get_access_key()?),
            Some(config.get_secret_key()?),
            None, // security_token
            None, // session_token
            None, // expiration
        )?;

        // Create bucket instance
        let bucket = Bucket::new(&config.bucket_name, region, credentials)?;

        // Configure path-style if specified
        let bucket = if config.is_path_style() {
            *bucket.with_path_style()
        } else {
            *bucket
        };

        Ok(MinioStorage { bucket })
    }

    pub fn from_config_file(config_path: &str) -> Result<Self> {
        let config = MinioConfig::from_file(config_path)?;
        Self::from_config(&config)
    }

    #[allow(dead_code)]
    pub fn from_config_file_with_env_prefix(config_path: &str, env_prefix: &str) -> Result<Self> {
        let config = MinioConfig::from_file_with_env_prefix(config_path, env_prefix)?;
        Self::from_config(&config)
    }

    pub async fn ensure_bucket(&self) -> Result<()> {
        // Check if bucket exists
        match self.bucket.exists().await {
            Ok(true) => {
                info!("Bucket '{}' already exists", self.bucket.name);
            }
            Ok(false) => {
                // Try to create the bucket
                let config = s3::BucketConfiguration::default();
                let response = s3::Bucket::create(
                    &self.bucket.name,
                    self.bucket.region.clone(),
                    self.bucket.credentials().await?,
                    config,
                )
                .await;
                match response {
                    Ok(_) => {
                        info!("Created bucket: {}", self.bucket.name);
                    }
                    Err(e) => {
                        return Err(anyhow!("Failed to create bucket: {}", e));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow!("Failed to check bucket existence: {}", e));
            }
        }
        Ok(())
    }

    pub async fn store_raw_json(&self, api_name: &str, data: &str) -> Result<String> {
        let date = Utc::now().format("%Y/%m/%d").to_string();
        let timestamp = Utc::now().format("%H%M%S").to_string();
        let file_name = format!(
            "raw/{}/{}-{}.json",
            api_name,
            date.replace("/", ""),
            timestamp
        );
        let key = format!("{}/{}", date, file_name);

        let response = self.bucket.put_object(&key, data.as_bytes()).await?;

        if response.status_code() == 200 {
            info!("Stored raw JSON: {}", key);
            Ok(key)
        } else {
            Err(anyhow!(
                "Failed to store object: HTTP {}",
                response.status_code()
            ))
        }
    }

    pub async fn store_parquet(&self, api_name: &str, data: &[u8]) -> Result<String> {
        let date = Utc::now().format("%Y/%m/%d").to_string();
        let timestamp = Utc::now().format("%H%M%S").to_string();
        let key = format!(
            "clean/{}/{}-{}.parquet",
            api_name,
            date.replace("/", ""),
            timestamp
        );

        let response = self.bucket.put_object(&key, data).await?;

        if response.status_code() == 200 {
            info!("Stored Parquet file: {}", key);
            Ok(key)
        } else {
            Err(anyhow!(
                "Failed to store parquet file: HTTP {}",
                response.status_code()
            ))
        }
    }

    #[allow(dead_code)]
    pub async fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let prefix_str = prefix.unwrap_or("").to_string();
        let list = self.bucket.list(prefix_str, None).await?;

        let mut object_names = Vec::new();
        for result in list {
            for object in result.contents {
                object_names.push(object.key);
            }
        }

        Ok(object_names)
    }

    pub async fn get_object(&self, object_name: &str) -> Result<Vec<u8>> {
        let response = self.bucket.get_object(object_name).await?;

        if response.status_code() == 200 {
            Ok(response.bytes().to_vec())
        } else {
            Err(anyhow!(
                "Failed to get object: HTTP {}",
                response.status_code()
            ))
        }
    }

    /// Get raw JSON data as string from S3/MinIO
    pub async fn get_raw_json(&self, object_name: &str) -> Result<String> {
        let bytes = self.get_object(object_name).await?;
        let json_str = String::from_utf8(bytes)
            .map_err(|e| anyhow!("Failed to parse JSON as UTF-8: {}", e))?;
        Ok(json_str)
    }

    /// List all raw JSON files for a specific API source
    pub async fn list_raw_files(&self, api_name: &str) -> Result<Vec<String>> {
        // List all objects and filter for raw files of this API
        let list = self.bucket.list("".to_string(), None).await?;

        let mut raw_files = Vec::new();
        for result in list {
            for object in result.contents {
                // Check if this is a raw JSON file for the specified API
                if object.key.contains(&format!("raw/{}/", api_name)) && object.key.ends_with(".json") {
                    raw_files.push(object.key);
                }
            }
        }

        // Sort by modification time (most recent first)
        raw_files.sort_by(|a, b| b.cmp(a));
        Ok(raw_files)
    }

    /// Get the most recent raw JSON file for a specific API source
    pub async fn get_latest_raw_file(&self, api_name: &str) -> Result<Option<String>> {
        let raw_files = self.list_raw_files(api_name).await?;
        Ok(raw_files.into_iter().next())
    }

    /// Load and parse raw JSON data from the most recent file for an API source
    pub async fn load_latest_raw_data(&self, api_name: &str) -> Result<Vec<serde_json::Value>> {
        let latest_file = self.get_latest_raw_file(api_name).await?
            .ok_or_else(|| anyhow!("No raw data files found for API: {}", api_name))?;

        info!("Loading raw data from: {}", latest_file);
        let json_str = self.get_raw_json(&latest_file).await?;
        let data: Vec<serde_json::Value> = serde_json::from_str(&json_str)
            .map_err(|e| anyhow!("Failed to parse JSON data: {}", e))?;

        Ok(data)
    }

    /// Stream raw JSON data in batches from the most recent file for an API source
    /// This is memory-efficient for large datasets
    pub async fn stream_latest_raw_data_batched(
        &self,
        api_name: &str,
        batch_size: usize
    ) -> Result<impl Iterator<Item = Result<Vec<serde_json::Value>>>> {
        let latest_file = self.get_latest_raw_file(api_name).await?
            .ok_or_else(|| anyhow!("No raw data files found for API: {}", api_name))?;

        info!("Streaming raw data in batches of {} from: {}", batch_size, latest_file);
        let json_str = self.get_raw_json(&latest_file).await?;

        // Parse the entire JSON array first (we need to do this to get individual items)
        let data: Vec<serde_json::Value> = serde_json::from_str(&json_str)
            .map_err(|e| anyhow!("Failed to parse JSON data: {}", e))?;

        info!("Total items to process: {}, batch size: {}", data.len(), batch_size);

        // Create an iterator that yields batches
        let batches = data.chunks(batch_size)
            .map(|chunk| Ok(chunk.to_vec()))
            .collect::<Vec<_>>();

        Ok(batches.into_iter())
    }

    /// Get metadata about the latest raw data file without loading it
    pub async fn get_latest_raw_data_info(&self, api_name: &str) -> Result<(String, usize)> {
        let latest_file = self.get_latest_raw_file(api_name).await?
            .ok_or_else(|| anyhow!("No raw data files found for API: {}", api_name))?;

        // Get file size by loading just the JSON structure
        let json_str = self.get_raw_json(&latest_file).await?;
        let data: Vec<serde_json::Value> = serde_json::from_str(&json_str)
            .map_err(|e| anyhow!("Failed to parse JSON data: {}", e))?;

        Ok((latest_file, data.len()))
    }

    #[allow(dead_code)]
    pub async fn delete_object(&self, object_name: &str) -> Result<()> {
        let response = self.bucket.delete_object(object_name).await?;

        if response.status_code() == 204 || response.status_code() == 200 {
            info!("Deleted object: {}", object_name);
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to delete object: HTTP {}",
                response.status_code()
            ))
        }
    }

    #[allow(dead_code)]
    pub fn get_bucket_name(&self) -> &str {
        &self.bucket.name
    }
}

impl Default for MinioStorage {
    fn default() -> Self {
        // Try to create from default config file first
        if let Ok(storage) = Self::from_config_file("src/configs/minio.toml") {
            return storage;
        }

        // Fallback to hardcoded defaults if config file doesn't exist
        Self::new(
            "http://localhost:9000",
            "minioadmin",
            "minioadmin",
            "data-pipeline",
        )
        .expect("Failed to create default MinIO client")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_minio_client_creation() {
        let result = MinioStorage::new(
            "http://localhost:9000",
            "test_access_key",
            "test_secret_key",
            "test-bucket",
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_minio_from_config() {
        // Set up test environment variables
        unsafe {
            env::set_var("TEST_MINIO_ACCESS_KEY", "test_access");
            env::set_var("TEST_MINIO_SECRET_KEY", "test_secret");
        }

        let mut config = MinioConfig::default();
        config.env_access_key = Some("TEST_MINIO_ACCESS_KEY".to_string());
        config.env_secret_key = Some("TEST_MINIO_SECRET_KEY".to_string());

        // Load credentials
        let mut config_clone = config.clone();
        config_clone.load_credentials().unwrap();
        config_clone.access_key = Some("test_access".to_string());
        config_clone.secret_key = Some("test_secret".to_string());

        let result = MinioStorage::from_config(&config_clone);
        assert!(result.is_ok());

        // Clean up
        unsafe {
            env::remove_var("TEST_MINIO_ACCESS_KEY");
            env::remove_var("TEST_MINIO_SECRET_KEY");
        }
    }

    #[tokio::test]
    async fn test_bucket_operations() {
        // This test requires a running MinIO instance
        if std::env::var("MINIO_TEST_ENABLED").is_ok() {
            let storage = MinioStorage::new(
                "http://localhost:9000",
                "minioadmin",
                "minioadmin",
                "test-bucket",
            )
            .unwrap();

            // Test bucket creation
            let result = storage.ensure_bucket().await;
            assert!(result.is_ok());

            // Test object storage
            let result = storage
                .store_raw_json("test-api", r#"{"test": "data"}"#)
                .await;
            assert!(result.is_ok());

            // Test object listing
            let result = storage.list_objects(Some("raw/")).await;
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_endpoint_parsing() {
        // Test HTTP endpoint
        let storage =
            MinioStorage::new("http://localhost:9000", "key", "secret", "bucket").unwrap();
        assert_eq!(storage.get_bucket_name(), "bucket");

        // Test HTTPS endpoint
        let storage =
            MinioStorage::new("https://minio.example.com", "key", "secret", "bucket").unwrap();
        assert_eq!(storage.get_bucket_name(), "bucket");
    }
}
