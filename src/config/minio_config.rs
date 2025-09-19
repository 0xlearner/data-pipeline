use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinioConfigFile {
    pub minio: MinioSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinioSection {
    pub endpoint: String,
    pub bucket_name: String,
    pub region: Option<String>,
    pub path_style: Option<bool>,
    pub ssl: Option<bool>,
    // Optional environment variable names for customization
    pub env_access_key: Option<String>,
    pub env_secret_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MinioConfig {
    pub endpoint: String,
    pub bucket_name: String,
    pub region: Option<String>,
    pub path_style: Option<bool>,
    pub ssl: Option<bool>,
    // These fields will be loaded from environment variables
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    // Optional environment variable names for customization
    pub env_access_key: Option<String>,
    pub env_secret_key: Option<String>,
}

impl MinioConfig {
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read MinIO config file: {}", path))?;

        let config_file: MinioConfigFile = toml::from_str(&content)
            .with_context(|| format!("Failed to parse MinIO config file: {}", path))?;

        let mut config = Self::from_section(config_file.minio);

        // Load credentials from environment variables
        config.load_credentials()?;

        Ok(config)
    }

    #[allow(dead_code)]
    pub fn from_file_with_env_prefix(path: &str, env_prefix: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read MinIO config file: {}", path))?;

        let config_file: MinioConfigFile = toml::from_str(&content)
            .with_context(|| format!("Failed to parse MinIO config file: {}", path))?;

        let mut config = Self::from_section(config_file.minio);

        // Load credentials with custom prefix
        config.load_credentials_with_prefix(env_prefix)?;

        Ok(config)
    }

    fn from_section(section: MinioSection) -> Self {
        Self {
            endpoint: section.endpoint,
            bucket_name: section.bucket_name,
            region: section.region,
            path_style: section.path_style,
            ssl: section.ssl,
            access_key: None,
            secret_key: None,
            env_access_key: section.env_access_key,
            env_secret_key: section.env_secret_key,
        }
    }

    pub fn load_credentials(&mut self) -> Result<()> {
        // Default environment variable names
        let access_key_var = self.env_access_key.as_deref().unwrap_or("MINIO_ACCESS_KEY");
        let secret_key_var = self.env_secret_key.as_deref().unwrap_or("MINIO_SECRET_KEY");

        self.access_key = env::var(access_key_var)
            .with_context(|| format!("Missing environment variable: {}", access_key_var))?
            .into();

        self.secret_key = env::var(secret_key_var)
            .with_context(|| format!("Missing environment variable: {}", secret_key_var))?
            .into();

        Ok(())
    }

    #[allow(dead_code)]
    fn load_credentials_with_prefix(&mut self, prefix: &str) -> Result<()> {
        // Environment variable names with custom prefix
        let access_key_var = format!("{}_ACCESS_KEY", prefix.to_uppercase());
        let secret_key_var = format!("{}_SECRET_KEY", prefix.to_uppercase());

        self.access_key = env::var(&access_key_var)
            .with_context(|| format!("Missing environment variable: {}", access_key_var))?
            .into();

        self.secret_key = env::var(&secret_key_var)
            .with_context(|| format!("Missing environment variable: {}", secret_key_var))?
            .into();

        Ok(())
    }

    pub fn get_access_key(&self) -> Result<&str> {
        self.access_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Access key not loaded"))
    }

    pub fn get_secret_key(&self) -> Result<&str> {
        self.secret_key
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("Secret key not loaded"))
    }

    #[allow(dead_code)]
    pub fn is_ssl(&self) -> bool {
        self.ssl
            .unwrap_or_else(|| self.endpoint.starts_with("https://"))
    }

    pub fn is_path_style(&self) -> bool {
        self.path_style.unwrap_or(true)
    }

    pub fn get_region(&self) -> &str {
        self.region.as_deref().unwrap_or("us-east-1")
    }

    pub fn validate(&self) -> Result<()> {
        if self.endpoint.is_empty() {
            return Err(anyhow::anyhow!("MinIO endpoint cannot be empty"));
        }

        if self.bucket_name.is_empty() {
            return Err(anyhow::anyhow!("MinIO bucket name cannot be empty"));
        }

        if self.access_key.is_none() {
            return Err(anyhow::anyhow!("MinIO access key not loaded"));
        }

        if self.secret_key.is_none() {
            return Err(anyhow::anyhow!("MinIO secret key not loaded"));
        }

        Ok(())
    }
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            bucket_name: "data-pipeline".to_string(),
            region: Some("us-east-1".to_string()),
            path_style: Some(true),
            ssl: Some(false),
            access_key: None,
            secret_key: None,
            env_access_key: None,
            env_secret_key: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_config() {
        let config = MinioConfig::default();
        assert_eq!(config.endpoint, "http://localhost:9000");
        assert_eq!(config.bucket_name, "data-pipeline");
        assert_eq!(config.get_region(), "us-east-1");
        assert!(config.is_path_style());
        assert!(!config.is_ssl());
    }

    #[test]
    fn test_ssl_detection() {
        let mut config = MinioConfig::default();
        config.endpoint = "https://minio.example.com".to_string();
        assert!(config.is_ssl());

        config.ssl = Some(false);
        assert!(!config.is_ssl());
    }

    #[test]
    fn test_credentials_loading() {
        unsafe {
            env::set_var("TEST_MINIO_ACCESS_KEY", "test_access");
            env::set_var("TEST_MINIO_SECRET_KEY", "test_secret");
        }

        let mut config = MinioConfig::default();
        config.env_access_key = Some("TEST_MINIO_ACCESS_KEY".to_string());
        config.env_secret_key = Some("TEST_MINIO_SECRET_KEY".to_string());

        let result = config.load_credentials();
        assert!(result.is_ok());
        assert_eq!(config.get_access_key().unwrap(), "test_access");
        assert_eq!(config.get_secret_key().unwrap(), "test_secret");

        // Clean up
        unsafe {
            env::remove_var("TEST_MINIO_ACCESS_KEY");
            env::remove_var("TEST_MINIO_SECRET_KEY");
        }
    }

    #[tokio::test]
    #[ignore] // Run with --ignored flag for integration tests
    async fn test_minio_integration() {
        // This test requires a running MinIO server
        if env::var("MINIO_INTEGRATION_TEST").is_err() {
            return;
        }

        unsafe {
            env::set_var("MINIO_ACCESS_KEY", "minioadmin");
            env::set_var("MINIO_SECRET_KEY", "minioadmin");
        }

        let mut config = MinioConfig::default();
        config.endpoint = "http://localhost:9000".to_string();
        config.bucket_name = "test-integration".to_string();

        let load_result = config.load_credentials();
        assert!(load_result.is_ok());

        let validation_result = config.validate();
        assert!(validation_result.is_ok());

        // Clean up
        unsafe {
            env::remove_var("MINIO_ACCESS_KEY");
            env::remove_var("MINIO_SECRET_KEY");
        }
    }
}
