pub mod api_config;
pub mod html_config;
pub mod minio_config;

pub use api_config::ApiConfig;
pub use html_config::HtmlConfig;
pub use minio_config::*;

// Re-export CategoryConfig with specific names to avoid ambiguity
pub use html_config::CategoryConfig as HtmlCategoryConfig;
