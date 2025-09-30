pub mod html_fetcher;
pub mod api_fetcher;
pub mod http_fetcher;
pub mod html_page_processor;

pub use html_fetcher::*;
pub use api_fetcher::{ApiFetcher, FetchedApiResponse};
pub use http_fetcher::HttpFetcher;

pub use html_page_processor::{HtmlPageProcessor, StoredPage, StorageStats};