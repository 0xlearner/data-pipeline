pub mod html_fetcher;
pub mod api_fetcher;
pub mod http_fetcher;

pub use html_fetcher::*;
pub use api_fetcher::ApiFetcher;
pub use http_fetcher::HttpFetcher;