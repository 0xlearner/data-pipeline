use anyhow::{Result, anyhow};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use wreq::Client;
use wreq_util::Emulation;
use scraper::{Html, Selector, ElementRef};
use regex::Regex;
use std::collections::{HashMap, HashSet};
use smartcore::ensemble::random_forest_classifier::RandomForestClassifier;
use smartcore::linalg::basic::matrix::DenseMatrix;

use crate::config::HtmlConfig;

/// HTML-based fetcher for web scraping data sources like Naheed store
pub struct HtmlFetcher {
    client: Client,
    config: HtmlConfig,
    ml_model: Option<ProductMLModel>,
    exclusion_detector: ExclusionDetector,
}

/// ML model for product extraction
pub struct ProductMLModel {
    pub classifier: RandomForestClassifier<f32, i32, DenseMatrix<f32>, Vec<i32>>,
    pub feature_extractor: FeatureExtractor,
    pub confidence_threshold: f32,
}

/// Feature extractor for ML model
pub struct FeatureExtractor {
    pub price_patterns: Vec<Regex>,
    pub name_patterns: Vec<Regex>,
}

/// Exclusion detector for filtering out non-product content
pub struct ExclusionDetector {
    pub excluded_sections: Vec<String>,
    pub excluded_keywords: HashSet<String>,
}

/// Product candidate for ML classification
#[derive(Debug, Clone)]
pub struct ProductCandidate {
    pub element_html: String,
    pub text_content: String,
    pub tag_name: String,
    pub classes: Vec<String>,
    pub attributes: HashMap<String, String>,
    pub depth: usize,
    pub parent_context: String,
    pub has_price_text: bool,
    pub has_link: bool,
}

/// Represents a scraped product from HTML
#[derive(Debug, Clone)]
pub struct ScrapedProduct {
    pub name: String,
    pub price: String,
    pub product_id: String,
    pub category: String,
    pub url: Option<String>,
    pub raw_html: String,
}

impl HtmlFetcher {
    pub fn new(config: HtmlConfig) -> Result<Self> {
        let client = Client::builder()
            .emulation(Emulation::Firefox136)
            .build()?;

        Ok(HtmlFetcher {
            client,
            config,
            ml_model: None,
            exclusion_detector: ExclusionDetector::new_default(),
        })
    }

    /// Initialize ML model for enhanced product extraction
    pub fn with_ml_model(mut self, model: ProductMLModel) -> Self {
        self.ml_model = Some(model);
        self
    }

    /// Fetch products from all configured categories
    pub async fn fetch_all_categories(&self) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        for (category_name, category_config) in &self.config.categories {
            info!("Scraping category: {}", category_name);

            match self.scrape_category(category_name, category_config).await {
                Ok(products) => {
                    info!("Scraped {} products from {}", products.len(), category_name);
                    all_products.extend(products);
                }
                Err(e) => {
                    error!("Failed to scrape category {}: {}", category_name, e);
                    continue;
                }
            }

            // Rate limiting between categories
            let delay = Duration::from_millis(
                self.config.scraping.delay_between_requests_ms + 
                (rand::random::<u64>() % 1000)
            );
            sleep(delay).await;
        }

        Ok(all_products)
    }

    /// Scrape a specific category
    async fn scrape_category(
        &self,
        category_name: &str,
        category_config: &crate::config::HtmlCategoryConfig,
    ) -> Result<Vec<ScrapedProduct>> {
        let mut all_products = Vec::new();

        // Handle pagination if configured
        let max_pages = self.config.scraping.max_pages_per_category;
        
        for page in 1..=max_pages {
            let url = if page == 1 {
                category_config.base_url.clone()
            } else {
                format!("{}?p={}", category_config.base_url, page)
            };

            info!("Scraping page {} of {}: {}", page, category_name, url);

            match self.scrape_page(&url, category_name).await {
                Ok(products) => {
                    if products.is_empty() {
                        info!("No products found on page {}, stopping pagination", page);
                        break;
                    }
                    all_products.extend(products);
                }
                Err(e) => {
                    warn!("Failed to scrape page {} of {}: {}", page, category_name, e);
                    break;
                }
            }

            // Rate limiting between pages
            let delay = Duration::from_millis(
                self.config.scraping.delay_between_requests_ms + 
                (rand::random::<u64>() % 2000)
            );
            sleep(delay).await;
        }

        Ok(all_products)
    }

    /// Scrape a single page
    async fn scrape_page(&self, url: &str, category_name: &str) -> Result<Vec<ScrapedProduct>> {
        let html = self.fetch_page_with_retry(url, 3).await?;
        self.extract_products_from_html(&html, category_name, Some(url.to_string()))
    }

    /// Fetch HTML page with retry logic
    async fn fetch_page_with_retry(&self, url: &str, max_retries: usize) -> Result<String> {
        let mut attempts = 0;

        while attempts < max_retries {
            match self.fetch_page_smart(url).await {
                Ok(html) => return Ok(html),
                Err(e) => {
                    attempts += 1;
                    if attempts < max_retries {
                        // Exponential backoff with jitter
                        let delay = Duration::from_millis(
                            1000 * (2_u64.pow(attempts as u32)) + (rand::random::<u64>() % 1000)
                        );
                        warn!("Attempt {} failed for {}, retrying in {:?}: {}", 
                              attempts, url, delay, e);
                        sleep(delay).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(anyhow!("Failed to fetch {} after {} attempts", url, max_retries))
    }

    /// Smart page fetching with anti-bot measures
    async fn fetch_page_smart(&self, url: &str) -> Result<String> {
        // Random delay to mimic human behavior
        let delay = Duration::from_millis(500 + (rand::random::<u64>() % 2000));
        sleep(delay).await;

        let response = self.client
            .get(url)
            .send()
            .await
            .map_err(|e| anyhow!("Network error: {}", e))?;

        if !response.status().is_success() {
            return Err(anyhow!("HTTP error: {}", response.status()));
        }

        let html = response
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response text: {}", e))?;

        if html.is_empty() {
            return Err(anyhow!("Empty HTML response"));
        }

        // Basic HTML validation
        if !html.contains("<html") && !html.contains("<div") && !html.contains("<body") {
            return Err(anyhow!("Invalid HTML content"));
        }

        // Check for bot detection
        if html.contains("blocked") || html.contains("bot detected") {
            return Err(anyhow!("Bot detection detected"));
        }

        info!("Successfully fetched {} characters from {}", html.len(), url);
        Ok(html)
    }

    /// Extract products from HTML using configured selectors with ML fallback
    pub fn extract_products_from_html(
        &self,
        html: &str,
        category_name: &str,
        source_url: Option<String>,
    ) -> Result<Vec<ScrapedProduct>> {
        // Primary: Use rule-based extraction
        match self.extract_with_rules(html, category_name, source_url.clone()) {
            Ok(products) if !products.is_empty() => {
                info!("Rule-based extraction found {} products", products.len());
                return Ok(products);
            }
            Ok(_) => info!("Rule-based extraction found no products, trying ML..."),
            Err(e) => warn!("Rule-based extraction failed: {:?}, trying ML...", e),
        }

        // Secondary: Use ML-based extraction if available
        if let Some(ref ml_model) = self.ml_model {
            match self.extract_with_ml(html, category_name, source_url, ml_model) {
                Ok(products) if !products.is_empty() => {
                    info!("ML-based extraction found {} products", products.len());
                    return Ok(products);
                }
                Ok(_) => info!("ML-based extraction found no products"),
                Err(e) => warn!("ML-based extraction failed: {:?}", e),
            }
        }

        // If both methods fail, return empty result
        info!("No products found using available methods");
        Ok(vec![])
    }

    /// Rule-based product extraction
    fn extract_with_rules(
        &self,
        html: &str,
        category_name: &str,
        source_url: Option<String>,
    ) -> Result<Vec<ScrapedProduct>> {
        let document = Html::parse_document(html);
        let mut products = Vec::new();

        // Extract category from page if configured
        let page_category = self.extract_category_from_page(&document)
            .unwrap_or_else(|| category_name.to_string());

        // Try each product selector
        for selector_str in &self.config.selectors.product_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                let elements: Vec<_> = document.select(&selector).collect();

                if !elements.is_empty() {
                    info!("Using selector '{}' found {} elements", selector_str, elements.len());

                    for element in elements {
                        if let Some(product) = self.extract_single_product(element, &page_category, source_url.clone()) {
                            products.push(product);
                        }
                    }
                    break; // Use first working selector
                }
            }
        }

        // Filter out excluded products
        let filtered_products = self.filter_excluded_products(products)?;
        info!("Extracted {} products from HTML (after filtering)", filtered_products.len());
        Ok(filtered_products)
    }

    /// ML-based product extraction
    fn extract_with_ml(
        &self,
        html: &str,
        category_name: &str,
        source_url: Option<String>,
        ml_model: &ProductMLModel,
    ) -> Result<Vec<ScrapedProduct>> {
        let candidates = self.find_product_candidates(html);
        let mut products = Vec::new();

        let document = Html::parse_document(html);
        let page_category = self.extract_category_from_page(&document)
            .unwrap_or_else(|| category_name.to_string());

        for candidate in candidates {
            let features = ml_model.feature_extractor.extract_features(&candidate);
            let feature_matrix = DenseMatrix::from_2d_vec(&vec![features]);

            match ml_model.classifier.predict(&feature_matrix) {
                Ok(predictions) => {
                    if let Some(&prediction) = predictions.get(0) {
                        if prediction == 1 {
                            if let Some(product) = self.candidate_to_product(&candidate, &page_category, source_url.clone()) {
                                products.push(product);
                            }
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        Ok(products)
    }

    /// Extract category from page title or breadcrumb
    fn extract_category_from_page(&self, document: &Html) -> Option<String> {
        // Try configured category selectors
        for selector_str in &self.config.selectors.category_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(element) = document.select(&selector).next() {
                    let category = element.text().collect::<Vec<_>>().join(" ").trim().to_string();
                    if !category.is_empty() {
                        return Some(category);
                    }
                }
            }
        }
        None
    }

    /// Extract a single product from HTML element
    fn extract_single_product(
        &self,
        element: ElementRef,
        category: &str,
        source_url: Option<String>,
    ) -> Option<ScrapedProduct> {
        // Debug: Log the element HTML for inspection
        let element_html = element.html();
        if element_html.len() > 200 {
            info!("Processing element: {}...", &element_html[..200]);
        } else {
            info!("Processing element: {}", element_html);
        }

        let name = match self.extract_product_name(element) {
            Some(n) => {
                info!("✅ Extracted name: {}", n);
                n
            }
            None => {
                warn!("❌ Failed to extract product name");
                return None;
            }
        };

        let price = match self.extract_product_price(element) {
            Some(p) => {
                info!("✅ Extracted price: {}", p);
                p
            }
            None => {
                warn!("❌ Failed to extract product price");
                return None;
            }
        };

        let product_id = match self.extract_product_id(element) {
            Some(id) => {
                info!("✅ Extracted product_id: {}", id);
                id
            }
            None => {
                warn!("❌ Failed to extract product ID");
                return None;
            }
        };

        info!("🎉 Successfully extracted product: {} (ID: {}, Price: {})", name, product_id, price);

        Some(ScrapedProduct {
            name,
            price,
            product_id,
            category: category.to_string(),
            url: source_url,
            raw_html: element.html(),
        })
    }

    /// Extract product name using configured selectors
    fn extract_product_name(&self, element: ElementRef) -> Option<String> {
        info!("🔍 Trying to extract product name with {} selectors", self.config.selectors.name_selectors.len());

        for selector_str in &self.config.selectors.name_selectors {
            info!("  Trying name selector: {}", selector_str);
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(name_element) = element.select(&selector).next() {
                    let name = name_element.text().collect::<Vec<_>>().join(" ").trim().to_string();
                    info!("  Found text: '{}'", name);
                    if !name.is_empty() && name.len() > 2 {
                        info!("  ✅ Valid name found: {}", name);
                        return Some(name);
                    }
                } else {
                    info!("  ❌ No element found for selector: {}", selector_str);
                }
            } else {
                warn!("  ❌ Invalid selector: {}", selector_str);
            }
        }

        info!("🔍 Trying fallback: extract from element text");
        // Fallback: extract from element text
        let text = element.text().collect::<Vec<_>>().join(" ");
        info!("  Element text: '{}'", text);
        let lines: Vec<&str> = text.lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect();

        for line in lines {
            info!("  Checking line: '{}'", line);
            if line.len() > 3 && !self.looks_like_price(line) {
                info!("  ✅ Valid fallback name found: {}", line);
                return Some(line.to_string());
            }
        }

        warn!("🔍 No valid product name found");
        None
    }

    /// Extract product price using configured selectors and patterns
    fn extract_product_price(&self, element: ElementRef) -> Option<String> {
        info!("💰 Trying to extract product price with {} selectors", self.config.selectors.price_selectors.len());

        // Try configured price selectors
        for selector_str in &self.config.selectors.price_selectors {
            info!("  Trying price selector: {}", selector_str);
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(price_element) = element.select(&selector).next() {
                    info!("  Found price element");

                    // Check for data-price-amount attribute first
                    if let Some(price_amount) = price_element.value().attr("data-price-amount") {
                        info!("  ✅ Found data-price-amount: {}", price_amount);
                        return Some(price_amount.to_string());
                    }

                    // Extract from text content
                    let price_text = price_element.text().collect::<Vec<_>>().join(" ").trim().to_string();
                    info!("  Price element text: '{}'", price_text);
                    if let Some(price) = self.extract_price_from_text(&price_text) {
                        info!("  ✅ Valid price found: {}", price);
                        return Some(price);
                    }
                } else {
                    info!("  ❌ No element found for price selector: {}", selector_str);
                }
            } else {
                warn!("  ❌ Invalid price selector: {}", selector_str);
            }
        }

        info!("💰 Trying fallback: search in all text for price patterns");
        // Fallback: search in all text for price patterns
        let all_text = element.text().collect::<Vec<_>>().join(" ");
        info!("  All element text: '{}'", all_text);
        if let Some(price) = self.extract_price_from_text(&all_text) {
            info!("  ✅ Fallback price found: {}", price);
            Some(price)
        } else {
            warn!("💰 No valid product price found");
            None
        }
    }

    /// Extract product ID from data attributes
    fn extract_product_id(&self, element: ElementRef) -> Option<String> {
        info!("🆔 Trying to extract product ID");

        // Look for data-product-id attribute
        if let Some(product_id) = element.value().attr("data-product-id") {
            info!("  ✅ Found data-product-id on root element: {}", product_id);
            return Some(product_id.to_string());
        } else {
            info!("  ❌ No data-product-id on root element");
        }

        // Look in child elements for data-product-id
        if let Ok(selector) = Selector::parse("[data-product-id]") {
            if let Some(id_element) = element.select(&selector).next() {
                if let Some(product_id) = id_element.value().attr("data-product-id") {
                    info!("  ✅ Found data-product-id in child element: {}", product_id);
                    return Some(product_id.to_string());
                }
            } else {
                info!("  ❌ No child elements with data-product-id found");
            }
        }

        warn!("🆔 No valid product ID found");
        None
    }

    /// Extract price from text using regex patterns
    fn extract_price_from_text(&self, text: &str) -> Option<String> {
        let price_patterns = [
            Regex::new(r"Rs\.?\s*[\d,]+").unwrap(),
            Regex::new(r"PKR\.?\s*[\d,]+").unwrap(),
            Regex::new(r"₨\.?\s*[\d,]+").unwrap(),
            Regex::new(r"\d+\s*Rs").unwrap(),
        ];

        for pattern in &price_patterns {
            if let Some(captures) = pattern.captures(text) {
                if let Some(price_match) = captures.get(0) {
                    return Some(price_match.as_str().trim().to_string());
                }
            }
        }
        None
    }

    /// Check if text looks like a price
    fn looks_like_price(&self, text: &str) -> bool {
        let price_patterns = [
            Regex::new(r"Rs\.?\s*[\d,]+").unwrap(),
            Regex::new(r"PKR\.?\s*[\d,]+").unwrap(),
            Regex::new(r"₨\.?\s*[\d,]+").unwrap(),
            Regex::new(r"\d+\s*Rs").unwrap(),
        ];

        for pattern in &price_patterns {
            if pattern.is_match(text) {
                return true;
            }
        }
        false
    }

    /// Filter out excluded products
    fn filter_excluded_products(&self, products: Vec<ScrapedProduct>) -> Result<Vec<ScrapedProduct>> {
        let filtered: Vec<ScrapedProduct> = products
            .into_iter()
            .filter(|product| !self.is_in_excluded_section(&product.name))
            .collect();
        Ok(filtered)
    }

    /// Check if product is in excluded section
    fn is_in_excluded_section(&self, text: &str) -> bool {
        let text_lower = text.to_lowercase();

        // Check excluded sections
        for section in &self.exclusion_detector.excluded_sections {
            if text_lower.contains(&section.to_lowercase()) {
                return true;
            }
        }

        // Check excluded keywords
        for keyword in &self.exclusion_detector.excluded_keywords {
            if text_lower.contains(&keyword.to_lowercase()) {
                return true;
            }
        }

        false
    }

    /// Find product candidates for ML classification
    fn find_product_candidates(&self, html: &str) -> Vec<ProductCandidate> {
        let mut candidates = Vec::new();
        let document = Html::parse_document(html);

        // Look for potential product elements
        let selectors = ["div", "article", "li", "section"];

        for selector_str in &selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                for element in document.select(&selector) {
                    let candidate = self.element_to_candidate(element);
                    candidates.push(candidate);
                }
            }
        }

        candidates
    }

    /// Convert HTML element to product candidate
    fn element_to_candidate(&self, element: ElementRef) -> ProductCandidate {
        let text_content = element.text().collect::<Vec<_>>().join(" ");
        let classes: Vec<String> = element.value().classes().map(|s| s.to_string()).collect();
        let mut attributes = HashMap::new();

        for attr in element.value().attrs() {
            attributes.insert(attr.0.to_string(), attr.1.to_string());
        }

        ProductCandidate {
            element_html: element.html(),
            text_content: text_content.clone(),
            tag_name: element.value().name().to_string(),
            classes,
            attributes,
            depth: self.calculate_depth(element),
            parent_context: self.get_parent_context(element),
            has_price_text: self.looks_like_price(&text_content),
            has_link: element.html().contains("<a"),
        }
    }

    /// Calculate element depth in DOM
    fn calculate_depth(&self, element: ElementRef) -> usize {
        let mut depth = 0;
        let mut current = Some(element);

        while let Some(elem) = current {
            depth += 1;
            current = elem.parent().and_then(|p| ElementRef::wrap(p));
        }

        depth
    }

    /// Get parent context classes
    fn get_parent_context(&self, element: ElementRef) -> String {
        if let Some(parent) = element.parent().and_then(|p| ElementRef::wrap(p)) {
            parent.value().classes().collect::<Vec<_>>().join(" ")
        } else {
            String::new()
        }
    }

    /// Convert candidate to product
    fn candidate_to_product(
        &self,
        candidate: &ProductCandidate,
        category: &str,
        source_url: Option<String>,
    ) -> Option<ScrapedProduct> {
        let html = Html::parse_fragment(&candidate.element_html);
        let root = html.root_element();

        if let Some(element_ref) = ElementRef::wrap(root.first_child()?) {
            self.extract_single_product(element_ref, category, source_url)
        } else {
            None
        }
    }
}

/// Implementation for ExclusionDetector
impl ExclusionDetector {
    pub fn new_default() -> Self {
        let mut excluded_keywords = HashSet::new();
        excluded_keywords.insert("advertisement".to_string());
        excluded_keywords.insert("sponsored".to_string());
        excluded_keywords.insert("banner".to_string());
        excluded_keywords.insert("footer".to_string());
        excluded_keywords.insert("header".to_string());
        excluded_keywords.insert("navigation".to_string());
        excluded_keywords.insert("menu".to_string());

        Self {
            excluded_sections: vec![
                "header".to_string(),
                "footer".to_string(),
                "nav".to_string(),
                "advertisement".to_string(),
                "sidebar".to_string(),
            ],
            excluded_keywords,
        }
    }
}

/// Implementation for FeatureExtractor
impl FeatureExtractor {
    pub fn new() -> Self {
        Self {
            price_patterns: vec![
                Regex::new(r"Rs\.?\s*[\d,]+").unwrap(),
                Regex::new(r"PKR\.?\s*[\d,]+").unwrap(),
                Regex::new(r"₨\.?\s*[\d,]+").unwrap(),
                Regex::new(r"\d+\s*Rs").unwrap(),
            ],
            name_patterns: vec![
                Regex::new(r"\b[A-Z][a-z]+(\s+[A-Z][a-z]+)*\b").unwrap(),
                Regex::new(r"\d+\s*(kg|g|ml|l|pack|pcs)\b").unwrap(),
            ],
        }
    }

    pub fn extract_features(&self, candidate: &ProductCandidate) -> Vec<f32> {
        let mut features = Vec::new();

        // Text-based features
        features.push(candidate.text_content.len() as f32);
        features.push(
            candidate
                .text_content
                .chars()
                .filter(|c| c.is_uppercase())
                .count() as f32,
        );
        features.push(
            candidate
                .text_content
                .chars()
                .filter(|c| c.is_numeric())
                .count() as f32,
        );
        features.push(candidate.text_content.split_whitespace().count() as f32);

        // Price pattern matching
        for pattern in &self.price_patterns {
            features.push(if pattern.is_match(&candidate.text_content) {
                1.0
            } else {
                0.0
            });
        }

        // Name pattern matching
        for pattern in &self.name_patterns {
            features.push(if pattern.is_match(&candidate.text_content) {
                1.0
            } else {
                0.0
            });
        }

        // Structural features
        features.push(candidate.depth as f32);
        features.push(candidate.classes.len() as f32);
        features.push(candidate.attributes.len() as f32);
        features.push(if candidate.has_link { 1.0 } else { 0.0 });

        // Context features
        features.push(if candidate.parent_context.contains("product") {
            1.0
        } else {
            0.0
        });
        features.push(if candidate.parent_context.contains("item") {
            1.0
        } else {
            0.0
        });
        features.push(if candidate.parent_context.contains("grid") {
            1.0
        } else {
            0.0
        });

        features
    }
}

/// Convert scraped products to JSON format for unified processing
impl ScrapedProduct {
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "price": self.price,
            "product_id": self.product_id,
            "category": self.category,
            "url": self.url,
            "source_type": "html"
        })
    }
}
