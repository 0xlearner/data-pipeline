use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use smartcore::ensemble::random_forest_classifier::{
    RandomForestClassifier, RandomForestClassifierParameters,
};
use smartcore::linalg::basic::matrix::DenseMatrix;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use wreq::Client;
use wreq_util::Emulation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Product {
    pub name: String,
    pub price: String,
    pub url: Option<String>,
}

#[derive(Debug)]
pub enum NaheedParseError {
    RuleFailed,
    MLFailed,
    NetworkError,
    InvalidHTML,
}

impl fmt::Display for NaheedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NaheedParseError::RuleFailed => write!(f, "Rule-based parsing failed"),
            NaheedParseError::MLFailed => write!(f, "ML model failed"),
            NaheedParseError::NetworkError => write!(f, "Network error occurred"),
            NaheedParseError::InvalidHTML => write!(f, "Invalid HTML content"),
        }
    }
}

impl Error for NaheedParseError {}

pub struct NaheedProductParser {
    rules: ProductExtractionRules,
    ml_model: Option<ProductMLModel>,
    exclusion_detector: ExclusionDetector,
    http_client: Option<Client>,
}

#[derive(Clone)]
pub struct ProductExtractionRules {
    pub product_selectors: Vec<String>,
    pub name_selectors: Vec<String>,
    pub price_selectors: Vec<String>,
    pub price_patterns: Vec<Regex>,
}

pub struct ProductMLModel {
    pub classifier: RandomForestClassifier<f32, i32, DenseMatrix<f32>, Vec<i32>>,
    pub feature_extractor: NaheedFeatureExtractor,
    pub confidence_threshold: f32,
}

pub struct ExclusionDetector {
    pub excluded_sections: Vec<String>,
    pub excluded_keywords: HashSet<String>,
}

#[derive(Clone)]
pub struct NaheedFeatureExtractor {
    pub price_patterns: Vec<Regex>,
    pub name_patterns: Vec<Regex>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TrainingExample {
    pub html_fragment: String,
    pub is_product: bool,
    pub product_name: Option<String>,
    pub product_price: Option<String>,
    pub section_context: String,
}

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

impl NaheedProductParser {
    pub fn new() -> Self {
        Self {
            rules: ProductExtractionRules::new_naheed_rules(),
            ml_model: None,
            exclusion_detector: ExclusionDetector::new_naheed_exclusions(),
            http_client: None,
        }
    }

    pub async fn initialize_client(&mut self) -> Result<(), NaheedParseError> {
        // Create a sophisticated wreq client with browser emulation
        let client = Client::builder()
            .emulation(Emulation::Firefox136)
            // // Set reasonable timeouts
            // .timeout(std::time::Duration::from_secs(30))
            // .connect_timeout(std::time::Duration::from_secs(10))
            // // Enable compression
            // .gzip(true)
            // .brotli(true)
            // // Add TLS configuration
            // .cert_verification(true)
            .build()
            .map_err(|_| NaheedParseError::NetworkError)?;

        self.http_client = Some(client);
        Ok(())
    }

    pub async fn scrape_and_parse(&self, url: &str) -> Result<Vec<Product>, NaheedParseError> {
        // Fetch the webpage with smart retry logic
        let html = self.fetch_page_with_retry(url, 3).await?;

        // Parse products
        self.extract_products(&html)
    }

    async fn fetch_page_with_retry(
        &self,
        url: &str,
        max_retries: usize,
    ) -> Result<String, NaheedParseError> {
        let mut attempts = 0;
        let mut last_error = NaheedParseError::NetworkError;

        while attempts < max_retries {
            match self.fetch_page_smart(url).await {
                Ok(html) => return Ok(html),
                Err(e) => {
                    last_error = e;
                    attempts += 1;

                    if attempts < max_retries {
                        // Exponential backoff with jitter
                        let delay = std::time::Duration::from_millis(
                            1000 * (2_u64.pow(attempts as u32)) + (rand::random::<u64>() % 1000),
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error)
    }

    async fn fetch_page_smart(&self, url: &str) -> Result<String, NaheedParseError> {
        let client = match &self.http_client {
            Some(client) => client,
            None => return Err(NaheedParseError::NetworkError),
        };

        // Add random delay to mimic human behavior
        let delay = std::time::Duration::from_millis(500 + (rand::random::<u64>() % 2000));
        tokio::time::sleep(delay).await;

        // Build request with comprehensive headers
        let response = client
            .get(url)
            // // Core browser headers
            // .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
            // .header("Accept-Language", "en-US,en;q=0.9,ur;q=0.8") // Include Urdu for Pakistani sites
            // .header("Accept-Encoding", "gzip, deflate, br")
            // .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            // // Security headers
            // .header("DNT", "1")
            // .header("Sec-Ch-Ua", "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"")
            // .header("Sec-Ch-Ua-Mobile", "?0")
            // .header("Sec-Ch-Ua-Platform", "\"Windows\"")
            // // Fetch metadata
            // .header("Sec-Fetch-Dest", "document")
            // .header("Sec-Fetch-Mode", "navigate")
            // .header("Sec-Fetch-Site", "none")
            // .header("Sec-Fetch-User", "?1")
            // // Connection management
            // .header("Connection", "keep-alive")
            // .header("Upgrade-Insecure-Requests", "1")
            // .header("Cache-Control", "max-age=0")
            // // Add referer for internal pages (if not the first page)
            // .header("Referer", if url.contains("?p=") {
            //     "https://www.naheed.pk/groceries-pets/fresh-products"
            // } else {
            //     "https://www.google.com/"
            // })
            .send()
            .await
            .map_err(|_| NaheedParseError::NetworkError)?;

        if !response.status().is_success() {
            return Err(NaheedParseError::NetworkError);
        }

        let html = response
            .text()
            .await
            .map_err(|_| NaheedParseError::NetworkError)?;

        if html.is_empty() {
            return Err(NaheedParseError::InvalidHTML);
        }

        // Basic HTML validation
        if !html.contains("<html") && !html.contains("<div") && !html.contains("<body") {
            return Err(NaheedParseError::InvalidHTML);
        }

        // Additional anti-bot detection checks
        // if html.contains("captcha") || html.contains("blocked") || html.contains("bot detected") {
        //     println!("⚠️ Potential bot detection detected in response");
        //     return Err(NaheedParseError::NetworkError);
        // }

        println!(
            "✅ Successfully fetched {} characters from {}",
            html.len(),
            url
        );
        Ok(html)
    }

    pub fn extract_products(&self, html: &str) -> Result<Vec<Product>, NaheedParseError> {
        // Primary: Use rule-based extraction
        match self.extract_with_rules(html) {
            Ok(products) if !products.is_empty() => {
                println!("✅ Rule-based extraction found {} products", products.len());
                return Ok(products);
            }
            Ok(_) => println!("⚠️ Rule-based extraction found no products, trying ML..."),
            Err(e) => println!("⚠️ Rule-based extraction failed: {:?}, trying ML...", e),
        }

        // Secondary: Use ML-based extraction
        if let Some(ref ml_model) = self.ml_model {
            match self.extract_with_ml(html, ml_model) {
                Ok(products) if !products.is_empty() => {
                    println!("✅ ML-based extraction found {} products", products.len());
                    return Ok(products);
                }
                Ok(_) => println!("⚠️ ML-based extraction found no products"),
                Err(e) => println!("⚠️ ML-based extraction failed: {:?}", e),
            }
        }

        // If both methods fail, return empty result rather than error
        println!("ℹ️ No products found using available methods");
        Ok(vec![])
    }

    fn extract_with_rules(&self, html: &str) -> Result<Vec<Product>, NaheedParseError> {
        let document = Html::parse_document(html);
        let mut products = Vec::new();

        // Try each product selector until we find one that works
        for selector_str in &self.rules.product_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                let elements: Vec<_> = document.select(&selector).collect();

                if !elements.is_empty() {
                    println!(
                        "📍 Using selector: {} (found {} elements)",
                        selector_str,
                        elements.len()
                    );

                    for element in elements {
                        if let Some(product) = self.extract_single_product(element) {
                            products.push(product);
                        }
                    }
                    break; // Use the first working selector
                }
            }
        }

        self.filter_excluded_products(products)
    }

    fn extract_single_product(&self, element: ElementRef) -> Option<Product> {
        let name = self.extract_product_name(element)?;
        let price = self.extract_product_price(element)?;
        let url = self.extract_product_url(element);

        let product = Product { name, price, url };

        if self.validate_product(&product) {
            Some(product)
        } else {
            None
        }
    }

    fn extract_product_name(&self, element: ElementRef) -> Option<String> {
        // Try each name selector
        for selector_str in &self.rules.name_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(name_element) = element.select(&selector).next() {
                    let name = name_element
                        .text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim()
                        .to_string();
                    if !name.is_empty() && name.len() > 2 {
                        return Some(name);
                    }
                }
            }
        }

        // Fallback: extract from element text
        let text = element.text().collect::<Vec<_>>().join(" ");
        let lines: Vec<&str> = text
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty())
            .collect();

        for line in lines {
            if line.len() > 3 && !self.looks_like_price(line) {
                return Some(line.to_string());
            }
        }

        None
    }

    fn extract_product_price(&self, element: ElementRef) -> Option<String> {
        // Try each price selector
        for selector_str in &self.rules.price_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(price_element) = element.select(&selector).next() {
                    let price_text = price_element
                        .text()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .trim()
                        .to_string();
                    if let Some(price) = self.extract_price_from_text(&price_text) {
                        return Some(price);
                    }
                }
            }
        }

        // Fallback: search in all text for price patterns
        let all_text = element.text().collect::<Vec<_>>().join(" ");
        self.extract_price_from_text(&all_text)
    }

    fn extract_price_from_text(&self, text: &str) -> Option<String> {
        for pattern in &self.rules.price_patterns {
            if let Some(captures) = pattern.captures(text) {
                if let Some(price_match) = captures.get(0) {
                    return Some(price_match.as_str().trim().to_string());
                }
            }
        }
        None
    }

    fn extract_product_url(&self, element: ElementRef) -> Option<String> {
        // Look for links within the element
        if let Ok(link_selector) = Selector::parse("a") {
            if let Some(link_element) = element.select(&link_selector).next() {
                if let Some(href) = link_element.value().attr("href") {
                    if href.starts_with("/") {
                        return Some(format!("https://www.naheed.pk{}", href));
                    } else if href.starts_with("http") {
                        return Some(href.to_string());
                    }
                }
            }
        }

        // Check if the element itself is a link
        if element.value().name() == "a" {
            if let Some(href) = element.value().attr("href") {
                if href.starts_with("/") {
                    return Some(format!("https://www.naheed.pk{}", href));
                } else if href.starts_with("http") {
                    return Some(href.to_string());
                }
            }
        }

        None
    }

    fn looks_like_price(&self, text: &str) -> bool {
        for pattern in &self.rules.price_patterns {
            if pattern.is_match(text) {
                return true;
            }
        }
        false
    }

    fn validate_product(&self, product: &Product) -> bool {
        // Basic validation rules
        product.name.len() >= 3
            && product.name.len() <= 200
            && !product.price.is_empty()
            && product.name.chars().any(|c| c.is_alphabetic())
    }

    fn filter_excluded_products(
        &self,
        products: Vec<Product>,
    ) -> Result<Vec<Product>, NaheedParseError> {
        let filtered: Vec<Product> = products
            .into_iter()
            .filter(|product| !self.is_in_excluded_section(&product.name))
            .collect();

        Ok(filtered)
    }

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

    pub fn generate_training_data(&self, html_samples: &[String]) -> Vec<TrainingExample> {
        let mut examples = Vec::new();

        for html in html_samples {
            let document = Html::parse_document(html);

            // Extract positive examples from known product elements
            if let Ok(selector) = Selector::parse("div[class*='product'], .product-item, .item") {
                for element in document.select(&selector) {
                    if let Some(example) = self.create_positive_example(element) {
                        examples.push(example);
                    }
                }
            }

            // Generate negative examples
            examples.extend(self.create_negative_examples(html));
        }

        examples
    }

    pub async fn scrape_multiple_pages(
        &self,
        base_url: &str,
        pages: std::ops::Range<usize>,
    ) -> Result<Vec<String>, NaheedParseError> {
        let mut all_html = Vec::new();

        for page_num in pages {
            let url = if page_num == 1 {
                base_url.to_string()
            } else {
                format!("{}?p={}", base_url, page_num)
            };

            println!("Scraping page {}: {}", page_num, url);

            match self.fetch_page_with_retry(&url, 3).await {
                Ok(html) => {
                    all_html.push(html);
                    println!("✅ Successfully scraped page {}", page_num);
                }
                Err(e) => {
                    println!("❌ Failed to scrape page {}: {:?}", page_num, e);
                    // Continue with other pages instead of failing completely
                }
            }

            // Smart rate limiting - be respectful to the server
            let delay = std::time::Duration::from_millis(2000 + (rand::random::<u64>() % 3000));
            tokio::time::sleep(delay).await;
        }

        if all_html.is_empty() {
            Err(NaheedParseError::NetworkError)
        } else {
            Ok(all_html)
        }
    }

    pub async fn collect_training_data(
        &self,
        base_urls: &[&str],
        pages_per_url: usize,
    ) -> Result<Vec<TrainingExample>, NaheedParseError> {
        let mut all_training_data = Vec::new();

        for base_url in base_urls {
            println!("Collecting training data from: {}", base_url);

            let html_pages = self
                .scrape_multiple_pages(base_url, 1..(pages_per_url + 1))
                .await?;
            let training_examples = self.generate_training_data(&html_pages);

            println!(
                "Generated {} training examples from {}",
                training_examples.len(),
                base_url
            );
            all_training_data.extend(training_examples);

            // Longer delay between different URLs
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        Ok(all_training_data)
    }

    fn create_positive_example(&self, element: ElementRef) -> Option<TrainingExample> {
        let html_fragment = element.html();
        let _text_content = element.text().collect::<Vec<_>>().join(" ");

        // Try to extract product info
        let product_name = self.extract_product_name(element);
        let product_price = self.extract_product_price(element);

        // Only create example if we found some product-like content
        if product_name.is_some() || product_price.is_some() {
            Some(TrainingExample {
                html_fragment,
                is_product: true,
                product_name,
                product_price,
                section_context: "product-section".to_string(),
            })
        } else {
            None
        }
    }

    fn create_negative_examples(&self, html: &str) -> Vec<TrainingExample> {
        let mut examples = Vec::new();
        let document = Html::parse_document(html);

        // Select non-product elements
        let negative_selectors = [
            "header",
            "footer",
            "nav",
            ".navigation",
            ".menu",
            ".breadcrumb",
            ".sidebar",
            ".advertisement",
        ];

        for selector_str in &negative_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                for element in document.select(&selector).take(2) {
                    // Limit to avoid too many negatives
                    let text_content = element.text().collect::<Vec<_>>().join(" ");
                    if !text_content.trim().is_empty() && text_content.len() > 10 {
                        examples.push(TrainingExample {
                            html_fragment: element.html(),
                            is_product: false,
                            product_name: None,
                            product_price: None,
                            section_context: selector_str.to_string(),
                        });
                    }
                }
            }
        }

        examples
    }

    pub fn train_ml_model(
        &mut self,
        training_examples: &[TrainingExample],
    ) -> Result<(), NaheedParseError> {
        let feature_extractor = NaheedFeatureExtractor::new();
        let (features, labels) =
            self.prepare_ml_training_data(training_examples, &feature_extractor);

        let params = RandomForestClassifierParameters::default();

        let feature_matrix = DenseMatrix::from_2d_vec(&features);
        let classifier = RandomForestClassifier::fit(&feature_matrix, &labels, params)
            .map_err(|_| NaheedParseError::MLFailed)?;

        self.ml_model = Some(ProductMLModel {
            classifier,
            feature_extractor,
            confidence_threshold: 0.7,
        });

        Ok(())
    }

    fn prepare_ml_training_data(
        &self,
        examples: &[TrainingExample],
        extractor: &NaheedFeatureExtractor,
    ) -> (Vec<Vec<f32>>, Vec<i32>) {
        let mut features = Vec::new();
        let mut labels = Vec::new();

        for example in examples {
            let candidate = self.example_to_candidate(example);
            let feature_vector = extractor.extract_features(&candidate);

            features.push(feature_vector);
            labels.push(if example.is_product { 1 } else { 0 });
        }

        (features, labels)
    }

    fn example_to_candidate(&self, example: &TrainingExample) -> ProductCandidate {
        let document = Html::parse_fragment(&example.html_fragment);

        ProductCandidate {
            element_html: example.html_fragment.clone(),
            text_content: document.root_element().text().collect::<Vec<_>>().join(" "),
            tag_name: "div".to_string(), // Simplified
            classes: vec![],
            attributes: HashMap::new(),
            depth: 3,
            parent_context: example.section_context.clone(),
            has_price_text: example.product_price.is_some(),
            has_link: example.html_fragment.contains("<a"),
        }
    }

    fn extract_with_ml(
        &self,
        html: &str,
        ml_model: &ProductMLModel,
    ) -> Result<Vec<Product>, NaheedParseError> {
        let candidates = self.find_product_candidates(html);
        let mut products = Vec::new();

        for candidate in candidates {
            let features = ml_model.feature_extractor.extract_features(&candidate);
            let feature_matrix = DenseMatrix::from_2d_vec(&vec![features]);

            match ml_model.classifier.predict(&feature_matrix) {
                Ok(predictions) => {
                    if let Some(&prediction) = predictions.get(0) {
                        if prediction == 1 {
                            if let Some(product) = self.candidate_to_product(&candidate) {
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

    fn find_product_candidates(&self, html: &str) -> Vec<ProductCandidate> {
        let mut candidates = Vec::new();
        let document = Html::parse_document(html);

        // Look for potential product elements
        let selectors = ["div", "article", "li", "section"];

        for selector_str in &selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                for element in document.select(&selector) {
                    let candidate = self.element_to_candidate(element, &document);
                    candidates.push(candidate);
                }
            }
        }

        candidates
    }

    fn element_to_candidate(&self, element: ElementRef, _document: &Html) -> ProductCandidate {
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

    fn calculate_depth(&self, element: ElementRef) -> usize {
        let mut depth = 0;
        let mut current = Some(element);

        while let Some(elem) = current {
            depth += 1;
            current = elem.parent().and_then(|p| ElementRef::wrap(p));
        }

        depth
    }

    fn get_parent_context(&self, element: ElementRef) -> String {
        if let Some(parent) = element.parent().and_then(|p| ElementRef::wrap(p)) {
            parent.value().classes().collect::<Vec<_>>().join(" ")
        } else {
            String::new()
        }
    }

    fn candidate_to_product(&self, candidate: &ProductCandidate) -> Option<Product> {
        let html = Html::parse_fragment(&candidate.element_html);
        let root = html.root_element();

        if let Some(element_ref) = ElementRef::wrap(root.first_child()?) {
            self.extract_single_product(element_ref)
        } else {
            None
        }
    }
}

impl ProductExtractionRules {
    fn new_naheed_rules() -> Self {
        Self {
            product_selectors: vec![
                ".product-item".to_string(),
                ".product-list .item".to_string(),
                "[data-product]".to_string(),
                ".grid-item".to_string(),
                ".product-card".to_string(),
                "div[class*='product']".to_string(),
                "li[class*='item']".to_string(),
                "article".to_string(),
                "div[class*='grid-item']".to_string(),
            ],
            name_selectors: vec![
                ".product-name".to_string(),
                ".item-title".to_string(),
                ".title".to_string(),
                "h3".to_string(),
                "h4".to_string(),
                "a[href*='product']".to_string(),
                ".name".to_string(),
                "strong".to_string(),
            ],
            price_selectors: vec![
                ".price".to_string(),
                ".cost".to_string(),
                ".amount".to_string(),
                "[class*='price']".to_string(),
                ".product-price".to_string(),
                "span[class*='rs']".to_string(),
            ],
            price_patterns: vec![
                Regex::new(r"Rs\.?\s*[\d,]+").unwrap(),
                Regex::new(r"PKR\.?\s*[\d,]+").unwrap(),
                Regex::new(r"₨\.?\s*[\d,]+").unwrap(),
                Regex::new(r"\d+\s*Rs").unwrap(),
            ],
        }
    }
}

impl ExclusionDetector {
    fn new_naheed_exclusions() -> Self {
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

impl NaheedFeatureExtractor {
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

        // Naheed-specific features
        features.push(
            if candidate.text_content.contains("KG") || candidate.text_content.contains("kg") {
                1.0
            } else {
                0.0
            },
        );
        features.push(if candidate.text_content.contains("Local") {
            1.0
        } else {
            0.0
        });
        features.push(if candidate.text_content.contains("Piece") {
            1.0
        } else {
            0.0
        });

        features
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_naheed_scraping() {
        let mut parser = NaheedProductParser::new();
        let _ = parser.initialize_client().await;

        let sample_html = r#"
            <div class="product-item">
                <h3>Onion (Pyaaz) 1 KG</h3>
                <span class="price">Rs. 140</span>
            </div>
        "#;

        match parser.extract_products(sample_html) {
            Ok(products) => {
                println!("Found {} products", products.len());
            }
            Err(e) => println!("Error: {:?}", e),
        }
    }

    #[test]
    fn test_training_data_generation() {
        let parser = NaheedProductParser::new();
        let sample_html = r#"
            <div class="product-item">
                <a href="/onion-pyaaz-1-kg">Onion (Pyaaz) 1 KG</a>
                <span class="price">Rs. 140</span>
            </div>
        "#;

        let examples = parser.generate_training_data(&vec![sample_html.to_string()]);
        assert!(!examples.is_empty());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Naheed.pk Product Parser");

    let mut parser = NaheedProductParser::new();
    parser.initialize_client().await?;
    println!("✅ HTTP client initialized");

    // Test with sample HTML first
    let sample_html = r#"
        <div class="product-item">
            <h3>Onion (Pyaaz) 1 KG</h3>
            <span class="price">Rs. 140</span>
            <a href="/onion-1kg">View Product</a>
        </div>
        <div class="product-item">
            <h3>Potato (Aloo) 1 KG</h3>
            <span class="price">Rs. 100</span>
            <a href="/potato-1kg">View Product</a>
        </div>
    "#;

    println!("Testing with sample HTML...");
    match parser.extract_products(sample_html) {
        Ok(products) => {
            println!("✅ Found {} products in sample:", products.len());
            for (i, product) in products.iter().enumerate() {
                println!("  {}. {} - {}", i + 1, product.name, product.price);
            }
        }
        Err(e) => {
            println!("❌ Error with sample HTML: {:?}", e);
        }
    }

    // Try to scrape a real page
    let target_url = "https://www.naheed.pk/groceries-pets/fresh-products";
    println!("Attempting to scrape: {}", target_url);

    match parser.scrape_and_parse(target_url).await {
        Ok(products) => {
            println!("✅ Successfully extracted {} products:", products.len());
            for (i, product) in products.iter().take(10).enumerate() {
                println!("  {}. {} - {}", i + 1, product.name, product.price);
                if let Some(url) = &product.url {
                    println!("     🔗 {}", url);
                }
            }
        }
        Err(e) => {
            println!("❌ Error scraping real page: {:?}", e);
            println!("This might be due to network issues or website changes.");
        }
    }

    println!("🎯 Parser test completed!");
    Ok(())
}
