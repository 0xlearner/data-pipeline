use anyhow::{Result, anyhow};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use smartcore::ensemble::random_forest_classifier::RandomForestClassifier;
use smartcore::linalg::basic::matrix::DenseMatrix;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::HtmlConfig;

/// Pure HTML data extractor
/// Only handles HTML parsing and data extraction, no HTTP operations
pub struct HtmlExtractor {
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

impl HtmlExtractor {
    pub fn new(config: HtmlConfig) -> Self {
        Self {
            config,
            ml_model: None,
            exclusion_detector: ExclusionDetector::new_default(),
        }
    }

    /// Initialize ML model for enhanced product extraction
    pub fn with_ml_model(mut self, model: ProductMLModel) -> Self {
        self.ml_model = Some(model);
        self
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

    /// Rule-based product extraction using configured selectors
    fn extract_with_rules(
        &self,
        html: &str,
        category_name: &str,
        source_url: Option<String>,
    ) -> Result<Vec<ScrapedProduct>> {
        let document = Html::parse_document(html);
        let mut products = Vec::new();

        // Check if category exists
        let _category_config =
            self.config.categories.get(category_name).ok_or_else(|| {
                anyhow!("Category '{}' not found in configuration", category_name)
            })?;

        // Try all product selectors until we find elements
        for product_selector_str in &self.config.selectors.product_selectors {
            if let Ok(product_selector) = Selector::parse(product_selector_str) {
                let mut found_products = false;
                for element in document.select(&product_selector) {
                    // Skip elements that should be excluded
                    if self.should_exclude_element(&element) {
                        continue;
                    }

                    if let Some(product) =
                        self.extract_single_product(&element, category_name, &source_url)
                    {
                        products.push(product);
                        found_products = true;
                    }
                }
                // If we found products with this selector, don't try other selectors
                if found_products {
                    break;
                }
            }
        }

        Ok(products)
    }

    /// Extract a single product from an HTML element
    fn extract_single_product(
        &self,
        element: &ElementRef,
        category_name: &str,
        source_url: &Option<String>,
    ) -> Option<ScrapedProduct> {
        // Try all name selectors until we find text
        let mut name = None;
        for name_selector in &self.config.selectors.name_selectors {
            if let Some(text) = self.extract_text_by_selector(element, name_selector) {
                name = Some(text);
                break;
            }
        }
        let name = name?;

        // Try all price selectors until we find text
        let mut price = None;
        for price_selector in &self.config.selectors.price_selectors {
            if let Some(text) = self.extract_text_by_selector(element, price_selector) {
                price = Some(text);
                break;
            }
        }
        let price = price?;

        // Extract product ID (optional) - generate random ID since no specific selector
        let product_id = format!("scraped_{}", rand::random::<u32>());

        Some(ScrapedProduct {
            name,
            price,
            product_id,
            category: category_name.to_string(),
            url: source_url.clone(),
            raw_html: element.html(),
        })
    }

    /// Extract text content using CSS selector
    fn extract_text_by_selector(&self, element: &ElementRef, selector_str: &str) -> Option<String> {
        let selector = Selector::parse(selector_str).ok()?;
        element
            .select(&selector)
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
            .filter(|text| !text.is_empty())
    }

    /// ML-based product extraction (placeholder for future implementation)
    fn extract_with_ml(
        &self,
        html: &str,
        category_name: &str,
        source_url: Option<String>,
        ml_model: &ProductMLModel,
    ) -> Result<Vec<ScrapedProduct>> {
        let document = Html::parse_document(html);
        let mut candidates = Vec::new();

        // Extract product candidates using a broader selector approach
        let broad_selectors = [
            "div[class*='product']",
            "article[class*='item']",
            "li[class*='product']",
            ".product-item",
            ".item-container",
        ];

        for selector_str in &broad_selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                for element in document.select(&selector) {
                    let candidate = ProductCandidate {
                        element_html: element.html(),
                        text_content: element.text().collect::<String>(),
                        tag_name: element.value().name().to_string(),
                        classes: element
                            .value()
                            .attr("class")
                            .unwrap_or("")
                            .split_whitespace()
                            .map(|s| s.to_string())
                            .collect(),
                        attributes: element
                            .value()
                            .attrs()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect(),
                        depth: 0, // Could be calculated based on DOM depth
                        parent_context: "".to_string(), // Could be extracted from parent elements
                        has_price_text: element.text().collect::<String>().contains('$')
                            || element.text().collect::<String>().contains("Rs"),
                        has_link: element.value().name() == "a"
                            || element
                                .select(&scraper::Selector::parse("a").unwrap())
                                .next()
                                .is_some(),
                    };
                    candidates.push(candidate);
                }
            }
        }

        // Use ML model to classify and extract products from candidates
        let mut products = Vec::new();
        let candidate_count = candidates.len();
        for candidate in candidates {
            if self.classify_product_candidate(&candidate, ml_model) {
                if let Some(product) =
                    self.extract_product_from_candidate(&candidate, category_name, &source_url)
                {
                    products.push(product);
                }
            }
        }

        info!(
            "ML extraction processed {} candidates, found {} products",
            candidate_count,
            products.len()
        );
        Ok(products)
    }

    /// Classify if a candidate is likely a product using ML model
    fn classify_product_candidate(
        &self,
        candidate: &ProductCandidate,
        ml_model: &ProductMLModel,
    ) -> bool {
        // Extract features from candidate
        let _features = self.extract_features_from_candidate(candidate);

        // For now, use simple heuristics as ML placeholder
        // In real implementation, this would use ml_model.classifier
        let has_price_indicator = candidate.text_content.contains('$')
            || candidate.text_content.contains("price")
            || candidate.text_content.contains("PKR")
            || candidate.text_content.contains("Rs");

        let has_product_keywords = candidate
            .classes
            .iter()
            .any(|class| class.contains("product") || class.contains("item"));

        let confidence_score = if has_price_indicator && has_product_keywords {
            0.9
        } else {
            0.3
        };

        confidence_score >= ml_model.confidence_threshold
    }

    /// Extract features from a product candidate for ML classification
    fn extract_features_from_candidate(&self, candidate: &ProductCandidate) -> Vec<f32> {
        let mut features = Vec::new();

        // Text-based features
        features.push(candidate.text_content.len() as f32);
        features.push(if candidate.text_content.contains('$') {
            1.0
        } else {
            0.0
        });
        features.push(candidate.classes.len() as f32);
        features.push(candidate.attributes.len() as f32);
        features.push(candidate.depth as f32);

        // Add more sophisticated features in real implementation
        features
    }

    /// Extract product information from a classified candidate
    fn extract_product_from_candidate(
        &self,
        candidate: &ProductCandidate,
        category_name: &str,
        source_url: &Option<String>,
    ) -> Option<ScrapedProduct> {
        // Parse the candidate HTML to extract product details
        let fragment = Html::parse_fragment(&candidate.element_html);

        // Use existing selectors to extract product information
        let name =
            self.extract_text_with_selectors(&fragment, &self.config.selectors.name_selectors)?;
        let price_text =
            self.extract_text_with_selectors(&fragment, &self.config.selectors.price_selectors);

        Some(ScrapedProduct {
            name,
            price: price_text.unwrap_or_default(),
            product_id: format!("ml_{}", Uuid::new_v4().to_string()[..8].to_string()),
            category: category_name.to_string(),
            url: source_url.clone(),
            raw_html: candidate.element_html.clone(),
        })
    }

    /// Helper method to extract text using multiple selectors on a fragment
    fn extract_text_with_selectors(&self, fragment: &Html, selectors: &[String]) -> Option<String> {
        for selector_str in selectors {
            if let Ok(selector) = Selector::parse(selector_str) {
                if let Some(element) = fragment.select(&selector).next() {
                    let text = element.text().collect::<String>().trim().to_string();
                    if !text.is_empty() {
                        return Some(text);
                    }
                }
            }
        }
        None
    }
}

impl ExclusionDetector {
    pub fn new_default() -> Self {
        let mut excluded_keywords = HashSet::new();
        excluded_keywords.insert("advertisement".to_string());
        excluded_keywords.insert("sponsored".to_string());
        excluded_keywords.insert("banner".to_string());

        Self {
            excluded_sections: vec![
                "header".to_string(),
                "footer".to_string(),
                "nav".to_string(),
                "sidebar".to_string(),
            ],
            excluded_keywords,
        }
    }

    /// Check if an element contains excluded keywords
    pub fn contains_excluded_keywords(&self, text: &str) -> bool {
        let text_lower = text.to_lowercase();
        self.excluded_keywords
            .iter()
            .any(|keyword| text_lower.contains(keyword))
    }

    /// Check if an element is in an excluded section
    pub fn is_excluded_section(&self, element: &ElementRef) -> bool {
        // Check if any parent element has a class or tag that matches excluded sections
        let mut current = Some(element.clone());
        while let Some(elem) = current {
            let element_name = elem.value().name();
            if self.excluded_sections.contains(&element_name.to_string()) {
                return true;
            }

            // Check classes
            if let Some(classes) = elem.value().attr("class") {
                for class in classes.split_whitespace() {
                    if self
                        .excluded_sections
                        .iter()
                        .any(|section| class.contains(section))
                    {
                        return true;
                    }
                }
            }

            current = elem
                .parent()
                .and_then(|p| p.value().as_element().map(|_| ElementRef::wrap(p).unwrap()));
        }
        false
    }
}

impl HtmlExtractor {
    /// Check if an element should be excluded from extraction
    fn should_exclude_element(&self, element: &ElementRef) -> bool {
        // Get element text for keyword checking
        let element_text = element.text().collect::<String>();

        // Check for excluded keywords in the text
        if self
            .exclusion_detector
            .contains_excluded_keywords(&element_text)
        {
            return true;
        }

        // Check if element is in an excluded section
        if self.exclusion_detector.is_excluded_section(element) {
            return true;
        }

        // Check element attributes for excluded patterns
        if let Some(class_attr) = element.value().attr("class") {
            if self
                .exclusion_detector
                .contains_excluded_keywords(class_attr)
            {
                return true;
            }
        }

        if let Some(id_attr) = element.value().attr("id") {
            if self.exclusion_detector.contains_excluded_keywords(id_attr) {
                return true;
            }
        }

        false
    }
}

impl FeatureExtractor {
    pub fn new() -> Self {
        let price_patterns = vec![
            Regex::new(r"\$\d+\.?\d*").unwrap(),
            Regex::new(r"Rs\.?\s*\d+").unwrap(),
            Regex::new(r"\d+\s*Rs").unwrap(),
        ];

        let name_patterns = vec![
            Regex::new(r"[A-Z][a-z]+\s+[A-Z][a-z]+").unwrap(),
            Regex::new(r"\b\w+\s+\w+\b").unwrap(),
        ];

        Self {
            price_patterns,
            name_patterns,
        }
    }
}
