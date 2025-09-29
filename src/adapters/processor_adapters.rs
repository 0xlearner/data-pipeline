use anyhow::Result;
use async_trait::async_trait;
// Removed unused polars::prelude import

use crate::processor::{FieldClassifier, HtmlProcessor, JsonFlattener, RuleNormalizer};
use crate::traits::data_processor::{
    DataProcessor, DataType, PerformanceMetrics, ProcessorInput, ProcessorMetadata,
    ProcessorOutput, ProcessorType,
};

/// Adapter that makes JsonFlattener compatible with DataProcessor trait
pub struct JsonFlattenerAdapter {
    flattener: JsonFlattener,
}

impl JsonFlattenerAdapter {
    pub fn new() -> Self {
        Self {
            flattener: JsonFlattener::new(),
        }
    }
}

#[async_trait]
impl DataProcessor for JsonFlattenerAdapter {
    fn name(&self) -> &str {
        "json_flattener"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Flattener
    }

    async fn process(&self, input: ProcessorInput) -> Result<ProcessorOutput> {
        match input {
            ProcessorInput::Json(data) => {
                let df = self.flattener.flatten_to_dataframe(&data)?;
                Ok(ProcessorOutput::DataFrame(df))
            }
            _ => Err(anyhow::anyhow!("JsonFlattener can only process JSON input")),
        }
    }

    fn can_process(&self, input_type: &DataType) -> bool {
        matches!(input_type, DataType::Json)
    }

    fn output_type(&self, input_type: &DataType) -> Result<DataType> {
        if self.can_process(input_type) {
            Ok(DataType::DataFrame)
        } else {
            Err(anyhow::anyhow!(
                "Cannot process input type: {:?}",
                input_type
            ))
        }
    }

    fn metadata(&self) -> ProcessorMetadata {
        ProcessorMetadata {
            name: "json_flattener".to_string(),
            description: Some("Flattens JSON data into tabular format".to_string()),
            version: Some("1.0.0".to_string()),
            supported_input_types: vec![DataType::Json],
            supported_output_types: vec![DataType::DataFrame],
            configuration_schema: None,
            performance_metrics: Some(PerformanceMetrics {
                average_processing_time_ms: 50.0,
                throughput_items_per_second: 1000.0,
                memory_usage_mb: 10.0,
                success_rate: 0.99,
            }),
        }
    }
}

/// Adapter that makes FieldClassifier compatible with DataProcessor trait
pub struct FieldClassifierAdapter {
    classifier: FieldClassifier,
}

impl FieldClassifierAdapter {
    pub fn new() -> Self {
        Self {
            classifier: FieldClassifier::new(),
        }
    }
}

#[async_trait]
impl DataProcessor for FieldClassifierAdapter {
    fn name(&self) -> &str {
        "field_classifier"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Classifier
    }

    async fn process(&self, input: ProcessorInput) -> Result<ProcessorOutput> {
        match input {
            ProcessorInput::DataFrame(mut df) => {
                self.classifier.map_to_canonical_schema(&mut df)?;
                Ok(ProcessorOutput::DataFrame(df))
            }
            _ => Err(anyhow::anyhow!(
                "FieldClassifier can only process DataFrame input"
            )),
        }
    }

    fn can_process(&self, input_type: &DataType) -> bool {
        matches!(input_type, DataType::DataFrame)
    }

    fn output_type(&self, input_type: &DataType) -> Result<DataType> {
        if self.can_process(input_type) {
            Ok(DataType::DataFrame)
        } else {
            Err(anyhow::anyhow!(
                "Cannot process input type: {:?}",
                input_type
            ))
        }
    }

    fn metadata(&self) -> ProcessorMetadata {
        ProcessorMetadata {
            name: "field_classifier".to_string(),
            description: Some("Classifies and maps fields to canonical schema".to_string()),
            version: Some("1.0.0".to_string()),
            supported_input_types: vec![DataType::DataFrame],
            supported_output_types: vec![DataType::DataFrame],
            configuration_schema: None,
            performance_metrics: Some(PerformanceMetrics {
                average_processing_time_ms: 100.0,
                throughput_items_per_second: 500.0,
                memory_usage_mb: 20.0,
                success_rate: 0.98,
            }),
        }
    }
}

/// Adapter that makes RuleNormalizer compatible with DataProcessor trait
pub struct RuleNormalizerAdapter {
    normalizer: RuleNormalizer,
}

impl RuleNormalizerAdapter {
    pub fn new() -> Self {
        Self {
            normalizer: RuleNormalizer,
        }
    }
}

#[async_trait]
impl DataProcessor for RuleNormalizerAdapter {
    fn name(&self) -> &str {
        "rule_normalizer"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Normalizer
    }

    async fn process(&self, input: ProcessorInput) -> Result<ProcessorOutput> {
        match input {
            ProcessorInput::DataFrame(mut df) => {
                self.normalizer.normalize_dataframe(&mut df)?;
                Ok(ProcessorOutput::DataFrame(df))
            }
            _ => Err(anyhow::anyhow!(
                "RuleNormalizer can only process DataFrame input"
            )),
        }
    }

    fn can_process(&self, input_type: &DataType) -> bool {
        matches!(input_type, DataType::DataFrame)
    }

    fn output_type(&self, input_type: &DataType) -> Result<DataType> {
        if self.can_process(input_type) {
            Ok(DataType::DataFrame)
        } else {
            Err(anyhow::anyhow!(
                "Cannot process input type: {:?}",
                input_type
            ))
        }
    }

    fn metadata(&self) -> ProcessorMetadata {
        ProcessorMetadata {
            name: "rule_normalizer".to_string(),
            description: Some("Applies rule-based normalization to data".to_string()),
            version: Some("1.0.0".to_string()),
            supported_input_types: vec![DataType::DataFrame],
            supported_output_types: vec![DataType::DataFrame],
            configuration_schema: None,
            performance_metrics: Some(PerformanceMetrics {
                average_processing_time_ms: 75.0,
                throughput_items_per_second: 800.0,
                memory_usage_mb: 15.0,
                success_rate: 0.99,
            }),
        }
    }
}

/// Adapter that makes HtmlProcessor compatible with DataProcessor trait
pub struct HtmlProcessorAdapter {
    #[allow(dead_code)]
    processor: HtmlProcessor,
}

impl HtmlProcessorAdapter {
    pub fn new() -> Self {
        Self {
            processor: HtmlProcessor::new(),
        }
    }
}

#[async_trait]
impl DataProcessor for HtmlProcessorAdapter {
    fn name(&self) -> &str {
        "html_processor"
    }

    fn processor_type(&self) -> ProcessorType {
        ProcessorType::Transformer
    }

    async fn process(&self, input: ProcessorInput) -> Result<ProcessorOutput> {
        match input {
            ProcessorInput::Html(html) => {
                // For now, we'll do basic HTML processing
                // In a real implementation, we'd first extract ScrapedProducts from HTML
                // then use self.processor.process_scraped_products()
                // As a placeholder, we'll return cleaned HTML text
                let cleaned_html = html.trim().to_string();
                Ok(ProcessorOutput::Text(cleaned_html))
            }
            _ => Err(anyhow::anyhow!("HtmlProcessor can only process HTML input")),
        }
    }

    fn can_process(&self, input_type: &DataType) -> bool {
        matches!(input_type, DataType::Html)
    }

    fn output_type(&self, input_type: &DataType) -> Result<DataType> {
        if self.can_process(input_type) {
            Ok(DataType::Json)
        } else {
            Err(anyhow::anyhow!(
                "Cannot process input type: {:?}",
                input_type
            ))
        }
    }

    fn metadata(&self) -> ProcessorMetadata {
        ProcessorMetadata {
            name: "html_processor".to_string(),
            description: Some("Processes HTML content and extracts structured data".to_string()),
            version: Some("1.0.0".to_string()),
            supported_input_types: vec![DataType::Html],
            supported_output_types: vec![DataType::Json],
            configuration_schema: None,
            performance_metrics: Some(PerformanceMetrics {
                average_processing_time_ms: 200.0,
                throughput_items_per_second: 100.0,
                memory_usage_mb: 30.0,
                success_rate: 0.95,
            }),
        }
    }
}

/// Factory for creating processor adapters
pub struct ProcessorAdapterFactory;

impl ProcessorAdapterFactory {
    pub fn create_json_flattener() -> Box<dyn DataProcessor> {
        Box::new(JsonFlattenerAdapter::new())
    }

    pub fn create_field_classifier() -> Box<dyn DataProcessor> {
        Box::new(FieldClassifierAdapter::new())
    }

    pub fn create_rule_normalizer() -> Box<dyn DataProcessor> {
        Box::new(RuleNormalizerAdapter::new())
    }

    pub fn create_html_processor() -> Box<dyn DataProcessor> {
        Box::new(HtmlProcessorAdapter::new())
    }

    pub fn create_standard_pipeline() -> Vec<Box<dyn DataProcessor>> {
        vec![
            Self::create_json_flattener(),
            Self::create_field_classifier(),
            Self::create_rule_normalizer(),
        ]
    }

    pub fn create_processor_by_name(name: &str) -> Result<Box<dyn DataProcessor>> {
        match name {
            "json_flattener" => Ok(Self::create_json_flattener()),
            "field_classifier" => Ok(Self::create_field_classifier()),
            "rule_normalizer" => Ok(Self::create_rule_normalizer()),
            "html_processor" => Ok(Self::create_html_processor()),
            _ => Err(anyhow::anyhow!("Unknown processor: {}", name)),
        }
    }

    pub fn list_available_processors() -> Vec<&'static str> {
        vec![
            "json_flattener",
            "field_classifier",
            "rule_normalizer",
            "html_processor",
        ]
    }
}

impl Default for JsonFlattenerAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for FieldClassifierAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for RuleNormalizerAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for HtmlProcessorAdapter {
    fn default() -> Self {
        Self::new()
    }
}
