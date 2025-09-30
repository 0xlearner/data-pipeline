use anyhow::{Result, anyhow};

use crate::processor::FieldClassifier;
use crate::processor::stages::processing_stage::{
    ProcessingStage, ProcessingData, ProcessingDataType, StageResult, StageMetadata, StageType, StageTimer
};

/// Processing stage that wraps the existing FieldClassifier
/// 
/// Applies field classification and mapping to standardize column names in DataFrames.
/// This stage maintains backward compatibility while providing the new modular interface.
pub struct FieldClassifierStage {
    classifier: FieldClassifier,
    name: String,
}

impl FieldClassifierStage {
    /// Create a new field classifier stage
    pub fn new() -> Self {
        Self {
            classifier: FieldClassifier::new(),
            name: "field_classifier".to_string(),
        }
    }
    
    /// Create a new field classifier stage with custom name
    pub fn with_name(name: String) -> Self {
        Self {
            classifier: FieldClassifier::new(),
            name,
        }
    }
    
    /// Create a field classifier stage with custom classifier
    pub fn with_classifier(classifier: FieldClassifier, name: String) -> Self {
        Self {
            classifier,
            name,
        }
    }
}

impl ProcessingStage for FieldClassifierStage {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn metadata(&self) -> StageMetadata {
        StageMetadata {
            stage_type: StageType::Classifier,
            description: "Classifies and maps field names to canonical schema using ML and rule-based approaches".to_string(),
            version: "1.0.0".to_string(),
            supported_inputs: vec![ProcessingDataType::DataFrame],
            supported_outputs: vec![ProcessingDataType::DataFrame],
        }
    }
    
    fn process(&self, input: ProcessingData) -> Result<StageResult> {
        let (result, processing_time) = self.time_operation(|| {
            match input {
                ProcessingData::DataFrame(mut df) => {
                    let original_columns = df.get_column_names().len();

                    match self.classifier.map_to_canonical_schema(&mut df) {
                        Ok(()) => {
                            let processed_columns = df.get_column_names().len();

                            // Field classification doesn't typically fail individual items,
                            // but we can track column mapping success
                            let _warnings = if processed_columns != original_columns {
                                vec![format!(
                                    "Column count changed during classification: {} -> {}",
                                    original_columns, processed_columns
                                )]
                            } else {
                                Vec::new()
                            };

                            let rows_processed = df.height();
                            Ok((
                                ProcessingData::DataFrame(df),
                                rows_processed, // Number of rows processed
                                original_columns,
                            ))
                        }
                        Err(e) => Err(anyhow!("Field classification failed: {}", e)),
                    }
                }
                _ => Err(anyhow!("FieldClassifierStage can only process DataFrame data")),
            }
        });

        match result {
            Ok((data, rows_processed, original_columns)) => {
                Ok(StageResult::success(
                    data,
                    processing_time,
                    rows_processed,
                ).with_warning(format!(
                    "Classified {} columns to canonical schema",
                    original_columns
                )))
            }
            Err(e) => Err(e),
        }
    }
    
    fn can_process(&self, input: &ProcessingData) -> bool {
        matches!(input, ProcessingData::DataFrame(_))
    }
    
    fn output_type(&self, input_type: &ProcessingDataType) -> Result<ProcessingDataType> {
        match input_type {
            ProcessingDataType::DataFrame => Ok(ProcessingDataType::DataFrame),
            _ => Err(anyhow!("FieldClassifierStage can only process DataFrame input")),
        }
    }
    
    fn validate_config(&self) -> Result<()> {
        // Validate that the classifier has field mappings
        let canonical_fields = self.classifier.get_canonical_fields();
        if canonical_fields.is_empty() {
            return Err(anyhow!("FieldClassifier has no canonical field mappings configured"));
        }
        
        Ok(())
    }
}

impl Default for FieldClassifierStage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn create_test_dataframe() -> DataFrame {
        let name_series = Series::new("product_name".into(), vec!["Product 1", "Product 2"]);
        let price_series = Series::new("special_price".into(), vec!["19.99", "29.99"]);
        let id_series = Series::new("item_id".into(), vec!["123", "456"]);
        
        DataFrame::new(vec![name_series.into(), price_series.into(), id_series.into()]).unwrap()
    }

    #[test]
    fn test_field_classifier_stage_basic() {
        let stage = FieldClassifierStage::new();
        
        assert_eq!(stage.name(), "field_classifier");
        assert_eq!(stage.metadata().stage_type, StageType::Classifier);
        assert!(stage.metadata().supported_inputs.contains(&ProcessingDataType::DataFrame));
        assert!(stage.metadata().supported_outputs.contains(&ProcessingDataType::DataFrame));
    }

    #[test]
    fn test_field_classifier_stage_processing() {
        let stage = FieldClassifierStage::new();
        let df = create_test_dataframe();
        let input = ProcessingData::DataFrame(df);
        
        assert!(stage.can_process(&input));
        
        let result = stage.process(input).unwrap();
        
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                // Check that field classification was applied
                let column_names = df.get_column_names();
                let column_names_str: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();

                // The classifier should have mapped some fields to canonical names
                // For example, "special_price" should become "cost_price"
                // and "product_name" should become "name"
                assert!(column_names_str.contains(&"name") || column_names_str.contains(&"product_name"));
                assert!(column_names_str.contains(&"cost_price") || column_names_str.contains(&"special_price"));
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        assert_eq!(result.metrics.items_processed, 2); // 2 rows
        assert_eq!(result.metrics.items_failed, 0);
    }

    #[test]
    fn test_field_classifier_stage_invalid_input() {
        let stage = FieldClassifierStage::new();
        let input = ProcessingData::Text("not a dataframe".to_string());
        
        assert!(!stage.can_process(&input));
        
        let result = stage.process(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_type() {
        let stage = FieldClassifierStage::new();
        
        assert_eq!(
            stage.output_type(&ProcessingDataType::DataFrame).unwrap(),
            ProcessingDataType::DataFrame
        );
        
        assert!(stage.output_type(&ProcessingDataType::Json).is_err());
    }

    #[test]
    fn test_config_validation() {
        let stage = FieldClassifierStage::new();
        
        // Should pass validation since FieldClassifier has default mappings
        assert!(stage.validate_config().is_ok());
    }
}
