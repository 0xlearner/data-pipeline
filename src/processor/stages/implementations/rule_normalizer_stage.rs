use anyhow::{Result, anyhow};

use crate::processor::RuleNormalizer;
use crate::processor::stages::processing_stage::{
    ProcessingStage, ProcessingData, ProcessingDataType, StageResult, StageMetadata, StageType, StageTimer
};

/// Processing stage that wraps the existing RuleNormalizer
/// 
/// Applies rule-based normalization to clean and standardize data in DataFrames.
/// This stage maintains backward compatibility while providing the new modular interface.
pub struct RuleNormalizerStage {
    normalizer: RuleNormalizer,
    name: String,
}

impl RuleNormalizerStage {
    /// Create a new rule normalizer stage
    pub fn new() -> Self {
        Self {
            normalizer: RuleNormalizer,
            name: "rule_normalizer".to_string(),
        }
    }
    
    /// Create a new rule normalizer stage with custom name
    pub fn with_name(name: String) -> Self {
        Self {
            normalizer: RuleNormalizer,
            name,
        }
    }
}

impl ProcessingStage for RuleNormalizerStage {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn metadata(&self) -> StageMetadata {
        StageMetadata {
            stage_type: StageType::Normalizer,
            description: "Applies rule-based normalization including price cleaning, name standardization, and unit extraction".to_string(),
            version: "1.0.0".to_string(),
            supported_inputs: vec![ProcessingDataType::DataFrame],
            supported_outputs: vec![ProcessingDataType::DataFrame],
        }
    }
    
    fn process(&self, input: ProcessingData) -> Result<StageResult> {
        let (result, processing_time) = self.time_operation(|| {
            match input {
                ProcessingData::DataFrame(mut df) => {
                    let original_rows = df.height();
                    let original_columns = df.get_column_names().len();

                    match self.normalizer.normalize_dataframe(&mut df) {
                        Ok(()) => {
                            let processed_rows = df.height();
                            let processed_columns = df.get_column_names().len();

                            let mut warnings = Vec::new();

                            // Check if normalization changed the data structure
                            if processed_rows != original_rows {
                                warnings.push(format!(
                                    "Row count changed during normalization: {} -> {}",
                                    original_rows, processed_rows
                                ));
                            }

                            if processed_columns != original_columns {
                                warnings.push(format!(
                                    "Column count changed during normalization: {} -> {}",
                                    original_columns, processed_columns
                                ));
                            }

                            // Rule normalization typically doesn't fail individual items,
                            // but we can track any data quality issues
                            let failed_items = original_rows.saturating_sub(processed_rows);

                            Ok((
                                ProcessingData::DataFrame(df),
                                processed_rows,
                                failed_items,
                                warnings,
                            ))
                        }
                        Err(e) => Err(anyhow!("Rule normalization failed: {}", e)),
                    }
                }
                _ => Err(anyhow!("RuleNormalizerStage can only process DataFrame data")),
            }
        });

        match result {
            Ok((data, processed_rows, failed_items, warnings)) => {
                Ok(if failed_items > 0 {
                    StageResult::partial_success(
                        data,
                        processing_time,
                        processed_rows,
                        failed_items,
                        warnings,
                    )
                } else {
                    StageResult::success(
                        data,
                        processing_time,
                        processed_rows,
                    )
                })
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
            _ => Err(anyhow!("RuleNormalizerStage can only process DataFrame input")),
        }
    }
    
    fn validate_config(&self) -> Result<()> {
        // RuleNormalizer doesn't have configuration to validate
        // All normalization rules are hardcoded
        Ok(())
    }
}

impl Default for RuleNormalizerStage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    fn create_test_dataframe() -> DataFrame {
        let name_series = Series::new("name".into(), vec!["Product 1 (500g)", "Product 2 1kg"]);
        let cost_price_series = Series::new("cost_price".into(), vec!["$19.99", "₹1,234.50"]);
        let mrp_series = Series::new("mrp".into(), vec!["$29.99", "₹1,500.00"]);
        let category_series = Series::new("category".into(), vec!["  Electronics  ", "FOOD & BEVERAGES"]);
        
        DataFrame::new(vec![
            name_series.into(),
            cost_price_series.into(),
            mrp_series.into(),
            category_series.into(),
        ]).unwrap()
    }

    #[test]
    fn test_rule_normalizer_stage_basic() {
        let stage = RuleNormalizerStage::new();
        
        assert_eq!(stage.name(), "rule_normalizer");
        assert_eq!(stage.metadata().stage_type, StageType::Normalizer);
        assert!(stage.metadata().supported_inputs.contains(&ProcessingDataType::DataFrame));
        assert!(stage.metadata().supported_outputs.contains(&ProcessingDataType::DataFrame));
    }

    #[test]
    fn test_rule_normalizer_stage_processing() {
        let stage = RuleNormalizerStage::new();
        let df = create_test_dataframe();
        let input = ProcessingData::DataFrame(df);
        
        assert!(stage.can_process(&input));
        
        let result = stage.process(input).unwrap();
        
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 2);
                
                // Check that normalization was applied
                let column_names = df.get_column_names();
                let column_names_str: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();
                assert!(column_names_str.contains(&"name"));
                assert!(column_names_str.contains(&"cost_price"));
                assert!(column_names_str.contains(&"mrp"));

                // Check that units_of_mass column was added
                assert!(column_names_str.contains(&"units_of_mass"));
                
                // Verify price normalization (should be numeric now)
                if let Ok(cost_price_col) = df.column("cost_price") {
                    // After normalization, prices should be numeric
                    assert!(cost_price_col.dtype().is_numeric());
                }
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        assert_eq!(result.metrics.items_processed, 2); // 2 rows
        assert_eq!(result.metrics.items_failed, 0);
    }

    #[test]
    fn test_rule_normalizer_stage_invalid_input() {
        let stage = RuleNormalizerStage::new();
        let input = ProcessingData::Json(vec![]);
        
        assert!(!stage.can_process(&input));
        
        let result = stage.process(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_type() {
        let stage = RuleNormalizerStage::new();
        
        assert_eq!(
            stage.output_type(&ProcessingDataType::DataFrame).unwrap(),
            ProcessingDataType::DataFrame
        );
        
        assert!(stage.output_type(&ProcessingDataType::Json).is_err());
    }

    #[test]
    fn test_config_validation() {
        let stage = RuleNormalizerStage::new();
        
        // Should pass validation since RuleNormalizer doesn't require configuration
        assert!(stage.validate_config().is_ok());
    }
}
