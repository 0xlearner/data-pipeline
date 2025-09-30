use anyhow::{Result, anyhow};

use crate::processor::JsonFlattener;
use crate::processor::stages::processing_stage::{
    ProcessingStage, ProcessingData, ProcessingDataType, StageResult, StageMetadata, StageType, StageTimer
};

/// Processing stage that wraps the existing JsonFlattener
/// 
/// Converts JSON data to DataFrame format using the existing JsonFlattener logic.
/// This stage maintains backward compatibility while providing the new modular interface.
pub struct JsonFlattenerStage {
    flattener: JsonFlattener,
    name: String,
}

impl JsonFlattenerStage {
    /// Create a new JSON flattener stage
    pub fn new() -> Self {
        Self {
            flattener: JsonFlattener::new(),
            name: "json_flattener".to_string(),
        }
    }
    
    /// Create a new JSON flattener stage with custom name
    pub fn with_name(name: String) -> Self {
        Self {
            flattener: JsonFlattener::new(),
            name,
        }
    }
}

impl ProcessingStage for JsonFlattenerStage {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn metadata(&self) -> StageMetadata {
        StageMetadata {
            stage_type: StageType::Transformer,
            description: "Converts JSON data to DataFrame format with field extraction and validation".to_string(),
            version: "1.0.0".to_string(),
            supported_inputs: vec![ProcessingDataType::Json],
            supported_outputs: vec![ProcessingDataType::DataFrame],
        }
    }
    
    fn process(&self, input: ProcessingData) -> Result<StageResult> {
        let (result, processing_time) = self.time_operation(|| {
            match input {
                ProcessingData::Json(json_data) => {
                    let original_count = json_data.len();

                    match self.flattener.flatten_to_dataframe(&json_data) {
                        Ok(df) => {
                            let processed_count = df.height();
                            let failed_count = original_count.saturating_sub(processed_count);

                            let mut warnings = Vec::new();
                            if failed_count > 0 {
                                warnings.push(format!(
                                    "Failed to process {} out of {} items during JSON flattening",
                                    failed_count, original_count
                                ));
                            }

                            Ok((
                                ProcessingData::DataFrame(df),
                                processed_count,
                                failed_count,
                                warnings,
                            ))
                        }
                        Err(e) => Err(anyhow!("JSON flattening failed: {}", e)),
                    }
                }
                _ => Err(anyhow!("JsonFlattenerStage can only process JSON data")),
            }
        });

        match result {
            Ok((data, processed_count, failed_count, warnings)) => {
                Ok(StageResult::partial_success(
                    data,
                    processing_time,
                    processed_count,
                    failed_count,
                    warnings,
                ))
            }
            Err(e) => Err(e),
        }
    }
    
    fn can_process(&self, input: &ProcessingData) -> bool {
        matches!(input, ProcessingData::Json(_))
    }
    
    fn output_type(&self, input_type: &ProcessingDataType) -> Result<ProcessingDataType> {
        match input_type {
            ProcessingDataType::Json => Ok(ProcessingDataType::DataFrame),
            _ => Err(anyhow!("JsonFlattenerStage can only process JSON input")),
        }
    }
    
    fn validate_config(&self) -> Result<()> {
        // JsonFlattener doesn't have configuration to validate
        Ok(())
    }
}

impl Default for JsonFlattenerStage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_flattener_stage_basic() {
        let stage = JsonFlattenerStage::new();
        
        assert_eq!(stage.name(), "json_flattener");
        assert_eq!(stage.metadata().stage_type, StageType::Transformer);
        assert!(stage.metadata().supported_inputs.contains(&ProcessingDataType::Json));
        assert!(stage.metadata().supported_outputs.contains(&ProcessingDataType::DataFrame));
    }

    #[test]
    fn test_json_flattener_stage_processing() {
        let stage = JsonFlattenerStage::new();
        let json_data = vec![
            json!({
                "name": "Test Product",
                "cost_price": "19.99",
                "mrp": "29.99",
                "sku": "TEST123",
                "product_id": "123",
                "category_name": "Electronics",
                "units_of_mass": "1kg",
                "sku_percent_off": "33%"
            })
        ];
        let input = ProcessingData::Json(json_data);
        
        assert!(stage.can_process(&input));
        
        let result = stage.process(input).unwrap();
        
        match result.data {
            ProcessingData::DataFrame(df) => {
                assert_eq!(df.height(), 1);
                assert!(df.column("name").is_ok());
                assert!(df.column("cost_price").is_ok());
                assert!(df.column("mrp").is_ok());
            }
            _ => panic!("Expected DataFrame output"),
        }
        
        assert_eq!(result.metrics.items_processed, 1);
        assert_eq!(result.metrics.items_failed, 0);
    }

    #[test]
    fn test_json_flattener_stage_invalid_input() {
        let stage = JsonFlattenerStage::new();
        let input = ProcessingData::Text("not json".to_string());
        
        assert!(!stage.can_process(&input));
        
        let result = stage.process(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_type() {
        let stage = JsonFlattenerStage::new();
        
        assert_eq!(
            stage.output_type(&ProcessingDataType::Json).unwrap(),
            ProcessingDataType::DataFrame
        );
        
        assert!(stage.output_type(&ProcessingDataType::Text).is_err());
    }
}
