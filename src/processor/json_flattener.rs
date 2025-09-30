use anyhow::{Result, anyhow};
use polars::prelude::*;
use serde_json::Value;

use tracing::{info, warn};

use super::preprocessors::base::{PreprocessorRegistry};
use super::preprocessors::kravemart::KraveMartPreprocessor;
use super::field_extractors::{FieldExtractor, StandardFieldExtractor};
use super::dataframe_builder::DataFrameBuilder;

/// Main JSON flattener that coordinates preprocessing, field extraction, and DataFrame creation
pub struct JsonFlattener {
    preprocessor_registry: PreprocessorRegistry,
    field_extractor: Box<dyn FieldExtractor>,
    dataframe_builder: DataFrameBuilder,
}

impl JsonFlattener {
    pub fn new() -> Self {
        // Set up preprocessor registry with all available preprocessors
        let preprocessor_registry = PreprocessorRegistry::new()
            .register(KraveMartPreprocessor::new());

        // Use standard field extractor
        let field_extractor = Box::new(StandardFieldExtractor::new());

        // Create DataFrame builder
        let dataframe_builder = DataFrameBuilder::new();

        Self {
            preprocessor_registry,
            field_extractor,
            dataframe_builder,
        }
    }

    pub fn flatten_to_dataframe(&self, json_data: &[Value]) -> Result<DataFrame> {
        let mut records = Vec::new();
        let mut successful_count = 0;
        let mut failed_count = 0;

        for (index, item) in json_data.iter().enumerate() {
            // Preprocess item for source-specific transformations
            let processed_item = self.preprocessor_registry.process_item(item)?;

            match self.field_extractor.extract_fields(&processed_item) {
                Ok(record) => {
                    records.push(record);
                    successful_count += 1;
                }
                Err(e) => {
                    failed_count += 1;
                    warn!(
                        "Failed to extract fields from product at index {}: {}",
                        index, e
                    );

                    // Log some details about the failed item
                    if let Some(product_name) = item.get("name").and_then(|v| v.as_str()) {
                        warn!("Failed product name: {}", product_name);
                    }
                    if let Some(product_id) = item.get("product_id") {
                        warn!("Failed product ID: {}", product_id);
                    }
                }
            }
        }

        info!(
            "Field extraction summary: {} successful, {} failed out of {} total",
            successful_count,
            failed_count,
            json_data.len()
        );

        self.dataframe_builder.build_dataframe(records)
    }

    /// Process JSON data in batches and return a combined DataFrame
    /// This is more memory efficient for large datasets

    pub fn flatten_to_dataframe_batched(
        &self,
        batches: impl Iterator<Item = Result<Vec<Value>>>,
    ) -> Result<DataFrame> {
        let mut all_dataframes = Vec::new();
        let mut total_successful = 0;
        let mut total_failed = 0;
        let mut batch_count = 0;

        for batch_result in batches {
            let batch = batch_result?;
            batch_count += 1;

            info!(
                "Processing batch {} with {} items",
                batch_count,
                batch.len()
            );

            let mut records = Vec::new();
            let mut successful_count = 0;
            let mut failed_count = 0;

            for (index, item) in batch.iter().enumerate() {
                // Preprocess item for source-specific transformations
                let processed_item = self.preprocessor_registry.process_item(item)?;

                match self.field_extractor.extract_fields(&processed_item) {
                    Ok(record) => {
                        records.push(record);
                        successful_count += 1;
                    }
                    Err(e) => {
                        failed_count += 1;
                        warn!(
                            "Failed to extract fields from product at batch {} index {}: {}",
                            batch_count, index, e
                        );
                    }
                }
            }

            total_successful += successful_count;
            total_failed += failed_count;

            if !records.is_empty() {
                let batch_df = self.dataframe_builder.build_dataframe(records)?;
                all_dataframes.push(batch_df);
                info!(
                    "Batch {} processed: {} successful, {} failed",
                    batch_count, successful_count, failed_count
                );
            }
        }

        info!(
            "Batched processing complete: {} total successful, {} total failed across {} batches",
            total_successful, total_failed, batch_count
        );

        // Combine all DataFrames
        if all_dataframes.is_empty() {
            Ok(DataFrame::empty())
        } else if all_dataframes.len() == 1 {
            Ok(all_dataframes.into_iter().next().unwrap())
        } else {
            // Concatenate all DataFrames
            let mut iter = all_dataframes.into_iter();
            let mut combined = iter.next().unwrap();
            for df in iter {
                combined = combined
                    .vstack(&df)
                    .map_err(|e| anyhow!("Failed to combine DataFrames: {}", e))?;
            }
            Ok(combined)
        }
    }
}
