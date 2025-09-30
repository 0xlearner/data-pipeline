use anyhow::{Result, anyhow};
use polars::prelude::*;
use std::collections::HashMap;
use tracing::info;

/// Builder for creating DataFrames from extracted field records
pub struct DataFrameBuilder;

impl DataFrameBuilder {
    pub fn new() -> Self {
        Self
    }
    
    /// Convert field records to a Polars DataFrame
    pub fn build_dataframe(&self, records: Vec<HashMap<String, String>>) -> Result<DataFrame> {
        if records.is_empty() {
            return Err(anyhow!("Cannot create DataFrame from empty records"));
        }

        // Collect all unique field names across all records
        let mut all_fields = std::collections::HashSet::new();
        for record in &records {
            for key in record.keys() {
                all_fields.insert(key.clone());
            }
        }

        let mut all_fields: Vec<String> = all_fields.into_iter().collect();
        all_fields.sort(); // Sort for consistent column ordering

        info!("Creating DataFrame with {} columns: {:?}", all_fields.len(), all_fields);

        // Create columns for the DataFrame
        let mut columns = Vec::new();
        for field in &all_fields {
            let values: Vec<Option<String>> = records
                .iter()
                .map(|record| record.get(field).cloned())
                .collect();

            let series = Series::new(field.into(), values);
            columns.push(series.into());
        }

        DataFrame::new(columns).map_err(|e| anyhow!("Failed to create DataFrame: {}", e))
    }
    
    /// Build DataFrame with batched processing for memory efficiency
    pub fn build_dataframe_batched(
        &self,
        record_batches: Vec<Vec<HashMap<String, String>>>,
    ) -> Result<DataFrame> {
        if record_batches.is_empty() {
            return Err(anyhow!("Cannot create DataFrame from empty record batches"));
        }

        let mut dataframes = Vec::new();
        for (batch_index, batch) in record_batches.into_iter().enumerate() {
            if !batch.is_empty() {
                info!("Processing batch {} with {} records", batch_index, batch.len());
                let df = self.build_dataframe(batch)?;
                dataframes.push(df);
            }
        }

        if dataframes.is_empty() {
            return Err(anyhow!("No valid batches to combine"));
        }

        // Combine all DataFrames
        let mut iter = dataframes.into_iter();
        let mut combined = iter.next().unwrap();
        for df in iter {
            combined = combined
                .vstack(&df)
                .map_err(|e| anyhow!("Failed to combine DataFrames: {}", e))?;
        }

        info!("Combined DataFrame has {} rows", combined.height());
        Ok(combined)
    }
}

impl Default for DataFrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}
