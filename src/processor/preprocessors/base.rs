use anyhow::Result;
use serde_json::Value;

/// Trait for source-specific data preprocessing before field extraction
pub trait Preprocessor: Send + Sync {
    /// Check if this preprocessor can handle the given data item
    fn can_process(&self, item: &Value) -> bool;

    /// Process the data item with source-specific transformations
    fn process(&self, item: &Value) -> Result<Value>;

    /// Get the name of this preprocessor for logging
    fn name(&self) -> &'static str;
}

/// Registry for managing multiple preprocessors
pub struct PreprocessorRegistry {
    preprocessors: Vec<Box<dyn Preprocessor>>,
}

impl PreprocessorRegistry {
    pub fn new() -> Self {
        Self {
            preprocessors: Vec::new(),
        }
    }
    
    /// Register a new preprocessor
    pub fn register<P: Preprocessor + 'static>(mut self, preprocessor: P) -> Self {
        self.preprocessors.push(Box::new(preprocessor));
        self
    }
    
    /// Process an item using the first matching preprocessor
    pub fn process_item(&self, item: &Value) -> Result<Value> {
        for preprocessor in &self.preprocessors {
            if preprocessor.can_process(item) {
                return preprocessor.process(item);
            }
        }
        
        // No preprocessor matched, return item as-is
        Ok(item.clone())
    }
}

impl Default for PreprocessorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
