use anyhow::Result;
use polars::prelude::*;
use regex::Regex;
use std::str::FromStr;

pub struct RuleNormalizer;

impl RuleNormalizer {
    pub fn normalize_dataframe(&self, df: &mut DataFrame) -> Result<()> {
        // Normalize price columns
        self.normalize_price_column(df, "cost_price")?;
        self.normalize_price_column(df, "mrp")?;

        // Normalize name and extract units
        self.normalize_name_and_extract_units(df)?;

        // Normalize other string columns
        if df.column("category").is_ok() {
            self.normalize_string_column(df, "category")?;
        }

        // Normalize discount column (after field classification it's called "discount")
        if df.column("discount").is_ok() {
            self.normalize_discount_column(df, "discount")?;
        }

        // Calculate missing discounts from price difference
        self.calculate_missing_discounts(df)?;

        Ok(())
    }

    fn normalize_name_and_extract_units(&self, df: &mut DataFrame) -> Result<()> {
        let name_series = df.column("name")?.str()?;

        let mut units = Vec::with_capacity(name_series.len());
        let mut cleaned_names = Vec::with_capacity(name_series.len());

        // Enhanced regex patterns for better unit extraction (order matters - most specific first)
        let unit_patterns = vec![
            // Parenthetical weight/volume units: (800gm), (1 Kg), (500ml), etc.
            Regex::new(r"(?i)\s*[-–]?\s*\(\s*(\d+(?:\.\d+)?\s*(?:gm|g|kg|ml|l|gram|grams|kilogram|kilograms|liter|liters|milliliter|milliliters)(?:\s*-\s*\d+(?:\.\d+)?\s*(?:gm|g|kg|ml|l|gram|grams|kilogram|kilograms|liter|liters|milliliter|milliliters))?)\s*\)")?,
            // Parenthetical count/pack units: (pack of 6), (1 piece), (1 bundles), etc.
            Regex::new(r"(?i)\s*[-–]?\s*\(\s*(pack\s+of\s+\d+|\d+\s+(?:piece|pieces|bundle|bundles|dozen|half\s+dozen))\s*\)")?,
            // Dash-separated count units: - 1 piece, - 1 bundles, - half dozen, etc.
            Regex::new(r"(?i)\s*[-–]\s*(pack\s+of\s+\d+|\d+\s+(?:piece|pieces|bundle|bundles|dozen)|half\s+dozen)\s*")?,
            // Dash-separated weight/volume units: - 800gm, - 1 kg, etc.
            Regex::new(r"(?i)\s*[-–]\s*(\d+(?:\.\d+)?\s*(?:gm|g|kg|ml|l|gram|grams|kilogram|kilograms|liter|liters|milliliter|milliliters))\s*")?,
            // Space-separated units at end: 3 Kg, 1 kg, etc.
            Regex::new(r"(?i)\s+(\d+(?:\.\d+)?\s*(?:gm|g|kg|ml|l|gram|grams|kilogram|kilograms|liter|liters|milliliter|milliliters))\s*$")?,
        ];

        // Regex for removing promotional text and extra info
        let promo_regex = Regex::new(r"\s*\|\s*.*$")?;

        // Regex for cleaning parenthetical descriptions (like translations)
        let description_regex = Regex::new(r"\s*\(\s*(aalu|pyaaz|kheera|sabzi|dal|atta|masala|spice|powder|paste|sauce|pickle|jam|honey|sugar|salt|tea|coffee|milk|butter|cheese|paneer|curd|yogurt|bread|biscuit|cake|sweet|namkeen|snack|chips|noodles|pasta|soup|juice|water|cold drink|soda|[a-zA-Z\s]+)\s*\)")?;

        for name_opt in name_series.into_iter() {
            if let Some(name) = name_opt {
                let mut unit_found = "N/A".to_string();
                let mut cleaned_name = name.to_string();

                // Remove promotional text first
                cleaned_name = promo_regex.replace(&cleaned_name, "").to_string();

                // Try to extract units using different patterns
                for pattern in &unit_patterns {
                    if let Some(captures) = pattern.captures(&cleaned_name) {
                        if let Some(unit_match) = captures.get(1) {
                            unit_found = unit_match.as_str().trim().to_string();
                            cleaned_name = pattern.replace(&cleaned_name, "").to_string();
                            break;
                        }
                    }
                }

                // Remove descriptive parentheses (like translations) - but only if no units were found in them
                if unit_found == "N/A" {
                    cleaned_name = description_regex.replace(&cleaned_name, "").to_string();
                } else {
                    // If units were found, only remove non-unit parentheses
                    let non_unit_desc_regex = Regex::new(r"\s*\(\s*(aalu|pyaaz|kheera|sabzi|dal|atta|masala|spice|powder|paste|sauce|pickle|jam|honey|sugar|salt|tea|coffee|milk|butter|cheese|paneer|curd|yogurt|bread|biscuit|cake|sweet|namkeen|snack|chips|noodles|pasta|soup|juice|water|cold drink|soda|[a-zA-Z\s]*[a-zA-Z])\s*\)")?;
                    cleaned_name = non_unit_desc_regex.replace(&cleaned_name, "").to_string();
                }

                // Clean up extra spaces and normalize
                cleaned_name = cleaned_name
                    .trim()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_lowercase();

                units.push(unit_found);
                cleaned_names.push(cleaned_name);
            } else {
                units.push("N/A".to_string());
                cleaned_names.push("".to_string());
            }
        }

        let units_series = Series::new("units_of_mass".into(), units);
        let cleaned_names_series = Series::new("name".into(), cleaned_names);

        df.with_column(cleaned_names_series)?;
        df.with_column(units_series)?;

        Ok(())
    }

    fn normalize_price_column(&self, df: &mut DataFrame, col_name: &str) -> Result<()> {
        if let Ok(series) = df.column(col_name).cloned() {
            let normalized: Vec<Option<f64>> = series
                .str()
                .unwrap()
                .into_no_null_iter()
                .map(|s| {
                    let cleaned = s.replace("$", "").replace(",", "");
                    let trimmed = cleaned.trim();
                    f64::from_str(trimmed).ok()
                })
                .collect();

            let new_series = Series::new(col_name.into(), normalized);
            df.with_column(new_series)?;
        }

        Ok(())
    }

    fn normalize_string_column(&self, df: &mut DataFrame, col_name: &str) -> Result<()> {
        if let Ok(series) = df.column(col_name).cloned() {
            let normalized: Vec<String> = series
                .str()
                .unwrap()
                .into_no_null_iter()
                .map(|s| s.trim().to_lowercase())
                .collect();

            let new_series = Series::new(col_name.into(), normalized);
            df.with_column(new_series)?;
        }

        Ok(())
    }

    fn normalize_discount_column(&self, df: &mut DataFrame, col_name: &str) -> Result<()> {
        if let Ok(series) = df.column(col_name).cloned() {
            let normalized: Vec<Option<f64>> = series
                .str()
                .unwrap()
                .into_no_null_iter()
                .map(|s| {
                    // Handle various discount formats: "40% off", "25%", "30 percent off", etc.
                    let cleaned = s
                        .to_lowercase()
                        .replace("%", "")
                        .replace("percent", "")
                        .replace("off", "")
                        .replace("discount", "")
                        .replace("sale", "")
                        .trim()
                        .to_string();

                    // Extract the first number found
                    let re = Regex::new(r"(\d+(?:\.\d+)?)").unwrap();
                    if let Some(captures) = re.captures(&cleaned) {
                        if let Some(number_match) = captures.get(1) {
                            return f64::from_str(number_match.as_str()).ok();
                        }
                    }

                    // Fallback: try to parse the whole cleaned string
                    f64::from_str(&cleaned).ok()
                })
                .collect();

            let new_series = Series::new(col_name.into(), normalized);
            df.with_column(new_series)?;
        }

        Ok(())
    }

    fn calculate_missing_discounts(&self, df: &mut DataFrame) -> Result<()> {
        // Only proceed if we have the required columns
        if let (Ok(cost_price_col), Ok(mrp_col), Ok(discount_col)) =
            (df.column("cost_price"), df.column("mrp"), df.column("discount")) {

            let cost_prices = cost_price_col.f64()?;
            let mrps = mrp_col.f64()?;
            let discounts = discount_col.f64()?;

            let calculated_discounts: Vec<Option<f64>> = discounts
                .into_iter()
                .zip(cost_prices.into_iter())
                .zip(mrps.into_iter())
                .map(|((existing_discount, cost_opt), mrp_opt)| {
                    // If discount already exists and is valid, keep it
                    if let Some(discount_val) = existing_discount {
                        if !discount_val.is_nan() {
                            return Some(discount_val);
                        }
                    }

                    // Calculate discount from price difference
                    if let (Some(cost), Some(mrp)) = (cost_opt, mrp_opt) {
                        if mrp > 0.0 && cost < mrp {
                            // Discount percentage = ((MRP - Cost Price) / MRP) * 100
                            let discount_percentage = ((mrp - cost) / mrp) * 100.0;
                            Some((discount_percentage * 100.0).round() / 100.0) // Round to 2 decimal places
                        } else {
                            Some(0.0) // No discount if cost >= mrp
                        }
                    } else {
                        None // Missing price data
                    }
                })
                .collect();

            let new_discount_series = Series::new("discount".into(), calculated_discounts);
            df.with_column(new_discount_series)?;
        }

        Ok(())
    }
}
