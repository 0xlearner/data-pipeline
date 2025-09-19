use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    pub cost_price: Option<f64>,
    pub mrp: Option<f64>,
    pub name: String,
    pub sku_percent_off: Option<String>,
    pub category_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawApiResponse {
    pub data: Vec<ApiResponseData>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponseData {
    pub l2_products: Vec<KraveMartProduct>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KraveMartProduct {
    pub store_id: u32,
    pub special_price: Option<String>,
    pub product_price: Option<String>,
    pub product_display_order: Option<u32>,
    pub maximum_order_quantity: Option<u32>,
    pub sku: Option<String>,
    pub default_image: Option<String>,
    pub is_enabled: Option<u32>,
    pub meta_keywords: Option<String>,
    pub images: Option<Vec<serde_json::Value>>,
    pub categories: Option<Vec<Category>>,
    pub inventories: Option<Inventory>,
    pub sku_promotion_text: Option<String>,
    pub video_youtube_link: Option<String>,
    pub sticker_image_link: Option<String>,
    pub search_boost: Option<String>,
    pub display_in_store: Option<u32>,
    pub sku_percent_off: Option<String>,
    pub product_id: u32,
    pub name: String,
    pub description: Option<String>,
    pub store_type: Option<String>,
    pub deals: Option<String>,
    pub mrp: Option<f64>,
    pub cost_price: Option<f64>,
    pub search_no_space: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Category {
    pub store_id: u32,
    pub category_name: String,
    pub category_id: u32,
    pub product_id: u32,
    pub parent_category: ParentCategory,
    pub cat_search_elastic: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ParentCategory {
    pub parent_name: String,
    pub parent_id: u32,
    pub id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Inventory {
    pub sku: String,
    pub store_id: u32,
    pub quantity: u32,
}
