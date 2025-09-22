use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    pub cost_price: Option<f64>,
    pub mrp: Option<f64>,
    pub name: String,
    pub sku_percent_off: Option<String>,
    pub category_name: String,
}

// BazaarApp specific models
#[derive(Debug, Serialize, Deserialize)]
pub struct BazaarAppProduct {
    #[serde(rename = "variantTitleSlug")]
    pub variant_title_slug: String,
    pub id: String,
    pub title: String,
    pub vendor: String,
    #[serde(rename = "variantId")]
    pub variant_id: String,
    pub sku: String,
    pub description: String,
    #[serde(rename = "actualPrice")]
    pub actual_price: u32,
    #[serde(rename = "discountedPrice")]
    pub discounted_price: u32,
    #[serde(rename = "retailPrice")]
    pub retail_price: u32,
    #[serde(rename = "uomSale")]
    pub uom_sale: String,
    #[serde(rename = "imageUrl")]
    pub image_url: String,
    #[serde(rename = "brandId")]
    pub brand_id: String,
    #[serde(rename = "inventoryWarehouseId")]
    pub inventory_warehouse_id: String,
    #[serde(rename = "availableStock")]
    pub available_stock: u32,
    #[serde(rename = "inventoryStatus")]
    pub inventory_status: String,
    pub category: String,
    #[serde(rename = "categoryId")]
    pub category_id: String,
    #[serde(rename = "promoId")]
    pub promo_id: String,
    #[serde(rename = "cartonSize")]
    pub carton_size: u32,
    #[serde(rename = "perBundleTotalQuantity")]
    pub per_bundle_total_quantity: u32,
    pub tag: String,
    #[serde(rename = "mediaGallery")]
    pub media_gallery: Vec<MediaGalleryItem>,
    #[serde(rename = "categoryStatus")]
    pub category_status: bool,
    #[serde(rename = "variantZoneId")]
    pub variant_zone_id: String,
    #[serde(rename = "productRatingScore")]
    pub product_rating_score: f64,
    #[serde(rename = "productRatingCount")]
    pub product_rating_count: u32,
    #[serde(rename = "coreCategoryId")]
    pub core_category_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MediaGalleryItem {
    #[serde(rename = "imageUrl")]
    pub image_url: String,
    #[serde(rename = "sortingOrder")]
    pub sorting_order: u32,
}

// BazaarApp POST request models
#[allow(dead_code)]
#[derive(Serialize, Debug)]
pub struct PaginationRequestDTO {
    pub page: i32,
    pub size: i32,
}

#[allow(dead_code)]
#[derive(Serialize, Debug)]
pub struct BazaarAppProductRequest {
    #[serde(rename = "productChannel")]
    pub product_channel: String,
    #[serde(rename = "paginationRequestDTO")]
    pub pagination_request_dto: PaginationRequestDTO,
    #[serde(rename = "searchKey")]
    pub search_key: String,
    #[serde(rename = "brandIds")]
    pub brand_ids: Vec<String>,
    #[serde(rename = "coreCategorySlug")]
    pub core_category_slug: String,
    #[serde(rename = "subcategorySlugs")]
    pub subcategory_slugs: Vec<String>,
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
