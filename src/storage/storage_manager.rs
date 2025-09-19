use chrono::Utc;
use uuid::Uuid;

#[allow(dead_code)]
pub struct StorageManager;

impl StorageManager {
    #[allow(dead_code)]
    pub fn generate_raw_path(api_name: &str) -> String {
        let date = Utc::now().format("%Y/%m/%d").to_string();
        let file_id = Uuid::new_v4();
        format!("raw/{}/{}/{}.json", api_name, date, file_id)
    }

    #[allow(dead_code)]
    pub fn generate_clean_path(api_name: &str) -> String {
        let date = Utc::now().format("%Y/%m/%d").to_string();
        format!("clean/{}/{}/data.parquet", api_name, date)
    }
}
