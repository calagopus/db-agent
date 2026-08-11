use serde::Serialize;
use utoipa::ToSchema;

pub mod reader;

#[derive(Debug, Clone, Copy, ToSchema, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[schema(rename_all = "snake_case")]
pub enum CompressionType {
    #[default]
    None,
    Gz,
}

impl CompressionType {
    pub fn from_file_name(file_name: &str) -> Self {
        if file_name.ends_with(".gz") {
            CompressionType::Gz
        } else {
            CompressionType::None
        }
    }
}
