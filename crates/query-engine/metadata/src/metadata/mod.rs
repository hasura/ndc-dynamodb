//! Metadata information regarding the database and tracked information.

pub mod database;

// re-export without modules
pub use database::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Metadata information.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub tables: TablesInfo,
    pub scalar_types: ScalarTypes,
}

impl Metadata {
    pub fn empty() -> Self {
        Metadata {
            tables: TablesInfo::empty(),
            scalar_types: ScalarTypes::empty(),
        }
    }
}
