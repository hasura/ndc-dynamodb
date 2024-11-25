//! Database connection settings.

use crate::values::{Secret, ServiceKey};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_CONNECTION_URI_PLACEHOLDER: &str = "HASURA_DYNAMODB_CONNECTION_URI_PLACEHOLDER";

/// Database connection settings.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSettings {
    /// Connection string for a Postgres-compatible database.
    pub connection_placeholder: ServiceKey,
}

impl DatabaseConnectionSettings {
    pub fn empty() -> Self {
        Self {
            connection_placeholder: ServiceKey(Secret::FromEnvironment {
                variable: DEFAULT_CONNECTION_URI_PLACEHOLDER.into(),
            })
        }
    }
}
