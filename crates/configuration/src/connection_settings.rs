//! Database connection settings.

use crate::values::{connection_info::Url, AccessKeyId, Region, Secret, SecretAccessKey};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_ACCESS_KEY_ID_VARIABLE: &str = "HASURA_DYNAMODB_AWS_ACCESS_KEY_ID";
pub const DEFAULT_SECRET_ACCESS_KEY_VARIABLE: &str = "HASURA_DYNAMODB_AWS_SECRET_ACCESS_KEY";
pub const DEFAULT_URL_VARIABLE: &str = "HASURA_DYNAMODB_URL";
pub const DEFAULT_REGION_VARIABLE: &str = "HASURA_DYNAMODB_AWS_REGION";

/// Database connection settings.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSettings {
    pub access_key_id: AccessKeyId,
    pub secret_access_key: SecretAccessKey,
    pub url: Url,
    pub region: Region,
}

impl DatabaseConnectionSettings {
    pub fn empty() -> Self {
        Self {
            access_key_id: AccessKeyId(Secret::FromEnvironment {
                variable: DEFAULT_ACCESS_KEY_ID_VARIABLE.into(),
            }),
            secret_access_key: SecretAccessKey(Secret::FromEnvironment {
                variable: DEFAULT_SECRET_ACCESS_KEY_VARIABLE.into(),
            }),
            url: Url(Secret::FromEnvironment {
                variable: DEFAULT_URL_VARIABLE.into(),
            }),
            region: Region(Secret::FromEnvironment {
                variable: DEFAULT_REGION_VARIABLE.into(),
            }),
        }
    }
}
