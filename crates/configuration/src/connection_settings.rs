//! Database connection settings.

use crate::values::{Secret, AccessKeyId, SecretAccessKey, Region};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_ACCESS_KEY_ID_VARIABLE: &str = "HASURA_DYNAMODB_AWS_ACCESS_KEY_ID";
pub const DEFAULT_SECRET_ACCESS_KEY_VARIABLE: &str = "HASURA_DYNAMODB_AWS_SECRET_ACCESS_KEY";
pub const DEFAULT_PROVIDER_NAME: &str = "HASURA_DYNAMODB_AWS_PROVIDER_NAME";
pub const DEFAULT_REGION_VARIABLE: &str = "HASURA_DYNAMODB_AWS_REGION";

/// Database connection settings.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseConnectionSettings {
    pub access_key_id: AccessKeyId,
    pub secret_access_key: SecretAccessKey,
    // pub provider_name: ProviderName,
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
            // provider_name: ProviderName(Secret::FromEnvironment {
            //     variable: DEFAULT_PROVIDER_NAME.into(),
            // }),
            region: Region(Secret::FromEnvironment {
                variable: DEFAULT_REGION_VARIABLE.into(),
            }),
        }
    }
}
