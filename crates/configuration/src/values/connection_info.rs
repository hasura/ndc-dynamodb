use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Secret;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct ServiceKey(pub Secret);

impl From<String> for ServiceKey {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for ServiceKey {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}