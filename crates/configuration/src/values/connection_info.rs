use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::Secret;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct AccessKeyId(pub Secret);

impl From<String> for AccessKeyId {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for AccessKeyId {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct SecretAccessKey(pub Secret);

impl From<String> for SecretAccessKey {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for SecretAccessKey {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct ProviderName(pub Secret);

impl From<String> for ProviderName {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for ProviderName {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct Region(pub Secret);

impl From<String> for Region {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<&str> for Region {
    fn from(value: &str) -> Self {
        Self::from(value.to_string())
    }
}