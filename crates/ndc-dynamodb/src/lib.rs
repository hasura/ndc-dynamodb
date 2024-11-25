//! A Hasura v3 DynamoDB Data Connector.

pub mod capabilities;
pub mod connector;
pub mod error;
pub mod health;
pub mod query;
pub mod schema;
pub mod state;

// we expose the sdk used for this connector so that we are able to use it in multitenant
pub use ndc_sdk as sdk;
