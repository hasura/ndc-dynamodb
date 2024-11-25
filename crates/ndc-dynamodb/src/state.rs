//! Transient state used by the connector.
//!
//! This is initialized on startup.

use aws_config::Region;
use aws_sdk_dynamodb::Config;
use thiserror::Error;
use tracing::{info_span, Instrument};

// use ndc_bigquery_configuration::PoolSettings;
// use ndc_bigquery_configuration::ConfigurationError;
// use super::configuration::{Configuration, ConfigurationError};
// use query_engine_execution::database_info::{self, DatabaseInfo, DatabaseVersion};
use query_engine_execution::metrics;

/// State for our connector.
#[derive(Clone)]
pub struct State {
    pub metrics: metrics::Metrics,
    pub client: aws_sdk_dynamodb::Client,
    // pub project_id: String,
    // pub dataset_id: String,
}

/// Create a connection pool and wrap it inside a connector State.
pub async fn create_state(
    configuration: &ndc_dynamodb_configuration::Configuration,
    metrics_registry: &mut prometheus::Registry,
) -> Result<State, InitializationError> {
    let metrics = async {
        let metrics_inner = metrics::Metrics::initialize(metrics_registry)
            .map_err(InitializationError::MetricsError)?;
        Ok(metrics_inner)
    }
    .instrument(info_span!("Setup metrics"))
    .await?;

    let access_key_id = configuration.access_key_id.clone();
    let secret_access_key = configuration.secret_access_key.clone();
    let region = configuration.region.clone();

    let credentials = aws_sdk_dynamodb::config::Credentials::new(
        access_key_id.to_string(),
        secret_access_key.to_string(),
        None,           // Optional session token
        None,           // Expiration (None for non-expiring)
        "my-provider",  // Provider name
    );

    let config = Config::builder()
        .region(aws_config::Region::new(region.to_string()))
        .credentials_provider(credentials)
        .build();

    // let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    //     .test_credentials()
    //     .region(Region::new("us-west-2"))
    //     // DynamoDB run locally uses port 8000 by default.
    //     .endpoint_url("http://localhost:8085")
    //     .load()
    //     .await;
    // let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config).build();
    let client = aws_sdk_dynamodb::Client::from_conf(config);

    // let service_account_key =
    //     yup_oauth2::parse_service_account_key(configuration.service_key.clone()).unwrap();

    // let config = aws_config::load_from_env().await;
    // let client = aws_sdk_dynamodb::Client::new(&config);

    // // Init BigQuery client
    // let client =
    //     aws_sdk_dynamodb::Client::from_service_account_key(service_account_key, false)
    //         .await
    //         .unwrap();

    Ok(State {
        metrics,
        client,
    })
}

/// State initialization error.
#[derive(Debug, Error)]
pub enum InitializationError {
    #[error("unable to initialize connection pool: {0}")]
    UnableToCreatePool(sqlx::Error),
    #[error("error initializing metrics: {0}")]
    MetricsError(prometheus::Error),
}
