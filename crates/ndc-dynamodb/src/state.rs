//! Transient state used by the connector.
//!
//! This is initialized on startup.

use aws_sdk_dynamodb::Config;
use thiserror::Error;
use tracing::{info_span, Instrument};

use query_engine_execution::metrics;

/// State for our connector.
#[derive(Clone)]
pub struct State {
    pub metrics: metrics::Metrics,
    pub client: aws_sdk_dynamodb::Client,
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
        access_key_id,
        secret_access_key,
        None,          // Optional session token
        None,          // Expiration (None for non-expiring)
        "my-provider", // Provider name
    );

    let config = Config::builder()
        .region(aws_config::Region::new(region))
        .credentials_provider(credentials)
        .behavior_version_latest()
        .build();

    let client = aws_sdk_dynamodb::Client::from_conf(config);

    Ok(State { metrics, client })
}

/// State initialization error.
#[derive(Debug, Error)]
pub enum InitializationError {
    #[error("unable to initialize connection pool: {0}")]
    UnableToCreatePool(sqlx::Error),
    #[error("error initializing metrics: {0}")]
    MetricsError(prometheus::Error),
}
