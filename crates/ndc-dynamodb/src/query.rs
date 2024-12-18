//! Implement the `/query` endpoint to run a query against postgres.
//! See the Hasura
//! [Native Data Connector Specification](https://hasura.github.io/ndc-spec/specification/queries/index.html)
//! for further details.

use tracing::{info_span, Instrument};

// pub use explain::explain;
use ndc_sdk::connector;
use ndc_sdk::json_response::JsonResponse;
use ndc_sdk::models;
use query_engine_sql::sql;
use query_engine_translation::translation;

// use super::configuration;
use ndc_dynamodb_configuration as configuration;

use crate::error::convert;
use crate::error::record;
use crate::state;

/// Execute a query
///
/// This function implements the [query endpoint](https://hasura.github.io/ndc-spec/specification/queries/index.html)
/// from the NDC specification.
pub async fn query(
    configuration: &configuration::Configuration,
    state: &state::State,
    query_request: models::QueryRequest,
) -> Result<JsonResponse<models::QueryResponse>, connector::ErrorResponse> {
    // let timer = state.query_metrics.time_query_total();

    // See https://docs.rs/tracing/0.1.29/tracing/span/struct.Span.html#in-asynchronous-code
    async move {
        tracing::info!(
            query_request_json = serde_json::to_string(&query_request).unwrap(),
            query_request = ?query_request
        );

        let plan = async {
            plan_query(configuration, state, query_request).map_err(|err| {
                record::translation_error(&err, &state.metrics);
                convert::translation_error_to_response(&err)
            })
        }
        .instrument(info_span!("Plan query"))
        .await?;

        let result = async {
            execute_query(state, plan).await.map_err(|err| {
                record::execution_error(&err, &state.metrics);
                convert::execution_error_to_response(err)
            })
        }
        .instrument(info_span!("Execute query"))
        .await?;

        state.metrics.record_successful_query();
        Ok(result)
    }
    .instrument(info_span!("/query"))
    .await

    // timer.complete_with(result)
}

fn plan_query(
    configuration: &configuration::Configuration,
    state: &state::State,
    query_request: models::QueryRequest,
) -> Result<sql::execution_plan::ExecutionPlan<sql::execution_plan::Query>, translation::error::Error>
{
    let timer = state.metrics.time_query_plan();
    let result = translation::query::translate(&configuration.metadata, query_request);
    timer.complete_with(result)
}

async fn execute_query(
    state: &state::State,
    plan: sql::execution_plan::ExecutionPlan<sql::execution_plan::Query>,
) -> Result<JsonResponse<models::QueryResponse>, query_engine_execution::error::Error> {
    let timer = state.metrics.time_query_execution();
    let result = query_engine_execution::query::execute(&state.client, &state.metrics, plan)
        .await
        .map(JsonResponse::Serialized);
    timer.complete_with(result)
}
