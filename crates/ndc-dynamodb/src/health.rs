//! Health check endpoint for the connector.

use ndc_sdk::connector::ErrorResponse;

use aws_sdk_dynamodb::Client;

/// Check the health of the connector.
///
/// For example, this function should check that the connector
/// is able to reach its data source over the network.
/// TODO
pub async fn health_check(
    client: &Client,
) -> Result<(), ErrorResponse> {
    // Query
    // let mut rs = client
    //     .job()
    //     .query(
    //         project_id,
    //         QueryRequest::new("SELECT 1 as count".to_string()),
    //     )
    //     .await
    //     .unwrap();

    let tables_result = client.list_tables().send().await;
    let tables = tables_result.map_err(|_op| {
        ndc_dynamodb_configuration::error::ParseConfigurationError::IoErrorButStringified(format!(
            "Failed to list tables"
        ))
    }); //TODO: handle error

    match tables {
        Ok(_res) => {
            Ok(())
        }
        Err(_e) => {
            Err(ErrorResponse::new_internal_with_details(serde_json::Value::Null))
        }
    }

    // // silly check
    // let mut count = 0;

    // while rs.next_row() {
    //     count += 1;
    // }

    // assert_eq!(count, 1);

    // Ok(())
}
