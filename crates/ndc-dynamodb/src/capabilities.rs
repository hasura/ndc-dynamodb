//! `/capabilities` endpoint for the connector.

use ndc_sdk::models;

/// Get the connector's capabilities.
///
/// This function implements the [capabilities endpoint](https://hasura.github.io/ndc-spec/specification/capabilities.html)
/// from the NDC specification.
pub fn get_capabilities() -> models::Capabilities {
    models::Capabilities {
        query: models::QueryCapabilities {
            aggregates: Some(models::LeafCapability {}),
            variables: None,
            explain: None,
            exists: models::ExistsCapabilities {
                nested_collections: None,
            },
            nested_fields: models::NestedFieldCapabilities {
                filter_by: None,
                order_by: None,
                aggregates: None,
            },
        },
        mutation: models::MutationCapabilities {
            transactional: None,
            explain: None,
        },
        relationships: None,
    }
}
