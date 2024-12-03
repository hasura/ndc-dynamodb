//! Implement the `/schema` endpoint to return the connector's schema.
//! See the Hasura
//! [Native Data Connector Specification](https://hasura.github.io/ndc-spec/specification/schema/index.html)
//! for further details.

use std::collections::BTreeMap;

use ndc_sdk::connector;
use ndc_sdk::models;
use query_engine_metadata::metadata;
use query_engine_metadata::metadata::OperatorKind;

use ndc_dynamodb_configuration::configuration;

/// Get the connector's schema.
///
/// This function implements the [schema endpoint](https://hasura.github.io/ndc-spec/specification/schema/index.html)
/// from the NDC specification.
pub fn get_schema(
    configuration: &configuration::Configuration,
) -> Result<models::SchemaResponse, connector::ErrorResponse> {
    let metadata = &configuration.metadata;
    let scalar_types: BTreeMap<models::ScalarTypeName, models::ScalarType> = metadata
        .scalar_types
        .0
        .iter()
        .map(|(scalar_type_name, scalar_type_info)| {
            let result = models::ScalarType {
                representation: scalar_type_info
                    .type_representation
                    .as_ref()
                    .map(map_type_representation),
                aggregate_functions: BTreeMap::new(),
                comparison_operators: scalar_type_info
                    .comparison_operators
                    .iter()
                    .map(|(op_name, op_def)| {
                        (
                            op_name.clone(),
                            match op_def.operator_kind {
                                OperatorKind::Equal => models::ComparisonOperatorDefinition::Equal,
                                OperatorKind::In => models::ComparisonOperatorDefinition::In,
                                OperatorKind::Custom => {
                                    models::ComparisonOperatorDefinition::Custom {
                                        argument_type: models::Type::Named {
                                            name: op_def.argument_type.as_str().into(),
                                        },
                                    }
                                }
                            },
                        )
                    })
                    .collect(),
            };
            (scalar_type_name.clone(), result)
        })
        .collect();

    let collections = metadata
        .tables
        .0
        .iter()
        .map(|(table_name, table)| models::CollectionInfo {
            name: table_name.clone(),
            description: table.description.clone(),
            arguments: BTreeMap::new(),
            collection_type: table_name.as_str().into(),
            uniqueness_constraints: BTreeMap::new(),
            foreign_keys: BTreeMap::new(),
        })
        .collect();

    let object_types = metadata //BTreeMap::from_iter(metadata.tables.0.iter().map(|(table_name, table)| {
        .tables
        .0
        .iter()
        .map(|(table_name, table)| {
            let object_type = models::ObjectType {
                description: table.description.clone(),
                fields: table
                    .columns
                    .iter()
                    .map(|(column_name, column_info)| {
                        (
                            column_name.clone(),
                            models::ObjectField {
                                description: column_info.description.clone(),
                                r#type: column_to_type(column_info),
                                arguments: BTreeMap::new(),
                            },
                        )
                    })
                    .collect(),
            };
            (table_name.as_str().into(), object_type)
        })
        .collect::<BTreeMap<_, _>>();

    Ok(models::SchemaResponse {
        collections,
        procedures: vec![],
        functions: vec![],
        object_types,
        scalar_types,
    })
}

/// Map our local type representation to ndc-spec type representation.
#[allow(clippy::match_same_arms)] // merging arms would require changing the order, making this harder to understand
fn map_type_representation(
    type_representation: &metadata::TypeRepresentation,
) -> models::TypeRepresentation {
    match type_representation {
        metadata::TypeRepresentation::Boolean => models::TypeRepresentation::Boolean,
        metadata::TypeRepresentation::String => models::TypeRepresentation::String,
        metadata::TypeRepresentation::Float32 => models::TypeRepresentation::Float32,
        metadata::TypeRepresentation::Float64 => models::TypeRepresentation::Float64,
        metadata::TypeRepresentation::Int16 => models::TypeRepresentation::Int16,
        metadata::TypeRepresentation::Int32 => models::TypeRepresentation::Int32,
        // Int64 returns a number.
        metadata::TypeRepresentation::Int64 => models::TypeRepresentation::JSON,
        // Int64AsString returns a string.
        metadata::TypeRepresentation::Int64AsString => models::TypeRepresentation::Int64,
        // BigDecimal returns a number.
        metadata::TypeRepresentation::BigDecimal => models::TypeRepresentation::JSON,
        // BigDecimalAsString returns a string.
        metadata::TypeRepresentation::BigDecimalAsString => models::TypeRepresentation::BigDecimal,
        metadata::TypeRepresentation::Timestamp => models::TypeRepresentation::Timestamp,
        metadata::TypeRepresentation::Timestamptz => models::TypeRepresentation::TimestampTZ,
        metadata::TypeRepresentation::Time => models::TypeRepresentation::String,
        metadata::TypeRepresentation::Timetz => models::TypeRepresentation::String,
        metadata::TypeRepresentation::Date => models::TypeRepresentation::Date,
        metadata::TypeRepresentation::Geometry => models::TypeRepresentation::Geometry,
        metadata::TypeRepresentation::Geography => models::TypeRepresentation::Geography,
        metadata::TypeRepresentation::UUID => models::TypeRepresentation::UUID,
        metadata::TypeRepresentation::Json => models::TypeRepresentation::JSON,
        metadata::TypeRepresentation::Enum(variants) => models::TypeRepresentation::Enum {
            one_of: variants.clone(),
        },
    }
}

// ! Helper functions for generating ndc-spec schema objects.

// use ndc_sdk::models;

// use query_engine_metadata::metadata;

/// Extract the models::Type representation of a column.
pub fn column_to_type(column: &metadata::ColumnInfo) -> models::Type {
    match &column.nullable {
        metadata::Nullable::NonNullable => type_to_type(&column.r#type),
        metadata::Nullable::Nullable => models::Type::Nullable {
            underlying_type: Box::new(type_to_type(&column.r#type)),
        },
    }
}

// /// Extract the models::Type representation of a readonly column.
// pub fn readonly_column_to_type(column: &metadata::ReadOnlyColumnInfo) -> models::Type {
//     match &column.nullable {
//         metadata::Nullable::NonNullable => type_to_type(&column.r#type),
//         metadata::Nullable::Nullable => models::Type::Nullable {
//             underlying_type: Box::new(type_to_type(&column.r#type)),
//         },
//     }
// }

pub fn type_to_type(typ: &metadata::Type) -> models::Type {
    match typ {
        metadata::Type::ArrayType(typ) => models::Type::Array {
            element_type: Box::new(type_to_type(typ)),
        },
        metadata::Type::ScalarType(scalar_type) => models::Type::Named {
            name: scalar_type.as_str().into(),
        },
    }
}
