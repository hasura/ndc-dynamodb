//! Convert the parsed configuration metadata to internal engine metadata
//! That can be used by the connector at runtime.

use super::version1::ParsedConfiguration;
use crate::environment::{Environment, Variable};
use crate::error::MakeRuntimeConfigurationError;
use crate::values::{AccessKeyId, Region, Secret, SecretAccessKey};
use query_engine_metadata::{self, metadata};
// use crate::VersionTag;

/// Convert the parsed configuration metadata to internal engine metadata
/// That can be used by the connector at runtime.
pub fn make_runtime_configuration(
    parsed_config: ParsedConfiguration,
    environment: impl Environment,
) -> Result<crate::Configuration, MakeRuntimeConfigurationError> {
    let access_key_id = match parsed_config.connection_settings.access_key_id {
        AccessKeyId(Secret::Plain(key)) => Ok(key),
        AccessKeyId(Secret::FromEnvironment { variable }) => {
            environment.read(&variable).map_err(|error| {
                MakeRuntimeConfigurationError::MissingEnvironmentVariable {
                    file_path: super::version1::CONFIGURATION_FILENAME.into(),
                    message: error.to_string(),
                }
            })
        }
    }?;
    let secret_access_key = match parsed_config.connection_settings.secret_access_key {
        SecretAccessKey(Secret::Plain(key)) => Ok(key),
        SecretAccessKey(Secret::FromEnvironment { variable }) => {
            environment.read(&variable).map_err(|error| {
                MakeRuntimeConfigurationError::MissingEnvironmentVariable {
                    file_path: super::version1::CONFIGURATION_FILENAME.into(),
                    message: error.to_string(),
                }
            })
        }
    }?;
    let region = match parsed_config.connection_settings.region {
        Region(Secret::Plain(key)) => Ok(key),
        Region(Secret::FromEnvironment { variable }) => {
            environment.read(&variable).map_err(|error| {
                MakeRuntimeConfigurationError::MissingEnvironmentVariable {
                    file_path: super::version1::CONFIGURATION_FILENAME.into(),
                    message: error.to_string(),
                }
            })
        }
    }?;

    let url = environment
        .read(&Variable::from("HASURA_DYNAMODB_URL"))
        .ok();

    Ok(crate::Configuration {
        metadata: convert_metadata(parsed_config.metadata),
        access_key_id,
        secret_access_key,
        url,
        region,
    })
}

/// Convert the metadata specified in the parsed configuration to an engine metadata.
/// This function is used by tests as well
pub fn convert_metadata(metadata: metadata::Metadata) -> query_engine_metadata::metadata::Metadata {
    query_engine_metadata::metadata::Metadata {
        tables: convert_tables(metadata.tables),
        scalar_types: convert_scalar_types(metadata.scalar_types),
    }
}

fn convert_scalar_types(
    scalar_types: metadata::ScalarTypes,
) -> query_engine_metadata::metadata::ScalarTypes {
    query_engine_metadata::metadata::ScalarTypes(
        scalar_types
            .0
            .into_iter()
            .map(|(scalar_type_name, scalar_type)| {
                (
                    scalar_type_name,
                    query_engine_metadata::metadata::ScalarType {
                        type_name: scalar_type.type_name,
                        // schema_name: (scalar_type.schema_name),
                        description: scalar_type.description,
                        comparison_operators: scalar_type
                            .comparison_operators
                            .into_iter()
                            .map(|(k, v)| (k, convert_comparison_operator(v)))
                            .collect(),
                        type_representation: scalar_type
                            .type_representation
                            .map(convert_type_representation),
                    },
                )
            })
            .collect(),
    )
}

fn convert_nullable(nullable: &metadata::Nullable) -> query_engine_metadata::metadata::Nullable {
    match nullable {
        metadata::Nullable::Nullable => query_engine_metadata::metadata::Nullable::Nullable,
        metadata::Nullable::NonNullable => query_engine_metadata::metadata::Nullable::NonNullable,
    }
}

fn convert_type(r#type: metadata::Type) -> query_engine_metadata::metadata::Type {
    match r#type {
        metadata::Type::ScalarType(t) => query_engine_metadata::metadata::Type::ScalarType(t),
        metadata::Type::ArrayType(t) => {
            query_engine_metadata::metadata::Type::ArrayType(Box::new(convert_type(*t)))
        }
    }
}

fn convert_type_representation(
    type_representation: metadata::TypeRepresentation,
) -> query_engine_metadata::metadata::TypeRepresentation {
    match type_representation {
        metadata::TypeRepresentation::Boolean => {
            query_engine_metadata::metadata::TypeRepresentation::Boolean
        }
        metadata::TypeRepresentation::String => {
            query_engine_metadata::metadata::TypeRepresentation::String
        }
        metadata::TypeRepresentation::Float32 => {
            query_engine_metadata::metadata::TypeRepresentation::Float32
        }
        metadata::TypeRepresentation::Float64 => {
            query_engine_metadata::metadata::TypeRepresentation::Float64
        }
        metadata::TypeRepresentation::Int16 => {
            query_engine_metadata::metadata::TypeRepresentation::Int16
        }
        metadata::TypeRepresentation::Int32 => {
            query_engine_metadata::metadata::TypeRepresentation::Int32
        }
        metadata::TypeRepresentation::Int64 => {
            query_engine_metadata::metadata::TypeRepresentation::Int64
        }
        metadata::TypeRepresentation::Int64AsString => {
            query_engine_metadata::metadata::TypeRepresentation::Int64AsString
        }
        metadata::TypeRepresentation::BigDecimal => {
            query_engine_metadata::metadata::TypeRepresentation::BigDecimal
        }
        metadata::TypeRepresentation::BigDecimalAsString => {
            query_engine_metadata::metadata::TypeRepresentation::BigDecimalAsString
        }
        metadata::TypeRepresentation::Timestamp => {
            query_engine_metadata::metadata::TypeRepresentation::Timestamp
        }
        metadata::TypeRepresentation::Timestamptz => {
            query_engine_metadata::metadata::TypeRepresentation::Timestamptz
        }
        metadata::TypeRepresentation::Time => {
            query_engine_metadata::metadata::TypeRepresentation::Time
        }
        metadata::TypeRepresentation::Timetz => {
            query_engine_metadata::metadata::TypeRepresentation::Timetz
        }
        metadata::TypeRepresentation::Date => {
            query_engine_metadata::metadata::TypeRepresentation::Date
        }
        metadata::TypeRepresentation::UUID => {
            query_engine_metadata::metadata::TypeRepresentation::UUID
        }
        metadata::TypeRepresentation::Geography => {
            query_engine_metadata::metadata::TypeRepresentation::Geography
        }
        metadata::TypeRepresentation::Geometry => {
            query_engine_metadata::metadata::TypeRepresentation::Geometry
        }
        // This is deprecated in ndc-spec
        // TODO(PY): do we want to include number and integer?
        // metadata::TypeRepresentation::Number
        // | metadata::TypeRepresentation::Integer
        metadata::TypeRepresentation::Json => {
            query_engine_metadata::metadata::TypeRepresentation::Json
        }
        metadata::TypeRepresentation::Enum(v) => {
            query_engine_metadata::metadata::TypeRepresentation::Enum(v)
        }
    }
}

fn convert_comparison_operator(
    comparison_operator: metadata::ComparisonOperator,
) -> query_engine_metadata::metadata::ComparisonOperator {
    query_engine_metadata::metadata::ComparisonOperator {
        operator_name: comparison_operator.operator_name,
        operator_kind: convert_operator_kind(&comparison_operator.operator_kind),
        argument_type: comparison_operator.argument_type,
        is_infix: comparison_operator.is_infix,
    }
}

fn convert_operator_kind(
    operator_kind: &metadata::OperatorKind,
) -> query_engine_metadata::metadata::OperatorKind {
    match operator_kind {
        metadata::OperatorKind::Equal => query_engine_metadata::metadata::OperatorKind::Equal,
        metadata::OperatorKind::In => query_engine_metadata::metadata::OperatorKind::In,
        metadata::OperatorKind::Custom => query_engine_metadata::metadata::OperatorKind::Custom,
    }
}

// fn convert_composite_types(
//     composite_types: metadata::CompositeTypes,
// ) -> query_engine_metadata::metadata::CompositeTypes {
//     query_engine_metadata::metadata::CompositeTypes(
//         composite_types
//             .0
//             .into_iter()
//             .map(|(k, composite_type)| (k, convert_composite_type(composite_type)))
//             .collect(),
//     )
// }

// fn convert_composite_type(
//     composite_type: metadata::CompositeType,
// ) -> query_engine_metadata::metadata::CompositeType {
//     query_engine_metadata::metadata::CompositeType {
//         type_name: composite_type.type_name,
//         schema_name: (composite_type.schema_name),
//         fields: composite_type
//             .fields
//             .into_iter()
//             .map(|(k, field)| (k, convert_composite_type_field_info(field)))
//             .collect(),
//         description: composite_type.description,
//     }
// }

// fn convert_composite_type_field_info(
//     field: metadata::FieldInfo,
// ) -> query_engine_metadata::metadata::FieldInfo {
//     query_engine_metadata::metadata::FieldInfo {
//         field_name: field.field_name,
//         r#type: convert_type(field.r#type),
//         description: field.description,
//     }
// }

pub fn convert_tables(tables: metadata::TablesInfo) -> query_engine_metadata::metadata::TablesInfo {
    query_engine_metadata::metadata::TablesInfo(
        tables
            .0
            .into_iter()
            .map(|(k, table_info)| (k, convert_table_info(table_info)))
            .collect(),
    )
}

fn convert_table_info(
    table_info: metadata::TableInfo,
) -> query_engine_metadata::metadata::TableInfo {
    query_engine_metadata::metadata::TableInfo {
        // schema_name: table_info.schema_name,
        table_name: table_info.table_name,
        columns: table_info
            .columns
            .into_iter()
            .map(|(k, column_info)| (k, convert_column_info(column_info)))
            .collect(),
        // uniqueness_constraints: (table_info.uniqueness_constraints),
        // foreign_relations: convert_foreign_relations(table_info.foreign_relations),
        description: table_info.description,
        partition_key: table_info.partition_key,
        sort_key: table_info.sort_key,
        gsi: table_info.gsi,
    }
}

fn convert_column_info(
    column_info: metadata::ColumnInfo,
) -> query_engine_metadata::metadata::ColumnInfo {
    query_engine_metadata::metadata::ColumnInfo {
        name: column_info.name,
        r#type: convert_type(column_info.r#type),
        nullable: convert_nullable(&column_info.nullable),
        // has_default: convert_has_default(&column_info.has_default),
        // is_identity: convert_is_identity(&column_info.is_identity),
        // is_generated: convert_is_generated(&column_info.is_generated),
        description: column_info.description,
    }
}
