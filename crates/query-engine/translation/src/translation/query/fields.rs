//! Handle 'rows' and 'aggregates' translation.

use indexmap::IndexMap;

use ndc_models as models;

use crate::translation::error::Error;
use crate::translation::error::UnsupportedCapabilities;
use crate::translation::helpers::FieldsInfo;
use crate::translation::helpers::{Env, State, TableNameAndReference};
use query_engine_metadata::metadata::{Type, TypeRepresentation};
use query_engine_sql::sql;

/// Translate the field-selection of a query to SQL.
/// Because field selection may be nested this function is mutually recursive with
/// 'translate_nested_field'.
pub(crate) fn translate_fields(
    env: &Env,
    _state: &mut State,
    fields: IndexMap<models::FieldName, models::Field>,
    current_table: &TableNameAndReference,
    from: sql::ast::From,
) -> Result<sql::ast::Select, Error> {
    // find the table according to the metadata.
    let fields_info = env.lookup_fields_info(&current_table.name)?;

    let columns: Vec<(sql::ast::ColumnAlias, sql::ast::Expression)> = fields
        .into_iter()
        .map(|(alias, field)| match field {
            models::Field::Column {
                column,
                fields: None,
                arguments,
            } if arguments.is_empty() => unpack_and_wrap_fields(
                env,
                current_table,
                &column,
                sql::helpers::make_column_alias(alias.to_string()),
                &fields_info,
            ),
            models::Field::Column {
                column: _,
                fields: _,
                arguments: _,
            } => Err(Error::CapabilityNotSupported(
                UnsupportedCapabilities::FieldArguments,
            )),
            ndc_models::Field::Relationship { .. } => todo!(),
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let mut select = sql::helpers::simple_select(columns);

    select.from = Some(from);

    Ok(select)
}

#[allow(clippy::too_many_arguments)]
/// In order to return the expected type representation for each column,
/// we need to wrap columns in type representation cast, and unpack composite types
/// so we can wrap them.
fn unpack_and_wrap_fields(
    env: &Env,
    current_table: &TableNameAndReference,

    column: &models::FieldName,
    alias: sql::ast::ColumnAlias,
    fields_info: &FieldsInfo<'_>,
) -> Result<(sql::ast::ColumnAlias, sql::ast::Expression), Error> {
    let column_info = fields_info.lookup_column(column)?;

    // Different kinds of types have different strategy for converting to their
    // type representation.
    match column_info.r#type {
        // Scalar types can just be wrapped in a cast.
        Type::ScalarType(scalar_type) => {
            let column_type_representation = env.lookup_type_representation(&scalar_type);
            let (alias, expression) = sql::helpers::make_column(
                current_table.reference.clone(),
                column_info.name.clone(),
                alias,
            );
            Ok((
                alias,
                wrap_in_type_representation(expression, column_type_representation),
            ))
        }
        Type::ArrayType(ref type_boxed) => match **type_boxed {
            Type::ArrayType(_) => Err(Error::NestedArraysNotSupported {
                field_name: column.clone(),
            }),
            Type::ScalarType(ref scalar_type) => {
                let inner_column_type_representation = env.lookup_type_representation(scalar_type);
                let (alias, expression) = sql::helpers::make_column(
                    current_table.reference.clone(),
                    column_info.name.clone(),
                    alias,
                );
                Ok((
                    alias,
                    wrap_array_in_type_representation(expression, inner_column_type_representation),
                ))
            }
        },
    }
}

/// Certain type representations require that we provide a different json representation
/// than what postgres will return.
/// For array columns of those type representation, we wrap the result in a cast.
fn wrap_array_in_type_representation(
    expression: sql::ast::Expression,
    column_type_representation: Option<&TypeRepresentation>,
) -> sql::ast::Expression {
    match column_type_representation {
        None => expression,
        Some(type_rep) => {
            if let Some(cast_type) = get_type_representation_cast_type(type_rep) {
                sql::ast::Expression::Cast {
                    expression: Box::new(expression),
                    // make it an array of cast type
                    r#type: sql::ast::ScalarType::ArrayType(cast_type),
                }
            } else {
                expression
            }
        }
    }
}

/// Certain type representations require that we provide a different json representation
/// than what postgres will return.
/// For columns of those type representation, we wrap the result in a cast.
fn wrap_in_type_representation(
    expression: sql::ast::Expression,
    column_type_representation: Option<&TypeRepresentation>,
) -> sql::ast::Expression {
    match column_type_representation {
        None => expression,
        Some(type_rep) => {
            if let Some(cast_type) = get_type_representation_cast_type(type_rep) {
                sql::ast::Expression::Cast {
                    expression: Box::new(expression),
                    r#type: sql::ast::ScalarType::BaseType(cast_type),
                }
            } else {
                expression
            }
        }
    }
}

/// If a type representation requires a cast, return the scalar type name.
fn get_type_representation_cast_type(
    type_representation: &TypeRepresentation,
) -> Option<sql::ast::ScalarTypeName> {
    match type_representation {
        // In these situations, we expect to cast the expression according
        // to the type representation.
        TypeRepresentation::Int64AsString | TypeRepresentation::BigDecimalAsString => {
            Some(sql::helpers::text_type_name())
        }

        // In these situations the type representation should be the same as
        // the expression, so we don't cast it.
        TypeRepresentation::Boolean
        | TypeRepresentation::String
        | TypeRepresentation::Float32
        | TypeRepresentation::Float64
        | TypeRepresentation::Int16
        | TypeRepresentation::Int32
        | TypeRepresentation::Int64
        | TypeRepresentation::BigDecimal
        | TypeRepresentation::Timestamp
        | TypeRepresentation::Timestamptz
        | TypeRepresentation::Time
        | TypeRepresentation::Timetz
        | TypeRepresentation::Date
        | TypeRepresentation::UUID
        | TypeRepresentation::Geography
        | TypeRepresentation::Geometry
        | TypeRepresentation::Json
        | TypeRepresentation::Enum(_) => None,
    }
}
