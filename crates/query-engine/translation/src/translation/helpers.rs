//! Helpers for processing requests and building SQL.

use ndc_models::{self as models};

use super::error::Error;
use query_engine_metadata::metadata;
use query_engine_sql::sql;

#[derive(Debug)]
/// Static information from the query and metadata.
pub struct Env<'request> {
    pub(crate) metadata: &'request metadata::Metadata,
}

#[derive(Debug)]
/// Stateful information changed throughout the translation process.
pub struct State {
    // native_queries: NativeQueries,
    global_table_index: TableAliasIndex,
}

#[derive(Debug)]
/// Used for generating a unique name for intermediate tables.
pub struct TableAliasIndex(pub u64);

/// For the root table in the query, and for the current table we are processing,
/// We'd like to track what is their reference in the query (the name we can use to address them,
/// an alias we generate), and what is their name in the metadata (so we can get
/// their information such as which columns are available for that table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootAndCurrentTables {
    /// The root (top-most) table in the query.
    pub root_table: TableNameAndReference,
    /// The current table we are processing.
    pub current_table: TableNameAndReference,
}

/// For a table in the query, We'd like to track what is its reference in the query
/// (the name we can use to address them, an alias we generate), and what is their name in the
/// metadata (so we can get their information such as which columns are available for that table).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableNameAndReference {
    /// Table name for column lookup
    pub name: models::CollectionName,
    /// Table alias to query from
    pub reference: sql::ast::TableReference,
}

#[derive(Debug)]
/// Information about columns
pub struct ColumnInfo {
    pub name: sql::ast::ColumnName,
    pub r#type: metadata::Type,
}

#[derive(Debug)]
/// Metadata information about a specific collection, i.e. something which can be queried at the
/// top level.
pub enum CollectionInfo<'env> {
    Table {
        name: &'env models::CollectionName,
        info: &'env metadata::TableInfo,
    },
}

#[derive(Debug)]
/// Metadata information about any object that can have fields
pub enum FieldsInfo<'env> {
    Table {
        name: &'env models::CollectionName,
        info: &'env metadata::TableInfo,
    },
}

impl<'a> From<&'a CollectionInfo<'a>> for FieldsInfo<'a> {
    fn from(value: &'a CollectionInfo<'a>) -> Self {
        match value {
            CollectionInfo::Table { name, info } => FieldsInfo::Table { name, info },
        }
    }
}

impl<'request> Env<'request> {
    /// Run a closure with an empty environment.
    /// This should only be used for tests.
    ///
    /// The reason we cannot just construct and return an empty `Env` is that it contains borrowed
    /// data. Therefore we take a continuation instead which can do what it likes with the `Env`.
    /// Both the `Env` and its borrowed data can then be dropped when the continuation returns.
    pub fn with_empty<F, R>(f: F) -> R
    where
        F: FnOnce(Env) -> R,
    {
        let temp_metadata = metadata::Metadata::empty();
        let temp_env = Env {
            metadata: &temp_metadata,
        };
        f(temp_env)
    }

    /// Create a new Env by supplying the metadata and relationships.
    pub fn new(
        metadata: &'request metadata::Metadata,
    ) -> Self {
        Env {
            metadata,
        }
    }

    /// Lookup a metadata object that may contain fields. This may be any of Tables, Native
    /// Queries, and Composite Types.
    ///
    /// This is used to translate field selection, where any of these may occur.
    pub fn lookup_fields_info(
        &self,
        type_name: &'request models::CollectionName,
    ) -> Result<FieldsInfo<'request>, Error> {
        // Lookup the fields of a type name in a specific order:
        // tables, then composite types, then native queries.
        let info = self
            .metadata
            .tables
            .0
            .get(type_name)
            .map(|t| FieldsInfo::Table {
                name: type_name,
                info: t,
            });

        info.ok_or(Error::CollectionNotFound(type_name.as_str().into()))
    }

    /// Lookup a collection's information in the metadata.
    pub fn lookup_collection(
        &self,
        collection_name: &'request models::CollectionName,
    ) -> Result<CollectionInfo<'request>, Error> {
        let table = self
            .metadata
            .tables
            .0
            .get(collection_name.as_str())
            .map(|t| CollectionInfo::Table {
                name: collection_name,
                info: t,
            });

        if let Some(table) = table {
            Ok(table)
        } else {
            Err(Error::CollectionNotFound(collection_name.clone()))
        }
    }

    /// Looks up the binary comparison operator's PostgreSQL name and arguments' type in the metadata.
    pub fn lookup_comparison_operator(
        &self,
        scalar_type: &models::ScalarTypeName,
        name: &models::ComparisonOperatorName,
    ) -> Result<&'request metadata::ComparisonOperator, Error> {
        self.metadata
            .scalar_types
            .0
            .get(scalar_type)
            .and_then(|t| t.comparison_operators.get(name))
            .ok_or(Error::OperatorNotFound {
                operator_name: name.clone(),
                type_name: scalar_type.clone(),
            })
    }

    /// Lookup type representation of a type.
    pub fn lookup_type_representation(
        &self,
        scalar_type: &models::ScalarTypeName
    ) -> Option<&metadata::TypeRepresentation> {
        self.metadata
            .scalar_types
            .0
            .get(scalar_type)
            .and_then(|t| t.type_representation.as_ref())
    }

    /// Lookup a scalar type by its name in the ndc schema.
    pub(crate) fn lookup_scalar_type(
        &self,
        t: &models::ScalarTypeName,
    ) -> Result<&metadata::ScalarType, Error> {
        self.metadata
            .scalar_types
            .0
            .get(t)
            .ok_or(Error::ScalarTypeNotFound(t.clone()))
    }
}

impl FieldsInfo<'_> {
    /// Lookup a column in a collection.
    pub fn lookup_column(&self, column_name: &models::FieldName) -> Result<ColumnInfo, Error> {
        match self {
            FieldsInfo::Table { name, info } => info
                .columns
                .get(column_name.as_str())
                .map(|column_info| ColumnInfo {
                    name: sql::ast::ColumnName(column_info.name.clone()),
                    r#type: column_info.r#type.clone(),
                })
                .ok_or_else(|| {
                    Error::ColumnNotFoundInCollection(column_name.clone(), (*name).clone())
                }),
            // FieldsInfo::NativeQuery { name, info } => info
            //     .columns
            //     .get(column_name)
            //     .map(|column_info| ColumnInfo {
            //         name: sql::ast::ColumnName(column_info.name.clone()),
            //         r#type: column_info.r#type.clone(),
            //     })
            //     .ok_or_else(|| {
            //         Error::ColumnNotFoundInCollection(column_name.clone(), name.as_str().into())
            //     }),
        }
    }
}

impl CollectionInfo<'_> {
    /// Lookup a column in a collection.
    pub fn lookup_column(&self, column_name: &models::FieldName) -> Result<ColumnInfo, Error> {
        FieldsInfo::from(self).lookup_column(column_name)
    }
}

impl Default for State {
    fn default() -> State {
        State {
            // native_queries: NativeQueries::new(),
            global_table_index: TableAliasIndex(0),
        }
    }
}

impl State {
    /// Build a new state.
    pub fn new() -> State {
        State::default()
    }

    // aliases

    /// Create table aliases using this function so they get a unique index.
    pub fn make_table_alias(&mut self, name: String) -> sql::ast::TableAlias {
        self.global_table_index.make_table_alias(name)
    }

    /// Create a table alias for order by target part.
    /// Provide an index and a source table name (to disambiguate the table being queried),
    /// and get an alias.
    pub fn make_order_path_part_table_alias(&mut self, table_name: &str) -> sql::ast::TableAlias {
        self.make_table_alias(format!("ORDER_PART_{table_name}"))
    }

    /// Create a table alias for order by column.
    /// Provide an index and a source table name (to point at the table being ordered),
    /// and get an alias.
    pub fn make_order_by_table_alias(&mut self, source_table_name: &str) -> sql::ast::TableAlias {
        self.make_table_alias(format!("ORDER_FOR_{source_table_name}"))
    }

    pub fn make_native_query_table_alias(&mut self, name: &str) -> sql::ast::TableAlias {
        self.make_table_alias(format!("NATIVE_QUERY_{name}"))
    }

    /// Create a table alias for boolean expressions.
    /// Provide state for fresh names and a source table name (to point at the table
    /// being filtered), and get an alias.
    pub fn make_boolean_expression_table_alias(
        &mut self,
        source_table_name: &str,
    ) -> sql::ast::TableAlias {
        self.make_table_alias(format!("BOOLEXP_{source_table_name}"))
    }
}

impl TableAliasIndex {
    /// increment the table index and return the current one.
    fn next_global_table_index(&mut self) -> TableAliasIndex {
        let index = self.0;
        *self = TableAliasIndex(index + 1);
        TableAliasIndex(index)
    }

    /// Create table aliases using this function so they get a unique index.
    pub fn make_table_alias(&mut self, name: String) -> sql::ast::TableAlias {
        sql::ast::TableAlias {
            unique_index: self.next_global_table_index().0,
            name,
        }
    }
}

/// A newtype wrapper around an ndc-spec type which represents accessing a nested field.
#[derive(Debug, Clone)]
pub struct FieldPath(pub Vec<models::FieldName>);

impl From<&Option<Vec<models::FieldName>>> for FieldPath {
    fn from(field_path: &Option<Vec<models::FieldName>>) -> Self {
        FieldPath(match field_path {
            // The option has no logical function other than to avoid breaking changes.
            None => vec![],
            Some(vec) => vec.clone(),
        })
    }
}

/// Fold an expression inside of a chain of field path accessors.
pub fn wrap_in_field_path(
    field_path: &FieldPath,
    expression: sql::ast::Expression,
) -> sql::ast::Expression {
    field_path.0.iter().fold(expression, |expression, field| {
        sql::ast::Expression::NestedFieldSelect {
            expression: Box::new(expression),
            nested_field: sql::ast::NestedField(field.clone().into()),
        }
    })
}
