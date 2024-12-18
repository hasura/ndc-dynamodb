//! Metadata information regarding the database and tracked information.

use models::ScalarTypeName;
use ndc_models as models;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The scalar types supported by the Engine.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalarTypeTypeName(pub String);

/// The type of values that a column, field, or argument may take.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    ScalarType(models::ScalarTypeName),
    ArrayType(Box<Type>),
}

/// Map of all known/occurring scalar types.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalarTypes(pub BTreeMap<models::ScalarTypeName, ScalarType>);

impl ScalarTypes {
    pub fn empty() -> Self {
        ScalarTypes(BTreeMap::new())
    }
}

/// Information about a scalar type. A scalar type is completely characterized by its name and the
/// operations you can do on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalarType {
    pub type_name: ScalarTypeName,
    // pub schema_name: Option<String>, // TODO(PY): see what happens when option is removed
    pub description: Option<String>,
    pub comparison_operators: BTreeMap<models::ComparisonOperatorName, ComparisonOperator>,
    pub type_representation: Option<TypeRepresentation>,
}

/// The complete list of supported binary operators for scalar types.
/// Not all of these are supported for every type.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComparisonOperators(
    pub  BTreeMap<
        models::ScalarTypeName,
        BTreeMap<models::ComparisonOperatorName, ComparisonOperator>,
    >,
);

impl ComparisonOperators {
    pub fn empty() -> Self {
        ComparisonOperators(BTreeMap::new())
    }
}

/// Information about a composite type field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FieldInfo {
    pub field_name: String,
    pub r#type: Type,
    pub description: Option<String>,
}

/// Represents a postgres binary comparison operator
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ComparisonOperator {
    pub operator_name: String,
    pub operator_kind: OperatorKind,
    pub argument_type: models::ScalarTypeName,
    pub is_infix: bool,
}

/// Is it a built-in operator, or a custom operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OperatorKind {
    Equal,
    In,
    Custom,
}

/// Mapping from a "table" name to its information.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TablesInfo(pub BTreeMap<models::CollectionName, TableInfo>);

impl TablesInfo {
    pub fn empty() -> Self {
        TablesInfo(BTreeMap::new())
    }

    pub fn merge(&mut self, other: TablesInfo) {
        for (table_name, table_info) in other.0 {
            self.0.insert(table_name, table_info);
        }
    }
}

/// Information about a database table (or any other kind of relation).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableInfo {
    // pub schema_name: String,
    pub table_name: String,
    pub columns: BTreeMap<models::FieldName, ColumnInfo>,
    pub partition_key: String,
    pub sort_key: String,

    // pub uniqueness_constraints: UniquenessConstraints,
    pub gsi: GlobalSecondaryIndexes,

    pub description: Option<String>,
}

/// Can this column contain null values
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Nullable {
    #[default]
    Nullable,
    NonNullable,
}

/// Does this column have a default value.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum HasDefault {
    #[default]
    NoDefault,
    HasDefault,
}

/// Is this column an identity column.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum IsIdentity {
    #[default]
    NotIdentity,
    IdentityByDefault,
    IdentityAlways,
}

/// Is this column a generated column.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum IsGenerated {
    #[default]
    NotGenerated,
    Stored,
}

/// Information about a database column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    pub name: String,
    pub r#type: Type,
    #[serde(default)]
    pub nullable: Nullable,
    #[serde(default)]
    pub description: Option<String>,
}

/// A mapping from the name of a unique constraint to its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UniquenessConstraints(pub BTreeMap<String, UniquenessConstraint>);

/// The set of columns that make up a uniqueness constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UniquenessConstraint(pub BTreeSet<models::FieldName>);

/// A mapping from the name of a foreign key constraint to its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ForeignRelations(pub BTreeMap<String, ForeignRelation>);

/// A foreign key constraint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]

pub struct ForeignRelation {
    pub foreign_schema: Option<String>,
    pub foreign_table: String,
    pub column_mapping: BTreeMap<models::FieldName, models::FieldName>,
}

/// A mapping from the name of a foreign key constraint to its value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct GlobalSecondaryIndexes(pub BTreeMap<String, GlobalSecondaryIndexInfo>);

/// A  Global Secondary Index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GlobalSecondaryIndexInfo {
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub projection_type: ProjectionTypeInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionTypeInfo {
    pub projection_type: String,
    pub non_key_attributes: Vec<String>,
}

/// All supported aggregate functions, grouped by type.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct AggregateFunctions(
    pub BTreeMap<models::ScalarTypeName, BTreeMap<models::AggregateFunctionName, AggregateFunction>>,
);

impl AggregateFunctions {
    pub fn empty() -> Self {
        AggregateFunctions(BTreeMap::new())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AggregateFunction {
    pub return_type: models::TypeName,
}

/// Type representation of scalar types, grouped by type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TypeRepresentations(pub BTreeMap<models::ScalarTypeName, TypeRepresentation>);

impl TypeRepresentations {
    pub fn empty() -> Self {
        TypeRepresentations(BTreeMap::new())
    }
}

/// Type representation of a scalar type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum TypeRepresentation {
    /// JSON booleans
    Boolean,
    /// Any JSON string
    String,
    /// float4
    Float32,
    /// float8
    Float64,
    /// int2
    Int16,
    /// int4
    Int32,
    /// int8 as integer
    Int64,
    /// int8 as string
    Int64AsString,
    /// numeric
    BigDecimal,
    /// numeric as string
    BigDecimalAsString,
    /// timestamp
    Timestamp,
    /// timestamp with timezone
    Timestamptz,
    /// time
    Time,
    /// time with timezone
    Timetz,
    /// date
    Date,
    /// uuid
    UUID,
    /// geography
    Geography,
    /// geometry
    Geometry,
    /// An arbitrary json.
    Json,
    /// One of the specified string values
    Enum(Vec<String>),
}
