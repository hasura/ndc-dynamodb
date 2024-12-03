//! Helpers for building sql::ast types in certain shapes and patterns.

use super::ast::*;

/// Used as input to helpers to construct SELECTs which return 'rows' and/or 'aggregates' results.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectSet {
    Rows(Select),
}

// Empty clauses //

/// An empty `WITH` clause.
pub fn empty_with() -> With {
    With {
        common_table_expressions: vec![],
    }
}

/// Add a `WITH` clause to a select.
pub fn wrap_with(with: With, mut select: Select) -> Select {
    select.with = with;
    select
}

/// An empty `WHERE` clause.
pub fn empty_where() -> Expression {
    Expression::Value(Value::Bool(true))
}

/// An empty `GROUP BY` clause.
pub fn empty_group_by() -> GroupBy {
    GroupBy {}
}

/// An empty `ORDER BY` clause.
pub fn empty_order_by() -> OrderBy {
    OrderBy { elements: vec![] }
}

/// Empty `LIMIT` and `OFFSET` clauses.
pub fn empty_limit() -> Limit {
    Limit {
        limit: None,
        offset: None,
    }
}

/// A `true` expression.
pub fn true_expr() -> Expression {
    Expression::Value(Value::Bool(true))
}

/// A `false` expression.
pub fn false_expr() -> Expression {
    Expression::Value(Value::Bool(false))
}

// Aliasing //

/// Generate a column expression refering to a specific table.
pub fn make_column(
    table: TableReference,
    name: ColumnName,
    alias: ColumnAlias,
) -> (ColumnAlias, Expression) {
    (
        alias,
        Expression::ColumnReference(ColumnReference::TableColumn { table, name }),
    )
}
/// Create column aliases using this function so we build everything in one place.
pub fn make_column_alias(name: String) -> ColumnAlias {
    ColumnAlias { name }
}

// SELECTs //

/// Build a simple select with a select list and the rest are empty.
pub fn simple_select(select_list: Vec<(ColumnAlias, Expression)>) -> Select {
    Select {
        with: empty_with(),
        select_list: SelectList::SelectList(select_list),
        from: None,
        // joins: vec![],
        where_: Where(empty_where()),
        // group_by: empty_group_by(),
        order_by: empty_order_by(),
        // limit: empty_limit(),
    }
}

/// Build a simple select *
pub fn star_select(from: From) -> Select {
    Select {
        with: empty_with(),
        select_list: SelectList::SelectStar,
        from: Some(from),
        // joins: vec![],
        where_: Where(empty_where()),
        // group_by: empty_group_by(),
        order_by: empty_order_by(),
        // limit: empty_limit(),
    }
}

/// Build a simple select <table>.*
pub fn star_from_select(table: TableReference, from: From) -> Select {
    Select {
        with: empty_with(),
        select_list: SelectList::SelectStarFrom(table),
        from: Some(from),
        where_: Where(empty_where()),
        order_by: empty_order_by(),
    }
}

/// Generate an EXISTS where expression.
pub fn where_exists_select(from: From, where_: Where) -> Expression {
    Expression::Exists {
        select: Box::new(Select {
            with: empty_with(),
            select_list: SelectList::Select1,
            from: Some(from),
            where_,
            order_by: empty_order_by(),
        }),
    }
}

/// Do we want to aggregate results or return a single row?
#[derive(Clone, Copy)]
pub enum ResultsKind {
    AggregateResults,
    ObjectResults,
}

/// Given a set of rows, a set of aggregate queries and a variables from clause & table reference,
/// combine them into one Select.
pub fn select_rowset(select_set: SelectSet, returns_field: &ReturnsFields) -> Select {
    match select_set {
        SelectSet::Rows(row_select) => match returns_field {
            ReturnsFields::FieldsWereRequested => row_select,
            ReturnsFields::NoFieldsWereRequested => {
                todo!("not supported yet")
            }
        },
    }
}

/// An unqualified scalar type representing int4.
pub fn int4_type() -> ScalarType {
    ScalarType::BaseType(ScalarTypeName::Unqualified("int4".to_string()))
}

/// Turn all rows of a query result into a single json array of objects.
///
/// Wrap a query that returns multiple rows in the following format:
///
/// ```sql
/// SELECT
///   coalesce(json_agg(row_to_json(<table_alias>)), '[]') AS <column_alias>
/// FROM <query> as <table_alias>
/// ```
///
/// - `row_to_json` takes a row and converts it to a json object.
/// - `json_agg` aggregates the json objects to a json array.
/// - `coalesce(<thing>, <otherwise>)` returns `<thing>` if it is not null, and `<otherwise>` if it is null.
pub fn select_rows_as_json(
    row_select: Select,
    column_alias: ColumnAlias,
    table_alias: TableAlias,
) -> Select {
    let expression = Expression::FunctionCall {
        function: Function::Coalesce,
        args: vec![
            Expression::FunctionCall {
                function: Function::JsonAgg,
                args: vec![Expression::RowToJson(TableReference::AliasedTable(
                    table_alias.clone(),
                ))],
            },
            Expression::Value(Value::EmptyJsonArray),
        ],
    };
    let mut select = simple_select(vec![(column_alias, expression)]);
    select.from = Some(From::Select {
        select: Box::new(row_select),
        alias: table_alias,
    });
    select
}

/// SQL field name to be used for keeping the values of variable sets.
pub const VARIABLES_FIELD: &str = "%variables";

/// This name will be used as a placeholder for a postgres parameter to which the
/// user variables sets will be passed.
pub const VARIABLES_OBJECT_PLACEHOLDER: &str = "%VARIABLES_OBJECT_PLACEHOLDER";

/// SQL field name to be used for ordering results with multiple variable sets.
pub const VARIABLE_ORDER_FIELD: &str = "%variable_order";

/// An unqualified scalar type representing jsonb.
pub fn jsonb_type() -> ScalarType {
    ScalarType::BaseType(ScalarTypeName::Unqualified("jsonb".to_string()))
}

/// An unqualified scalar type name representing text.
pub fn text_type_name() -> ScalarTypeName {
    ScalarTypeName::Unqualified("text".to_string())
}

/// Wrap a query that returns a single row in the following:
///
/// ```sql
/// SELECT
///   coalesce(row_to_json(<table_alias>), '{}'::json)) AS <column_alias>
/// FROM <query> as <table_alias>
/// ```
///
/// - `row_to_json` takes a row and converts it to a json object.
/// - `coalesce(<thing>, <otherwise>)` returns `<thing>` if it is not null, and `<otherwise>` if it is null.
///
pub fn select_row_as_json_with_default(
    select: Select,
    column_alias: ColumnAlias,
    table_alias: TableAlias,
) -> Select {
    let expression = Expression::FunctionCall {
        function: Function::Coalesce,
        args: vec![
            Expression::RowToJson(TableReference::AliasedTable(table_alias.clone())),
            Expression::Value(Value::EmptyJsonArray),
        ],
    };
    let mut final_select = simple_select(vec![(column_alias, expression)]);
    final_select.from = Some(From::Select {
        select: Box::new(select),
        alias: table_alias,
    });
    final_select
}
