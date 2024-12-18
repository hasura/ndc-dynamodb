//! Convert a SQL AST to a low-level SQL string.

use super::ast::*;
use super::helpers;
use super::string::*;

// Convert to SQL strings

impl With {
    pub fn to_sql(&self, sql: &mut SQL) {
        if self.common_table_expressions.is_empty() {
        } else {
            sql.append_syntax("WITH ");

            let ctes = &self.common_table_expressions;
            for (index, cte) in ctes.iter().enumerate() {
                cte.to_sql(sql);
                if index < (ctes.len() - 1) {
                    sql.append_syntax(", ");
                }
            }
        }
    }
}

impl CommonTableExpression {
    pub fn to_sql(&self, sql: &mut SQL) {
        self.alias.to_sql(sql);
        match &self.column_names {
            None => {}
            Some(names) => {
                sql.append_syntax("(");
                for name in names {
                    name.to_sql(sql);
                }
                sql.append_syntax(")");
            }
        }

        sql.append_syntax(" AS (");
        self.select.to_sql(sql);
        // the newline is important because a native query might end with a comment
        sql.append_syntax("\n)");
    }
}

impl CTExpr {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            CTExpr::Select(select) => {
                select.to_sql(sql);
            }
            CTExpr::RawSql(raw_vec) => {
                for item in raw_vec {
                    item.to_sql(sql);
                }
            } // CTExpr::Delete(delete) => delete.to_sql(sql),
              // CTExpr::Insert(insert) => insert.to_sql(sql),
              // CTExpr::Update(update) => update.to_sql(sql),
        }
    }
}

impl RawSql {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            RawSql::RawText(text) => sql.append_syntax(text),
            RawSql::Expression(exp) => exp.to_sql(sql),
        }
    }
}

impl Explain<'_> {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax("EXPLAIN ");
        match self {
            Explain::Select(select) => select.to_sql(sql),
        }
    }
}

impl SelectList {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            SelectList::SelectList(select_list) => {
                for (index, (_alias, expr)) in select_list.iter().enumerate() {
                    expr.to_sql(sql);
                    // sql.append_syntax(" AS ");
                    // col.to_sql(sql);
                    if index < (select_list.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
            }
            SelectList::SelectStar => {
                sql.append_syntax("*");
            }
            SelectList::SelectStarFrom(table_reference) => {
                table_reference.to_sql(sql);
                sql.append_syntax(".*");
            }
            SelectList::Select1 => {
                sql.append_syntax("1");
            }
        }
    }
}

impl Select {
    pub fn to_sql(&self, sql: &mut SQL) {
        self.with.to_sql(sql);

        sql.append_syntax("SELECT ");

        self.select_list.to_sql(sql);

        sql.append_syntax(" ");

        match &self.from {
            Some(from) => from.to_sql(sql),
            None => (),
        }

        // for join in &self.joins {
        //     join.to_sql(sql);
        // }

        self.where_.to_sql(sql);

        self.order_by.to_sql(sql);

        // self.limit.to_sql(sql);
    }
}

impl Insert {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax("INSERT INTO ");

        self.schema.to_sql(sql);
        sql.append_syntax(".");
        self.table.to_sql(sql);

        if let Some(columns) = &self.columns {
            sql.append_syntax("(");
            for (index, column_name) in columns.iter().enumerate() {
                column_name.to_sql(sql);
                if index < (columns.len() - 1) {
                    sql.append_syntax(", ");
                }
            }
            sql.append_syntax(")");
        }

        sql.append_syntax(" ");

        self.from.to_sql(sql);

        sql.append_syntax(" ");

        self.returning.to_sql(sql);
    }
}

impl InsertFrom {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            InsertFrom::Select(select) => select.to_sql(sql),
            InsertFrom::Values(values) => {
                sql.append_syntax("VALUES ");

                for (index, object) in values.iter().enumerate() {
                    sql.append_syntax("(");
                    for (index, value) in object.iter().enumerate() {
                        value.to_sql(sql);
                        if index < (object.len() - 1) {
                            sql.append_syntax(", ");
                        }
                    }
                    sql.append_syntax(")");

                    if index < (values.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
            }
        }
    }
}

impl Delete {
    pub fn to_sql(&self, sql: &mut SQL) {
        let Delete {
            from,
            where_,
            returning,
        } = &self;

        sql.append_syntax("DELETE ");

        from.to_sql(sql);

        sql.append_syntax(" ");

        where_.to_sql(sql);

        sql.append_syntax(" ");

        returning.to_sql(sql);
    }
}

impl Update {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax("UPDATE ");

        self.schema.to_sql(sql);
        sql.append_syntax(".");
        self.table.to_sql(sql);

        sql.append_syntax(" SET ");

        // Set values to columns
        for (index, (column, expression)) in self.set.iter().enumerate() {
            column.to_sql(sql);
            sql.append_syntax(" = ");
            expression.to_sql(sql);
            if index < (self.set.len() - 1) {
                sql.append_syntax(", ");
            }
        }

        sql.append_syntax(" ");

        self.where_.to_sql(sql);

        sql.append_syntax(" ");

        self.returning.to_sql(sql);
    }
}

impl Returning {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax("RETURNING ");
        self.0.to_sql(sql);
    }
}

impl From {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax("FROM ");
        match &self {
            From::Table {
                reference,
                alias: _,
            } => {
                reference.to_sql(sql);
                // sql.append_syntax(" AS ");
                // alias.to_sql(sql);
            }
            From::Select { select, alias: _ } => {
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
                // sql.append_syntax(" AS ");
                // alias.to_sql(sql);
            }
            From::JsonbToRecordset {
                expression,
                alias,
                columns,
            } => {
                sql.append_syntax("jsonb_to_recordset");
                sql.append_syntax("(");
                expression.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
                sql.append_syntax("(");

                for (index, (column, scalar_type)) in columns.iter().enumerate() {
                    column.to_sql(sql);
                    sql.append_syntax(" ");
                    scalar_type.to_sql(sql);
                    if index < (columns.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
                sql.append_syntax(")");
            }
            From::JsonbArrayElements {
                expression,
                alias,
                column,
            } => {
                sql.append_syntax("jsonb_array_elements");
                sql.append_syntax("(");
                expression.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
                sql.append_syntax("(");
                column.to_sql(sql);
                sql.append_syntax(")");
            }
            From::Unnest {
                expression,
                alias,
                column,
            } => {
                sql.append_syntax("UNNEST");
                sql.append_syntax("(");
                expression.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
                sql.append_syntax("(");
                column.to_sql(sql);
                sql.append_syntax(")");
            }
        }
    }
}

// todo(PY): correct the join syntax for each join type
impl Join {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            Join::LeftOuterJoin(LeftOuterJoin { select, alias, on }) => {
                sql.append_syntax(" LEFT OUTER JOIN ");
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
                sql.append_syntax(" ON (");
                on.to_sql(sql);
                sql.append_syntax(") ");
            }
            Join::InnerJoin(InnerJoin { select, alias }) => {
                sql.append_syntax(" INNER JOIN ");
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
                sql.append_syntax(" ON ('true') ");
            }
            Join::CrossJoin(CrossJoin { select, alias }) => {
                sql.append_syntax(" CROSS JOIN ");
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                alias.to_sql(sql);
            }
            Join::FullOuterJoin(join) => {
                sql.append_syntax(" FULL OUTER JOIN ");
                sql.append_syntax("(");
                join.select.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(" AS ");
                join.alias.to_sql(sql);
                sql.append_syntax(" ON ('true') ");
            }
        }
    }
}

impl Where {
    pub fn to_sql(&self, sql: &mut SQL) {
        let Where(expression) = self;
        if *expression != helpers::true_expr() {
            sql.append_syntax(" WHERE ");
            expression.to_sql(sql);
        }
    }
}

// scalars
impl Expression {
    pub fn to_sql(&self, sql: &mut SQL) {
        match &self {
            Expression::ColumnReference(column_reference) => column_reference.to_sql(sql),
            Expression::TableReference(table_reference) => table_reference.to_sql(sql),
            Expression::Value(value) => value.to_sql(sql),
            Expression::Cast {
                expression,
                r#type: _,
            } => {
                // There is no cast expression in DynamoDB
                expression.to_sql(sql);
            }
            Expression::And { left, right } => {
                sql.append_syntax("(");
                left.to_sql(sql);
                sql.append_syntax(" AND ");
                right.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::Or { left, right } => {
                sql.append_syntax("(");
                left.to_sql(sql);
                sql.append_syntax(" OR ");
                right.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::Not(expr) => {
                sql.append_syntax("NOT ");
                expr.to_sql(sql);
            }
            Expression::BinaryOperation {
                left,
                operator,
                right,
            } => {
                sql.append_syntax("(");
                left.to_sql(sql);
                operator.to_sql(sql);
                right.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::BinaryArrayOperation {
                left,
                operator,
                right,
            } => {
                sql.append_syntax("(");
                {
                    left.to_sql(sql);
                    operator.to_sql(sql);
                    sql.append_syntax("(");
                    for (index, item) in right.iter().enumerate() {
                        item.to_sql(sql);
                        if index < (right.len() - 1) {
                            sql.append_syntax(", ");
                        }
                    }
                    sql.append_syntax(")");
                }
                sql.append_syntax(")");
            }
            Expression::UnaryOperation {
                expression,
                operator,
            } => {
                sql.append_syntax("(");
                expression.to_sql(sql);
                operator.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::FunctionCall { function, args } => {
                function.to_sql(sql);
                sql.append_syntax("(");
                for (index, arg) in args.iter().enumerate() {
                    arg.to_sql(sql);
                    if index < (args.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
                sql.append_syntax(")");
            }
            Expression::JoinExpressions(expressions) => {
                for (index, expression) in expressions.iter().enumerate() {
                    expression.to_sql(sql);
                    if index < (expressions.len() - 1) {
                        sql.append_syntax("");
                    }
                }
            }
            Expression::SafeOffSet { offset } => {
                sql.append_syntax(format!("[SAFE_OFFSET({offset})]").as_str());
            }
            Expression::Exists { select } => {
                sql.append_syntax("EXISTS ");
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::JsonBuildObject(map) => {
                sql.append_syntax("JSON_OBJECT");
                sql.append_syntax("(");

                for (index, (label, item)) in map.iter().enumerate() {
                    sql.append_syntax("'");
                    sql.append_syntax(label);
                    sql.append_syntax("'");
                    sql.append_syntax(", ");
                    item.to_sql(sql);

                    if index < (map.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }

                sql.append_syntax(")");
            }
            Expression::RowToJson(select) => {
                sql.append_syntax("row_to_json");
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::Count(count_type) => {
                sql.append_syntax("COUNT");
                sql.append_syntax("(");
                count_type.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::ArrayConstructor(elements) => {
                sql.append_syntax("ARRAY[");
                for (index, element) in elements.iter().enumerate() {
                    element.to_sql(sql);

                    if index < (elements.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
                sql.append_syntax("]");
            }
            Expression::CorrelatedSubSelect(select) => {
                sql.append_syntax("(");
                select.to_sql(sql);
                sql.append_syntax(")");
            }
            Expression::NestedFieldSelect {
                expression,
                nested_field,
            } => {
                sql.append_syntax("(");
                expression.to_sql(sql);
                sql.append_syntax(")");
                sql.append_syntax(".");
                nested_field.to_sql(sql);
            } // Expression::JsonQuery(target, path) => {
              //     sql.append_syntax("JSON_QUERY");
              //     sql.append_syntax("(");
              //     target.to_sql(sql);
              //     sql.append_syntax(", ");
              //     path.to_sql(sql);
              //     sql.append_syntax(")")
              // }
              // Expression::JsonValue(target, path) => {
              //     sql.append_syntax("JSON_VALUE");
              //     sql.append_syntax("(");
              //     target.to_sql(sql);
              //     sql.append_syntax(", ");
              //     path.to_sql(sql);
              //     sql.append_syntax(")")
              // }
        }
    }
}

// impl JsonPath {
//     pub fn to_sql(&self, sql: &mut SQL) {
//         sql.append_syntax("'$");
//         for ColumnAlias {
//             name: path_item, ..
//         } in self.elements.iter()
//         {
//             sql.append_syntax(".");
//             sql.append_syntax(path_item);
//         }
//         sql.append_syntax("'");
//     }
// }

impl UnaryOperator {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            UnaryOperator::IsNull => sql.append_syntax(" IS NULL "),
        }
    }
}

impl BinaryOperator {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_syntax(" ");
        sql.append_syntax(&self.0);
        sql.append_syntax(" ");
    }
}

impl BinaryArrayOperator {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            BinaryArrayOperator::In => sql.append_syntax(" IN "),
        }
    }
}

impl NestedField {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_identifier(&self.0);
    }
}

impl Function {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            Function::Coalesce => sql.append_syntax("coalesce"),
            Function::JsonAgg => sql.append_syntax("json_agg"),
            Function::JsonbPopulateRecord => sql.append_syntax("jsonb_populate_record"),
            Function::ArrayAgg => sql.append_syntax("ARRAY_AGG"),
            Function::Unnest => sql.append_syntax("unnest"),
            Function::Unknown(name) => sql.append_syntax(name),
            Function::SafeOffSet(index) => {
                sql.append_syntax(format!("[SAFE_OFFSET({index})]").as_str());
            }
        }
    }
}

impl CountType {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            CountType::Star => sql.append_syntax("*"),
            CountType::Simple(column) => column.to_sql(sql),
            CountType::Distinct(column) => {
                sql.append_syntax("DISTINCT ");
                column.to_sql(sql);
            }
        }
    }
}

impl Value {
    pub fn to_sql(&self, sql: &mut SQL) {
        match &self {
            Value::EmptyJsonArray => sql.append_syntax("'[]'"),
            Value::Int8(i) => sql.append_syntax(format!("{i}").as_str()),
            Value::Float8(n) => sql.append_syntax(format!("{n}").as_str()),
            Value::Character(s) | Value::String(s) => sql.append_syntax(format!("'{s}'").as_str()),
            // Value::Character(s) | Value::String(s) => sql.append_param(Param::String(s.clone())),
            Value::Variable(v) => sql.append_param(Param::Variable(v.clone())),
            Value::Bool(true) => sql.append_syntax("true"),
            Value::Bool(false) => sql.append_syntax("false"),
            Value::Null => sql.append_syntax("null"),
            Value::JsonValue(v) => sql.append_param(Param::Value(v.clone())),
            Value::Array(items) => {
                sql.append_syntax("ARRAY [");
                for (index, item) in items.iter().enumerate() {
                    item.to_sql(sql);
                    if index < (items.len() - 1) {
                        sql.append_syntax(", ");
                    }
                }
                sql.append_syntax("]");
            }
        }
    }
}

impl MutationValueExpression {
    pub fn to_sql(&self, sql: &mut SQL) {
        match &self {
            MutationValueExpression::Expression(expression) => expression.to_sql(sql),
            MutationValueExpression::Default => sql.append_syntax("DEFAULT"),
        }
    }
}

impl ScalarType {
    pub fn to_sql(&self, sql: &mut SQL) {
        match &self {
            ScalarType::BaseType(scalar_type_name) => {
                scalar_type_name.to_sql(sql);
            }
            ScalarType::ArrayType(scalar_type_name) => {
                scalar_type_name.to_sql(sql);
                sql.append_syntax("[]");
            }
        };
    }
}

impl ScalarTypeName {
    pub fn to_sql(&self, sql: &mut SQL) {
        match &self {
            ScalarTypeName::Qualified {
                schema_name,
                type_name,
            } => {
                schema_name.to_sql(sql);
                sql.append_syntax(".");
                sql.append_identifier(type_name);
            }
            ScalarTypeName::Unqualified(type_name) => {
                sql.append_identifier(type_name);
            }
        };
    }
}

impl Limit {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self.limit {
            None => (),
            Some(limit) => {
                sql.append_syntax(" LIMIT ");
                sql.append_syntax(format!("{limit}").as_str());
            }
        };
        match self.offset {
            None => (),
            Some(offset) => {
                sql.append_syntax(" OFFSET ");
                sql.append_syntax(format!("{offset}").as_str());
            }
        };
    }
}

// names
impl TableReference {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            TableReference::DBTable { table, gsi } => {
                sql.append_identifier(&table.0);
                match gsi {
                    None => (),
                    Some(gsi) => {
                        sql.append_syntax(".");
                        sql.append_identifier(&gsi.0);
                    }
                }
            }
            TableReference::AliasedTable(alias) => alias.to_sql(sql),
        };
    }
}

impl SchemaName {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_identifier(&self.0);
    }
}

impl TableName {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_identifier(&self.0);
    }
}

impl TableAlias {
    pub fn to_sql(&self, sql: &mut SQL) {
        let name = self.to_aliased_string();
        sql.append_identifier(&name);
    }

    pub fn to_aliased_string(&self) -> String {
        format!("{}_{}", self.name, self.unique_index)
    }
}

impl ColumnReference {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            ColumnReference::TableColumn { table: _, name } => {
                // table.to_sql(sql);
                // sql.append_syntax(".");
                sql.append_identifier(&name.0.to_string());
            }
            ColumnReference::AliasedColumn { column, table: _ } => {
                // table.to_sql(sql);
                // sql.append_syntax(".");
                column.to_sql(sql);
            }
        };
    }
}

impl ColumnName {
    pub fn to_sql(&self, sql: &mut SQL) {
        sql.append_identifier(&self.0);
    }
}

impl ColumnAlias {
    pub fn to_sql(&self, sql: &mut SQL) {
        let name = self.name.to_string();
        sql.append_identifier(&name);
    }
}

impl OrderBy {
    pub fn to_sql(&self, sql: &mut SQL) {
        if !self.elements.is_empty() {
            sql.append_syntax(" ORDER BY ");
            for (index, order_by_item) in self.elements.iter().enumerate() {
                order_by_item.to_sql(sql);
                if index < (self.elements.len() - 1) {
                    sql.append_syntax(", ");
                }
            }
        }
    }
}

impl OrderByElement {
    pub fn to_sql(&self, sql: &mut SQL) {
        self.target.to_sql(sql);
        self.direction.to_sql(sql);
    }
}

impl OrderByDirection {
    pub fn to_sql(&self, sql: &mut SQL) {
        match self {
            OrderByDirection::Asc => sql.append_syntax(" ASC "),
            OrderByDirection::Desc => sql.append_syntax(" DESC "),
        }
    }
}
