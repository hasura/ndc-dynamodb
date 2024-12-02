//! Handle 'rows' and 'aggregates' translation.

use std::collections::BTreeMap;

use indexmap::IndexMap;

use ndc_models as models;
use query_engine_sql::sql::ast::ReturnsFields;

use super::fields;
use super::filtering;
use super::sorting;
use crate::translation::error::Error;
use crate::translation::helpers::{
    CollectionInfo, Env, RootAndCurrentTables, State, TableNameAndReference,
};
use query_engine_sql::sql;

/// Translate a query to sql ast.
/// We return a select set with a SQL query for the two components - the rows and the aggregates.
pub fn translate_query(
    env: &Env,
    state: &mut State,
    make_from: &MakeFrom,
    join_predicate: &Option<JoinPredicate<'_, '_>>,
    query_request: &models::Query,
) -> Result<(Option<u32>, ReturnsFields, sql::helpers::SelectSet), Error> {
    // translate rows selection.
    let (returns_field, row_select) =
        translate_rows_select(env, state, make_from, join_predicate, query_request)?;

    // // translate aggregate selection.
    // let aggregate_select =
    //     translate_aggregate_select(env, state, make_from, join_predicate, query_request)?;

    // Create a structure describing the selection set - only rows, only aggregates, or both.
    let (_, rows) = (&returns_field, row_select);
    let select_set = (
        query_request.limit,
        returns_field,
        sql::helpers::SelectSet::Rows(rows),
    );

    Ok(select_set)
}

/// Translate rows part of query to sql ast.
fn translate_rows_select(
    env: &Env,
    state: &mut State,
    make_from: &MakeFrom,
    join_predicate: &Option<JoinPredicate<'_, '_>>,
    query: &models::Query,
) -> Result<(ReturnsFields, sql::ast::Select), Error> {
    let (current_table, from_clause) = make_reference_and_from_clause(env, state, make_from)?;

    // translate fields to select list
    let fields = query.fields.clone().unwrap_or_default();

    // remember whether we fields were requested or not.
    // The case were fields were not requested, and also no aggregates were requested,
    // can be used for `__typename` queries.
    let returns_fields = if IndexMap::is_empty(&fields) {
        ReturnsFields::NoFieldsWereRequested
    } else {
        ReturnsFields::FieldsWereRequested
    };

    let mut fields_select = match returns_fields {
        ReturnsFields::FieldsWereRequested => {
            fields::translate_fields(env, state, fields, &current_table, from_clause)?
        }
        ReturnsFields::NoFieldsWereRequested => {
            let select_1 = sql::ast::SelectList::Select1;

            sql::ast::Select {
                with: sql::helpers::empty_with(),
                select_list: select_1,
                from: Some(from_clause),
                where_: sql::ast::Where(sql::helpers::empty_where()),
                order_by: sql::helpers::empty_order_by(),
            }
        }
    };

    // Translate the common part of the query - where, order by, limit, etc.
    translate_query_part(
        env,
        state,
        &current_table,
        join_predicate,
        query,
        &mut fields_select,
    )?;

    Ok((returns_fields, fields_select))
}

/// Translate the lion (or common) part of 'rows' or 'aggregates' part of a query.
/// Specifically, from, joins, order bys, where, limit and offset clauses.
///
/// This expects to get the relevant information about tables, relationships, the root table,
/// and the query, as well as the columns and join fields after processing.
pub fn translate_query_part(
    env: &Env,
    state: &mut State,
    current_table: &TableNameAndReference,
    _join_predicate: &Option<JoinPredicate<'_, '_>>,
    query: &models::Query,
    select: &mut sql::ast::Select,
) -> Result<(), Error> {
    // the root table and the current table are the same at this point
    let root_and_current_tables = RootAndCurrentTables {
        root_table: current_table.clone(),
        current_table: current_table.clone(),
    };

    // translate order_by
    let (order_by, _order_by_joins) =
        sorting::translate_order_by(env, state, &root_and_current_tables, &query.order_by)?;

    // select.joins.extend(order_by_joins);

    // translate where
    let filter = match &query.predicate {
        None => Ok(sql::helpers::true_expr()),
        Some(predicate) => {
            filtering::translate_expression(env, state, &root_and_current_tables, predicate)
        }
    }?;

    select.where_ = sql::ast::Where(filter);

    select.order_by = order_by;

    Ok(())
}

/// Create a from clause from a collection name and its reference.
pub fn make_from_clause_and_reference(
    collection_name: &models::CollectionName,
    gsi_name: Option<sql::ast::Gsi>,
    env: &Env,
    state: &mut State,
    collection_alias: Option<sql::ast::TableAlias>,
) -> Result<(TableNameAndReference, sql::ast::From), Error> {
    let collection_alias = match collection_alias {
        None => state.make_table_alias(collection_name.to_string()),
        Some(_alias) => todo!("alias not supported"),
    };
    // find the table according to the metadata.
    let collection_info = env.lookup_collection(collection_name)?;
    let from_clause = make_from_clause(state, &collection_alias, &collection_info, gsi_name);

    let collection_alias_name = sql::ast::TableReference::AliasedTable(collection_alias);
    let current_table = TableNameAndReference {
        name: collection_name.clone(),
        reference: collection_alias_name,
    };
    Ok((current_table, from_clause))
}

/// Build a FROM clause from a collection info and an alias.
/// Will add a Native Query to the 'State' if the collection is a native query.
fn make_from_clause(
    _state: &mut State,
    current_table_alias: &sql::ast::TableAlias,
    collection_info: &CollectionInfo,
    gsi_name: Option<sql::ast::Gsi>,
) -> sql::ast::From {
    match collection_info {
        CollectionInfo::Table { info, .. } => {
            let db_table = sql::ast::TableReference::DBTable {
                table: sql::ast::TableName(info.table_name.clone()),
                gsi: gsi_name,
            };
            sql::ast::From::Table {
                reference: db_table,
                alias: current_table_alias.clone(),
            }
        }
    }
}

/// Join predicate.
pub struct JoinPredicate<'a, 'b> {
    /// Join the current table with this table.
    pub join_with: &'a TableNameAndReference,
    /// This is the description of the relationship.
    pub relationship: &'b models::Relationship,
}

/// Arguments to build a from clause.
pub enum MakeFrom {
    /// From a collection (db table, native query).
    Collection {
        /// Used for generating aliases.
        name: models::CollectionName,
        gsi: Option<sql::ast::Gsi>,
        /// Native query arguments.
        arguments: BTreeMap<models::ArgumentName, models::Argument>,
    },
    /// From an existing relation.
    TableReference {
        /// Used for generating aliases.
        name: models::CollectionName,
        /// The reference name to the existing relation.
        reference: sql::ast::TableReference,
    },
}

/// Build a from clause and return the table name and reference.
fn make_reference_and_from_clause(
    env: &Env,
    state: &mut State,
    make_from: &MakeFrom,
) -> Result<(TableNameAndReference, sql::ast::From), Error> {
    match make_from {
        MakeFrom::Collection {
            name,
            gsi,
            arguments: _,
        } => make_from_clause_and_reference(name, gsi.clone(), env, state, None),
        MakeFrom::TableReference { name, reference } => {
            let table_alias = state.make_table_alias(name.to_string());
            let from_clause = sql::ast::From::Table {
                reference: reference.clone(),
                alias: table_alias.clone(),
            };
            let reference = sql::ast::TableReference::AliasedTable(table_alias);
            Ok((
                TableNameAndReference {
                    name: name.clone(),
                    reference,
                },
                from_clause,
            ))
        }
    }
}
