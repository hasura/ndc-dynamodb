//! Translate an incoming `QueryRequest`.

pub mod fields;
pub mod filtering;
pub mod root;
mod sorting;
pub mod values;

use ndc_models::{self as models};

use crate::translation::error::Error;
use crate::translation::helpers::{Env, State};
use query_engine_metadata::metadata;
use query_engine_sql::sql;

/// Translate the incoming QueryRequest to an ExecutionPlan (SQL) to be run against the database.
pub fn translate(
    metadata: &metadata::Metadata,
    query_request: models::QueryRequest,
) -> Result<sql::execution_plan::ExecutionPlan<sql::execution_plan::Query>, Error> {
    let mut state = State::new();
    let env = Env::new(metadata);

    let collection_string = query_request.collection.as_str();
    let collection = if collection_string.contains(':') {
        let split: Vec<&str> = collection_string.split(':').collect();
        if split.len() != 2 {
            return Err(Error::InvalidCollectionName(collection_string.to_string()));
        };
        let collection_name = models::CollectionName::new(split[0].into());
        let gsi_name = split[1];
        (collection_name, Some(sql::ast::Gsi(gsi_name.to_string())))
    } else {
        (models::CollectionName::new(collection_string.into()), None)
    };

    let (query_limit, returns_field, select_set) = root::translate_query(
        &env,
        &mut state,
        &root::MakeFrom::Collection {
            name: collection.0.clone(),
            arguments: query_request.arguments.clone(),
            gsi: collection.1.clone(),
        },
        &None,
        &query_request.query,
    )?;

    // form a single JSON item shaped `{ rows: [] }`
    // that matches the models::RowSet type
    let json_select = sql::helpers::select_rowset(select_set, &returns_field);

    // normalize ast
    let json_select = sql::rewrites::constant_folding::normalize_select(json_select);

    Ok(sql::execution_plan::simple_query_execution_plan(
        query_request.variables,
        query_request.collection,
        json_select,
        query_limit,
    ))
}
