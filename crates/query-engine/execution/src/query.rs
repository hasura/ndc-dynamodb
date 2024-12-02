//! Execute an execution plan against the database.

use std::{collections::HashMap, vec};

use crate::error::Error;
use crate::metrics;
use aws_sdk_dynamodb::Client;
use bytes::{BufMut, Bytes, BytesMut};
use serde_json::{self, to_string, Value};

use query_engine_sql::sql;

/// Execute a query against dynamodb.
#[allow(clippy::cast_possible_wrap)]
pub async fn execute(
    client: &Client,
    _metrics: &metrics::Metrics,
    plan: sql::execution_plan::ExecutionPlan<sql::execution_plan::Query>,
) -> Result<Bytes, Error> {
    let mut buffer = BytesMut::new();

    // run the query on each set of variables. The result is a vector of rows each
    // element in the vector is the result of running the query on one set of variables.
    match plan.query.variables {
        None => {
            let query_request = plan.query.query_sql().sql;
            dbg!(&query_request);
            let query_limit: Option<i32> = plan.query.limit.map(|limit| limit as i32);

            // Query
            let rs = client
                .execute_statement()
                .statement(query_request.to_string())
                .set_parameters(None)
                .set_limit(query_limit)
                .send()
                .await
                .unwrap();

            let mut res_map: Vec<HashMap<String, String>> = vec![];

            for item in &rs.items.unwrap() {
                dbg!(item);
                let mut hashmap = HashMap::new();
                for (key, attribute_value) in item.clone() {
                    if attribute_value.is_s() {
                        let s = attribute_value.as_s().unwrap().to_string();
                        println!("String: {s}");
                        hashmap.insert(key, s);
                    } else if attribute_value.is_n() {
                        let n = attribute_value.as_n().unwrap().to_string();
                        println!("Number: {n}");
                        hashmap.insert(key, n);
                    } else if attribute_value.is_bool() {
                        let bool = attribute_value.as_bool().unwrap();
                        let bool_str = bool.to_string();
                        hashmap.insert(key, bool_str);
                    } else {
                        println!("Unknown");
                    }
                }
                dbg!(item);
                res_map.push(hashmap);
            }

            dbg!(&res_map);

            let mut rows: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            rows.insert("rows".into(), res_map);

            dbg!(&rows);

            let rows_stringified = serde_json::to_string(&rows).unwrap();
            dbg!(&rows_stringified);

            let row_value: Value = serde_json::from_str(&rows_stringified).unwrap();

            let row_value_array = Value::Array(vec![row_value]);
            let final_row = to_string(&row_value_array).unwrap();

            let b: Bytes = Bytes::from(final_row);
            buffer.put(b);
        }
        Some(_variable_sets) => {
            todo!("foreach/variables not implemented in query engine / execution")
        }
    };

    Ok(buffer.freeze())
}
