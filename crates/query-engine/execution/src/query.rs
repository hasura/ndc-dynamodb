//! Execute an execution plan against the database.

use std::{collections::HashMap, vec};

use crate::error::Error;
use crate::metrics;
use aws_sdk_dynamodb::Client;
use base64;
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
                    } else if attribute_value.is_null() {
                        hashmap.insert(key, "null".to_string());
                    } else if attribute_value.is_b() {
                        let b = attribute_value.as_b().unwrap();
                        let b_str = base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            b.as_ref(),
                        );
                        hashmap.insert(key, b_str);
                    } else if attribute_value.is_ss() {
                        let ss = attribute_value.as_ss().unwrap();
                        let ss_str = ss.join(", ");
                        hashmap.insert(key, ss_str);
                    } else if attribute_value.is_ns() {
                        let ns = attribute_value.as_ns().unwrap();
                        let ns_str = ns.join(", ");
                        hashmap.insert(key, ns_str);
                    } else if attribute_value.is_bs() {
                        let bs = attribute_value.as_bs().unwrap();
                        let bs_str = bs
                            .iter()
                            .map(|b| {
                                base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    b.as_ref(),
                                )
                            })
                            .collect::<Vec<String>>()
                            .join(", ");
                        hashmap.insert(key, bs_str);
                    } else if attribute_value.is_m() {
                        let m = attribute_value.as_m().unwrap();
                        let mut m_str = "{".to_string();
                        for (k, v) in m {
                            m_str.push_str(&format!("{k}: {v:?}, "));
                        }
                        m_str.push('}');
                        hashmap.insert(key, m_str);
                    } else if attribute_value.is_l() {
                        let l = attribute_value.as_l().unwrap();
                        let mut l_str = "[".to_string();
                        for v in l {
                            l_str.push_str(&format!("{v:?}, "));
                        }
                        l_str.push(']');
                        hashmap.insert(key, l_str);
                    } else {
                        println!("Unknown");
                    }
                }
                res_map.push(hashmap);
            }

            let mut rows: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            rows.insert("rows".into(), res_map);

            let rows_stringified = serde_json::to_string(&rows).unwrap();

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
