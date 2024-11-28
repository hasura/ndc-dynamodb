//! Execute an execution plan against the database.

use std::{collections::HashMap, vec};

use crate::error::Error;
use crate::metrics;
use bytes::{BufMut, Bytes, BytesMut};
use query_engine_sql::sql::string::Param;
use serde_json::{self, to_string, Value};
use aws_sdk_dynamodb::Client;

use query_engine_sql::sql;

/// Execute a query against dynamodb.
pub async fn execute(
    client: &Client,
    _metrics: &metrics::Metrics,
    // project_id: &str,
    plan: sql::execution_plan::ExecutionPlan<sql::execution_plan::Query>,
) -> Result<Bytes, Error> {
    let mut buffer = BytesMut::new();

    // run the query on each set of variables. The result is a vector of rows each
    // element in the vector is the result of running the query on one set of variables.
    match plan.query.variables {
        None => {
            let query_request = (plan.query.query_sql().sql);
            dbg!(&query_request);
            // let temp_query = "select * from test";
            let query_limit: Option<i32> = plan.query.limit.map(|limit| limit as i32);

            // smash query.params in here pls
            // let params = Some(
            //     plan.query
            //         .query_sql()
            //         .params
            //         // .params
            //         .iter()
            //         .enumerate()
            //         .map(|(i, param)| match param {
            //             Param::String(str) => {
            //                 // let value = query_parameter_value::QueryParameterValue {
            //                 //     array_values: None,
            //                 //     struct_values: None,
            //                 //     value: Some(str.to_string()),
            //                 // };
            //                 // let value_type = query_parameter_type::QueryParameterType {
            //                 //     array_type: None,
            //                 //     struct_types: None,
            //                 //     r#type: "STRING".to_string(),
            //                 // };
            //                 // query_parameter::QueryParameter {
            //                 //     name: Some(format!("param{}", i + 1)),
            //                 //     parameter_type: Some(value_type),
            //                 //     parameter_value: Some(value),
            //                 // }

            //             }
            //             Param::Variable(_var) => todo!("Variables not implemented yet"), // error that `Err(Error::Query(QueryError::VariableNotFound(var.to_string())))`
            //             Param::Value(_value) => todo!("Values not implemented yet"),     // todo(PY)
            //         })
            //         .collect(),
            // );

            // let temp_param: Option<Vec<AttributeValue>> = Some(vec![AttributeValue::S("foo".to_string())]);

            // Query
            let rs = client
                .execute_statement()
                .statement(
                    format!(
                        r#"{}"#,
                        query_request
                    )
                )
                .set_parameters(None)
                .set_limit(query_limit)
                .send()
                .await
                .unwrap();

            // let result = match rs
            // {
            //     Ok(resp) => {
            //         resp.items.unwrap()
            //     }
            //     Err(e) => {
            //         println!("Got an error querying table:");
            //         println!("{}", e);
            //         exit(1)
            //     }
            // };

            // dbg!(&result);

            let mut res_map: Vec<HashMap<String, String>> = vec![];


            for item in rs.items.unwrap().iter() {
                dbg!(item);
                let mut hashmap = HashMap::new();
                for (key, attribute_value) in item.clone(){
                    if attribute_value.is_s() {
                        let s = attribute_value.as_s().unwrap().to_string();
                        println!("String: {}", s);
                        // *attribute_value = s;
                        hashmap.insert(key, s);
                    } else if attribute_value.is_n() {
                        let n = attribute_value.as_n().unwrap().to_string();
                        println!("Number: {}", n);
                        hashmap.insert(key, n);
                        // *attribute_value = n;
                    } 
                    else if attribute_value.is_bool() {
                        let bool = attribute_value.as_bool().unwrap();
                        // println!("Number: {}", n);
                        let bool_str = bool.to_string();
                        hashmap.insert(key, bool_str);
                        // *attribute_value = n;
                    } 
                    else {
                        println!("Unknown");
                        // hashmap.insert(key, &"unknown type".to_string());
                    }
                };
                dbg!(item);
                res_map.push(hashmap);
            }

            dbg!(&res_map);


            let mut foo: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            foo.insert("rows".into(), res_map);

            dbg!(&foo);

            let bar = serde_json::to_string(&foo).unwrap();
            dbg!(&bar);

            let row_value: Value = serde_json::from_str(&bar).unwrap();

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
