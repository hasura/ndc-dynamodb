//! Internal Configuration and state for our connector.

use crate::{connection_settings, AccessKeyId, ProviderName, SecretAccessKey};
use crate::environment::Environment;
use crate::error::WriteParsedConfigurationError;
use crate::values::{PoolSettings, Secret};

use super::error::ParseConfigurationError;
use aws_config::meta::region::RegionProviderChain;
// use aws_config::Region;
// use aws_smithy_http::endpoint::Endpoint;
use aws_sdk_dynamodb::operation::list_tables;
use aws_sdk_dynamodb::types::{GlobalSecondaryIndex, KeyType, ProjectionType};
use aws_sdk_dynamodb::Config;
use ndc_models::{AggregateFunctionName, CollectionName, ComparisonOperatorName, FieldName, ScalarTypeName, TypeName};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::exit;
use tokio::fs;

use aws_sdk_dynamodb::config::Builder;
// use aws_sdk_dynamodb::Endpoint;
// use aws_sdk_dynamodb::Region;

//TODO(PY): temp, needs to be removed from the crate
// use ndc_sdk::connector;

use query_engine_metadata::metadata::{self, database, ColumnInfo, Nullable, ProjectionTypeInfo, ScalarTypes, TablesInfo};

const CURRENT_VERSION: u32 = 1;
pub const CONFIGURATION_FILENAME: &str = "configuration.json";
const CHARACTER_STRINGS: [&str; 3] = ["character", "text", "string"];
const UNICODE_CHARACTER_STRINGS: [&str; 3] = ["nchar", "ntext", "nvarchar"];
const CANNOT_COMPARE: [&str; 3] = ["text", "ntext", "image"];
// const CONFIGURATION_JSONSCHEMA_FILENAME: &str = "schema.json";


/// Initial configuration, just enough to connect to a database and elaborate a full
/// 'Configuration'.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParsedConfiguration {
    // Which version of the configuration format are we using
    pub version: u32,
    pub connection_settings: connection_settings::DatabaseConnectionSettings,
    #[serde(default)]
    pub metadata: metadata::Metadata,
    // #[serde(default)]
    // pub introspection_options: options::IntrospectionOptions,
}

impl ParsedConfiguration {
    pub fn initial() -> Self {
        ParsedConfiguration::empty()
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, JsonSchema)]
pub enum Version {
    #[serde(rename = "1")]
    This,
}

impl ParsedConfiguration {
    pub fn empty() -> Self {
        Self {
            version: CURRENT_VERSION,
            connection_settings: connection_settings::DatabaseConnectionSettings::empty(),
            metadata: metadata::Metadata::default(),
            // aggregate_functions: metadata::AggregateFunctions::default(),
        }
    }
}

pub async fn introspect(
    args: &ParsedConfiguration,
    environment: impl Environment,
) -> anyhow::Result<ParsedConfiguration> {
    let access_key_id = match &args.connection_settings.access_key_id {
        AccessKeyId(Secret::Plain(value)) => Cow::Borrowed(value),
        AccessKeyId(Secret::FromEnvironment { variable }) => Cow::Owned(environment.read(variable)?),
    };
    let secret_access_key = match &args.connection_settings.secret_access_key {
        SecretAccessKey(Secret::Plain(value)) => Cow::Borrowed(value),
        SecretAccessKey(Secret::FromEnvironment { variable }) => Cow::Owned(environment.read(variable)?),
    };
    // let provider_name = match &args.connection_settings.provider_name {
    //     ProviderName(Secret::Plain(value)) => Cow::Borrowed(value),
    //     ProviderName(Secret::FromEnvironment { variable }) => Cow::Owned(environment.read(variable)?),
    // };
    let region = match &args.connection_settings.region {
        crate::Region(Secret::Plain(value)) => Cow::Borrowed(value),
        crate::Region(Secret::FromEnvironment { variable }) => Cow::Owned(environment.read(variable)?),
    };
    // let access_key_id = args.connection_settings.access_key_id.clone();
    // let secret_access_key = args.connection_settings.secret_access_key.clone();
    // let session_token = args.connection_settings.session_token.clone();
    // let region = args.connection_settings.region.clone();
    // let config = aws_config::load_from_env().await;
    let credentials = aws_sdk_dynamodb::config::Credentials::new(
        access_key_id.to_string(),
        secret_access_key.to_string(),
        None,           // Optional session token
        None,           // Expiration (None for non-expiring)
        "my-provider",  // Provider name
    );
    
    // Configure AWS SDK with explicit credentials
    let config = Config::builder()
    .region(aws_config::Region::new(region.to_string()))
    .credentials_provider(credentials)
    .build();

    // To use localhost url
    // let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    //     .test_credentials()
    //     .region(aws_config::Region::new("us-west-2"))
    //     // DynamoDB run locally uses port 8000 by default.
    //     .endpoint_url("http://localhost:8085")
    //     .load()
    //     .await;
    // let dynamodb_local_config = aws_sdk_dynamodb::config::Builder::from(&config).build();

    let client = aws_sdk_dynamodb::Client::from_conf(config);
    let tables_result = client.list_tables().send().await;
    // dbg!(&tables_result);
    let tables = tables_result.map_err(|_op| {
        ParseConfigurationError::IoErrorButStringified(format!(
            "Failed to list tables:",
            // op.error_message.unwrap()
        ))
    }).unwrap(); //TODO: handle error
    // dbg!(&tables);
    let table_names = tables.table_names.unwrap_or_default();
    let mut scalars_list: BTreeSet<ScalarTypeName> = BTreeSet::new();
    let mut tables_info: BTreeMap<CollectionName, metadata::TableInfo> = BTreeMap::new();
    for table_name in table_names {
        let table_result = client.describe_table().table_name(table_name).send().await;
        let table = table_result.unwrap(); //TODO: handle error
        let table = table.table.unwrap();
        // dbg!(&table);
        let table_name = table.table_name.unwrap();
        let attribute_definitions = table.attribute_definitions.unwrap();
        let mut columns_info: BTreeMap<FieldName, ColumnInfo> = BTreeMap::new();
        for columns in attribute_definitions {
            let scalar_name = columns.attribute_name;
            let scalar_field_name = FieldName::new(scalar_name.clone().into());
            let scalar_type_attribute = columns.attribute_type;
            let scalar_type_name = match scalar_type_attribute.as_str() {
                "S" => ScalarTypeName::new("String".into()),
                "N" => ScalarTypeName::new("Number".into()),
                "B" => ScalarTypeName::new("Binary".into()),
                // "SS" => ScalarTypeName::new("StringSet".into()),
                // "NS" => ScalarTypeName::new("NumberSet".into()),
                // "BS" => ScalarTypeName::new("BinarySet".into()),
                // "BOOL" => ScalarTypeName::new("Boolean".into()),
                // "NULL" => ScalarTypeName::new("Null".into()),
                // "M" => ScalarTypeName::new("Object".into()),
                // "L" => ScalarTypeName::new("Array".into()),

                _ => ScalarTypeName::new("Any".into()),
            };
            scalars_list.insert(scalar_type_name.clone());
            let scalar_type = metadata::Type::ScalarType(scalar_type_name);
            let column_info = ColumnInfo {
                name: scalar_name,
                r#type: scalar_type,
                nullable: Nullable::Nullable,
                description: None,
            };
            columns_info.insert(scalar_field_name, column_info);
        }

        //get non key attributes
        let result = client
                .execute_statement()
                .statement(
                    format!(
                        r#"select * from {}"#,
                        table_name
                    )
                )
                .set_parameters(None)
                .set_limit(Some(20))
                .send()
                .await
                .unwrap();

        // let result = match row_1
        // {
        //     Ok(resp) => {
        //         resp.items.unwrap()
        //     }
        //     Err(e) => {
        //         println!("Got an error querying table:");
        //         println!("{}", e);
        //         exit(1) //fixme
        //     }
        // };
        // dbg!(&result);

        // let row = result.first().unwrap();
        for item in result.items.unwrap().iter() {
            for (key, attribute_value) in item {
                let column_name = FieldName::new(key.clone().into());
                // dbg!(&column_name);
                let column_type = 
                    if attribute_value.is_s() {
                        let scalar_type_name = ScalarTypeName::new("String".into());
                        scalars_list.insert(scalar_type_name.clone());
                        metadata::Type::ScalarType(scalar_type_name)
                    }
                    else if attribute_value.is_n() {
                        let scalar_type_name = ScalarTypeName::new("Number".into());
                        scalars_list.insert(scalar_type_name.clone());
                        metadata::Type::ScalarType(scalar_type_name)
                    }
                    else if attribute_value.is_bool() {
                        let scalar_type_name = ScalarTypeName::new("Boolean".into());
                        scalars_list.insert(scalar_type_name.clone());
                        metadata::Type::ScalarType(scalar_type_name)
                    }
                    else {
                        metadata::Type::ScalarType(ScalarTypeName::new("Any".into()))
                    };
                let column_info = ColumnInfo {
                    name: key.clone(),
                    r#type: column_type,
                    nullable: Nullable::Nullable,
                    description: None,
                };
                columns_info.insert(column_name, column_info);

            }
    }


        //
        let mut key_info: BTreeMap<KeyType, String> = BTreeMap::new();
        let key_schema = table.key_schema.unwrap();
        for key in key_schema {
            let name = key.attribute_name;
            let key_type = key.key_type;

            if key_type == KeyType::Hash || key_type == KeyType::Range {
                key_info.insert(key_type, name);
            }
        }
        let partition_key = key_info.get(&KeyType::Hash).unwrap();
        let sort_key = key_info.get(&KeyType::Range).unwrap();

        let mut gsi_indexes:BTreeMap<String, metadata::GlobalSecondaryIndexInfo>  = BTreeMap::new();
        let gsis = table.global_secondary_indexes.unwrap();
        for gsi in gsis {
            let index_name = gsi.index_name.unwrap();
            let mut index_keys_info: BTreeMap<KeyType, String> = BTreeMap::new();
            let index_keys = gsi.key_schema.unwrap();
            for key in index_keys {
                let name = key.attribute_name;
                let key_type = key.key_type;
    
                if key_type == KeyType::Hash || key_type == KeyType::Range {
                    index_keys_info.insert(key_type, name);
                }
            }
            let partition_key = index_keys_info.get(&KeyType::Hash).unwrap();
            let sort_key: Option<String> = index_keys_info.get(&KeyType::Range).cloned();

            let projection_type = gsi.projection.clone().unwrap().projection_type.unwrap().as_str().to_string();
            let non_key_attributes = gsi.projection.unwrap().non_key_attributes.unwrap_or_default();
            gsi_indexes.insert(index_name, metadata::GlobalSecondaryIndexInfo {
                partition_key: partition_key.to_owned(),
                sort_key,
                projection_type: ProjectionTypeInfo {
                    projection_type,
                    non_key_attributes,
                }
            });
        }
        let table_info = metadata::TableInfo {
            table_name: table_name.clone(),
            columns: columns_info,
            partition_key: partition_key.to_owned(),
            sort_key: sort_key.to_owned(),
            gsi: metadata::GlobalSecondaryIndexes(gsi_indexes),
            description: None,
        };
        tables_info.insert(CollectionName::new(table_name.into()), table_info);
    }
    // Scalars
    let mut scalars: BTreeMap<ScalarTypeName, metadata::ScalarType> = BTreeMap::new();
    for scalar in scalars_list {
        let scalar_type = metadata::ScalarType {
            type_name: scalar.clone(),
            description: None,
            aggregate_functions: BTreeMap::new(),
            comparison_operators: get_comparison_operators_for_type(&scalar),
            type_representation: None,
        };
        scalars.insert(scalar.clone(), scalar_type);
    }
    Ok(ParsedConfiguration {
        version: 1,
        connection_settings: connection_settings::DatabaseConnectionSettings {
            access_key_id: args.connection_settings.access_key_id.clone(),
            secret_access_key: args.connection_settings.secret_access_key.clone(),
            // provider_name: args.connection_settings.provider_name.clone(),
            region: args.connection_settings.region.clone(),
            // connection_placeholder: args.connection_settings.connection_placeholder.clone(),
        },
        metadata: metadata::Metadata {
            tables: TablesInfo(tables_info),
            scalar_types: ScalarTypes(scalars),
        },
    })
}

/// Parse the configuration format from a directory.
pub async fn parse_configuration(
    configuration_dir: impl AsRef<Path> + Send,
) -> Result<ParsedConfiguration, ParseConfigurationError> {
    let configuration_file = configuration_dir.as_ref().join(CONFIGURATION_FILENAME);

    let configuration_file_contents =
        fs::read_to_string(&configuration_file)
            .await
            .map_err(|err| {
                ParseConfigurationError::IoErrorButStringified(format!(
                    "{}: {}",
                    &configuration_file.display(),
                    err
                ))
            })?;

    let parsed_config: ParsedConfiguration = serde_json::from_str(&configuration_file_contents)
        .map_err(|error| ParseConfigurationError::ParseError {
            file_path: configuration_file.clone(),
            line: error.line(),
            column: error.column(),
            message: error.to_string(),
        })?;

    Ok(parsed_config)
}

/// Write the parsed configuration into a directory on disk.
pub async fn write_parsed_configuration(
    parsed_config: ParsedConfiguration,
    out_dir: impl AsRef<Path>,
) -> Result<(), WriteParsedConfigurationError> {
    let configuration_file = out_dir.as_ref().to_owned().join(CONFIGURATION_FILENAME);
    fs::create_dir_all(out_dir.as_ref()).await?;

    // create the configuration file
    fs::write(
        configuration_file,
        serde_json::to_string_pretty(&parsed_config)
            .map_err(|e| WriteParsedConfigurationError::IoError(e.into()))?
            + "\n",
    )
    .await?;

    // // create the jsonschema file
    // let configuration_jsonschema_file_path = out_dir
    //     .as_ref()
    //     .to_owned()
    //     .join(CONFIGURATION_JSONSCHEMA_FILENAME);

    // let output = schemars::schema_for!(ParsedConfiguration);
    // fs::write(
    //     &configuration_jsonschema_file_path,
    //     serde_json::to_string_pretty(&output)
    //         .map_err(|e| WriteParsedConfigurationError::IoError(e.into()))?
    //         + "\n",
    // )
    // .await?;

    Ok(())
}

// we hard code these, essentially
// we look up available types in `sys.types` but hard code their behaviour by looking them up below
// categories taken from https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql
fn get_comparison_operators_for_type(
    type_name: &ndc_models::ScalarTypeName,
) -> BTreeMap<ComparisonOperatorName, database::ComparisonOperator> {
    let mut comparison_operators = BTreeMap::new();

    // in ndc-spec, all things can be `==`
    comparison_operators.insert(
        ComparisonOperatorName::new("_eq".into()),
        database::ComparisonOperator {
            operator_name: "=".to_string(),
            argument_type: type_name.clone(),
            operator_kind: database::OperatorKind::Equal,
            is_infix: true,
        },
    );

    comparison_operators.insert(
        ComparisonOperatorName::new("_in".into()),
        database::ComparisonOperator {
            operator_name: "IN".to_string(),
            argument_type: type_name.clone(),
            operator_kind: database::OperatorKind::In,
            is_infix: true,
        },
    );

    // include LIKE and NOT LIKE for string-ish types
    if CHARACTER_STRINGS.contains(&type_name.as_str())
        || UNICODE_CHARACTER_STRINGS.contains(&type_name.as_str())
    {
        comparison_operators.insert(
            ComparisonOperatorName::new("_like".into()),
            database::ComparisonOperator {
                operator_name: "LIKE".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
        comparison_operators.insert(
            ComparisonOperatorName::new("_nlike".into()),
            database::ComparisonOperator {
                operator_name: "NOT LIKE".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
    }

    // include comparison operators for types that are comparable, according to
    // https://learn.microsoft.com/en-us/sql/t-sql/language-elements/comparison-operators-transact-sql?view=sql-server-ver16
    if !CANNOT_COMPARE.contains(&type_name.as_str()) {
        comparison_operators.insert(
            ComparisonOperatorName::new("_neq".into()),
            database::ComparisonOperator {
                operator_name: "!=".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
        comparison_operators.insert(
            ComparisonOperatorName::new("_lt".into()),
            database::ComparisonOperator {
                operator_name: "<".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
        comparison_operators.insert(
            ComparisonOperatorName::new("_gt".into()),
            database::ComparisonOperator {
                operator_name: ">".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );

        comparison_operators.insert(
            ComparisonOperatorName::new("_gte".into()),
            database::ComparisonOperator {
                operator_name: ">=".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
        comparison_operators.insert(
            ComparisonOperatorName::new("_lte".into()),
            database::ComparisonOperator {
                operator_name: "<=".to_string(),
                argument_type: type_name.clone(),
                operator_kind: database::OperatorKind::Custom,
                is_infix: true,
            },
        );
    }
    comparison_operators
}
