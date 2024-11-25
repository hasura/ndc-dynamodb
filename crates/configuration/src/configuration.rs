use query_engine_metadata::metadata;

pub const CONFIGURATION_FILENAME: &str = "configuration.json";
pub const CONFIGURATION_JSONSCHEMA_FILENAME: &str = "schema.json";

pub const DEFAULT_CONNECTION_URI_VARIABLE: &str = "CONNECTION_URI";

/// The 'Configuration' type collects all the information necessary to serve queries at runtime.
///
/// 'ParsedConfiguration' deals with a multitude of different concrete version formats, and each
/// version is responsible for interpreting its serialized format into the current 'Configuration'.
/// Values of this type are produced from a 'ParsedConfiguration' using
/// 'make_runtime_configuration'.
///
/// Separating 'ParsedConfiguration' and 'Configuration' simplifies the main query translation
/// logic by placing the responsibility of dealing with configuration format evolution in
/// 'ParsedConfiguration.
///
#[derive(Debug)]
pub struct Configuration {
    pub metadata: metadata::Metadata,
    // pub service_key: String,
    // pub project_id: String,
    // pub dataset_id: String,
    // pub mutations_version: Option<metadata::mutations::MutationsVersion>,
}