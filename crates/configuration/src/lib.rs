pub mod configuration;
pub mod connection_settings;
pub mod environment;
pub mod error;
mod to_runtime_configuration;
mod values;
pub mod version1;

pub use configuration::Configuration;
pub use to_runtime_configuration::make_runtime_configuration;
pub use values::connection_info::{AccessKeyId, ProviderName, Region, SecretAccessKey};
pub use version1::{
    introspect,
    parse_configuration,
    write_parsed_configuration,
    // single_connection_uri, // for tests only
    // validate_raw_configuration,
    // Configuration,
    // ConfigurationError,
    // PoolSettings,
    ParsedConfiguration,
};
