use std::process::ExitCode;

use ndc_dynamodb_configuration::environment::ProcessEnvironment;
use ndc_dynamodb::connector::BigQuerySetup;
use ndc_sdk::default_main::default_main_with;

#[tokio::main]
pub async fn main() -> ExitCode {
    let result = default_main_with(BigQuerySetup::new(ProcessEnvironment)).await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
