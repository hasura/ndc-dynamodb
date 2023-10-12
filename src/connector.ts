import { CapabilitiesResponse, Connector, ExplainResponse, MutationRequest, MutationResponse, QueryRequest, QueryResponse, SchemaResponse } from "@hasura/ndc-sdk-typescript";
import { JSONSchemaObject } from "@json-schema-tools/meta-schema";
import { capabilities } from "./capabilities";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { Configuration, configurationSchema, makeEmptyConfiguration, updateConfiguration } from "./configuration";


type State = {
  dynamodbClient: DynamoDBClient,
};

export const connector: Connector<Configuration, State> = {
  get_capabilities: function (configuration: Configuration): CapabilitiesResponse {
    return capabilities;
  },

  get_configuration_schema: function (): JSONSchemaObject {
    return configurationSchema;
  },

  make_empty_configuration: makeEmptyConfiguration,

  update_configuration: async function (configuration: Configuration): Promise<Configuration> {
    const dynamoDbClient = createDynamoDbClient(configuration);
    return await updateConfiguration(dynamoDbClient, configuration);
  },

  validate_raw_configuration: async function (configuration: Configuration): Promise<Configuration> {
    return configuration;
  },

  try_init_state: async function (configuration: Configuration, metrics: unknown): Promise<State> {
    return {
      dynamodbClient: createDynamoDbClient(configuration),
    }
  },

  get_schema: function (configuration: Configuration): Promise<SchemaResponse> {
    throw new Error("Function not implemented.");
  },
  query: function (configuration: Configuration, state: State, request: QueryRequest): Promise<QueryResponse> {
    throw new Error("Function not implemented.");
  },
  explain: function (configuration: Configuration, state: State, request: QueryRequest): Promise<ExplainResponse> {
    throw new Error("Function not implemented.");
  },
  mutation: function (configuration: Configuration, state: State, request: MutationRequest): Promise<MutationResponse> {
    throw new Error("Function not implemented.");
  },

  fetch_metrics: function (configuration: Configuration, state: State): Promise<undefined> {
    throw new Error("Function not implemented.");
  },
  health_check: async function (configuration: Configuration, state: State): Promise<undefined> {
    return;
  },

  get_read_regions: function (configuration: Configuration): string[] {
    throw new Error("Function not implemented.");
  },
  get_write_regions: function (configuration: Configuration): string[] {
    throw new Error("Function not implemented.");
  },
};

function createDynamoDbClient(configuration: Configuration): DynamoDBClient {
  // Use the credentials provided in the config, if they exist, otherwise
  // let the AWS client libs look up the credentials from the environment
  // (default behaviour)
  const credentialsConfig =
    configuration.awsCredentials
      ? { credentials: {
            accessKeyId: configuration.awsCredentials.accessKeyId,
            secretAccessKey: configuration.awsCredentials.secretAccessKey
          }
        }
      : {};

  const endpointConfig =
    configuration.localDynamoDbEndpoint
      ? { endpoint: configuration.localDynamoDbEndpoint }
      : {}

  return new DynamoDBClient({
    region: configuration.awsRegion,
    ...credentialsConfig,
    ...endpointConfig,
  });
}
