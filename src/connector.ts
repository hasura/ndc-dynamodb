import { BadRequest, CapabilitiesResponse, Connector, ExplainResponse, MutationRequest, MutationResponse, NotSupported, QueryRequest, QueryResponse, SchemaResponse } from "@hasura/ndc-sdk-typescript";
import { JSONSchemaObject } from "@json-schema-tools/meta-schema";
import { capabilities } from "./capabilities";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { RawConfiguration, rawConfigurationSchema, makeEmptyConfiguration, updateConfiguration } from "./configuration";
import { ConnectorSchema, createSchema } from "./schema-ndc";
import { Err, Ok } from "./result";
import { performQuery } from "./query";


type State = {
  dynamodbClient: DynamoDBClient,
  schema: ConnectorSchema
};

export const connector: Connector<RawConfiguration, RawConfiguration, State> = {
  get_capabilities: function (configuration: RawConfiguration): CapabilitiesResponse {
    return capabilities;
  },

  get_raw_configuration_schema: function(): JSONSchemaObject {
    return rawConfigurationSchema;
  },

  make_empty_configuration: makeEmptyConfiguration,

  update_configuration: async function (configuration: RawConfiguration): Promise<RawConfiguration> {
    const dynamoDbClient = createDynamoDbClient(configuration);
    return await updateConfiguration(dynamoDbClient, configuration);
  },

  validate_raw_configuration: async function (configuration: RawConfiguration): Promise<RawConfiguration> {
    return configuration;
  },

  try_init_state: async function (configuration: RawConfiguration, metrics: unknown): Promise<State> {
    // TODO: This shouldn't be in state, it should be in configuration, but we don't have the concept of
    // a non-raw Configuration in the ts-sdk currently, so we're munging it into state for now
    const result = createSchema(configuration.tables, configuration.objectTypes);
    if (result instanceof Err) {
      throw new BadRequest("Schema's busted, yo", result.error)
    }

    return {
      dynamodbClient: createDynamoDbClient(configuration),
      schema: result.data
    }
  },

  get_schema: async function (configuration: RawConfiguration): Promise<SchemaResponse> {
    // TODO: This should not be computed here, but we need a non-raw Configuration to exist in ts-sdk
    // so that we can compute this in validate_raw_configuration instead
    const result = createSchema(configuration.tables, configuration.objectTypes);
    if (result instanceof Ok) {
      return result.data.schemaResponse;
    } else {
      throw new BadRequest("Schema's busted, yo", result.error)
    }
  },
  query: async function (configuration: RawConfiguration, state: State, request: QueryRequest): Promise<QueryResponse> {
    return await performQuery(request, state.schema, state.dynamodbClient);
  },
  explain: function (configuration: RawConfiguration, state: State, request: QueryRequest): Promise<ExplainResponse> {
    throw new Error("Function not implemented.");
  },
  mutation: function (configuration: RawConfiguration, state: State, request: MutationRequest): Promise<MutationResponse> {
    throw new Error("Function not implemented.");
  },

  fetch_metrics: function (configuration: RawConfiguration, state: State): Promise<undefined> {
    throw new Error("Function not implemented.");
  },
  health_check: async function (configuration: RawConfiguration, state: State): Promise<undefined> {
    return;
  },
};

function createDynamoDbClient(configuration: RawConfiguration): DynamoDBClient {
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
