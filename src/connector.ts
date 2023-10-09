import { CapabilitiesResponse, Connector, ExplainResponse, MutationRequest, MutationResponse, QueryRequest, QueryResponse, SchemaResponse } from "@hasura/ndc-sdk-typescript";
import { JSONSchemaObject } from "@json-schema-tools/meta-schema";

type Configuration = {};

type State = {};

export const connector: Connector<Configuration, State> = {
    get_configuration_schema: function (): JSONSchemaObject {
        throw new Error("Function not implemented.");
    },
    get_read_regions: function (configuration: Configuration): string[] {
        throw new Error("Function not implemented.");
    },
    get_write_regions: function (configuration: Configuration): string[] {
        throw new Error("Function not implemented.");
    },
    make_empty_configuration: function (): Configuration {
        throw new Error("Function not implemented.");
    },
    update_configuration: function (configuration: Configuration): Promise<Configuration> {
        throw new Error("Function not implemented.");
    },
    validate_raw_configuration: function (configuration: Configuration): Promise<Configuration> {
        throw new Error("Function not implemented.");
    },
    try_init_state: function (configuration: Configuration, metrics: unknown): Promise<State> {
        throw new Error("Function not implemented.");
    },
    fetch_metrics: function (configuration: Configuration, state: State): Promise<undefined> {
        throw new Error("Function not implemented.");
    },
    health_check: function (configuration: Configuration, state: State): Promise<undefined> {
        throw new Error("Function not implemented.");
    },
    get_capabilities: function (configuration: Configuration): CapabilitiesResponse {
        throw new Error("Function not implemented.");
    },
    get_schema: function (configuration: Configuration): Promise<SchemaResponse> {
        throw new Error("Function not implemented.");
    },
    explain: function (configuration: Configuration, state: State, request: QueryRequest): Promise<ExplainResponse> {
        throw new Error("Function not implemented.");
    },
    mutation: function (configuration: Configuration, state: State, request: MutationRequest): Promise<MutationResponse> {
        throw new Error("Function not implemented.");
    },
    query: function (configuration: Configuration, state: State, request: QueryRequest): Promise<QueryResponse> {
        throw new Error("Function not implemented.");
    }
};