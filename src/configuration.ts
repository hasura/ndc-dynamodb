import { JSONSchemaObject } from "@json-schema-tools/meta-schema";
import { TableSchema } from "./schema";

export const CURRENT_VERSION = 1;

export type Configuration = {
  version: number,
  awsRegion: string,
  awsCredentials?: AwsCredentials,
  tables: TableSchema[],
};

export type AwsCredentials = {
  accessKeyId: string,
  secretAccessKey: string,
}

export function makeEmptyConfiguration(): Configuration {
  return {
    version: CURRENT_VERSION,
    awsRegion: "",
    tables: [],
  };
}

export const configuration_schema: JSONSchemaObject = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "Configuration",
  type: "object",
  properties: {
    awsRegion: {
      type: "string",
    },
    awsCredentials: {
      title: "AwsCredentials",
      type: "object",
      properties: {
        accessKeyId: {
          type: "string"
        },
        secretAccessKey: {
          type: "string",
        },
        required: ["accessKeyId", "secretAccessKey"]
      }
    },
  },
  required: ["awsRegion"]
};
