import { JSONSchemaObject } from "@json-schema-tools/meta-schema";
import { TableSchema } from "./schema";
import * as schema from "./schema";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { ObjectType } from "@hasura/ndc-sdk-typescript";
// configuration.schema.json can be regenerated from the Configuration type
// by running 'npm run regenerate-configuration-jsonschema'
import configurationSchemaJson from "./configuration-schema.generated.json";

export const CURRENT_VERSION = 1;

export type Configuration = {
  version: number,
  awsRegion: string,
  localDynamoDbEndpoint?: string,
  awsCredentials?: AwsCredentials,
  tables: TableSchema[],
  objectTypes: ObjectType[],
};

export type AwsCredentials = {
  accessKeyId: string,
  secretAccessKey: string,
}

export function makeEmptyConfiguration(): Configuration {
  return {
    version: CURRENT_VERSION,
    awsRegion: "us-west-1",
    tables: [],
    objectTypes: [],
  };
}

export const configurationSchema: JSONSchemaObject = configurationSchemaJson;

export async function updateConfiguration(dynamoDbClient: DynamoDBClient, existingConfig: Configuration): Promise<Configuration> {
  const generatedTableSchemas = await schema.getTables(dynamoDbClient);

  const tables = generatedTableSchemas.map(generatedTableSchema => {
    const existingTableSchema = existingConfig.tables.find(schema => schema.tableName === generatedTableSchema.tableName)
    return existingTableSchema !== undefined
      ? mergeTableSchema(generatedTableSchema, existingTableSchema)
      : generatedTableSchema
  })

  return {
    ...existingConfig,
    tables
  }
}

function mergeTableSchema(generatedTableSchema: TableSchema, existingTableSchema: TableSchema): TableSchema {
  const attributeSchemas: schema.AttributeSchema[] = [];
  for (const existingAttrSchema of existingTableSchema.attributeSchema) {
    const generatedAttrSchema = generatedTableSchema.attributeSchema.find(attr => attr.name === existingAttrSchema.name)

    if (generatedAttrSchema !== undefined) {
      // If an attribute has been generated, then it is used as a key attribute,
      // in which case we have an accurate generated schema for it, so we keep that
      attributeSchemas.push(generatedAttrSchema)
    } else {
      // If we don't have a generated attribute, then it must be user-created, so keep it
      attributeSchemas.push(existingAttrSchema);
    }
  }

  return {
    ...generatedTableSchema,
    attributeSchema: attributeSchemas
  };
}
