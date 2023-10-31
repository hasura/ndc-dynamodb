import { DescribeTableCommand, DynamoDBClient, DynamoDBServiceException, KeySchemaElement, ScalarAttributeType, paginateListTables } from "@aws-sdk/client-dynamodb";
import { BadRequest, Type } from "@hasura/ndc-sdk-typescript";
import { unreachable } from "./util";

export type TableSchema = {
  tableName: string,
  attributeSchema: AttributeSchema[],
  keySchema: KeySchema,
  globalSecondaryIndexes: SecondaryIndexSchema[],
  localSecondaryIndexes: SecondaryIndexSchema[],
}

export type KeySchema = {
  hashKeyAttributeName: string,
  rangeKeyAttributeName: string | null,
}

export type AttributeSchema = {
  name: string,
  description?: string,
  dynamoType: DynamoType,
  schemaType?: Type,
}

// Built-in DynamoDB types, excluding the explicit NULL type
// We use this to capture the type of an attribute in the schema,
// where it doesn't make sense for an attribute to be null in all
// rows. So we capture the non-null type and nullability becomes
// an optional extra that can be configured via the attribute's
// NDC schemaType.
export type DynamoType
  = "S" // String
  | "N" // Number
  | "B" // Binary
  | "BOOL" // Boolean
  | "M" // Map
  | "L" // List
  | "SS" // String Set
  | "NS" // Number Set
  | "BS"; // Binary Set

// The type of a concrete instance of dynamo attribute, where the
// particular instance can indeed be explicitly null.
// This is only valid in the context of a particular row, not in
// the context of the schema of the attribute across all rows
export type DynamoAttributeType = DynamoType | "NULL"

export const dynamoArrayTypes: DynamoType[] = ["L", "SS", "NS", "BS"];

export type SecondaryIndexSchema = {
  indexName: String,
  keySchema: KeySchema,
}

export async function getTables(dynamoDbClient: DynamoDBClient): Promise<TableSchema[]> {
  try {
    const paginator = paginateListTables({client: dynamoDbClient}, {});

    const tableNames: string[] = [];
    for await (const page of paginator) {
      tableNames.push(...page.TableNames ?? []);
    }

    const tableSchemas: TableSchema[] = [];
    for (const tableName of tableNames) {
      const tableDescription = await dynamoDbClient.send(new DescribeTableCommand({TableName: tableName}));

      const attributeSchema: AttributeSchema[] = (tableDescription.Table?.AttributeDefinitions ?? []).map(definition => ({
        name: definition.AttributeName!,
        dynamoType: definition.AttributeType as ScalarAttributeType,
        schemaType: dynamoTypeToType(definition.AttributeType as ScalarAttributeType) // We don't wrap these in a nullable type because they are key attributes, so they will always exist
      }));

      tableSchemas.push({
        tableName,
        attributeSchema,
        keySchema: getKeySchema(tableDescription.Table?.KeySchema ?? [], `table ${tableName}`),
        globalSecondaryIndexes: tableDescription.Table?.GlobalSecondaryIndexes?.map(index => ({
          indexName: index.IndexName!,
          keySchema: getKeySchema(index.KeySchema ?? [], `table ${tableName}, global secondary index ${index.IndexName}`)
        })) ?? [],
        localSecondaryIndexes: tableDescription.Table?.LocalSecondaryIndexes?.map(index => ({
          indexName: index.IndexName!,
          keySchema: getKeySchema(index.KeySchema ?? [], `table ${tableName}, global secondary index ${index.IndexName}`)
        })) ?? [],
      });
    }

    return tableSchemas;
  } catch (e) {
    if (e && typeof e === "object" && 'code' in e && 'message' in e && e.code === "ECONNREFUSED") {
      throw new BadRequest(`Unable to connect to DynamoDB`, { error: e.message })
    } else if (e instanceof DynamoDBServiceException) {
      throw new BadRequest(`Unable to introspect the tables in DynamoDB due to an error communicating with DynamoDB: ${e.message}`, { name: e.name, error: e.message })
    } else if (e instanceof Error) {
      throw new BadRequest(`Unable to introspect the tables in DynamoDB due to an error communicating with DynamoDB: ${e.message}`, { message: e.message })
    } else {
      throw new BadRequest(`Unable to introspect the tables in DynamoDB due to an unexpected error`, { error: e })
    }
  }
}

function getKeySchema(keySchemaElements: KeySchemaElement[], indexDescription: string): KeySchema {
  const hashKeyAttributeName = keySchemaElements.find(k => k.KeyType === "HASH")?.AttributeName;
    if (hashKeyAttributeName === undefined) {
      throw new Error(`Unable to find a hash key for ${indexDescription}`);
    }
    const rangeKeyAttributeName = keySchemaElements.find(k => k.KeyType === "RANGE")?.AttributeName ?? null;

    return {
      hashKeyAttributeName,
      rangeKeyAttributeName,
    };
}

export enum ScalarType {
  String = "String",
  Int = "Int",
  Float = "Float",
  Boolean = "Boolean",
  Binary = "Binary",
  Map = "Map",
  List = "List",
}

export function scalarTypeToDynamoType(scalarType: ScalarType): DynamoType {
  switch (scalarType) {
    case ScalarType.String:
      return "S";
    case ScalarType.Int:
      return "N";
    case ScalarType.Float:
      return "N";
    case ScalarType.Boolean:
      return "BOOL";
    case ScalarType.Binary:
      return "B";
    case ScalarType.Map:
      return "M";
    case ScalarType.List:
      return "L";
    default:
      return unreachable(scalarType);
  }
}

export type ScalarNamedType = {
  kind: "scalar",
  name: ScalarType
}

export type ObjectNamedType = {
  kind: "object"
  name: string,
}

export function determineNamedTypeKind(namedType: Extract<Type, {type: "named"}>): ScalarNamedType | ObjectNamedType {
  switch (namedType.name) {
    case ScalarType.String:
    case ScalarType.Int:
    case ScalarType.Float:
    case ScalarType.Boolean:
    case ScalarType.Binary:
    case ScalarType.Map:
    case ScalarType.List:
      return { kind: "scalar", name: namedType.name };
    default:
      return { kind: "object", name: namedType.name };
  }
}

export function dynamoTypeToType(attributeType: DynamoType): Type {
  switch (attributeType) {
    case "S":
      return {
        type: "named",
        name: ScalarType.String,
      };
    case "N":
      return {
        type: "named",
        name: ScalarType.Float,
      };
    case "B":
      return {
        type: "named",
        name: ScalarType.Binary,
      };
    case "BOOL":
      return {
        type: "named",
        name: ScalarType.Boolean,
      };
    case "M":
      return {
        type: "named",
        name: ScalarType.Map,
      };
    case "L":
      return {
        type: "named",
        name: ScalarType.List,
      };
    case "SS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: ScalarType.String
        },
      };
    case "NS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: ScalarType.Float
        },
      };
    case "BS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: ScalarType.Binary
        },
      };
    default:
      return unreachable(attributeType);
  }
}
