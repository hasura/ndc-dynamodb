import { DescribeTableCommand, DynamoDBClient, DynamoDBServiceException, KeySchemaElement, ScalarAttributeType, paginateListTables } from "@aws-sdk/client-dynamodb";
import { BadRequest, Type } from "@hasura/ndc-sdk-typescript";

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
  dynamoType: DynamoAttributeType,
  schemaType?: Type,
}

// Built-in DynamoDB types
export type DynamoAttributeType
  = "S" // String
  | "N" // Number
  | "B" // Binary
  | "BOOL" // Boolean
  | "M" // Map
  | "L" // List
  | "SS" // String Set
  | "NS" // Number Set
  | "BS"; // Binary Set

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
        schemaType: dynamoAttributeTypeToType(definition.AttributeType as ScalarAttributeType) // We don't wrap these in a nullable type because they are key attributes, so they will always exist
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

enum ScalarType {
  String = "String",
  Int = "Int",
  Float = "Float",
  Boolean = "Boolean",
  Binary = "Binary",
  Map = "Map",
  List = "List",
}

function dynamoAttributeTypeToType(attributeType: DynamoAttributeType): Type {
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
  }
}
