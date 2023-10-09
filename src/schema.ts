import { DescribeTableCommand, DynamoDBClient, KeySchemaElement, ScalarAttributeType, paginateListTables } from "@aws-sdk/client-dynamodb";
import { Type } from "@hasura/ndc-sdk-typescript";

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

export type DynamoAttributeType = "S" | "N" | "B" | "BOOL" | "M" | "L" | "SS" | "NS" | "BS";

export type SecondaryIndexSchema = {
  indexName: String,
  keySchema: KeySchema,
}

export async function getTables(dynamoDbClient: DynamoDBClient) {
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

function dynamoAttributeTypeToType(attributeType: DynamoAttributeType): Type {
  switch (attributeType) {
    case "S":
      return {
        type: "named",
        name: "String",
      };
    case "N":
      return {
        type: "named",
        name: "Float",
      };
    case "B":
      return {
        type: "named",
        name: "Binary",
      };
    case "BOOL":
      return {
        type: "named",
        name: "Boolean",
      };
    case "M":
      return {
        type: "named",
        name: "Map",
      };
    case "L":
      return {
        type: "named",
        name: "List",
      };
    case "SS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: "String"
        },
      };
    case "NS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: "Float"
        },
      };
    case "BS":
      return {
        type: "array",
        element_type: {
          type: "named",
          name: "Binary"
        },
      };
  }
}
