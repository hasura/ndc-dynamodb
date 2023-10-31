import { Argument, BadRequest, Field, InternalServerError, NotSupported, ObjectType, QueryRequest, QueryResponse, Type } from "@hasura/ndc-sdk-typescript"
import { ByKeysFunction, ConnectorSchema, QueryFields, QueryFunction, RowFieldValue, schemaConstants } from "./schema-ndc";
import { ifNotNull, isArray, mapObjectValues, unreachable } from "./util";
import { AttributeValue, BatchGetItemCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoAttributeType, DynamoType, ObjectNamedType, ScalarNamedType, ScalarType, determineNamedTypeKind } from "./schema-dynamo";
import { fromBase64, toBase64 } from "@smithy/util-base64";
import { ObjectTypes } from "./configuration";

export function performQuery(queryRequest: QueryRequest, schema: ConnectorSchema, dynamoDbClient: DynamoDBClient): Promise<QueryResponse> {
  const functionDefinition = schema.functions[queryRequest.collection];
  if (functionDefinition === undefined) {
    throw new BadRequest(`Unknown function '${queryRequest.collection}'`);
  }

  switch (functionDefinition.type) {
    case "by_keys": return performByKeysFunction(queryRequest, functionDefinition, schema.schemaResponse.object_types, dynamoDbClient);
    case "query": return performQueryFunction(queryRequest, functionDefinition)
  }
}

async function performByKeysFunction(queryRequest: QueryRequest, byKeysFnDef: ByKeysFunction, objectTypes: ObjectTypes, dynamoDbClient: DynamoDBClient): Promise<QueryResponse> {
  const args = getByKeysFunctionArguments(queryRequest, byKeysFnDef);
  const aliaser = new AttributeNameAliaser();
  // If no fields are to be selected, just select the hashkey so we have _something_ to return. If we
  // use an empty projection expression (ie nothing to select), dynamo will return all attributes, so
  // one attribute is better than all attributes when we really want zero attributes
  const fieldsToProject = queryRequest.query.fields ?? { "hashkey": { type: "column", column: byKeysFnDef.primaryKeySchema.hashKeySchema.attributeName } }
  const projectionExpression =  queryFieldsToProjectionExpression(fieldsToProject, aliaser);
  const expressionAttributeNames = aliaser.getExpressionAttributeNames();

  let unprocessedKeys = args.keys.map(k => ({
    [k.hashKey.attributeName]: k.hashKey.value,
    ...(
      k.rangeKey !== null
        ? { [k.rangeKey.attributeName]: k.rangeKey.value }
        : {}
    )
  }));
  const responseNdcRowObjectsArray: Record<string, unknown>[] = [];

  while (unprocessedKeys.length > 0) {
    const keysToProcess = unprocessedKeys.slice(0, 99); // Dynamo supports requests of 100 PKs at a time
    unprocessedKeys = keysToProcess.slice(100);

    const command = new BatchGetItemCommand({
      RequestItems: {
        [byKeysFnDef.tableSchema.tableName]: {
          Keys: keysToProcess,
          ExpressionAttributeNames: expressionAttributeNames,
          ProjectionExpression: projectionExpression,
          ConsistentRead: args.consistentRead,
        }
      }
    });

    // TODO: Retry logic with exponential backoff required to handle throughput exceeded errors
    const response = await dynamoDbClient.send(command);
    const responseRows = response.Responses?.[byKeysFnDef.tableSchema.tableName] ?? [];
    const ndcRowObjectsArray = responseRows.map(row => mkNdcResponseRowFromDynamoResponseRow(row, queryRequest.query.fields ?? {}, byKeysFnDef.tableRowType, objectTypes));
    responseNdcRowObjectsArray.push(...ndcRowObjectsArray);
    const unprocessedKeysFromThisResponse = response.UnprocessedKeys?.[byKeysFnDef.tableSchema.tableName]?.Keys;
    if (unprocessedKeysFromThisResponse) {
      unprocessedKeys = [ ...unprocessedKeysFromThisResponse, ...unprocessedKeys ];
    }
  }

  return [
    {
      rows: [
        // TODO: Remove hack to work around incorrect RowFieldValue type in typescript SDK, once updated
        { "__value": responseNdcRowObjectsArray as unknown as Record<string, unknown> }
      ]
    }
  ];
}

type ByKeysFunctionArgs = {
  keys: KeysArg[],
  consistentRead: boolean,
}

type KeysArg = {
  hashKey: KeyArg,
  rangeKey: KeyArg | null,
}

type KeyArg = {
  attributeName: string,
  value: AttributeValue,
}

function getByKeysFunctionArguments(queryRequest: QueryRequest, byKeysFnDef: ByKeysFunction): ByKeysFunctionArgs {
  const keysArg = queryRequest.arguments[schemaConstants.byKeysFn.keysArgName];
  if (keysArg === undefined) {
    throw new BadRequest(`Could not find '${schemaConstants.byKeysFn.keysArgName}' argument`);
  }
  const keysArgValue = resolveArgument(keysArg);
  if (!isArray(keysArgValue)) {
    throw new BadRequest(`The '${schemaConstants.byKeysFn.keysArgName}' argument value was not an array`);
  }
  const keys = keysArgValue.map<KeysArg>((keys, keyIndex) => {
    if (typeof keys !== "object" || keys === null) {
      throw new BadRequest(`The '${schemaConstants.byKeysFn.keysArgName}' argument array value, element '${keyIndex}', was not an object`);
    }
    const hashKeySchema = byKeysFnDef.primaryKeySchema.hashKeySchema;
    const hashKeyValue = mkDynamoAttributeValue(
      (keys as Record<string, unknown>)[hashKeySchema.attributeName],
      hashKeySchema.schemaType,
      hashKeySchema.dynamoType
    );
    const hashKey = {
      attributeName: hashKeySchema.attributeName,
      value: hashKeyValue,
    };

    const rangeKey = ifNotNull(byKeysFnDef.primaryKeySchema.rangeKeySchema, rangeKeySchema => {
      const rangeKeyValue = mkDynamoAttributeValue(
        (keys as Record<string, unknown>)[rangeKeySchema.attributeName],
        rangeKeySchema.schemaType,
        rangeKeySchema.dynamoType
      );

      return {
        attributeName: rangeKeySchema.attributeName,
        value: rangeKeyValue,
      };
    });

    return {
      hashKey,
      rangeKey,
    }
  });

  const consistentReadArg = queryRequest.arguments[schemaConstants.byKeysFn.consistentReadArgName];
  if (consistentReadArg === undefined) {
    throw new BadRequest(`Could not find '${schemaConstants.byKeysFn.consistentReadArgName}' argument`);
  }
  const consistentReadArgValue = resolveArgument(consistentReadArg);
  if (consistentReadArgValue !== null && typeof consistentReadArgValue !== "boolean") {
    throw new BadRequest(`The ${schemaConstants.byKeysFn.consistentReadArgName} argument value was not a nullable boolean`);
  }

  return {
    keys,
    consistentRead: consistentReadArgValue ?? false,
  }
}

function resolveArgument(arg: Argument): unknown {
  switch (arg.type) {
    case "literal": return arg.value;
    case "variable": throw new NotSupported("Query variables are not supported");
    default: return unreachable(arg["type"]);
  }
}

function mkDynamoAttributeValue(value: unknown, schemaType: Type, dynamoType: DynamoType | null): AttributeValue {
  switch (schemaType.type) {
    case "named":
      const namedType = determineNamedTypeKind(schemaType);
      switch (namedType.kind) {
        case "scalar":
          return mkScalarDynamoAttributeValue(value, namedType.name);
        case "object":
          throw new Error("not implemented");
      }
    case "nullable":
      if (value === null)
        return { NULL: true };
      else
        return mkDynamoAttributeValue(value, schemaType.underlying_type, dynamoType);
    case "array":
      return mkArrayDynamoAttributeValue(value, schemaType.element_type, dynamoType);
    default:
      return unreachable(schemaType["type"]);
  }
}

function mkScalarDynamoAttributeValue(value: unknown, scalarType: ScalarType): AttributeValue {
  switch (scalarType) {
    case ScalarType.String:
      if (typeof value !== "string")
        throw new BadRequest(`Unable to create a String AttributeValue as the actual value type (${typeof value}) is not a string`);
      return { S: value };
    case ScalarType.Int:
    case ScalarType.Float:
      if (typeof value !== "number")
        throw new BadRequest(`Unable to create a Number AttributeValue as the actual value type (${typeof value}) is not a number`);
      return { N: value.toString() }
    case ScalarType.Boolean:
      if (typeof value !== "boolean")
        throw new BadRequest(`Unable to create a Boolean AttributeValue as the actual value type (${typeof value}) is not a boolean`);
      return { BOOL: value }
    case ScalarType.Binary:
      if (typeof value !== "string")
        throw new BadRequest(`Unable to create a Binary AttributeValue as the actual value type (${typeof value}) is not a string`);

      try {
        return { B: fromBase64(value) };
      } catch (e) {
        const details = e instanceof Error ? { error: e.message } : {};
        throw new BadRequest("Unable to create a Binary AttributeValue as the actual value is not valid base64", details);
      }

    case ScalarType.Map:
      if (value === null || Array.isArray(value) || typeof value !== "object")
        throw new BadRequest(`Unable to create a Map AttributeValue as the actual value type (${typeof value}) is not a record object`);
      return { M: mapObjectValues(value, mkUntypedAttributeValue) };
    case ScalarType.List:
      if (!isArray(value))
        throw new BadRequest(`Unable to create a List AttributeValue as the actual value type (${typeof value}) is not an array`);
      return { L: value.map(mkUntypedAttributeValue) }
    default:
      return unreachable(scalarType);
  }
}

function mkArrayDynamoAttributeValue(value: unknown, arrayElementType: Type, dynamoType: DynamoType | null): AttributeValue {
  if (!isArray(value))
    throw new BadRequest(`Unable to create AttributeValue for an array schema type as the actual value type (${typeof value}) is not an array`);

  // If we don't know what the allowed dynamo type is, just treat this as an untyped List
  dynamoType = dynamoType ?? "L";

  if (dynamoType === "L") {
    return { L: value.map(v => mkDynamoAttributeValue(v, arrayElementType, null)) }
  } else if (dynamoType === "SS") {
    if (arrayElementType.type !== "named" || arrayElementType.name !== ScalarType.String)
      throw new BadRequest(`Unable to create a StringSet AttributeValue for an array where the schema array type's element_type is not a ${ScalarType.String}`);

    return {
      SS: value.map(arrayElementValue => {
        if (typeof arrayElementValue !== "string")
          throw new BadRequest(`Unable to create a StringSet AttributeValue as one of the actual array value elements is not a string (${typeof arrayElementType})`);
        return arrayElementValue;
      })
    }
  } else if (dynamoType === "NS") {
    if (arrayElementType.type !== "named" || (arrayElementType.name !== ScalarType.Int && arrayElementType.name !== ScalarType.Float))
      throw new BadRequest(`Unable to create a NumberSet AttributeValue for an array where the schema array type's element_type is not a ${ScalarType.Int} or ${ScalarType.Float}`);

    return {
      NS: value.map(arrayElementValue => {
        if (typeof arrayElementValue !== "number")
          throw new BadRequest(`Unable to create a NumberSet AttributeValue as one of the actual array value elements is not a number (${typeof arrayElementType})`);
        return arrayElementValue.toString();
      })
    }
  } else if (dynamoType === "BS") {
    if (arrayElementType.type !== "named" || arrayElementType.name !== ScalarType.Binary)
      throw new BadRequest(`Unable to create a BinarySet AttributeValue for an array where the schema array type's element_type is not a ${ScalarType.Binary}`);

    return {
      BS: value.map(arrayElementValue => {
        if (typeof arrayElementValue !== "string")
          throw new BadRequest(`Unable to create a BinarySet AttributeValue as one of the actual array value elements is not a base64 string (${typeof arrayElementType})`);
        try {
          return fromBase64(arrayElementValue);
        } catch (e) {
          const details = e instanceof Error ? { error: e.message } : {};
          throw new BadRequest("Unable to create a BinarySet AttributeValue as one of the actual array value elements is not valid base64", details);
        }
      })
    }
  } else {
    throw new BadRequest(`Unable to create AttributeValue for array type. Unexpected DynamoType '${dynamoType}' encountered`)
  }
}

function mkUntypedAttributeValue(value: unknown): AttributeValue {
  if (value === null || value === undefined) {
    return { NULL: true };
  } else if (typeof value === "number") {
    return { N: value.toString() };
  } else if (typeof value === "string") {
    return { S: value };
  } else if (typeof value === "boolean") {
    return { BOOL: value }
  } else if (Array.isArray(value)) {
    return { L: value.map(mkUntypedAttributeValue) }
  } else if (typeof value === "object") {
    return { M: mapObjectValues(value, mkUntypedAttributeValue) }
  } else {
    throw new BadRequest(`Unable to create AttributeValue for untyped value with unexpected type '${typeof value}'`);
  }
}

function performQueryFunction(queryRequest: QueryRequest, byIndexFnDef: QueryFunction): Promise<QueryResponse> {
  throw new NotSupported("Query functions are not supported");
}

class AttributeNameAliaser {
  private attributeNameToAliasMap: Record<string, string> = {};
  private count: number = 0;

  getAlias(attributeName: string): string {
    let alias = this.attributeNameToAliasMap[attributeName];
    if (alias === undefined) {
      // Strip all non alphanumeric characters and append a unique suffix to prevent name clashing
      // While the unique suffix would work on its own, we keep the original name (stripped) in there for debuggability
      // Why is this necessary? Enjoy: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html 😭
      alias = `#${attributeName.replace(/[^A-Za-z0-9_]/gi, "")}_${this.count.toString()}`;
      this.attributeNameToAliasMap[attributeName] = alias;
      this.count++;
    }
    return alias;
  }

  getExpressionAttributeNames(): Record<string, string> {
    return Object.fromEntries(Object.entries(this.attributeNameToAliasMap).map(([attributeName, alias]) => [alias, attributeName]))
  }
}

type AttributeProjectionExpression = string

function queryFieldsToProjectionExpression(fields: QueryFields, aliases: AttributeNameAliaser): AttributeProjectionExpression {
  const attrProjectionExprs = Array.from(new Set(Object.values(fields).map(field => fieldToProjectionExpression(field, aliases))));
  return attrProjectionExprs.join(",");
}

function fieldToProjectionExpression(field: Field, aliases: AttributeNameAliaser): AttributeProjectionExpression {
  switch (field.type) {
    case "column":
      return aliases.getAlias(field.column)
    case "relationship":
      throw new NotSupported("Relationship fields are not supported");
    default:
      return unreachable(field["type"]);
  }
}

function mkNdcResponseRowFromDynamoResponseRow(dynamoResponseRow: Record<string, AttributeValue>, queryFields: QueryFields, tableRowType: ObjectType, objectTypes: ObjectTypes): Record<string, RowFieldValue> {
  // TODO: All the error messages along this code path need to have pathing information in them and the final error probably should contain the dynamo row for debugging
  return mapObjectValues(queryFields, (field: Field) => {
    switch (field.type) {
      case "column":
        const attributeName = field.column;
        const attributeValue = dynamoResponseRow[attributeName] ?? { NULL: true };
        return resolveTypedValueFromAttributeValue(attributeValue, tableRowType.fields[attributeName].type, objectTypes);
      case "relationship":
        throw new NotSupported("Relationship fields are not supported");
      default:
        return unreachable(field["type"]);
    }
  });
}

function resolveTypedValueFromAttributeValue(attributeValue: AttributeValue, schemaType: Type, objectTypes: ObjectTypes): unknown {
  switch (schemaType.type) {
    case "nullable":
      return attributeValue.NULL
        ? null
        : resolveTypedValueFromAttributeValue(attributeValue, schemaType.underlying_type, objectTypes);
    case "named":
      const namedType = determineNamedTypeKind(schemaType);
      return resolveNameTypedValueFromAttributeValue(attributeValue, namedType, objectTypes);
    case "array":
      return resolveArrayTypedValueFromAttributeValue(attributeValue, schemaType.element_type, objectTypes);
    default:
      return unreachable(schemaType["type"])
  }
}

function resolveNameTypedValueFromAttributeValue(attributeValue: AttributeValue, namedSchemaType: ScalarNamedType | ObjectNamedType, objectTypes: ObjectTypes): unknown {
  // TODO: these error messages are suboptimal. They need to mention the schema type they are trying to match to
  switch (namedSchemaType.kind) {
    case "object":
      if (attributeValue.M === undefined)
        throw new InternalServerError(`Unexpected response from DynamoDB. Expected a M-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
      return resolveObjectTypedValue(attributeValue.M, namedSchemaType, objectTypes);
    case "scalar":
      switch (namedSchemaType.name) {
        case ScalarType.String:
          if (attributeValue.S === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a S-typed attribute, received '${getDynamoAttributeType(attributeValue)}'`);
          return attributeValue.S;

        case ScalarType.Int:
          if (attributeValue.N === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a N-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
          return parseInt(attributeValue.N);

        case ScalarType.Float:
          if (attributeValue.N === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a N-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
          return parseFloat(attributeValue.N);

        case ScalarType.Boolean:
          if (attributeValue.BOOL === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a BOOL-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
          return attributeValue.BOOL;

        case ScalarType.Binary:
          if (attributeValue.B === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a B-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
          return toBase64(attributeValue.B);

        case ScalarType.Map:
          if (attributeValue.M === undefined)
            throw new InternalServerError(`Unexpected response from DynamoDB. Expected a M-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);
          return mapObjectValues(attributeValue.M, resolveUntypedValueFromAttributeValue);

        case ScalarType.List:
          if (attributeValue.L !== undefined) return attributeValue.L.map(resolveUntypedValueFromAttributeValue);
          if (attributeValue.SS !== undefined) return attributeValue.SS;
          if (attributeValue.NS !== undefined) return attributeValue.NS.map(parseFloat);
          if (attributeValue.BS !== undefined) return attributeValue.BS.map(toBase64);
          throw new InternalServerError(`Unexpected response from DynamoDB. Expected either an L, SS, NS or BS-typed attribute value, received '${getDynamoAttributeType(attributeValue)}'`);

        default:
          return unreachable(namedSchemaType["name"])
      }
    default:
      return unreachable(namedSchemaType["kind"])
  }
}

function resolveUntypedValueFromAttributeValue(attributeValue: AttributeValue): unknown {
  if (attributeValue.B !== undefined) return toBase64(attributeValue.B);
  if (attributeValue.BOOL !== undefined) return attributeValue.BOOL;
  if (attributeValue.BS !== undefined) return attributeValue.BS.map(toBase64);
  if (attributeValue.L !== undefined) return attributeValue.L.map(resolveUntypedValueFromAttributeValue);
  if (attributeValue.M !== undefined) return mapObjectValues(attributeValue.M, resolveUntypedValueFromAttributeValue);
  if (attributeValue.N !== undefined) return parseFloat(attributeValue.N);
  if (attributeValue.NS !== undefined) return attributeValue.NS.map(parseFloat);
  if (attributeValue.NULL !== undefined) return null;
  if (attributeValue.S !== undefined) return attributeValue.S;
  if (attributeValue.SS !== undefined) return attributeValue.SS;
}

function resolveObjectTypedValue(dynamoMapValue: Record<string, AttributeValue>, objectSchemaType: ObjectNamedType, objectTypes: ObjectTypes): unknown {
  // TODO: Add support for object types. Requires nested object type support in NDC fields
  throw new NotSupported("Returning object-typed attributes is not supported")
}

function resolveArrayTypedValueFromAttributeValue(attributeValue: AttributeValue, elementType: Type, objectTypes: ObjectTypes): unknown {
  if (attributeValue.SS !== undefined) {
    if (elementType.type !== "named" || elementType.name !== ScalarType.String)
      throw new InternalServerError(`Unexpected response from DynamoDB. Received an SS-typed attribute value when expecting an array of ${prettyPrintSchemaType(elementType)}`);
    return attributeValue.SS;
  }
  if (attributeValue.BS !== undefined) {
    if (elementType.type !== "named" || elementType.name !== ScalarType.Binary)
      throw new InternalServerError(`Unexpected response from DynamoDB. Received a BS-typed attribute value when expecting an array of ${prettyPrintSchemaType(elementType)}`);
    return attributeValue.BS.map(toBase64);
  }
  if (attributeValue.NS !== undefined) {
    if (elementType.type === "named") {
      if (elementType.name === ScalarType.Int) {
        return attributeValue.NS.map(parseInt);
      } else if (elementType.name === ScalarType.Float) {
        return attributeValue.NS.map(parseFloat);
      }
    }
    throw new InternalServerError(`Unexpected response from DynamoDB. Received a NS-typed attribute value when expecting an array of ${prettyPrintSchemaType(elementType)}`);
  }
  if (attributeValue.L !== undefined) {
    return attributeValue.L.map(attrVal => resolveTypedValueFromAttributeValue(attrVal, elementType, objectTypes));
  }
}

function getDynamoAttributeType(attributeValue: AttributeValue): DynamoAttributeType | undefined {
  if (attributeValue.B !== undefined) return "B";
  if (attributeValue.BOOL !== undefined) return "BOOL";
  if (attributeValue.BS !== undefined) return "BS";
  if (attributeValue.L !== undefined) return "L";
  if (attributeValue.M !== undefined) return "M";
  if (attributeValue.N !== undefined) return "N";
  if (attributeValue.NS !== undefined) return "NS";
  if (attributeValue.NULL !== undefined) return "NULL";
  if (attributeValue.S !== undefined) return "S";
  if (attributeValue.SS !== undefined) return "SS";
  return undefined;
}

function prettyPrintSchemaType(schemaType: Type): string {
  switch (schemaType.type) {
    case "nullable":
      return `nullable ${prettyPrintSchemaType(schemaType.underlying_type)}`;
    case "named":
      return schemaType.name;
    case "array":
      return `array of ${prettyPrintSchemaType(schemaType.element_type)}`
    default:
      return unreachable(schemaType["type"])
  }
}
