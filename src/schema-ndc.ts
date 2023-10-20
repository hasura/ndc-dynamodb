import { ArgumentInfo, FunctionInfo, ObjectField, ObjectType, SchemaResponse, Type } from "@hasura/ndc-sdk-typescript";
import { TableSchema, ScalarType, dynamoAttributeTypeToType, AttributeSchema, DynamoAttributeType, dynamoArrayTypes, scalarTypeToDynamoAttributeType } from "./schema-dynamo";
import { ConfigurationPath, ConfigurationRangeError, InvalidConfigurationError, ObjectTypes } from "./configuration";
import { Err, Ok, Result } from "./result";
import { unreachable } from "./util";

export type ConnectorSchema = {
  schemaResponse: SchemaResponse,
  functions: {
    [functionName: string]: FunctionDefinition
  }
}

export type FunctionDefinition = ByKeysFunction | QueryFunction

export type ByKeysFunction = {
  type: "by_keys"
  tableSchema: TableSchema
  functionInfo: FunctionInfo
}

export type QueryFunction = {
  type: "query"
  tableSchema: TableSchema
  functionInfo: FunctionInfo
}

export function createSchema(tableSchema: TableSchema[], objectTypes: ObjectTypes): Result<ConnectorSchema, InvalidConfigurationError> {
  return createTableRowTypes(tableSchema, objectTypes)
    .bind(([tableRowTypeNames, tableRowTypes]) =>
      Result.traverseAndCollectErrors(
        tableSchema.map((table, tableIndex) =>
          createByKeysFunctionForTable(table, tableIndex, tableRowTypeNames[table.tableName], { ...objectTypes, ...tableRowTypes })
        )
      )
      .bind(generatedFunctionDefinitions => {
        const functionDefinitions = combineGenerated(...generatedFunctionDefinitions);
        const functionInfos = functionDefinitions.generated.map(def => def.functionInfo);
        const allObjectTypes = { ...objectTypes, ...tableRowTypes, ...functionDefinitions.newObjectTypes };

        return validateTypeUsages(functionInfos, allObjectTypes)
          .bind(usedObjectTypes => new Ok({
            schemaResponse: {
              functions: functionInfos,
              object_types: usedObjectTypes,
              collections: [],
              procedures: [],
              scalar_types: scalarTypes
            },
            functions: Object.fromEntries(functionDefinitions.generated.map(def => [def.functionInfo.name, def]))
          }));
      })
    )
    .mapErr(errors =>({
      type: "InvalidConfiguration",
      ranges: errors
    }));
}

type TableRowTypeNames = {
  [tableName: string]: string
}

function createTableRowTypes(tableSchema: TableSchema[], objectTypes: ObjectTypes): Result<[TableRowTypeNames, ObjectTypes], ConfigurationRangeError[]> {
  const errors: ConfigurationRangeError[] = [];
  const rowTypes: ObjectTypes = {};
  const tableRowTypeNames: TableRowTypeNames = {};

  tableSchema.forEach((table, index) => {
    const result = createTableRowType(table, index, objectTypes);
    if (result instanceof Ok) {
      rowTypes[result.data[0]] = result.data[1];
      tableRowTypeNames[table.tableName] = result.data[0];
    } else {
      errors.push(...result.error);
    }
  });

  if (errors.length > 0) {
    return new Err(errors)
  } else {
    return new Ok([tableRowTypeNames, rowTypes]);
  }
}

function createTableRowType(tableSchema: TableSchema, tableSchemaIndex: number, objectTypes: ObjectTypes): Result<[string, ObjectType], ConfigurationRangeError[]> {
  const tableRowTypeName = `${tableSchema.tableName}_row`;
  if (tableRowTypeName in objectTypes) {
    return new Err([{
      path: ["objectTypes", tableRowTypeName],
      message: `The object type name '${tableRowTypeName}' is already in use by the row type generated for the '${tableSchema.tableName}' table name. Please use a different object type name.`
    }]);
  }

  return Result.traverseAndCollectErrors(
      tableSchema.attributeSchema
        .map((attributeSchema, attributeSchemaIndex) =>
          attributeSchemaAsObjectField(attributeSchema, false, tableSchemaIndex, attributeSchemaIndex, objectTypes)
        )
    )
    .map(rowFields => {
      const rowType: ObjectType = {
        description: `A row in the '${tableSchema.tableName}' table`,
        fields: Object.fromEntries(rowFields)
      }

      return [tableRowTypeName, rowType]
    });
}

function dynamoAttributeTypeToNullableType(attributeType: DynamoAttributeType): Type {
  return { type: "nullable", underlying_type: dynamoAttributeTypeToType(attributeType) }
}

function attributeSchemaAsObjectField(attributeSchema: AttributeSchema, isPrimaryKeyAttribute: boolean, tableSchemaIndex: number, attributeSchemaIndex: number, objectTypes: ObjectTypes): Result<[string, ObjectField], ConfigurationRangeError[]> {
  const attributeType =
    attributeSchema.schemaType
    ?? (isPrimaryKeyAttribute
          ? dynamoAttributeTypeToType(attributeSchema.dynamoType)
          : dynamoAttributeTypeToNullableType(attributeSchema.dynamoType)
        );

  const schemaTypePath = ["tables", tableSchemaIndex, "attributeSchema", attributeSchemaIndex, "schemaType"];
  return validateAttributeSchemaType(attributeType, schemaTypePath, attributeSchema.dynamoType, isPrimaryKeyAttribute, objectTypes)
    .map(_ => {
      const objectField: ObjectField = {
        type: attributeType,
        description: attributeSchema.description
      };
      return [attributeSchema.name, objectField];
    });
}

function createTablePkObjectType(tableSchema: TableSchema, tableSchemaIndex: number, objectTypes: ObjectTypes): Result<[string, ObjectType], ConfigurationRangeError[]> {
  const tablePkObjectTypeName = new Ok<string,ConfigurationRangeError[]>(`${tableSchema.tableName}_pk`)
    .bind<string>(tablePkObjectTypeName =>
      tablePkObjectTypeName in objectTypes
        ? new Err([{
          path: ["objectTypes", tablePkObjectTypeName],
          message: `The object type name '${tablePkObjectTypeName}' is already in use by the primary key type generated for the '${tableSchema.tableName}' table name. Please use a different object type name.`
        }])
        : new Ok(tablePkObjectTypeName)
    );

  const hashAttrIndex = tableSchema.attributeSchema.findIndex(attr => attr.name === tableSchema.keySchema.hashKeyAttributeName)
  const hashAttrField: Result<{[k: string]: ObjectField}, ConfigurationRangeError[]> =
    hashAttrIndex !== -1
      ? attributeSchemaAsObjectField(tableSchema.attributeSchema[hashAttrIndex], true, tableSchemaIndex, hashAttrIndex, objectTypes)
          .map(([key, value]) => ({[key]: value}))
      : new Err([{
          path: ["tables", tableSchemaIndex, "keySchema", "hashKeyAttributeName"],
          message: `Cannot find an attribute schema defined for the specified hash key attribute '${tableSchema.keySchema.hashKeyAttributeName}'`
        }]);

  const rangeAttrField: Result<{[k: string]: ObjectField}, ConfigurationRangeError[]> =
    tableSchema.keySchema.rangeKeyAttributeName === null
      ? new Ok({})
      : new Ok<number, ConfigurationRangeError[]>(tableSchema.attributeSchema.findIndex(attr => attr.name === tableSchema.keySchema.rangeKeyAttributeName))
        .bind(rangeAttrIndex =>
          rangeAttrIndex !== -1
            ? attributeSchemaAsObjectField(tableSchema.attributeSchema[rangeAttrIndex], true, tableSchemaIndex, rangeAttrIndex, objectTypes)
                .map(([key, value]) => ({[key]: value}))
            : new Err([{
                path: ["tables", tableSchemaIndex, "keySchema", "rangeKeyAttributeName"],
                message: `Cannot find an attribute schema defined for the specified range key attribute '${tableSchema.keySchema.rangeKeyAttributeName}'`
              }])
        );

  return Result.collectErrors3(tablePkObjectTypeName, hashAttrField, rangeAttrField)
    .map(([tablePkObjectTypeName, hashAttrField, rangeAttrField]) => {
      const objectType: ObjectType = {
        description: `Values of the primary key for the '${tableSchema.tableName}' table`,
        fields: {
          ...hashAttrField,
          ...rangeAttrField,
        }
      }
      return [tablePkObjectTypeName, objectType];
    });
}

function createByKeysFunctionForTable(tableSchema: TableSchema, tableSchemaIndex: number, tableRowTypeName: string, objectTypes: ObjectTypes): Result<Generated<FunctionDefinition>, ConfigurationRangeError[]> {
  return createTablePkObjectType(tableSchema, tableSchemaIndex, objectTypes)
    .map(([tablePkObjectTypeName, tablePkObjectType]) => {
      const args: Record<string, ArgumentInfo> = {
        keys: {
          description: "The primary keys to look up the rows with",
          type: {
            type: "array",
            element_type: {
              type: "named",
              name: tablePkObjectTypeName,
            }
          }
        },
        consistent_read: {
          description: "The consistency of a read operation. If set to true, then a strongly consistent read is used; otherwise, an eventually consistent read is used.",
          type: {
            type: "named",
            name: ScalarType.Boolean
          }
        }
      }

      return {
        generated: {
          type: "by_keys",
          tableSchema: tableSchema,
          functionInfo: {
            name: `${tableSchema.tableName}_by_keys`,
            description: `Get one or more rows from the '${tableSchema.tableName}' table by primary key`,
            arguments: args,
            result_type: {
              type: "array",
              element_type: {
                type: "named",
                name: tableRowTypeName
              }
            },
          }
        },
        newObjectTypes: {
          [tablePkObjectTypeName]: tablePkObjectType
        }
      };
    });
}

function validateTypeUsages(functionInfos: FunctionInfo[], allObjectTypes: ObjectTypes): Result<ObjectTypes, ConfigurationRangeError[]> {
  // Get all the object types used in function arguments and return types
  const objectTypesInFunctions =
    functionInfos
      .flatMap(functionInfo =>
        [ ...Object.values(functionInfo.arguments).map(arg => arg.type),
          functionInfo.result_type
        ]
      )
      .flatMap<[string, ObjectType]>(usedType => {
        const underlyingType = getUnderlyingNamedType(usedType, []);
        return underlyingType.kind === "object"
          ? [[underlyingType.name, allObjectTypes[underlyingType.name]]]
          : []
        });

  const usedObjectTypes: ObjectTypes = {};
  const errors: ConfigurationRangeError[] = [];
  let objectTypesToValidate: [string, ObjectType][] = objectTypesInFunctions;
  while (objectTypesToValidate.length > 0) {

    // Record these object types as ones we've seen before
    for (const [objectTypeName, objectType] of objectTypesToValidate) {
      usedObjectTypes[objectTypeName] = objectType;
    }

    // Validate the fields of these object types
    const fieldTypeValidationResults = Result.partitionAndCollectErrors(
      objectTypesInFunctions
        .flatMap(([objectTypeName, objectType]) =>
          Object.entries(objectType.fields).map(([fieldName, objectField]) =>
            validateObjectFieldType(objectField.type, ["objectTypes", objectTypeName, "fields", fieldName, "type"], allObjectTypes)
          )
        )
    );

    // Get any object types used in the fields
    const objectTypesUsedInFields =
      fieldTypeValidationResults.oks
        .flatMap<[string, ObjectType]>(underlyingFieldType =>
          underlyingFieldType.kind === "object"
            ? [[underlyingFieldType.name, allObjectTypes[underlyingFieldType.name]]]
            : []
          );

    // Validate any object types we haven't seen before in the next loop iteration
    objectTypesToValidate = objectTypesUsedInFields.filter(([objectTypeName, _objectType]) => !(objectTypeName in usedObjectTypes));

    // Record any errors
    errors.push(...fieldTypeValidationResults.errs);
  }

  return errors.length <= 0
    ? new Ok(usedObjectTypes)
    : new Err(errors);
}

type UnderlyingNamedType = (ScalarNamedType | ObjectNamedType) & { path: ConfigurationPath }

type ScalarNamedType = {
  kind: "scalar",
  name: ScalarType
}

type ObjectNamedType = {
  kind: "object"
  name: string,
}

function getUnderlyingNamedType(type: Type, typePath: ConfigurationPath): UnderlyingNamedType {
  switch (type.type) {
    case "named":
      switch (type.name) {
        case ScalarType.String:
        case ScalarType.Int:
        case ScalarType.Float:
        case ScalarType.Boolean:
        case ScalarType.Binary:
        case ScalarType.Map:
        case ScalarType.List:
          return { kind: "scalar", name: type.name, path: typePath };
        default:
          return { kind: "object", name: type.name, path: typePath };
      }
    case "nullable":
      return getUnderlyingNamedType(type.underlying_type, [...typePath, "underlying_type"]);
    case "array":
      return getUnderlyingNamedType(type.element_type, [...typePath, "element_type"]);
    default:
      return unreachable(type["type"]);
  }
}

function validateObjectFieldType(objectFieldType: Type, objectFieldTypePath: ConfigurationPath, objectTypes: ObjectTypes): Result<UnderlyingNamedType, ConfigurationRangeError[]> {
  return validateAndUnwrapNullableSchemaType(objectFieldType, objectFieldTypePath)
    .bind(([nonNullableType, nonNullableTypePath]) => {
      const underlyingType = getUnderlyingNamedType(nonNullableType, nonNullableTypePath);
      return underlyingType.kind === "object"
        ? validateNamedObjectType(underlyingType, underlyingType.path, objectTypes).map<UnderlyingNamedType>(_ => underlyingType)
        : new Ok(underlyingType)
    });
}

function validateNamedObjectType(objectNamedType: ObjectNamedType, typePath: ConfigurationPath, objectTypes: ObjectTypes): Result<undefined, ConfigurationRangeError[]> {
  if (!(objectNamedType.name in objectTypes)) {
    return new Err([{
      path: typePath,
      message: `The named type '${objectNamedType.name}' is neither a scalar type nor a declared object type`
    }]);
  }
  return new Ok(undefined);
}

function validateAttributeSchemaType(schemaType: Type, schemaTypePath: ConfigurationPath, attributeDynamoType: DynamoAttributeType, isPrimaryKeyAttribute: boolean, objectTypes: ObjectTypes): Result<Type, ConfigurationRangeError[]> {
  if (schemaType.type === "nullable" && isPrimaryKeyAttribute) {
    return new Err([{
      path: schemaTypePath,
      message: "Attributes used as hash or range keys cannot be be nullable"
    }])
  }
  return validateAndUnwrapNullableSchemaType(schemaType, schemaTypePath)
    .bind(([nonNullableType, nonNullableTypePath]) => {
      if (nonNullableType.type === "array") {
        if (dynamoArrayTypes.includes(attributeDynamoType) === false) {
          return new Err([{
            path: nonNullableTypePath,
            message: `An array type cannot be used when the dynamoType of the attribute is not an array (valid array dynamoType values: ${dynamoArrayTypes.join(",")})`
          }]);
        }
        if (attributeDynamoType === "SS" && nonNullableType.element_type.type === "named" && nonNullableType.element_type.name !== ScalarType.String) {
          return new Err([{
            path: [...nonNullableTypePath, "element_type"],
            message: "The element_type of the array type of a String Set (SS) attribute must be the String named type"
          }]);
        }
        if (attributeDynamoType === "NS" && nonNullableType.element_type.type === "named" && nonNullableType.element_type.name !== ScalarType.Int && nonNullableType.element_type.name !== ScalarType.Float) {
          return new Err([{
            path: [...nonNullableTypePath, "element_type"],
            message: "The element_type of the array type of a Number Set (NS) attribute must be either the Float or the Int named type"
          }]);
        }
        if (attributeDynamoType === "BS" && nonNullableType.element_type.type === "named" && nonNullableType.element_type.name !== ScalarType.Binary) {
          return new Err([{
            path: [...nonNullableTypePath, "element_type"],
            message: "The element_type of the array type of a Binary Set (BS) attribute must be the Binary named type"
          }]);
        }
        return new Ok(schemaType);
      } else { // Object and Scalar Named Types
        const namedType = getUnderlyingNamedType(nonNullableType, nonNullableTypePath);
        if (namedType.kind == "scalar") {
          const expectedDynamoType = scalarTypeToDynamoAttributeType(namedType.name)
          if (attributeDynamoType !== expectedDynamoType) {
            return new Err([{
              path: namedType.path,
              message: `The scalar named type '${namedType.name}' can only be used with attributes of dynamoType '${expectedDynamoType}'`
            }]);
          }
          return new Ok(schemaType);
        } else { // Object type
          if (attributeDynamoType !== "M") {
            return new Err([{
              path: namedType.path,
              message: `Object types can only be used with attributes that of dynamoType 'M' (Map)`
            }]);
          }
          return validateNamedObjectType(namedType, namedType.path, objectTypes).map(_ => schemaType)
        }
      }
    });
}

function validateAndUnwrapNullableSchemaType(schemaType: Type, schemaTypePath: ConfigurationPath): Result<[NonNullableType, ConfigurationPath], ConfigurationRangeError[]> {
  if (schemaType.type !== "nullable")
    return new Ok([schemaType, schemaTypePath]);

  if (schemaType.underlying_type.type === "nullable") {
    return new Err([{
      path: [...schemaTypePath, "underlying_type"],
      message: "The underlying type of a nullable type cannot be nullable"
    }])
  }

  return new Ok([schemaType.underlying_type, [...schemaTypePath, "underlying_type"]]);
}

const scalarTypes = {
  [ScalarType.String]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.Int]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.Float]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.Boolean]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.Binary]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.Map]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
  [ScalarType.List]: {
    comparison_operators: {},
    aggregate_functions: {},
    update_operators: {}
  },
};

type Generated<T> = {
  generated: T,
  newObjectTypes: ObjectTypes
}

function combineGenerated<T>(...generateds: Generated<T>[]): Generated<T[]> {
  return {
    generated: generateds.map(g => g.generated),
    newObjectTypes: Object.assign({}, ...generateds.map(g => g.newObjectTypes))
  }
}

type NonNullableType = Exclude<Type, {type: "nullable"}>
