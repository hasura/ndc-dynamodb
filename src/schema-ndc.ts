import { ArgumentInfo, FunctionInfo, ObjectField, ObjectType, SchemaResponse, Type } from "@hasura/ndc-sdk-typescript";
import { TableSchema, ScalarType, dynamoAttributeTypeToType, AttributeSchema } from "./schema-dynamo";
import { ConfigurationPath, ConfigurationRangeError, InvalidConfigurationError, ObjectTypes } from "./configuration";
import { Err, Ok, Result } from "./result";
import { unreachable } from "./util";

export function createSchema(tableSchema: TableSchema[], objectTypes: ObjectTypes): Result<SchemaResponse, InvalidConfigurationError> {
  return createTableRowTypes(tableSchema, objectTypes)
    .bind(([tableRowTypeNames, tableRowTypes]) =>
      Result.traverseAndCollectErrors(
        tableSchema.map((table, tableIndex) =>
          createByKeysFunctionForTable(table, tableIndex, tableRowTypeNames[table.tableName], { ...objectTypes, ...tableRowTypes })
        )
      )
      .bind(generatedFunctionInfos => {
        const functionInfos = combineGenerated(...generatedFunctionInfos);
        const allObjectTypes = { ...objectTypes, ...tableRowTypes, ...functionInfos.newObjectTypes };

        return validateTypeUsages(functionInfos.generated, allObjectTypes)
          .bind(usedObjectTypes => new Ok({
            functions: functionInfos.generated,
            object_types: usedObjectTypes,
            collections: [],
            procedures: [],
            scalar_types: scalarTypes
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
          attributeSchemaAsObjectField(attributeSchema, tableSchemaIndex, attributeSchemaIndex, objectTypes)
        )
    )
    .map(rowFields => {
      const rowType: ObjectType = {
        description: `A row in the ${tableSchema.tableName} table`,
        fields: Object.fromEntries(rowFields)
      }

      return [tableRowTypeName, rowType]
    });
}

function attributeSchemaAsObjectField(attributeSchema: AttributeSchema, tableSchemaIndex: number, attributeSchemaIndex: number, objectTypes: ObjectTypes): Result<[string, ObjectField], ConfigurationRangeError[]> {
  const attributeType = attributeSchema.schemaType ?? dynamoAttributeTypeToType(attributeSchema.dynamoType)
  return validateUnderlyingType(attributeType, objectTypes, ["tables", tableSchemaIndex, "attributeSchema", attributeSchemaIndex, "schemaType"]).map(_ => {
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
    hashAttrIndex !== undefined
      ? attributeSchemaAsObjectField(tableSchema.attributeSchema[hashAttrIndex], tableSchemaIndex, hashAttrIndex, objectTypes)
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
          rangeAttrIndex !== undefined
            ? attributeSchemaAsObjectField(tableSchema.attributeSchema[rangeAttrIndex], tableSchemaIndex, rangeAttrIndex, objectTypes)
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

function createByKeysFunctionForTable(tableSchema: TableSchema, tableSchemaIndex: number, tableRowTypeName: string, objectTypes: ObjectTypes): Result<Generated<FunctionInfo>, ConfigurationRangeError[]> {
  return createTablePkObjectType(tableSchema, tableSchemaIndex, objectTypes)
    .map(([tablePkObjectTypeName, tablePkObjectType]) => {
      const args: Record<string, ArgumentInfo> = {
        keys: {
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
          name: `${tableSchema.tableName}_by_keys`,
          description: `Get one or more rows from ${tableSchema.tableName} by primary key`,
          arguments: args,
          result_type: {
            type: "array",
            element_type: {
              type: "named",
              name: tableRowTypeName
            }
          },
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
        const underlyingType = getUnderlyingType(usedType, []);
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
            validateUnderlyingType(objectField.type, allObjectTypes, ["objectTypes", objectTypeName, "fields", fieldName, "type"])
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

type UnderlyingType = {
  kind: "scalar",
  name: ScalarType,
  path: ConfigurationPath
} | {
  kind: "object",
  name: string,
  path: ConfigurationPath
}

function getUnderlyingType(type: Type, path: ConfigurationPath): UnderlyingType {
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
          return { kind: "scalar", name: type.name, path };
        default:
          return { kind: "object", name: type.name, path };
      }
    case "nullable":
      return getUnderlyingType(type.underlying_type, [...path, "underlying_type"]);
    case "array":
      return getUnderlyingType(type.element_type, [...path, "element_type"]);
    default:
      return unreachable(type["type"]);
  }
}

function validateUnderlyingType(type: Type, objectTypes: ObjectTypes, errPath: ConfigurationPath): Result<UnderlyingType, ConfigurationRangeError[]> {
  const underlyingType = getUnderlyingType(type, errPath);
  if (underlyingType.kind === "object" && !(underlyingType.name in objectTypes)) {
    return new Err([{
      path: underlyingType.path,
      message: `The named type '${underlyingType.name}' is neither a scalar type nor a declared object type`
    }]);
  }
  return new Ok(underlyingType);
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
