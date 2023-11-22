
import { FunctionDefinition, createSchema } from "../src/schema-ndc";
import { ScalarType, TableSchema } from "../src/schema-dynamo";
import { expectErr, expectOk } from "./util";
import { ArgumentInfo, FunctionInfo } from "@hasura/ndc-sdk-typescript";
import { InvalidConfigurationError, ObjectTypes } from "../src/configuration";

const consistentReadArgument: ArgumentInfo = {
  description: "The consistency of a read operation. If set to true, then a strongly consistent read is used; otherwise, an eventually consistent read is used.",
  type: {
    type: "nullable",
    underlying_type: {
      type: "named",
      name: ScalarType.Boolean
    }
  }
}

describe("by_keys function", () => {
  const artistsFunctionInfo: FunctionInfo = {
    name: "Artists_by_keys",
    description: "Get one or more rows from the 'Artists' table by primary key",
    arguments: {
      keys: {
        description: "The primary keys to look up the rows with",
        type: {
          type: "array",
          element_type: {
            type: "named",
            name: "Artists_pk",
          }
        }
      },
      consistent_read: consistentReadArgument
    },
    result_type: {
      type: "array",
      element_type: {
        type: "named",
        name: "Artists_row"
      }
    },
  };

  it("table with only partition key", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const schema = expectOk(createSchema([tableSchema], {}));

    const expectedObjectTypes: ObjectTypes = {
      Artists_pk: {
        description: "Values of the primary key for the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          }
        }
      },
      Artists_row: {
        description: "A row in the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          }
        }
      }
    }

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
      primaryKeySchema: {
        hashKeySchema: {
          attributeName: "ArtistName",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        },
        rangeKeySchema: null
      },
      tableRowType: expectedObjectTypes["Artists_row"]!,
    }

    expect(schema.functions).toHaveProperty("Artists_by_keys");
    expect(schema.functions["Artists_by_keys"]).toEqual(expectedFunctionDef);
    expect(schema.schemaResponse.object_types).toEqual(expectedObjectTypes);
  })

  it("table with both partition key and hash key", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "AlbumTitle",
          dynamoType: "S",
          description: "The title of the album",
          schemaType: { type: "named", name: "String" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: "AlbumTitle"
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const schema = expectOk(createSchema([tableSchema], {}));

    const expectedObjectTypes: ObjectTypes = {
      Artists_pk: {
        description: "Values of the primary key for the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          },
          AlbumTitle: {
            description: "The title of the album",
            type: { type: "named", name: "String" }
          }
        }
      },
      Artists_row: {
        description: "A row in the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          },
          AlbumTitle: {
            description: "The title of the album",
            type: { type: "named", name: "String" }
          }
        }
      }
    }

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
      primaryKeySchema: {
        hashKeySchema: {
          attributeName: "ArtistName",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        },
        rangeKeySchema: {
          attributeName: "AlbumTitle",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        }
      },
      tableRowType: expectedObjectTypes["Artists_row"]!,
    };

    expect(schema.functions).toHaveProperty("Artists_by_keys");
    expect(schema.functions["Artists_by_keys"]).toEqual(expectedFunctionDef);
    expect(schema.schemaResponse.object_types).toEqual(expectedObjectTypes);
  })

  it("table with non-key attributes", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "AlbumTitle",
          dynamoType: "S",
          description: "The title of the album",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ReleaseYear",
          dynamoType: "N",
          description: "The release year of the album",
          schemaType: { type: "named", name: "Int" }
        },
        {
          name: "SalePrice",
          dynamoType: "N",
          description: "The sale price of the album",
          // No explicit schema type
        },
        {
          name: "Tracks",
          dynamoType: "L",
          description: "List of tracks in the album",
          schemaType: { type: "array", element_type: { type: "named", name: "Track" } }
        },
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: "AlbumTitle"
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      Track: {
        description: "A track in an album",
        fields: {
          Name: {
            description: "The name of the track",
            type: {
              type: "named",
              name: "String"
            }
          },
          DurationSeconds: {
            description: "The duration of the track in seconds",
            type: {
              type: "named",
              name: "Int"
            }
          }
        }
      }
    }

    const schema = expectOk(createSchema([tableSchema], customObjectTypes));

    const expectedObjectTypes: ObjectTypes = {
      ...customObjectTypes,
      Artists_pk: {
        description: "Values of the primary key for the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          },
          AlbumTitle: {
            description: "The title of the album",
            type: { type: "named", name: "String" }
          }
        }
      },
      Artists_row: {
        description: "A row in the 'Artists' table",
        fields: {
          ArtistName: {
            description: "The name of the artist",
            type: { type: "named", name: "String" }
          },
          AlbumTitle: {
            description: "The title of the album",
            type: { type: "named", name: "String" }
          },
          ReleaseYear: {
            description: "The release year of the album",
            type: { type: "named", name: "Int" }
          },
          SalePrice: {
            description: "The sale price of the album",
            type: { type: "nullable", underlying_type: { type: "named", name: "Float" } }
          },
          Tracks: {
            description: "List of tracks in the album",
            type: { type: "array", element_type: { type: "named", name: "Track" } }
          }
        }
      }
    }

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
      primaryKeySchema: {
        hashKeySchema: {
          attributeName: "ArtistName",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        },
        rangeKeySchema: {
          attributeName: "AlbumTitle",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        }
      },
      tableRowType: expectedObjectTypes["Artists_row"]!,
    };

    expect(schema.functions).toHaveProperty("Artists_by_keys");
    expect(schema.functions["Artists_by_keys"]).toEqual(expectedFunctionDef);
    expect(schema.schemaResponse.object_types).toEqual(expectedObjectTypes);
  })

  it("errors when defining an object type with the same name as the primary key type", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      Artists_pk: {
        fields: {
          Test: {
            type: {
              type: "named",
              name: "String"
            }
          }
        }
      }
    }

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["objectTypes", "Artists_pk"],
          message: "The object type name 'Artists_pk' is already in use by the primary key type generated for the 'Artists' table name. Please use a different object type name."
        }
      ]
    }

    expect(error).toEqual(expectedError);
  })

  it("errors when failing to define attribute schemas for hash and range keys", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: "AlbumTitle"
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const error = expectErr(createSchema([tableSchema], {}));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "keySchema", "hashKeyAttributeName"],
          message: "Cannot find an attribute schema defined for the specified hash key attribute 'ArtistName'"
        },
        { path: ["tables", 0, "keySchema", "rangeKeyAttributeName"],
          message: "Cannot find an attribute schema defined for the specified range key attribute 'AlbumTitle'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  })

  it("errors when defining a nullable schema type for a hash or range key attribute", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "String" } }
        },
        {
          name: "AlbumTitle",
          dynamoType: "S",
          description: "The title of the album",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "String" } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: "AlbumTitle"
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        {
          path: ["tables", 0, "attributeSchema", 0, "schemaType"],
          message: "Attributes used as hash or range keys cannot be be nullable"
        },
        {
          path: ["tables", 0, "attributeSchema", 1, "schemaType"],
          message: "Attributes used as hash or range keys cannot be be nullable"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });
});

describe("attribute schema type validations", () => {

  it("errors when defining an object type with the same name as the row type", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      Artists_row: {
        fields: {
          Test: {
            type: {
              type: "named",
              name: "String"
            }
          }
        }
      }
    }

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["objectTypes", "Artists_row"],
          message: "The object type name 'Artists_row' is already in use by the row type generated for the 'Artists' table name. Please use a different object type name."
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining an attribute schema type that is doubly nullable", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "BustedAttribute",
          dynamoType: "S",
          schemaType: { type: "nullable", underlying_type: { type: "nullable", underlying_type: { type: "named", name: "String" } } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 1, "schemaType", "underlying_type"],
          message: "The underlying type of a nullable type cannot be nullable"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining an array attribute schema type when the attribute isn't an array type", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidStringListAttribute",
          dynamoType: "L",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        },
        {
          name: "ValidStringSetAttribute",
          dynamoType: "SS",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "S",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "An array type cannot be used when the dynamoType of the attribute is not an array (valid array dynamoType values: L,SS,NS,BS)"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-string array attribute schema type when the attribute is a string set", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidAttribute",
          dynamoType: "SS",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "SS",
          schemaType: { type: "array", element_type: { type: "named", name: "Number" } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 2, "schemaType", "element_type"],
          message: "The element_type of the array type of a String Set (SS) attribute must be the String named type"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-number array attribute schema type when the attribute is a number set", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidIntAttribute",
          dynamoType: "NS",
          schemaType: { type: "array", element_type: { type: "named", name: "Int" } }
        },
        {
          name: "ValidFloatAttribute",
          dynamoType: "NS",
          schemaType: { type: "array", element_type: { type: "named", name: "Float" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "NS",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType", "element_type"],
          message: "The element_type of the array type of a Number Set (NS) attribute must be either the Float or the Int named type"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-binary array attribute schema type when the attribute is a binary set", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidAttribute",
          dynamoType: "BS",
          schemaType: { type: "array", element_type: { type: "named", name: "Binary" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "BS",
          schemaType: { type: "array", element_type: { type: "named", name: "String" } }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 2, "schemaType", "element_type"],
          message: "The element_type of the array type of a Binary Set (BS) attribute must be the Binary named type"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-string attribute schema type when the attribute is a string", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidStringAttribute",
          dynamoType: "S",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidNullableStringAttribute",
          dynamoType: "S",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "String" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "S",
          schemaType: { type: "named", name: "Int" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "The scalar named type 'Int' can only be used with attributes of dynamoType 'N'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-number attribute schema type when the attribute is a number", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidIntAttribute",
          dynamoType: "N",
          schemaType: { type: "named", name: "Int" }
        },
        {
          name: "ValidNullableIntAttribute",
          dynamoType: "N",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "Int" } }
        },
        {
          name: "ValidFloatAttribute",
          dynamoType: "N",
          schemaType: { type: "named", name: "Float" }
        },
        {
          name: "ValidNullableFloatAttribute",
          dynamoType: "N",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "Float" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "N",
          schemaType: { type: "named", name: "String" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 5, "schemaType"],
          message: "The scalar named type 'String' can only be used with attributes of dynamoType 'S'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-boolean attribute schema type when the attribute is a boolean", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidBooleanAttribute",
          dynamoType: "BOOL",
          schemaType: { type: "named", name: "Boolean" }
        },
        {
          name: "ValidNullableBooleanAttribute",
          dynamoType: "BOOL",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "Boolean" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "BOOL",
          schemaType: { type: "named", name: "Float" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "The scalar named type 'Float' can only be used with attributes of dynamoType 'N'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-binary attribute schema type when the attribute is a binary", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidBinaryAttribute",
          dynamoType: "B",
          schemaType: { type: "named", name: "Binary" }
        },
        {
          name: "ValidNullableBinaryAttribute",
          dynamoType: "B",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "Binary" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "B",
          schemaType: { type: "named", name: "Boolean" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {}

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "The scalar named type 'Boolean' can only be used with attributes of dynamoType 'BOOL'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining a non-map scalar named attribute schema type when the attribute is a map", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidMapAttribute",
          dynamoType: "M",
          schemaType: { type: "named", name: "Map" }
        },
        {
          name: "ValidNullableMapAttribute",
          dynamoType: "M",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "Map" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "M",
          schemaType: { type: "named", name: "Binary" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      MyObjectType: {
        fields: {
          MyField: {
            type: { type: "named", name: "String" }
          }
        }
      }
    }

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "The scalar named type 'Binary' can only be used with attributes of dynamoType 'B'"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining an object type attribute schema type when the attribute is not map", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidObjectTypeAttribute",
          dynamoType: "M",
          schemaType: { type: "named", name: "MyObjectType" }
        },
        {
          name: "ValidNullableObjectTypeAttribute",
          dynamoType: "M",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "MyObjectType" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "N",
          schemaType: { type: "named", name: "MyObjectType" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      MyObjectType: {
        fields: {
          MyField: {
            type: { type: "named", name: "String" }
          }
        }
      }
    }

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "Object types can only be used with attributes that of dynamoType 'M' (Map)"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });

  it("errors when defining an object type attribute schema type when the object type has not been defined", () => {
    const tableSchema: TableSchema = {
      tableName: "Artists",
      attributeSchema: [
        {
          name: "ArtistName",
          dynamoType: "S",
          description: "The name of the artist",
          schemaType: { type: "named", name: "String" }
        },
        {
          name: "ValidObjectTypeAttribute",
          dynamoType: "M",
          schemaType: { type: "named", name: "MyObjectType" }
        },
        {
          name: "ValidNullableObjectTypeAttribute",
          dynamoType: "M",
          schemaType: { type: "nullable", underlying_type: { type: "named", name: "MyObjectType" } }
        },
        {
          name: "BustedAttribute",
          dynamoType: "M",
          schemaType: { type: "named", name: "MyUndefinedObjectType" }
        }
      ],
      keySchema: {
        hashKeyAttributeName: "ArtistName",
        rangeKeyAttributeName: null
      },
      globalSecondaryIndexes: [],
      localSecondaryIndexes: []
    }

    const customObjectTypes: ObjectTypes = {
      MyObjectType: {
        fields: {
          MyField: {
            type: { type: "named", name: "String" }
          }
        }
      }
    }

    const error = expectErr(createSchema([tableSchema], customObjectTypes));

    const expectedError: InvalidConfigurationError = {
      type: "InvalidConfiguration",
      ranges: [
        { path: ["tables", 0, "attributeSchema", 3, "schemaType"],
          message: "The named type 'MyUndefinedObjectType' is neither a scalar type nor a declared object type"
        }
      ]
    }

    expect(error).toEqual(expectedError);
  });
})
