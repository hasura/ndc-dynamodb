
import { FunctionDefinition, createSchema } from "../src/schema-ndc";
import { ScalarType, TableSchema } from "../src/schema-dynamo";
import { expectErr, expectOk } from "./util";
import { ArgumentInfo, FunctionInfo } from "@hasura/ndc-sdk-typescript";
import { InvalidConfigurationError, ObjectTypes } from "../src/configuration";

const consistentReadArgument: ArgumentInfo = {
  description: "The consistency of a read operation. If set to true, then a strongly consistent read is used; otherwise, an eventually consistent read is used.",
  type: {
    type: "named",
    name: ScalarType.Boolean
  }
}

describe("by_keys functions", () => {
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

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
    }

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

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
    };

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

    const expectedFunctionDef: FunctionDefinition = {
      type: "by_keys",
      tableSchema: tableSchema,
      functionInfo: artistsFunctionInfo,
    };

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

    expect(schema.functions).toHaveProperty("Artists_by_keys");
    expect(schema.functions["Artists_by_keys"]).toEqual(expectedFunctionDef);
    expect(schema.schemaResponse.object_types).toEqual(expectedObjectTypes);
  })

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
});
