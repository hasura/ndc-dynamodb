# Limitations of Queries
- No relationships
- No aggregations
  - Can do a COUNT but it costs the same as reading the rows, hardly worth it
- No schema (other than partition key and optional sort key)
- Filtering
  - Arbitrary filtering is very dangerous, performance and cost wise
  - Most queries should use a partition key
  - Filtering should be performed with partition key == value and optionally filtering by a sort key
  - Anything else results in an entire table scan ($$$)
  - Further filtering is done in server memory and costs the same as not doing it
  - Exists not supported
- Ordering
  - Only by sort key within a partition
- Pagination
  - No offset
  - Limit supported
- Global secondary indexes
  - Should appear like a normal collection (need to be queried separately)
  - No mutations
  - No query by primary key (duplicate PKs allowed)
  - Different schema to main table
- Local secondary indexes
  - Need to be queried explicitly. These could either appear as separate collections, or alternatively, the index name could be used as an optional collection parameter (how does this affect query by PK which is not allowed?), or we could determine which index to use based on the sort key requested.
  - Parameter required:
     - strongly/eventually consistent reads
  - No mutations
  - No query by primary key (duplicate PKs allowed)
  - Same schema to the main table (although worse performance when querying non-index projected columns)

# Design Notes
## Table Queries
Potentially could put the partition key equality filter as a table parameter ([key condition expression](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditions)). Also could make a custom filter expression for the sort key.
Omitting the key condition expression parameter would result in a table scan.
Could automatically switch between local secondary indexes by exposing the additional sort keys as table parameter options. How would we prevent querying multiple at once (not allowed!)?

However, can permissions push values into table parameters? Or only where?

```graphql
# Uses Query API command, or Scan if no where_key specified (scan_index cannot be used in that case)
my_ddb_table_query(
  where_key: { # Table parameter
    my_hash_key: "value",
    my_range_key: {
      # Also has _lt, _gt, _lte, _gte, _eq, _begins_with
      _between: {
        _gte: 10
        _lte: 20
      }
    }
  },
  where: { # No hash and sort key attributes
    _or: [
      { my_prop: { _eq: 10 } },
      { my_prop2: { _eq: 20 } },
    ]
  },
  order_by: { # Only the sort key allowed here
    my_sort_key: asc
  },
  # offset not supported
  limit: 5,
  consistent_read: false # Table parameter
) {
  my_hash_key
  my_sort_key
  my_map {
    map_prop_a
  }
  my_prop
  my_prop2
}

# Uses GetItem API command
my_ddb_table_by_pk(
  my_hash_key: "value",
  my_sort_key: 10,
  consistent_read: false # Table parameter
) {
  my_hash_key
  my_sort_key
}
```

## Queries as Functions
Alternatively, since Dynamo doesn't support a lot of query features, we could expose the tables as functions, exposing the necessary input parameters as function parameters.

However, this may prevent the permissions system from working, because where could it push the predicate to?

```graphql
# Uses Query API command, or Scan if no key_condition specified (scan_index cannot be used in that case)
my_ddb_table_query(
  key_condition: {
    my_hash_key: "value",
    my_sort_key: {
      # Also has _lt, _gt, _lte, _gte, _eq, _begins_with - you can only use one at a time, more than one is an error
      _between: {
        _gte: 10
        _lte: 20
      }
    },
    # Local secondary indexes would have sort their keys here too, but you can only specify one sort key at a time, or error
  },
  filter: { # No hash and sort key attributes (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression)
    _or: [
      { my_prop: { _eq: 10 } },
      { my_prop2: { _eq: 20 } },
    ]
    # Operators: _eq, _ne, _gt, _gte, _lt, _lte, _not, _in, _attribute_exists, _attribute_type, _begins_with, _contains
    # There is also _size that can be used on the LHS... but this might be out of scope
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
  }
  limit: 5,
  scan_index: forward
  consistent_read: false
) {
  my_hash_key
  my_sort_key
  my_map {
    map_prop_a
  }
  my_prop
  my_prop2
}

# Uses BatchGetItems API command
my_ddb_table_by_keys(
  keys: [
    { my_hash_key: "value", my_sort_key: 10 },
    { my_hash_key: "value", my_sort_key: 20 },
  ],
  consistent_read: false
) {
  my_hash_key
  my_sort_key
}
```

## Scans
Scans can be sharded and [run in parallel](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan). This could involve a client issuing multiple queries in parallel each with a different segment key. However, given the long running nature of a large scan that would require this, perhaps a subscription would be better suited so that the scan results could be streamed?
