//! Translate Order By clauses.
use multimap::MultiMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use ndc_models as models;

use crate::translation::error::Error;
use crate::translation::helpers::{
    wrap_in_field_path, CollectionInfo, Env, FieldPath, RootAndCurrentTables, State,
    TableNameAndReference,
};
use query_engine_sql::sql;

// Top-level //

/// Convert the order by fields from a QueryRequest to a SQL ORDER BY clause and potentially
/// JOINs when we order by relationship fields.
pub fn translate_order_by(
    env: &Env,
    state: &mut State,
    root_and_current_tables: &RootAndCurrentTables,
    order_by: &Option<models::OrderBy>,
) -> Result<(sql::ast::OrderBy, Vec<sql::ast::Join>), Error> {
    let mut joins: Vec<sql::ast::Join> = vec![];
    // skip if there's no order by clause.
    match order_by {
        None => Ok((sql::ast::OrderBy { elements: vec![] }, vec![])),
        Some(models::OrderBy { elements }) => {
            // Group order by elements by their paths, and translate each group
            // to result order by columns (and their indices in the order by list) and joins
            // containing selecting these columns from the relevant paths.
            let element_groups = group_elements(elements);
            let order_by_parts = element_groups
                .iter()
                .map(|element_group| {
                    translate_order_by_target_group(
                        env,
                        state,
                        root_and_current_tables,
                        element_group,
                        &mut joins,
                    )
                })
                .collect::<Result<Vec<Vec<(usize, sql::ast::OrderByElement)>>, Error>>()?;
            // flatten the result columns and sort by their indices in the order by list.
            let mut order_by_columns = order_by_parts.into_iter().flatten().collect::<Vec<_>>();
            order_by_columns.sort_by_key(|(index, _)| *index);

            // Discard the indices, construct an order by clause, and accompanied joins.
            Ok((
                sql::ast::OrderBy {
                    elements: order_by_columns
                        .into_iter()
                        .map(|(_, order_by_element)| order_by_element)
                        .collect(),
                },
                joins,
            ))
        }
    }
}

// Types //

/// Group columns or aggregates with the same path element.
/// Columns and aggregates need to be separated because they return
/// different amount on rows.
#[derive(Debug)]
enum OrderByElementGroup<'a> {
    Columns {
        path: &'a [models::PathElement],
        columns: Vec<GroupedOrderByElement<(Column, FieldPath)>>,
    },
    Aggregates {
        path: &'a [models::PathElement],
        aggregates: Vec<GroupedOrderByElement<Aggregate>>,
    },
}

/// A column or aggregate element with their index in the order by list
/// and their order by direction.
#[derive(Debug)]
struct GroupedOrderByElement<T> {
    index: usize,
    direction: models::OrderDirection,
    element: T,
}

/// A column to select from a table used in an order by.
#[derive(Debug)]
struct Column(models::FieldName);

/// An aggregate operation to select from a table used in an order by.
#[derive(Debug)]
enum Aggregate {
    CountStarAggregate,
    SingleColumnAggregate {
        column: models::FieldName,
    },
}

impl OrderByElementGroup<'_> {
    /// Extract the path component of a group.
    fn path(&self) -> &[models::PathElement] {
        match &self {
            Self::Columns { path, .. } | Self::Aggregates { path, .. } => path,
        }
    }
}

// Group elements //

/// Group order by elements with the same path. Separate columns and aggregates
/// because they each return different amount of rows.
fn group_elements(elements: &[models::OrderByElement]) -> Vec<OrderByElementGroup> {
    // We need to jump through some hoops to group path elements because serde_json::Value
    // does not have Ord or Hash instances. So we use u64 as a key derived from hashing the
    // string representation of a path.
    let hash_path = |path: &[models::PathElement]| {
        let mut s = DefaultHasher::new();
        format!("{path:?}").hash(&mut s);
        s.finish()
    };

    let mut column_element_groups: MultiMap<
        u64, // path hash
        (
            usize,                  // index
            &[models::PathElement], // path
            FieldPath,              // field path
            models::OrderDirection, // order by direction
            Column,                 // column
        ),
    > = MultiMap::new();

    let mut aggregate_element_groups: MultiMap<
        u64, // path hash
        (
            usize,                  // index
            &[models::PathElement], // path
            models::OrderDirection, // order by direction
            Aggregate,              // column
        ),
    > = MultiMap::new();

    // for each element, insert them to their respective group according to their kind and path.
    for (i, element) in elements.iter().enumerate() {
        match &element.target {
            models::OrderByTarget::Column {
                path,
                name,
                field_path,
            } => column_element_groups.insert(
                hash_path(path),
                (
                    i,
                    path,
                    field_path.into(),
                    element.order_direction,
                    Column(name.clone()),
                ),
            ),
            models::OrderByTarget::StarCountAggregate { path } => aggregate_element_groups.insert(
                hash_path(path),
                (
                    i,
                    path,
                    element.order_direction,
                    Aggregate::CountStarAggregate,
                ),
            ),
            models::OrderByTarget::SingleColumnAggregate {
                path,
                column,
                function: _,
                field_path: _,
            } => aggregate_element_groups.insert(
                hash_path(path),
                (
                    i,
                    path,
                    element.order_direction,
                    Aggregate::SingleColumnAggregate {
                        column: column.clone(),
                    },
                ),
            ),
        }
    }

    // Ignore the hash that was only used to group the paths and construct an OrderByElementGroup.
    let mut element_vecs = vec![];
    for (_, vec) in column_element_groups {
        element_vecs.push(OrderByElementGroup::Columns {
            // if it's here, there's at least one.
            path: vec.first().unwrap().1,
            columns: vec
                .into_iter()
                .map(
                    |(index, _, field_path, direction, element)| GroupedOrderByElement {
                        index,
                        direction,
                        element: (element, field_path),
                    },
                )
                .collect::<Vec<_>>(),
        });
    }
    // Ignore the hash that was only used to group the paths and construct an OrderByElementGroup.
    for (_, vec) in aggregate_element_groups {
        element_vecs.push(OrderByElementGroup::Aggregates {
            // if it's here, there's at least one.
            path: vec.first().unwrap().1,
            aggregates: vec
                .into_iter()
                .map(|(index, _, direction, element)| GroupedOrderByElement {
                    index,
                    direction,
                    element,
                })
                .collect::<Vec<_>>(),
        });
    }

    element_vecs
}

// Translate a group //

/// Translate an order by group and add additional JOINs to the wrapping SELECT
/// and return the order by elements which capture the references to the expressions
/// used for the sort by the wrapping SELECTs, together with their place in the order by list.
fn translate_order_by_target_group(
    env: &Env,
    state: &mut State,
    root_and_current_tables: &RootAndCurrentTables,
    element_group: &OrderByElementGroup,
    _joins: &mut Vec<sql::ast::Join>,
) -> Result<Vec<(usize, sql::ast::OrderByElement)>, Error> {
    let column_or_relationship_select = build_select_and_joins_for_order_by_group(
        env,
        state,
        root_and_current_tables,
        element_group,
    )?;

    match column_or_relationship_select {
        // The column is from the source table, we just need to query it directly.
        ColumnsOrSelect::Columns(columns) => Ok(columns
            .into_iter()
            .map(|(i, direction, field_path, column_name)| {
                (
                    i,
                    sql::ast::OrderByElement {
                        target: wrap_in_field_path(
                            &field_path,
                            sql::ast::Expression::ColumnReference(column_name),
                        ),
                        direction: match direction {
                            models::OrderDirection::Asc => sql::ast::OrderByDirection::Asc,
                            models::OrderDirection::Desc => sql::ast::OrderByDirection::Desc,
                        },
                    },
                )
            })
            .collect()),
    }
}

/// Used as the return type of `build_select_and_joins_for_order_by_group`.
/// Represents the direct references to the requested columns (if path is empty),
/// or a select query describing how to reach the columns.
enum ColumnsOrSelect {
    /// Columns represents target columns that are referenced from the current table.
    Columns(
        Vec<(
            usize,                     // The global order by index for this column.
            models::OrderDirection,    // The order direction.
            FieldPath,                 // The nested field path.
            sql::ast::ColumnReference, // A reference for this column.
        )>,
    ),
}

/// Generate a SELECT query representing querying the requested columns/aggregates from a table
/// (potentially a nested one using joins).
fn build_select_and_joins_for_order_by_group(
    env: &Env,
    _state: &mut State,
    root_and_current_tables: &RootAndCurrentTables,
    element_group: &OrderByElementGroup,
) -> Result<ColumnsOrSelect, Error> {
    // We want to build a select query where "Track" is the root table, and "Artist"."Name"
    // is the column we need for the order by. Our query will look like this:
    //
    // > ( SELECT "Artist"."Name" AS "Name" -- wanted column, might be wrapped with <function> if one is supplied
    // >   FROM
    // >     ( SELECT "Album"."ArtistId" ---- required for the next join condition
    // >       FROM "Album" AS "Album"
    // >       WHERE "Track"."AlbumId" = "Album"."AlbumId" --- requires 'AlbumId' from 'Track'
    // >     ) AS "Album"
    // >   LEFT OUTER JOIN LATERAL
    // >     ( SELECT "Artist"."Name" AS "Name" ---- the wanted column for the order by
    // >       FROM "Artist" AS "Artist" ---- the last relationship table
    // >       WHERE ("Album"."ArtistId" = "Artist"."ArtistId") ---- requires 'ArtistId' from 'Album'
    // >     ) AS "Artist" ON ('true')
    // > )
    //
    // Note that "Track" will be supplied by the caller of this function.

    let path = element_group.path();

    if path.is_empty() {
        match element_group {
            OrderByElementGroup::Aggregates { .. } => {
                // Cannot do an aggregation over an empty path. Must be a relationship.
                Err(Error::EmptyPathForOrderByAggregate)
            }
            OrderByElementGroup::Columns { .. } => {
                // If the path is empty, we don't need to build a query, just return the columns.
                let table = env.lookup_collection(&root_and_current_tables.current_table.name)?;
                let columns = translate_targets(
                    &table,
                    &root_and_current_tables.current_table,
                    element_group,
                )?
                .into_iter()
                .map(|column| {
                    (
                        column.index,
                        column.direction,
                        column.field_path,
                        sql::ast::ColumnReference::AliasedColumn {
                            table: root_and_current_tables.current_table.reference.clone(),
                            column: column.alias,
                        },
                    )
                })
                .collect();
                Ok(ColumnsOrSelect::Columns(columns))
            }
        }
    }
    // If we query a relationship, build a wrapping select query selecting the requested columns/aggregates
    // for the order by, and build a select of all the joins to select from.
    else {
        todo!("relationship order by");
    }
}

/// An expression selected from the target order by table.
struct OrderBySelectExpression {
    index: usize,
    direction: models::OrderDirection,
    field_path: FieldPath,
    alias: sql::ast::ColumnAlias,
}

/// Take an element group and convert all of the elements we want to select
/// to aliases and expressions, along with their order by direction and their index
/// in the order by list.
fn translate_targets(
    target_collection: &CollectionInfo,
    _table: &TableNameAndReference,
    element_group: &OrderByElementGroup,
) -> Result<Vec<OrderBySelectExpression>, Error> {
    match element_group {
        OrderByElementGroup::Columns { columns, path: _ } => {
            let columns = columns
                .iter()
                .map(|element| {
                    let (Column(target_column_name), field_path) = &element.element;
                    let selected_column = target_collection.lookup_column(target_column_name)?;
                    // we are going to deliberately use the table column name and not an alias we get from
                    // the query request because this is internal to the sorting mechanism.
                    let selected_column_alias =
                        sql::helpers::make_column_alias(selected_column.name.0);

                    // we use the real name of the column as an alias as well.
                    Ok::<OrderBySelectExpression, Error>(OrderBySelectExpression {
                        index: element.index,
                        direction: element.direction,
                        alias: selected_column_alias.clone(),
                        field_path: field_path.clone(),
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(columns)
        }
        OrderByElementGroup::Aggregates { aggregates, .. } => {
            let aggregates = aggregates
                .iter()
                .map(|element| {
                    match &element.element {
                        Aggregate::CountStarAggregate => {
                            let column_alias = sql::helpers::make_column_alias("count".to_string());
                            Ok(OrderBySelectExpression {
                                index: element.index,
                                direction: element.direction,
                                alias: column_alias,
                                // Aggregates do not have a field path.
                                field_path: (&None).into(),
                            })
                        }
                        Aggregate::SingleColumnAggregate { column } => {
                            let selected_column = target_collection.lookup_column(column)?;
                            // we are going to deliberately use the table column name and not an alias we get from
                            // the query request because this is internal to the sorting mechanism.
                            let selected_column_alias =
                                sql::helpers::make_column_alias(selected_column.name.0);
                            // we use the real name of the column as an alias as well.
                            Ok(OrderBySelectExpression {
                                index: element.index,
                                direction: element.direction,
                                alias: selected_column_alias.clone(),
                                // Aggregates do not have a field path.
                                field_path: (&None).into(),
                            })
                        }
                    }
                })
                .collect::<Result<Vec<_>, Error>>()?;

            Ok(aggregates)
        }
    }
}
