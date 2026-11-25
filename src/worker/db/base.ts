// Definition of the tables of the worker ORM schema.

import { createTableFactory, toSnakeCase } from "kern-orm"

/** Defines a table of the worker ORM schema, mapping each camelCase key to its snake_case column. */
export const { defineTable } = createTableFactory({ columnName: toSnakeCase })
