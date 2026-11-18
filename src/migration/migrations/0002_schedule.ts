import { createTable, defineMigration } from "kern-migrations"

export default defineMigration([
  createTable("schedule", {
    id: "text primary key",
    evaluated_through: "timestamptz not null",
  }),
])
