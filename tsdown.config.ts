import { defineConfig } from "tsdown"

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/migrations.ts",
    "src/worker.ts",
    "src/db/postgres-js.ts",
    "src/db/node-postgres.ts",
  ],
  format: "esm",
  dts: true,
  exports: true,
  publint: true,
  attw: {
    profile: "esm-only",
  },
})
