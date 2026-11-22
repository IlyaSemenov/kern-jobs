import base from "@ilyasemenov/oxc-config/oxlint"
import { defineConfig } from "oxlint"

export default defineConfig({
  extends: [base],
  overrides: [
    {
      // The main entry imports `src/sql/`, which must not import kern-db or kern-orm, not even their types.
      files: ["src/sql/**/*.ts"],
      excludeFiles: ["**/*.test.ts", "**/*.type-test.ts"],
      rules: {
        "no-restricted-imports": [
          "error",
          {
            patterns: [
              {
                group: ["kern-db", "kern-db/**", "kern-orm", "kern-orm/**"],
                message: "Modules of src/sql/ must not import kern-db or kern-orm.",
              },
            ],
          },
        ],
        // `no-restricted-imports` does not see a type written as `import("…")`.
        "typescript/consistent-type-imports": ["error", { disallowTypeAnnotations: true }],
      },
    },
  ],
})
