// Checks the packed package as a consumer project installs it; the `test:package` script builds it first.
import { afterAll, beforeAll, describe, expect, test } from "bun:test"
import { cp, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { dirname, join } from "node:path"

import { packPackage, run } from "../../testing/package"

const root = join(import.meta.dir, "../..")

let consumer: string
/**
 * Project that only enqueues: it has kern-jobs without its dependencies or a client library, in a directory outside the consumer.
 */
let producer: string

beforeAll(async () => {
  consumer = await mkdtemp(join(tmpdir(), "kern-jobs-consumer-"))
  const tarball = packPackage(consumer)
  const target = join(consumer, "node_modules/kern-jobs")
  await mkdir(target, { recursive: true })
  run(["tar", "-xzf", tarball, "-C", target, "--strip-components=1"], consumer)
  // The producer must not find the dependencies of kern-jobs or a client in a parent directory, so it gets a copy of kern-jobs rather than a link into the consumer.
  producer = await mkdtemp(join(tmpdir(), "kern-jobs-producer-"))
  await cp(target, join(producer, "node_modules/kern-jobs"), { recursive: true })
  await cp(join(import.meta.dir, "producer"), producer, { recursive: true })
  await writeFile(join(producer, "package.json"), JSON.stringify({ type: "module" }))
  // The dependencies, the clients, and the types come from the repository, without network.
  for (const name of [
    "kern-db",
    "kern-migrations",
    "pg",
    "@types/pg",
    "postgres",
    "@types/node",
    "@types/bun",
  ]) {
    const link = join(consumer, "node_modules", name)
    await mkdir(dirname(link), { recursive: true })
    await symlink(join(root, "node_modules", name), link)
  }
  await cp(join(import.meta.dir, "fixtures"), consumer, { recursive: true })
  await writeFile(join(consumer, "package.json"), JSON.stringify({ type: "module" }))
  await writeFile(
    join(consumer, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        module: "nodenext",
        target: "esnext",
        strict: true,
        exactOptionalPropertyTypes: true,
        skipLibCheck: false,
        declaration: true,
        emitDeclarationOnly: true,
        // The consumer imports its modules by their file names, as Node.js requires.
        allowImportingTsExtensions: true,
        outDir: "declarations",
        // Without the types of Bun, which only the type tests take, so that the declarations of the package must not need them.
        types: ["node"],
      },
      include: ["*.ts"],
    }),
  )
  await writeFile(
    join(consumer, "tsconfig.declarations.json"),
    JSON.stringify({
      extends: "./tsconfig.json",
      compilerOptions: { noEmit: true, emitDeclarationOnly: false },
      include: ["declarations/**/*.d.ts"],
      // TypeScript excludes `outDir` by default.
      exclude: [],
    }),
  )
  await writeFile(
    join(consumer, "tsconfig.type-tests.json"),
    JSON.stringify({
      extends: "./tsconfig.declarations.json",
      compilerOptions: {
        // Bun's ambient types fail the library check; tsconfig.declarations.json checks the emitted declarations without them.
        skipLibCheck: true,
        types: ["bun"],
      },
      include: ["declaration-consumer/*.type-test.ts"],
    }),
  )
}, 60_000)

afterAll(async () => {
  await Promise.all([consumer, producer].map(dir => rm(dir, { recursive: true, force: true })))
})

describe.each([
  ["pinned", [join(root, "node_modules/.bin/tsc")]],
  // The minimum version that consumers need, as the install section of docs/getting-started.md states.
  ["minimum", ["node", Bun.resolveSync("typescript-6/lib/tsc.js", import.meta.dir)]],
])("TypeScript %s", (_, tsc) => {
  test("emits declarations and type-checks their consumer", async () => {
    // Declarations of another version must not pass for the ones of this version.
    await rm(join(consumer, "declarations"), { recursive: true, force: true })
    run([...tsc, "-p", "tsconfig.json"], consumer)
    run([...tsc, "-p", "tsconfig.declarations.json"], consumer)
    run([...tsc, "-p", "tsconfig.type-tests.json"], consumer)
  }, 60_000)
})

describe.each(["node", "bun"])("%s", runtime => {
  test("enqueues through the main entry without loading kern-db, kern-migrations, or a client", () => {
    expect(JSON.parse(run([runtime, "producer.ts"], producer, { timeout: 30_000 }))).toEqual({
      id: "1",
      created: true,
    })
  }, 60_000)

  test("enqueues a job through node-postgres and postgres.js, and runs the manifest and a handler of the worker", () => {
    expect(JSON.parse(run([runtime, "consumer.ts"], consumer, { timeout: 30_000 }))).toEqual({
      created: [true, false],
      sameJob: true,
      manifest: ["search.indexProduct", "sessions.purgeExpired"],
      unimplementedTasks: ["reminders.send"],
      duplicateRejected: true,
      retryDelay: 10,
    })
  }, 60_000)

  test("migrates a schema and runs the worker in it on the drivers of postgres.js and node-postgres", () => {
    const outcome = {
      retryDelay: 10,
      rows: [["search.indexProduct", "pending", 1]],
      schemaError: expect.stringContaining("call migrateJobs of kern-jobs/migrations"),
    }
    expect(JSON.parse(run([runtime, "run-worker.ts"], consumer, { timeout: 60_000 }))).toEqual({
      "postgres.js": outcome,
      "node-postgres": outcome,
    })
  }, 90_000)
})
