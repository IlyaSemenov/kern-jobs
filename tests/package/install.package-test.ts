// Installs the packed package with npm into a new project, as the quickstart of docs/getting-started.md does, with its dependencies from the registry, and runs the quickstart; the `test:package` script builds the package first.
import { afterAll, beforeAll, expect, test } from "bun:test"
import { cp, mkdir, mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"

import postgres from "postgres"

import { databaseUrl } from "../../testing/database"
import { packPackage, run } from "../../testing/package"

/** Temporary directory of the tarball and the project. */
let dir: string
let project: string
/** Database of the quickstart, which creates the schema of kern-jobs under its default name. */
const databaseName = `kern_jobs_install_${Math.random().toString(36).slice(2, 10)}`
const admin = postgres(databaseUrl(), { max: 1, onnotice: () => {} })

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "kern-jobs-install-"))
  const tarball = packPackage(dir)
  project = join(dir, "project")
  await mkdir(project)
  const npm = (...args: string[]) => run(["npm", ...args], project, { timeout: 120_000 })
  npm("init", "-y")
  npm("pkg", "set", "type=module")
  npm("install", "--no-audit", "--no-fund", tarball, "postgres")
  await cp(join(import.meta.dir, "quickstart/quickstart.ts"), join(project, "quickstart.ts"))
  await admin.unsafe(`create database "${databaseName}"`)
}, 180_000)

afterAll(async () => {
  try {
    await admin.unsafe(`drop database if exists "${databaseName}" with (force)`)
  } finally {
    await admin.end()
    await rm(dir, { recursive: true, force: true })
  }
})

// The second run finds the schema up to date.
test.each(["node", "bun"])(
  "the quickstart runs a job on %s",
  runtime => {
    const url = new URL(databaseUrl())
    url.pathname = `/${databaseName}`
    const output = run([runtime, "quickstart.ts"], project, {
      timeout: 30_000,
      env: { ...process.env, DATABASE_URL: url.toString() },
    })
    expect(output).toBe("Hello, world!\n")
  },
  60_000,
)
