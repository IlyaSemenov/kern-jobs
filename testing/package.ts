// Shared harness of the tests of the packed package: commands of a consumer project and the tarball of the package.
import { join } from "node:path"

const root = join(import.meta.dir, "..")

/**
 * Runs a command in `cwd` and returns its standard output, throwing with its standard error on failure.
 *
 * `timeout` limits a command that can hang, in milliseconds: `spawnSync` blocks the event loop, so the timeout of the calling test cannot stop it.
 */
export function run(
  cmd: string[],
  cwd: string,
  options: { timeout?: number; env?: Record<string, string | undefined> } = {},
): string {
  const result = Bun.spawnSync({ cmd, cwd, ...options })
  if (result.exitCode !== 0) {
    const outcome = result.exitedDueToTimeout ? "timed out" : "failed"
    throw new Error(`${cmd.join(" ")} ${outcome}:\n${result.stdout}${result.stderr}`)
  }
  return result.stdout.toString()
}

/** Packs the built package into `destination` as npm publishes it and returns the path of the tarball. */
export function packPackage(destination: string): string {
  // `changeset publish` publishes with npm; without `--ignore-scripts`, npm runs `prepare`, which installs Git hooks.
  const [{ filename }] = JSON.parse(
    run(["npm", "pack", "--json", "--ignore-scripts", "--pack-destination", destination], root),
  ) as [{ filename: string }]
  return join(destination, filename)
}
