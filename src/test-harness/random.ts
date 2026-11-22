// Seeded random inputs of tests, which repeat their sequence for the same seed.
import { uniformFloat64 } from "pure-rand/distribution/uniformFloat64"
import { uniformInt } from "pure-rand/distribution/uniformInt"
import { xoroshiro128plus } from "pure-rand/generator/xoroshiro128plus"

/**
 * Creates a source of random test inputs that repeats its sequence for the same seed.
 *
 * `float()` returns a number from 0 inclusive to 1 exclusive, and `int(min, max)` an integer from `min` to `max` inclusive.
 */
export function createRandom(seed: number) {
  const rng = xoroshiro128plus(seed)
  return {
    float: () => uniformFloat64(rng),
    int: (min: number, max: number) => uniformInt(rng, min, max),
  }
}
