// Process for the test of a late signal: the stop settles at once, the clock of the helper passes the window, and a real SIGINT comes before the timer of the window has run; the process prints what the helper did, as JSON, and exits by itself.
import type { Worker } from "../runtime"
import { listenForStopSignals } from "../shutdown"

let now = 0
let calls = 0
const worker: Pick<Worker, "stop"> = {
  stop() {
    calls++
    return Promise.resolve()
  },
}
const outcome = listenForStopSignals(worker, {}, { now: () => now })
process.kill(process.pid, "SIGINT")
await outcome
now = 2000
process.kill(process.pid, "SIGINT")
// The signal is delivered, and the timer of the window, 2 s of real time, has not run.
await new Promise(resolve => setTimeout(resolve, 200))
console.log(
  JSON.stringify({
    calls,
    listeners: [process.listenerCount("SIGTERM"), process.listenerCount("SIGINT")],
  }),
)
process.exit(0)
