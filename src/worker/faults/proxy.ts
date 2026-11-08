// TCP proxy in front of PostgreSQL for fault injection: cut all connections, refuse new ones, or cut a connection around the next statement of a pattern (before it reaches the server, or after it, dropping the reply).
import net from "node:net"

/** Where a cut falls: before the statement reaches the server, or after the server ran it, dropping its replies. */
export type StatementCut = "before" | "after"

/** Controllable proxy to one PostgreSQL server. */
export interface Proxy {
  port: number
  /** Destroys every open connection. */
  cut(): void
  /** While true, new connections are reset immediately, as of a server that is down. */
  refuse: boolean
  /**
   * Cuts the connection that sends the next statement whose text matches `pattern`, by the simple or the extended protocol: `before` the statement reaches the server, or `after` the server ran it, without passing its replies on.
   * The extended protocol carries the text only when it prepares the statement, which a client does once per connection.
   */
  cutNext(pattern: RegExp, mode: StatementCut): void
  close(): Promise<void>
}

interface Pair {
  client: net.Socket
  server: net.Socket
  buffer: Buffer
  started: boolean
  /**
   * The statement to cut after was forwarded: the replies of the server are read but not passed on, and the connection is cut once the server reports that it is ready for the next query, after it ran the statement.
   */
  dropReplies: boolean
  /** Bytes of the replies since `dropReplies` that do not yet form a complete message. */
  replies: Buffer
}

/** Type of the message by which the server reports that it is ready for the next query. */
const readyForQuery = 0x5a

/** Codes of the requests that precede the startup message and get a one-byte answer. */
const preStartupCodes = new Set([80877103, 80877104])

/** Returns the text of a statement message: a simple-protocol Query or an extended-protocol Parse. */
function queryText(message: Buffer): string | undefined {
  if (message[0] === 0x51) return message.subarray(5, message.length - 1).toString("utf8")
  if (message[0] === 0x50) {
    // Parse: the statement name and the query text, both null-terminated.
    const nameEnd = message.indexOf(0, 5)
    const queryEnd = message.indexOf(0, nameEnd + 1)
    return message.subarray(nameEnd + 1, queryEnd).toString("utf8")
  }
  return undefined
}

/** Starts a proxy on a random local port. */
export async function createProxy(target: { host: string; port: number }): Promise<Proxy> {
  const pairs = new Set<Pair>()
  let pendingCut: { pattern: RegExp; mode: StatementCut } | null = null

  const destroy = (pair: Pair) => {
    pair.client.destroy()
    pair.server.destroy()
    pairs.delete(pair)
  }

  /** Forwards complete client messages and applies the cut rule. */
  const onClientData = (pair: Pair, chunk: Buffer) => {
    pair.buffer = Buffer.concat([pair.buffer, chunk])
    for (;;) {
      if (!pair.started) {
        if (pair.buffer.length < 8) return
        const len = pair.buffer.readInt32BE(0)
        if (pair.buffer.length < len) return
        pair.server.write(pair.buffer.subarray(0, len))
        pair.started = !preStartupCodes.has(pair.buffer.readInt32BE(4))
        pair.buffer = pair.buffer.subarray(len)
        continue
      }
      if (pair.buffer.length < 5) return
      const len = pair.buffer.readInt32BE(1)
      if (pair.buffer.length < 1 + len) return
      const message = pair.buffer.subarray(0, 1 + len)
      pair.buffer = pair.buffer.subarray(1 + len)
      if (pendingCut?.pattern.test(queryText(message) ?? "")) {
        const mode = pendingCut.mode
        pendingCut = null
        if (mode === "before") {
          destroy(pair)
          return
        }
        pair.dropReplies = true
      }
      pair.server.write(message)
    }
  }

  /**
   * Passes the replies of the server to the client; after the statement to cut after, drops them instead and cuts the connection at ReadyForQuery.
   */
  const onServerData = (pair: Pair, chunk: Buffer) => {
    if (!pair.dropReplies) {
      pair.client.write(chunk)
      return
    }
    // The client waits for the replies to its earlier statements before it sends the next one, so the replies since the statement start at a message boundary.
    pair.replies = Buffer.concat([pair.replies, chunk])
    while (pair.replies.length >= 5) {
      const len = pair.replies.readInt32BE(1)
      if (pair.replies.length < 1 + len) return
      const type = pair.replies[0]
      pair.replies = pair.replies.subarray(1 + len)
      if (type === readyForQuery) {
        destroy(pair)
        return
      }
    }
  }

  const server = net.createServer(client => {
    if (proxy.refuse) {
      // A reset rather than a close: postgres.js takes a connection closed during its startup for a transient failure and retries it until its connect timeout without failing the statement, and a plain close reaches the client as a reset on macOS but as a close on Linux.
      client.resetAndDestroy()
      return
    }
    const upstream = net.connect(target.port, target.host)
    const pair: Pair = {
      client,
      server: upstream,
      buffer: Buffer.alloc(0),
      started: false,
      dropReplies: false,
      replies: Buffer.alloc(0),
    }
    pairs.add(pair)
    upstream.on("data", d => onServerData(pair, d as Buffer))
    client.on("data", d => onClientData(pair, d as Buffer))
    const end = () => destroy(pair)
    client.on("close", end)
    upstream.on("close", end)
    client.on("error", () => {})
    upstream.on("error", () => {})
  })
  await new Promise<void>(r => server.listen(0, "127.0.0.1", r))

  const proxy: Proxy = {
    port: (server.address() as net.AddressInfo).port,
    refuse: false,
    cut() {
      for (const pair of pairs) destroy(pair)
    },
    cutNext(pattern, mode) {
      pendingCut = { pattern, mode }
    },
    close() {
      proxy.cut()
      return new Promise(r => server.close(() => r()))
    },
  }
  return proxy
}

/** Starts a proxy to the server of a PostgreSQL URL and returns it with the URL of the same database through it. */
export async function proxyTo(url: string): Promise<{ proxy: Proxy; url: string }> {
  const target = new URL(url)
  const proxy = await createProxy({ host: target.hostname, port: Number(target.port || 5432) })
  const via = new URL(url)
  via.hostname = "127.0.0.1"
  via.port = String(proxy.port)
  return { proxy, url: via.toString() }
}
