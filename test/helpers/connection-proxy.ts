import { once } from 'node:events';
import { connect, createServer } from 'node:net';
import type { Socket } from 'node:net';
import type { ConnectionOptions } from '@kequtech/pgclear';

type Direction = 'requests' | 'responses';
interface Relay {
  readonly source: Socket;
  readonly destination: Socket;
  readonly direction: Direction;
  readonly held: Buffer[];
}

/** Test-only, plaintext TCP relay. PostgreSQL still performs every operation against a real DB. */
export async function connectionProxy(connectionString: string) {
  const target = new URL(connectionString);
  const host = (target.searchParams.get('host') ?? target.hostname).replace(/^\[|\]$/g, '');
  const port = Number(target.searchParams.get('port') ?? (target.port || 5432));
  const held = new Set<Direction>();
  const sockets = new Set<Socket>();
  const relays = new Set<Relay>();
  const server = createServer((client) => {
    const upstream = connect({ host, port });
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.setNoDelay(true);
      socket.on('error', () => {
        client.destroy();
        upstream.destroy();
      });
      socket.once('close', () => {
        sockets.delete(socket);
        client.destroy();
        upstream.destroy();
      });
    }
    for (const [source, destination, direction] of [
      [client, upstream, 'requests'],
      [upstream, client, 'responses'],
    ] as const) {
      const relay: Relay = { source, destination, direction, held: [] };
      relays.add(relay);
      source.once('close', () => relays.delete(relay));
      source.on('end', () => destination.end());
      source.on('data', (chunk: Buffer) => {
        if (held.has(direction)) {
          relay.held.push(chunk);
        } else if (!destination.write(chunk)) {
          source.pause();
        }
      });
      destination.on('drain', () => source.resume());
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Expected a TCP proxy address.');
  const connection: ConnectionOptions = {
    connectionString,
    host: '127.0.0.1',
    port: address.port,
    tls: false,
    connectTimeoutMs: 1000,
  };
  return {
    connection,
    // Hold bytes without closing either socket, including subsequently opened connections.
    hold(...directions: Direction[]) {
      for (const direction of directions) held.add(direction);
    },
    buffered(direction: Direction): number {
      let size = 0;
      for (const relay of relays) {
        if (relay.direction === direction) {
          for (const chunk of relay.held) size += chunk.length;
        }
      }
      return size;
    },
    release() {
      held.clear();
      for (const relay of relays) {
        for (const chunk of relay.held.splice(0)) {
          if (!relay.destination.write(chunk)) relay.source.pause();
        }
      }
    },
    disconnect() {
      for (const socket of sockets) socket.destroy();
    },
    async close() {
      const closed = new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
      for (const socket of sockets) socket.destroy();
      await closed;
    },
  };
}
