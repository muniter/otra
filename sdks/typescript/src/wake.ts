import type pg from "pg";

/** Anything a worker or waiter can park on for wake notifications. */
export interface WakeSource {
  /**
   * Start receiving wakes. The listener gets the queue name a notification
   * was addressed to, or `null` for a "reset" (the connection was
   * (re)established, so anything sent meanwhile may have been missed --
   * poll once now). Returns an unsubscribe function.
   */
  subscribe(listener: (queue: string | null) => void): () => void;
}

type Listener = (queue: string | null) => void;

const RECONNECT_MIN_MS = 200;
const RECONNECT_MAX_MS = 5_000;

/**
 * One dedicated LISTEN connection per app, shared by every worker and every
 * getResult waiter. The schema emits pg_notify('otra_wake', <queue name>)
 * from every wake-worthy transition; this is the receiver.
 *
 * Lazy: the connection is checked out of the pool on the first subscriber
 * and released after the last one leaves, so tick()-driven tests and pure
 * spawn-only clients never hold one. NOTIFY is best-effort -- a dropped
 * connection silently loses whatever was sent while down -- so every
 * (re)connect emits a `null` reset wake, and subscribers keep their own
 * slow polling fallback.
 */
export class WakeHub implements WakeSource {
  private readonly pool: pg.Pool;
  private readonly listeners = new Set<Listener>();
  private client: pg.PoolClient | null = null;
  private connecting = false;
  private closed = false;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectMs = RECONNECT_MIN_MS;
  private readonly onError: (err: unknown) => void;

  constructor(pool: pg.Pool, onError?: (err: unknown) => void) {
    this.pool = pool;
    this.onError = onError ?? (() => {});
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    void this.connect();
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.disconnect();
    };
  }

  /** Permanently stop listening and release the connection. */
  close(): void {
    this.closed = true;
    this.listeners.clear();
    this.disconnect();
  }

  private emit(queue: string | null): void {
    for (const listener of [...this.listeners]) {
      try {
        listener(queue);
      } catch (err) {
        this.onError(err);
      }
    }
  }

  private async connect(): Promise<void> {
    if (
      this.closed ||
      this.connecting ||
      this.client !== null ||
      this.listeners.size === 0
    ) {
      return;
    }
    this.connecting = true;
    try {
      const client = await this.pool.connect();
      try {
        // Named so operators (and our own tests) can find it in
        // pg_stat_activity.
        await client.query("set application_name = 'otra-listen'");
        await client.query("listen otra_wake");
      } catch (err) {
        client.release(true);
        throw err;
      }
      client.on("notification", (msg) => this.emit(msg.payload ?? null));
      const drop = () => this.handleDrop(client);
      client.on("error", drop);
      client.on("end", drop);
      this.client = client;
      this.reconnectMs = RECONNECT_MIN_MS;
      // Reset wake: notifications sent before this instant were missed.
      this.emit(null);
    } catch (err) {
      this.onError(err);
      this.scheduleReconnect();
    } finally {
      this.connecting = false;
    }
  }

  private handleDrop(client: pg.PoolClient): void {
    if (this.client !== client) return;
    this.client = null;
    try {
      client.release(true);
    } catch {
      /* already destroyed */
    }
    if (!this.closed && this.listeners.size > 0) this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer !== null) return;
    const timer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.connect();
    }, this.reconnectMs);
    timer.unref?.();
    this.reconnectTimer = timer;
    this.reconnectMs = Math.min(this.reconnectMs * 2, RECONNECT_MAX_MS);
  }

  private disconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    const client = this.client;
    this.client = null;
    if (client !== null) {
      // unlisten is implicit: release(true) destroys the connection rather
      // than returning a LISTEN-encumbered client to the pool.
      try {
        client.release(true);
      } catch {
        /* already destroyed */
      }
    }
  }
}
