import net from "net"
import type https from "https"
import type tls from "tls"
import { Duplex } from "stream"
import pathModule from "path"
import { type BandwidthLimits } from "../types"

// Bytes handed out per grant. Small enough that parallel connections take turns and none of them sits idle long enough to
// trip its idle timeout, large enough to keep the per-grant overhead negligible.
const SLICE_BYTES = 16 * 1024
// Largest burst after an idle spell. Letting a whole 1 MiB upload chunk out at line speed would refill the modem's queue,
// which is exactly the congestion the limit exists to prevent.
const BURST_BYTES = 64 * 1024

/**
 * A token bucket shared by every connection that draws from it. Waiters are served first come first served, and a waiter is
 * granted as soon as the balance is positive, so the balance dips below zero by at most one slice.
 *
 * @export
 * @class TokenBucket
 */
export class TokenBucket {
	private rate: number = 0
	private tokens: number = BURST_BYTES
	private last: number = performance.now()
	private readonly waiters: { bytes: number; resolve: () => void }[] = []
	private timer: ReturnType<typeof setTimeout> | null = null

	/**
	 * Whether the bucket lets everything through (a rate of 0).
	 */
	public get unlimited(): boolean {
		return this.rate === 0
	}

	/**
	 * Change the rate. Applies immediately, including to connections already waiting.
	 *
	 * @param {number} bytesPerSecond 0 means unlimited.
	 */
	public setRate(bytesPerSecond: number): void {
		this.refill()
		this.rate = bytesPerSecond

		if (this.timer) {
			clearTimeout(this.timer)

			this.timer = null
		}

		this.drain()
	}

	/**
	 * Resolve once `bytes` may be sent (or received).
	 */
	public take(bytes: number): Promise<void> {
		return new Promise<void>(resolve => {
			this.waiters.push({ bytes, resolve })
			this.drain()
		})
	}

	private refill(): void {
		const now = performance.now()

		this.tokens = this.rate > 0 ? Math.min(BURST_BYTES, this.tokens + ((now - this.last) / 1000) * this.rate) : BURST_BYTES
		this.last = now
	}

	private drain(): void {
		if (this.timer) {
			return
		}

		this.refill()

		while (this.waiters.length > 0 && (this.rate === 0 || this.tokens > 0)) {
			const waiter = this.waiters.shift()!

			if (this.rate > 0) {
				this.tokens -= waiter.bytes
			}

			waiter.resolve()
		}

		if (this.waiters.length > 0) {
			this.timer = setTimeout(
				() => {
					this.timer = null

					this.drain()
				},
				Math.max(1, Math.ceil((-this.tokens / this.rate) * 1000))
			)
		}
	}
}

/**
 * Sits between TLS and the raw TCP socket, so it meters the bytes that actually hit the wire. Uploads are held back by
 * delaying write callbacks (backpressure all the way up to the HTTP request); downloads by pausing the raw socket, which
 * closes the TCP window and slows the server down instead of dropping anything. Emits "progress" after each slice.
 *
 * @export
 * @class ThrottledSocket
 * @extends {Duplex}
 */
export class ThrottledSocket extends Duplex {
	private readonly raw: net.Socket
	private readonly up: TokenBucket
	private readonly down: TokenBucket
	private slicing: boolean = false
	private rawEnded: boolean = false

	public constructor(raw: net.Socket, up: TokenBucket, down: TokenBucket) {
		// tls.connect inherits allowHalfOpen from the socket it wraps. Duplex defaults to true (net.Socket to false): left at
		// true, a server closing its idle keep-alive side leaves a write-only socket in the agent's free pool, and the next
		// request reusing it waits forever.
		super({ allowHalfOpen: false })

		this.raw = raw
		this.up = up
		this.down = down

		raw.on("data", (chunk: Buffer) => this.onData(chunk))
		// The server can deliver EOF while the last chunk is still being sliced out; ending right away would drop the rest.
		raw.on("end", () => {
			this.rawEnded = true

			if (!this.slicing) {
				this.push(null)
			}
		})
		raw.on("error", err => this.destroy(err))
		// After a clean EOF the readable side may still hold slices for TLS, and it ends on its own once they are read.
		raw.on("close", () => {
			if (!this.rawEnded) {
				this.destroy()
			}
		})
	}

	private async onData(chunk: Buffer): Promise<void> {
		if (this.down.unlimited && !this.slicing) {
			if (!this.push(chunk)) {
				this.raw.pause()
			}

			return
		}

		this.slicing = true
		this.raw.pause()

		let more = true

		for (let offset = 0; offset < chunk.length; offset += SLICE_BYTES) {
			const slice = chunk.subarray(offset, offset + SLICE_BYTES)

			await this.down.take(slice.length)

			if (this.destroyed) {
				return
			}

			more = this.push(slice)

			this.emit("progress")
		}

		this.slicing = false

		if (this.rawEnded) {
			this.push(null)
		} else if (more) {
			this.raw.resume()
		}
	}

	public override _read(): void {
		if (!this.slicing) {
			this.raw.resume()
		}
	}

	public override _write(chunk: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
		this.writeSlices(chunk).then(() => callback(), callback)
	}

	private async writeSlices(chunk: Buffer): Promise<void> {
		if (this.up.unlimited) {
			await this.rawWrite(chunk)

			return
		}

		for (let offset = 0; offset < chunk.length; offset += SLICE_BYTES) {
			const slice = chunk.subarray(offset, offset + SLICE_BYTES)

			await this.up.take(slice.length)

			const written = this.rawWrite(slice)

			this.emit("progress")

			await written
		}
	}

	// A write to a closed socket neither errors nor drains, so check first instead of waiting forever.
	private async rawWrite(buffer: Buffer): Promise<void> {
		if (this.destroyed || this.raw.destroyed || this.raw.writableEnded) {
			throw new Error("Socket closed")
		}

		if (this.raw.write(buffer)) {
			return
		}

		await new Promise<void>(resolve => {
			const done = (): void => {
				this.raw.off("drain", done)
				this.raw.off("close", done)

				resolve()
			}

			this.raw.on("drain", done)
			this.raw.on("close", done)
		})
	}

	public override _final(callback: (error?: Error | null) => void): void {
		this.raw.end()

		callback()
	}

	public override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
		this.raw.destroy()

		callback(error)
	}
}

const patchedAgents = new WeakSet<https.Agent>()

/**
 * Route every connection an https agent opens through a {@link ThrottledSocket}: raw TCP, then the throttle, then the agent's
 * own TLS createConnection (so TLS session reuse and the agent's socket bookkeeping stay untouched). Idempotent.
 *
 * @export
 * @param {https.Agent} agent
 * @param {TokenBucket} up
 * @param {TokenBucket} down
 */
export function throttleAgent(agent: https.Agent, up: TokenBucket, down: TokenBucket): void {
	if (patchedAgents.has(agent)) {
		return
	}

	patchedAgents.add(agent)

	const createConnection = agent.createConnection.bind(agent)

	agent.createConnection = (options, oncreate) => {
		const raw = net.connect({
			host: options.host ?? undefined,
			port: Number(options.port ?? 443),
			family: options.family,
			lookup: options.lookup,
			localAddress: options.localAddress
		})

		// setKeepAlive/setNoDelay on a TLS socket over a JS stream are no-ops, so apply them to the real socket.
		raw.setNoDelay(true)
		raw.setKeepAlive(true, 60 * 1000)

		const throttled = new ThrottledSocket(raw, up, down)
		// An https agent always hands back a TLS socket.
		const tlsSocket = createConnection({ ...options, socket: throttled } as typeof options, oncreate) as tls.TLSSocket

		// The TLS socket sees a whole upload chunk as one write, so its idle timer would not notice the bytes trickling out
		// underneath and would time the request out mid-upload.
		throttled.on("progress", () => {
			if (tlsSocket.timeout) {
				tlsSocket.setTimeout(tlsSocket.timeout)
			}
		})

		return tlsSocket
	}
}

const upload = new TokenBucket()
const download = new TokenBucket()

/**
 * The SDK's shared https agents in this JS realm (the main process and the worker thread each have their own). @filen/sync
 * normally resolves the same copy of the SDK as the desktop; if a version drift ever gives it a nested copy, that copy has an
 * agent of its own, which must be throttled too or sync would slip past the limit.
 */
function sdkAgents(): https.Agent[] {
	const entry = "@filen/sdk/dist/node/api/client"
	const paths = new Set<string>([require.resolve(entry)])

	try {
		paths.add(require.resolve(entry, { paths: [pathModule.dirname(require.resolve("@filen/sync"))] }))
	} catch {
		// @filen/sync not resolvable from here: nothing else to patch.
	}

	// eslint-disable-next-line @typescript-eslint/no-require-imports
	return [...paths].map(path => (require(path) as { keepAliveAgent: https.Agent }).keepAliveAgent)
}

/**
 * Throttle every Filen API connection of this JS realm. Call once per realm, before the SDK makes its first request; the
 * limits start unlimited until {@link setBandwidthLimits} is called.
 *
 * The main process, the worker thread and each rclone process meter separately, so their totals add up. Sync (the worker)
 * carries nearly all the traffic, which keeps that simplification harmless in practice.
 *
 * @export
 */
export function installBandwidthLimiter(): void {
	for (const agent of sdkAgents()) {
		throttleAgent(agent, upload, download)
	}
}

/**
 * Mbit/s (what speed tests report) to bytes/s. 0, negative or not a number means unlimited; anything below 1 Mbit/s is
 * raised to 1, because with dozens of parallel transfers a lower rate leaves each connection too few bytes to stay alive.
 *
 * @export
 * @param {number} [mbps]
 * @returns {number}
 */
export function mbpsToBytesPerSecond(mbps?: number): number {
	if (typeof mbps !== "number" || !Number.isFinite(mbps) || mbps <= 0) {
		return 0
	}

	return (Math.max(1, mbps) * 1000 * 1000) / 8
}

/**
 * Apply the limits to this JS realm. Takes effect immediately, including on open keep-alive connections.
 *
 * @export
 * @param {BandwidthLimits} [limits]
 */
export function setBandwidthLimits(limits?: BandwidthLimits): void {
	upload.setRate(mbpsToBytesPerSecond(limits?.uploadMbps))
	download.setRate(mbpsToBytesPerSecond(limits?.downloadMbps))
}

/**
 * The limits in rclone's `--bwlimit` / rc `core/bwlimit` syntax: `UP:DOWN`, each side in KiB/s or `off`.
 *
 * @export
 * @param {BandwidthLimits} [limits]
 * @returns {string}
 */
export function toRcloneRate(limits?: BandwidthLimits): string {
	const side = (mbps?: number): string => {
		const bytes = mbpsToBytesPerSecond(mbps)

		return bytes > 0 ? `${Math.round(bytes / 1024)}K` : "off"
	}

	const up = side(limits?.uploadMbps)
	const down = side(limits?.downloadMbps)

	return up === "off" && down === "off" ? "off" : `${up}:${down}`
}
