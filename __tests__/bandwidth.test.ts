import { test, before, after } from "node:test"
import assert from "node:assert/strict"
import https from "https"
import { type AddressInfo } from "net"
import { Readable } from "stream"
import { HttpsAgent } from "agentkeepalive"
import { generate } from "selfsigned"
import {
	TokenBucket,
	ChunkGate,
	throttleAgent,
	gateChunkUploads,
	installBandwidthLimiter,
	mbpsToBytesPerSecond,
	toRcloneRate,
	uploadChunkSlots
} from "../src/lib/bandwidth"

const KiB = 1024

let server: https.Server
let port: number
let cert: string

// Self-signed and marked as a CA, so it can be both the server's certificate and the client's trust anchor. BoringSSL
// (Electron's TLS) refuses a plain leaf certificate as a trust anchor, unlike Node's OpenSSL.
async function localhostCert(): Promise<{ cert: string; key: string }> {
	const notBeforeDate = new Date()
	const pems = await generate([{ name: "commonName", value: "localhost" }], {
		keySize: 2048,
		algorithm: "sha256",
		notBeforeDate,
		notAfterDate: new Date(notBeforeDate.getTime() + 24 * 60 * 60 * 1000),
		extensions: [
			{ name: "basicConstraints", cA: true },
			{ name: "keyUsage", keyCertSign: true, digitalSignature: true, keyEncipherment: true },
			{ name: "subjectAltName", altNames: [{ type: 2, value: "localhost" }] }
		]
	})

	return { cert: pems.cert, key: pems.private }
}

before(async () => {
	const pair = await localhostCert()

	cert = pair.cert
	server = https.createServer({ key: pair.key, cert: pair.cert }, (req, res) => {
		const url = new URL(req.url ?? "/", "https://localhost")

		if (req.method === "POST") {
			let bytes = 0

			req.on("data", (chunk: Buffer) => {
				bytes += chunk.length

				if (url.pathname === "/reset" && bytes > 64 * KiB) {
					req.socket.destroy()
				}
			})
			req.on("end", () => res.end(JSON.stringify({ bytes })))

			return
		}

		let left = Number(url.searchParams.get("size"))
		const block = Buffer.alloc(64 * KiB, 1)

		// Advertise keep-alive (so the client pools the connection) but close it soon after the response is written, while
		// a throttled client is still reading it: a clean EOF then races the slicing, and a pooled socket must not be left
		// half-open for the next request.
		res.on("finish", () => setTimeout(() => req.socket.end(), 200))

		const pump = (): void => {
			while (left > 0) {
				const n = Math.min(left, block.length)

				left -= n

				if (!res.write(block.subarray(0, n))) {
					res.once("drain", pump)

					return
				}
			}

			res.end()
		}

		pump()
	})
	await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve))

	port = (server.address() as AddressInfo).port
})

after(() => {
	server.close()
})

function throttled(upBytesPerSecond: number, downBytesPerSecond: number) {
	const up = new TokenBucket()
	const down = new TokenBucket()
	const agent = new HttpsAgent({ ca: cert })

	up.setRate(upBytesPerSecond)
	down.setRate(downBytesPerSecond)
	throttleAgent(agent, up, down)

	return { agent, up, down }
}

// Same shape as the SDK's upload: the whole body in a single write, a per-request timeout.
function post(agent: https.Agent, bytes: number, { path = "/up", timeout = 30000 } = {}): Promise<number> {
	return new Promise<number>((resolve, reject) => {
		const req = https.request({ method: "POST", hostname: "localhost", port, path, agent }, res => {
			const chunks: Buffer[] = []

			res.on("data", (chunk: Buffer) => chunks.push(chunk))
			res.on("end", () => resolve(JSON.parse(Buffer.concat(chunks).toString("utf-8")).bytes))
			res.on("error", reject)
		})

		req.on("error", reject)
		req.setTimeout(timeout, () => {
			req.destroy()
			reject(new Error("timed out"))
		})

		Readable.from([Buffer.alloc(bytes, 2)]).pipe(req)
	})
}

function get(agent: https.Agent, bytes: number): Promise<number> {
	return new Promise<number>((resolve, reject) => {
		const req = https.request({ hostname: "localhost", port, path: `/down?size=${bytes}`, agent }, res => {
			let received = 0

			res.on("data", (chunk: Buffer) => (received += chunk.length))
			res.on("end", () => resolve(received))
			res.on("error", reject)
		})

		req.on("error", reject)
		req.end()
	})
}

function withDeadline<T>(promise: Promise<T>, ms: number): Promise<T> {
	return Promise.race([promise, new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`still pending after ${ms} ms`)), ms))])
}

const seconds = (since: number): number => (performance.now() - since) / 1000

test("converts Mbit/s to bytes/s, treating 0 and junk as unlimited and raising tiny rates to 1 Mbit/s", () => {
	assert.equal(mbpsToBytesPerSecond(8), 1000000)
	assert.equal(mbpsToBytesPerSecond(0), 0)
	assert.equal(mbpsToBytesPerSecond(-3), 0)
	assert.equal(mbpsToBytesPerSecond(Number.NaN), 0)
	assert.equal(mbpsToBytesPerSecond(undefined), 0)
	assert.equal(mbpsToBytesPerSecond(0.1), 125000)
})

test("formats limits in rclone's UP:DOWN syntax", () => {
	assert.equal(toRcloneRate({ uploadMbps: 12, downloadMbps: 0 }), "1465K:off")
	assert.equal(toRcloneRate({ uploadMbps: 0, downloadMbps: 16 }), "off:1953K")
	assert.equal(toRcloneRate({ uploadMbps: 0, downloadMbps: 0 }), "off")
	assert.equal(toRcloneRate(undefined), "off")
})

test("uploads share the limit and take turns", async () => {
	const { agent } = throttled(256 * KiB, 0)
	const start = performance.now()
	// 512 KiB at 256 KiB/s, minus the 64 KiB burst: ~1.75 s.
	const finished = (await Promise.all(Array.from({ length: 4 }, () => post(agent, 128 * KiB).then(() => seconds(start))))).sort()
	const total = finished[3]!

	assert.ok(total > 1.4 && total < 2.6, `took ${total} s`)
	// The first connection to finish its TLS handshake also gets the initial burst, so it may finish early. The others
	// must finish together: one after the other would put the second one at about half the total time.
	assert.ok(total - finished[1]! < 0.25 * total, `finish times: ${finished.join(", ")}`)

	agent.destroy()
})

test("downloads share the limit and survive the server closing keep-alive mid-response", async () => {
	const { agent } = throttled(0, 256 * KiB)
	const start = performance.now()
	const received = await withDeadline(Promise.all(Array.from({ length: 4 }, () => get(agent, 128 * KiB))), 10000)

	assert.deepEqual(received, [128 * KiB, 128 * KiB, 128 * KiB, 128 * KiB])
	assert.ok(seconds(start) > 1.4 && seconds(start) < 2.6, `took ${seconds(start)} s`)

	// The server has closed those connections by now: they must be gone from the pool, not reused half-open.
	await new Promise(resolve => setTimeout(resolve, 300))
	assert.equal(await withDeadline(get(agent, 64 * KiB), 3000), 64 * KiB)

	agent.destroy()
})

test("a slow upload does not trip the request's idle timeout", async () => {
	const { agent } = throttled(64 * KiB, 0)
	// ~3 s of trickling against a 1 s idle timeout.
	assert.equal(await post(agent, 256 * KiB, { timeout: 1000 }), 256 * KiB)

	agent.destroy()
})

test("a rate change applies to a transfer already in flight", async () => {
	const { agent, up } = throttled(64 * KiB, 0)
	const start = performance.now()

	setTimeout(() => up.setRate(0), 300)

	// 512 KiB at 64 KiB/s would take ~7 s.
	await post(agent, 512 * KiB)

	assert.ok(seconds(start) < 1.5, `took ${seconds(start)} s`)

	agent.destroy()
})

test("a connection reset mid-upload rejects instead of hanging", async () => {
	const { agent } = throttled(128 * KiB, 0)

	await assert.rejects(
		withDeadline(post(agent, 512 * KiB, { path: "/reset" }), 5000),
		(err: Error) => !err.message.startsWith("still pending")
	)

	agent.destroy()
})

test("unlimited is a passthrough", async () => {
	const { agent } = throttled(0, 0)
	const start = performance.now()

	assert.equal(await post(agent, 16 * 1024 * KiB), 16 * 1024 * KiB)
	assert.equal(await get(agent, 16 * 1024 * KiB), 16 * 1024 * KiB)
	assert.ok(seconds(start) < 2, `took ${seconds(start)} s`)

	agent.destroy()
})

test("sizes the upload gate so a chunk takes about 4 s at the limit", () => {
	assert.equal(uploadChunkSlots(0), 0)
	assert.equal(uploadChunkSlots(mbpsToBytesPerSecond(1)), 1)
	assert.equal(uploadChunkSlots(mbpsToBytesPerSecond(1.5)), 1)
	assert.equal(uploadChunkSlots(mbpsToBytesPerSecond(12)), 6)
	assert.equal(uploadChunkSlots(mbpsToBytesPerSecond(1000)), 32)
})

test("the gate caps concurrent work and follows capacity changes", async () => {
	const gate = new ChunkGate()
	const releases: (() => void)[] = []
	let running = 0
	let peak = 0
	const work = () =>
		gate.run(
			() =>
				new Promise<void>(resolve => {
					running++
					peak = Math.max(peak, running)
					releases.push(() => {
						running--
						resolve()
					})
				})
		)
	const tick = () => new Promise(resolve => setImmediate(resolve))

	gate.setCapacity(2)

	const all = Promise.all(Array.from({ length: 6 }, work))

	await tick()
	assert.equal(running, 2)

	gate.setCapacity(4)
	await tick()
	assert.equal(running, 4)

	gate.setCapacity(0)
	await tick()
	assert.equal(running, 6)

	releases.forEach(release => release())
	await all

	assert.equal(peak, 6)
})

test("with the gate, chunks finish one after another instead of all together at the end", async () => {
	const { agent } = throttled(256 * KiB, 0)
	const gate = new ChunkGate()
	const uploader = {
		uploadChunkBuffer: async (...args: unknown[]) => await post(agent, args[0] as number)
	}

	gate.setCapacity(uploadChunkSlots(256 * KiB))
	gateChunkUploads(uploader, gate)

	const start = performance.now()
	// 8 x 128 KiB at 256 KiB/s. Shared by all 8 at once, every chunk would finish at ~3.75 s.
	const finished = (
		await Promise.all(Array.from({ length: 8 }, () => uploader.uploadChunkBuffer(128 * KiB).then(() => seconds(start))))
	).sort()

	assert.ok(finished[0]! < 1, `first chunk finished at ${finished[0]} s`)
	assert.ok(finished[3]! < 2.5, `fourth chunk finished at ${finished[3]} s`)
	assert.ok(finished[7]! < 5, `took ${finished[7]} s`)

	agent.destroy()
})

test("installBandwidthLimiter hooks the SDK's shared agent and chunk uploads", () => {
	// eslint-disable-next-line @typescript-eslint/no-require-imports
	const { keepAliveAgent, APIClient } = require("@filen/sdk/dist/node/api/client") as {
		keepAliveAgent: https.Agent
		APIClient: { prototype: { uploadChunkBuffer: unknown } }
	}
	const createConnection = keepAliveAgent.createConnection
	const uploadChunkBuffer = APIClient.prototype.uploadChunkBuffer

	assert.equal(installBandwidthLimiter(), true)
	assert.notEqual(keepAliveAgent.createConnection, createConnection)
	assert.notEqual(APIClient.prototype.uploadChunkBuffer, uploadChunkBuffer)
})
