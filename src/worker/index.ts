import { Worker as WorkerThread } from "worker_threads"
import pathModule from "path"
import { deserializeError } from "../utils"
import { app, Notification } from "electron"
import type FilenDesktop from ".."
import isDev from "../isDev"
import { type WorkerInvokeChannel, type WorkerMessage } from "../types"
import { Semaphore } from "../semaphore"
import { type SyncMessage } from "@filen/sync/dist/types"
import { getAppIcon } from "../assets"

export class Worker {
	private worker: WorkerThread | null = null
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	private invokes: Record<number, { resolve: (value: any | PromiseLike<any>) => void; reject: (err: Error) => void }> = {}
	private invokesId = 0
	private isQuittingApp = false
	private desktop: FilenDesktop
	public active: boolean = false
	private readonly startMutex = new Semaphore(1)
	private readonly stopMutex = new Semaphore(1)
	// What each pair's outstanding deletion prompt says, for the pairs we have already notified about.
	private readonly notifiedDeletions: Record<string, string> = {}

	public constructor(desktop: FilenDesktop) {
		this.desktop = desktop

		app.on("will-quit", async e => {
			if (this.isQuittingApp) {
				return
			}

			// Set synchronously (before any await) so a re-entrant will-quit is a no-op.
			this.isQuittingApp = true

			e.preventDefault()

			// Hard ceiling: the process WILL exit even if teardown misbehaves. killAll() is already self-bounded (each
			// role runs a core/quit -> SIGTERM -> SIGKILL ladder, all roles in parallel), so this only bites if rclone is
			// genuinely stuck flushing/unmounting; the crash-safety watchdog then reaps + unmounts whatever is left.
			const failsafe = setTimeout(() => app.exit(0), 15000)

			try {
				// Flush write-back + clean-unmount rclone before we go. We deliberately do NOT await worker.stop(): the
				// worker is an in-process worker_threads thread that is torn down with this process on app.exit(0), so
				// awaiting it buys nothing for cleanup and its untimed invoke("stop") is what could hang the quit. Sync
				// and the HTTP server are resumable/stateless, so stopping the thread hard is safe.
				await this.desktop.rclone.killAll()
			} catch {
				// Best-effort - the watchdog is the backstop.
			}

			clearTimeout(failsafe)

			app.exit(0)
		})
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	public invoke<T>(channel: WorkerInvokeChannel, data?: any): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			if (!this.worker) {
				reject(new Error("Worker not online."))

				return
			}

			// Start worker if it's not online for some reason. Should start on client init.
			;(this.active ? Promise.resolve() : this.start())
				.then(() => {
					const id = this.invokesId++

					this.invokes[id] = {
						resolve,
						reject
					}

					this.worker?.postMessage({
						type: "invokeRequest",
						data: {
							id,
							channel,
							data
						}
					} satisfies WorkerMessage)
				})
				.catch(reject)
		})
	}

	public async start(): Promise<void> {
		await this.startMutex.acquire()

		try {
			await new Promise<void>((resolve, reject) => {
				this.desktop.logger.log("info", "Starting worker")

				this.worker = new WorkerThread(pathModule.join(__dirname, !isDev ? "worker.js" : "worker.dev.js"))

				this.worker.on("error", err => {
					this.desktop.logger.log("error", err, "worker.onError")
					this.desktop.logger.log("error", err)

					reject(err)
				})

				this.worker.on("exit", code => {
					this.desktop.logger.log("error", `Worker exited with code ${code}.`)
				})

				if (isDev) {
					this.worker.stderr.on("data", chunk => {
						console.log("worker stderr", chunk.toString("utf-8"))
					})

					this.worker.stdout.on("data", chunk => {
						console.log("worker stdout", chunk.toString("utf-8"))
					})
				}

				this.worker.on("message", async (message: WorkerMessage) => {
					if (message.type === "error") {
						this.desktop.logger.log("error", deserializeError(message.data.error), "workerError")

						reject(deserializeError(message.data.error))
					} else if (message.type === "started") {
						this.desktop.logger.log("info", "Worker started")

						resolve()
					} else if (message.type === "invokeResponse") {
						if (this.invokes[message.data.id]) {
							this.invokes[message.data.id]!.resolve(message.data.result)

							delete this.invokes[message.data.id]
						}
					} else if (message.type === "invokeError") {
						this.desktop.logger.log("error", deserializeError(message.data.error), "workerError")

						if (this.invokes[message.data.id]) {
							this.invokes[message.data.id]!.reject(deserializeError(message.data.error))

							delete this.invokes[message.data.id]
						}
					} else if (message.type === "sync") {
						this.notifyConfirmDeletion(message.data)

						this.desktop.ipc.postMainToWindowMessage({
							type: "sync",
							message: message.data
						})
					}
				})
			})

			this.active = true
		} finally {
			this.startMutex.release()
		}
	}

	/**
	 * Raise a desktop notification when a sync pair is waiting on a large-deletion decision.
	 *
	 * This lives in the main process on purpose: the in-app warning bar only exists while the window does, and
	 * the window is routinely closed to the tray while sync keeps running. A deletion the user never sees the
	 * question for is a deletion that either never happens or happens unnoticed — both bad.
	 *
	 * The engine re-posts an unanswered prompt on EVERY cycle (~5s) so the renderer's bar survives, so one
	 * notification per message would be a notification every 5 seconds. One is raised per DISTINCT pending
	 * decision instead: keyed by pair, valued by what the prompt says. A changed value means the deletion set
	 * itself changed, which the user has to be asked about again anyway.
	 */
	private notifyConfirmDeletion(message: SyncMessage): void {
		// The engine suppresses cycleSuccess while a decision is outstanding, so receiving it means this pair
		// has nothing pending any more — forget it, or a later identical prompt would be swallowed as a repost.
		// cycleExited fires when the pair is removed or torn down; forgetting it there keeps this from holding a
		// fingerprint for a pair that no longer exists.
		if (message.type === "cycleSuccess" || message.type === "cycleExited") {
			delete this.notifiedDeletions[message.syncPair.uuid]

			return
		}

		if (message.type !== "confirmDeletion") {
			return
		}

		const fingerprint = `${message.data.where}:${message.data.count}`

		if (this.notifiedDeletions[message.syncPair.uuid] === fingerprint) {
			return
		}

		this.notifiedDeletions[message.syncPair.uuid] = fingerprint

		// Marked notified even when we cannot show anything, so a headless/unsupported desktop does not retry
		// every cycle forever. The in-app bar and the tray warning icon still carry the state.
		if (!Notification.isSupported()) {
			return
		}

		const where =
			message.data.where === "local"
				? "in the cloud"
				: message.data.where === "remote"
				? "on this computer"
				: "on both sides"

		const notification = new Notification({
			title: "Filen: confirm a large deletion",
			body: `"${message.syncPair.name}" wants to delete ${message.data.count} item(s) ${where}. Nothing is deleted until you decide.`,
			// Linux only: stay on screen until dismissed instead of fading after a few seconds. The whole point is
			// that this must not be missed. Ignored on Windows/macOS.
			urgency: "critical",
			icon: getAppIcon()
		})

		notification.on("click", () => {
			this.desktop.showOrOpenDriveWindow()
		})

		notification.show()
	}

	public async stop(): Promise<void> {
		await this.stopMutex.acquire()

		try {
			if (!this.worker) {
				this.active = false

				return
			}

			await this.invoke("stop")
			await this.worker.terminate()

			this.worker = null
			this.active = false
		} finally {
			this.stopMutex.release()
		}
	}
}

export default Worker
