import writeFileAtomic from "write-file-atomic"
import fs from "fs-extra"
import { app } from "electron"
import pathModule from "path"
import { type BandwidthLimits } from "../types"

export const OPTIONS_VERSION = 1

export type OptionsType = {
	minimizeToTray?: boolean
	startMinimized?: boolean
	bandwidthLimits?: BandwidthLimits
}

export class Options {
	private path: string | null = null
	private cache: OptionsType | null = null
	private updates: Promise<void> = Promise.resolve()

	private getPath(): string {
		if (!this.path) {
			this.path = pathModule.join(app.getPath("userData"), `options.v${OPTIONS_VERSION}.json`)
		}

		return this.path
	}

	public async reset(): Promise<void> {
		try {
			const path = this.getPath()

			await fs.rm(path, {
				force: true,
				maxRetries: 60 * 10,
				recursive: true,
				retryDelay: 100
			})

			this.cache = {}
		} catch (e) {
			console.error(e)
		}
	}

	public async get(): Promise<OptionsType> {
		if (this.cache) {
			return this.cache
		}

		try {
			const path = this.getPath()

			if (!(await fs.exists(path))) {
				return {}
			}

			const options: OptionsType = JSON.parse(await fs.readFile(path, "utf-8"))

			this.cache = options

			return options
		} catch (e) {
			console.error(e)

			return {}
		}
	}

	public async save(options: OptionsType): Promise<void> {
		try {
			const path = this.getPath()

			await writeFileAtomic(path, JSON.stringify(options))

			this.cache = options
		} catch (e) {
			console.error(e)
		}
	}

	/**
	 * Merge into the saved options. Updates run one at a time, each reading what the previous one wrote: the renderer pushes
	 * several options at once on start, and concurrent read-modify-writes would drop all but the last one's change.
	 */
	public update(options: Partial<OptionsType>): Promise<void> {
		const next = this.updates.then(async () => {
			const current = await this.get()

			await this.save({
				...current,
				...options
			})
		})

		this.updates = next.catch(() => {})

		return next
	}
}

export default Options
