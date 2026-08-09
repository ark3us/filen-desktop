<br/>
<p align="center">
  <h3 align="center">Filen Desktop</h3>

  <p align="center">
    Desktop client including Syncing, Virtual Drive mounting, S3, WebDAV, File Browsing, Chats, Notes, Contacts and more.
    <br/>
    <br/>
  </p>
</p>

![Contributors](https://img.shields.io/github/contributors/FilenCloudDienste/filen-desktop?color=dark-green) ![Forks](https://img.shields.io/github/forks/FilenCloudDienste/filen-desktop?style=social) ![Stargazers](https://img.shields.io/github/stars/FilenCloudDienste/filen-desktop?style=social) ![Issues](https://img.shields.io/github/issues/FilenCloudDienste/filen-desktop) ![License](https://img.shields.io/github/license/FilenCloudDienste/filen-desktop)

### Installation and building

1. Clone repository

```sh
git clone https://github.com/FilenCloudDienste/filen-desktop filen-desktop
```

2. Update dependencies

```sh
cd filen-desktop && npm install
```

3. Running a development build

To run a development build you need to have "@filen/web" (`npm run dev`) running locally.

```sh
npm run dev
```

4. Build

```sh
npm run build:<os>

Where <os> is either "win", "mac" or "linux"

Building the client requires setting up signing and notarization. See "build/" directory and package.json key.
```

## About this fork — large-deletion confirmation with a configurable threshold

Each sync pair can ask before deleting many files at once, instead of only when the whole pair would be wiped (the upstream rule). The threshold is configured per pair in the sync settings UI and the whole flow is designed to be impossible to miss:

- **A native desktop notification** is raised when a pair starts waiting on a deletion decision — the in-app warning only exists while the window does, and the window is routinely closed to the tray while sync keeps running. Clicking it opens the app. One notification per distinct decision, not one per sync cycle; on Linux it stays on screen until dismissed.
- **Nothing is deleted until someone decides.** Deletions (and renames) wait; uploads, downloads and new files keep syncing meanwhile. The decision survives app restarts.

### Details

- The engine lives in [`@filen/sync`](https://github.com/ark3us/filen-sync) — see its README for the gate's exact semantics (threshold only lowers the bar, non-blocking prompt, approval fingerprint).
- This repo wires it through: IPC channels `syncUpdateRequireConfirmationOnLargeDeletions` / `syncUpdateLargeDeletionThreshold` / `syncUpdateConfirmDeletion`, worker plumbing, and the notification (main process, `src/worker/index.ts`).
- The confirmation switch defaults to ON per pair but respects an explicit OFF; pairs created before the setting existed stay protected.
- Packaging fix along the way: the published `@filen/web` ↔ `@filen/desktop` dependency cycle made the app bundle a registry copy of itself (an unreachable, outdated 118 MB renderer). It is excluded from the build (`!node_modules/@filen/desktop/**` in `build.files`), shrinking the AppImage by ~47 MB.

## License

Distributed under the AGPL-3.0 License. See [LICENSE](https://github.com/FilenCloudDienste/filen-desktop/blob/main/LICENSE.md) for more information.
