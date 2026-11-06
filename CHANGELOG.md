# Changelog

All notable changes to ccx are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Claude hosts no longer re-read a worker's whole transcript after every turn.
  Context and model readings update from what Claude wrote since the last
  reading, so long-lived workers stay fast and use little memory.

### Removed

- The per-worker `transcript.json` file and the worker record's
  `transcriptJson` field. `ccx logs` still renders the conversation on demand.

## [0.1.0] - 2026-11-03

First public release.

- `ccx` CLI: create, send, steer, wait, report, fork, import, attach, stop and kill
  persistent Claude Code and Codex workers, on Linux, macOS and WSL2.
- Python API (`WorkerClient`, `CcxClient`, `CodexClient`) and durable controllers.
- Local JSON protocol (`ccx rpc`, `ccx rpc --serve`) for other languages.
- Shared worker variables, parent notifications, permission and question
  brokering, usage readings and caps.
- The `ccx` agent skill (`ccx skills install`).
