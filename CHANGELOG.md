# Changelog

All notable changes to ccx are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-11-03

First public release.

- `ccx` CLI: create, send, steer, wait, report, fork, import, attach, stop and kill
  persistent Claude Code and Codex workers, on Linux, macOS and WSL2.
- Python API (`WorkerClient`, `CcxClient`, `CodexClient`) and durable controllers.
- Local JSON protocol (`ccx rpc`, `ccx rpc --serve`) for other languages.
- Shared worker variables, parent notifications, permission and question
  brokering, usage readings and caps.
- The `ccx` agent skill (`ccx skills install`).
