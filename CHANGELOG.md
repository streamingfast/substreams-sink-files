# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v2.0.0

### Highlights

This release drops support for Substreams RPC protocol `sf.substreams.v1` and switch to Substreams RPC protocol `sf.substreams.rpc.v2`. As a end user, right now the transition is seamless. All StreamingFast endpoints have been updated to to support the legacy Substreams RPC protocol `sf.substreams.v1` as well as the newer Substreams RPC protocol `sf.substreams.rpc.v2`.

Support for legacy Substreams RPC protocol `sf.substreams.v1` is expected to end by June 6 2023. What this means is that you will need to update to at least this release if you are running `substreams-sink-files` in production. Otherwise, after this date, your current binary will stop working and will return errors that `sf.substreams.v1.Blocks` is not supported on the endpoint.

From a database and operator standpoint, this binary is **fully** backward compatible with your current schema. Updating to this binary will continue to sink just like if you used a prior release.

#### Retryable Errors

The errors coming from sink files handler are **not** retried anymore and will stop the binary immediately.

### Added

- Added `--infinite-retry` to never exit on error and retry indefinitely instead.

- Added `--development-mode` to run in development mode.

    > **Warning** You should use that flag for testing purposes, development mode drastically reduce performance you get from the server.

- Added `--final-blocks-only` to only deal with final (irreversible) blocks.
