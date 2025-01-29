# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Next

* The `substreams-sink-files run` has been overhaul to conform with `substreams run/gui` specifications around flags and arguments.

    **Before**:

    ```bash
    substreams-sink-files run --encoder="parquet" <endpoint> <file.spkg> <module> <output-path> [<block-range>]
    ```

    **After**:

    ```bash
    substreams-sink-files run --encoder="parquet" <file.spkg> [<manifest> [<module_name>] [<block-range>]] --output-path=<output-path> --network=<network>
    ```

* The `--encoder="proto:<jq like expression>"` is deprecated, replaced with `--encoder="protojson:<jq like expression>"` (the accepted `jq` expression is very limited, see [README](./README.md) for details).

* The `--encoder` flags is now fully optional and defaults to `parquet` (as it's the one best working with arbitrary protobuf).

* Added Parquet output support through `--encoder=parquet`, see [README](./README.md) for full details of Parquet mode and how it works.

## v2.1.0

* Fix last file erased when restarting from a cursor
* Bumped substreams to v1.7.3
* Enable gzip compression on substreams data on the wire

## v2.0.1

### Substreams Progress Messages

> [!IMPORTANT]
> This client only support progress messages sent from a to a server with substreams version >=v1.1.12

* Bumped substreams-sink to `v0.3.1` and substreams to `v1.1.12` to support the new progress message format. Progression now relates to **stages** instead of modules. You can get stage information using the `substreams info` command starting at version `v1.1.12`.

#### Changed Prometheus Metrics

* `substreams_sink_progress_message` removed in favor of `substreams_sink_progress_message_total_processed_blocks`
* `substreams_sink_progress_message_last_end_block` removed in favor of `substreams_sink_progress_message_last_block` (per stage)

#### Added Prometheus Metrics

* added `substreams_sink_progress_message_last_contiguous_block` (per stage)
* added `substreams_sink_progress_message_running_jobs`(per stage)

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
