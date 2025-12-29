# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v3.0.1

### **Breaking Changes**

* `protojson:` and `proto` outputs now use the `google.golang.org/protobuf/encoding/protojson` library to ensure it follows the correct definition (https://protobuf.dev/programming-guides/json/)

### Deprecation Warning

* Using this package binary will soon be deprecated in favor of the `substreams sink parquet` and `substreams sink protojson` commands.

## v3.0.0

### **Breaking Changes**

* **CLI Interface Change**: Updated the CLI interface to align with the new `substreams/sink` package:
  - **Endpoint inference**: The `<endpoint>` parameter has been removed and is now automatically inferred from the manifest's network definition, but can always be overridden with the `--endpoint` flag
  - **Updated command syntax**: Changed from `run <endpoint> <manifest> <module> <output_store> [<start>:<stop>]` to `run [<manifest> [<module>]] [--start-block=<num>] [--stop-block=<num>]`
  - **Output directory flag**: The output path is now specified via `--output-dir` (short: `-o`) flag instead of a positional argument
  - **Block range format**: Block ranges are now specified using separate `--start-block` and `--stop-block` flags instead of the `<start>:<stop>` format
  - **Flag changes**:
    - Added `--output-dir` (short: `-o`) for specifying output directory

* **Dependency Migration**: Replaced `github.com/streamingfast/substreams-sink` with `github.com/streamingfast/substreams/sink` which normalizes command line arguments across all substreams sink tools.

### Migration Examples

Here are common Parquet extraction scenarios showing the changes from v2.x to v3.0.0:

**Basic Parquet extraction (no block range):**
```bash
# v2.x
substreams-sink-files run mainnet.eth.streamingfast.io:443 substreams_ethereum_usdt@v0.1.0 map_events ./output

# v3.0.0 (endpoint inferred from manifest's network)
substreams-sink-files run substreams_ethereum_usdt@v0.1.0 map_events --output-dir ./output

# v3.0.0 (explicit endpoint override)
substreams-sink-files run substreams_ethereum_usdt@v0.1.0 map_events --output-dir ./output --endpoint mainnet.eth.streamingfast.io:443
```

**Parquet extraction with block range:**
```bash
# v2.x
substreams-sink-files run mainnet.eth.streamingfast.io:443 substreams_ethereum_usdt@v0.1.0 map_events ./output 20000000:20010000

# v3.0.0 (endpoint inferred from manifest)
substreams-sink-files run substreams_ethereum_usdt@v0.1.0 map_events --output-dir ./output --start-block 20000000 --stop-block 20010000
```

**Parquet extraction with custom endpoint:**
```bash
# v2.x
substreams-sink-files run my-custom-endpoint:443 substreams_ethereum_usdt@v0.1.0 map_events ./output --file-block-count 1000

# v3.0.0
substreams-sink-files run substreams_ethereum_usdt@v0.1.0 map_events --output-dir ./output --endpoint my-custom-endpoint:443 --file-block-count 1000
```

## v2.2.0

* **Beta** Ship first version of Parquet support.

* Trap panics when Parquet encoder inspects Protobuf for proper error reporting

* Properly shows an error when defining column-level compression on non-leaf types.

* Fixed default compression not working in presence of nested messages.

* Fixed panic when enum value is unknown, returns a clear error message now.

* Added initial support for enum type for Parquet sink, Parquet schema is `required binary <name> (ENUM)` in those case and the enum's string value is used as the value in the data.

* Added initial nested message and repeated nested message for Parquet sink (in beta).

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
