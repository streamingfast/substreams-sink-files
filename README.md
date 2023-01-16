# Substreams sink files

## Description

`substreams-sink-files` is a tool that allows developers to pipe data extracted from a blockchain into various types of local or Cloud files-based persistence solutions.

## Prerequisites

- A Substreams module prepared for a files-sink
- Cloud-based file storage mechanism (optional)

## Installation

Install `substreams-sink-files` by using the pre-built binary release [available in the official GitHub repository](https://github.com/streamingfast/substreams-sink-files/releases).

Extract `substreams-sink-files` into a folder and ensure this folder is referenced globally via your `PATH` environment variable.

## Using the `substreams-sink-files` tool

The `run` command is the primary way to work with the `substreams-sink-files` tool. The command for your project will resemble the following:

{% code overflow="wrap" %}

```bash
substreams-sink-files run --encoder=lines --state-store=./localdata/working/state.yaml mainnet.eth.streamingfast.io:443 substreams.yaml jsonl_out ./localdata/out
```

You'll need to use values for your Substreams module and desired output for each of the flags in the command.

{% endcode %}

Output resembling the following will be printed to the terminal window for properly issued commands and a properly set up and configured Substreams module.

```bash
2023-01-09T07:45:02.563-0800 INFO (substreams-sink-files) starting prometheus metrics server {"listen_addr": "localhost:9102"}
2023-01-09T07:45:02.563-0800 INFO (substreams-sink-files) sink to files {"file_output_path": "./localdata/out", "file_working_dir": "./localdata/working", "endpoint": "mainnet.eth.streamingfast.io:443", "encoder": "lines", "manifest_path": "substreams.yaml", "output_module_name": "jsonl_out", "block_range": "", "state_store": "./localdata/working/state.yaml", "blocks_per_file": 10000, "buffer_max_size": 67108864}
2023-01-09T07:45:02.563-0800 INFO (substreams-sink-files) reading substreams manifest {"manifest_path": "substreams.yaml"}
2023-01-09T07:45:02.563-0800 INFO (substreams-sink-files) starting pprof server {"listen_addr": "localhost:6060"}
2023-01-09T07:45:04.041-0800 INFO (pipeline) computed start block {"module_name": "jsonl_out", "start_block": 0}
2023-01-09T07:45:04.042-0800 INFO (substreams-sink-files) ready, waiting for signal to quit
2023-01-09T07:45:04.045-0800 INFO (substreams-sink-files) setting up sink {"block_range": {"start_block": 0, "end_block": "None"}, "cursor": {"Cursor":"","Block":{}}}
2023-01-09T07:45:04.048-0800 INFO (substreams-sink-files) starting new file boundary {"boundary": "[0, 10000)"}
2023-01-09T07:45:04.049-0800 INFO (substreams-sink-files) boundary started {"boundary": "[0, 10000)"}
2023-01-09T07:45:04.049-0800 INFO (substreams-sink-files) starting stats service {"runs_each": "2s"}
2023-01-09T07:45:06.052-0800 INFO (substreams-sink-files) substreams sink stats {"progress_msg_rate": "0.000 msg/s (0 total)", "block_rate": "650.000 blocks/s (1300 total)", "last_block": "#1299 (a0f0f283e0d297dd4bcf4bbff916b1df139d08336ad970e77f26b45f9a521802)"}
```

### Cursors

When you use Substreams, it sends back a block to a consumer using an opaque cursor. This cursor points to the exact location within the blockchain where the block is. In case your connection terminates or the process restarts, upon re-connection, Substreams sends back the cursor of the last written bundle in the request so that the stream of data can be resumed exactly where it left off and data integrity is maintained.

You will find that the cursor is saved in a file on disk. The location of this file is specified by the flag `--state-store` which points to a local folder. You must ensure that this file is properly saved to a persistent location. If the file is lost, the `substreams-sink-files` tool will restart from the beginning of the chain, redoing all the previous processing.

Therefore, It is crucial that this file is properly persisted and follows your deployment of `substreams-sink-files` to avoid any data loss.

### Cloud-based storage

You can use the `substreams-sink-files` tool to route data to files on your local file system and cloud-based storage solutions. To use a cloud-based solution such as Google Cloud Storage bucket, S3 compatible bucket, or Azure bucket, you need to make sure it is set up properly. Then, instead of referencing a local file in the `substreams-sink-files run` command, use the path to the bucket. The paths resemble `gs://<bucket>/<path>`, `s3://<bucket>/<path>`, and `az://<bucket>/<path>` respectively. Be sure to update the values according to your account and provider.

### Limitations

When you use the `substreams-sink-files` tool, you will find that it syncs up to the most recent "final" block of the chain. This means it is not real-time. Additionally, the tool writes bundles to disk when it has seen 10,000 blocks. As a result, the latency of the last available bundle can be delayed by around 10,000 blocks.

## Contributing

For additional information, [refer to the general StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md).

## License

The `substreams-sink-files` tool [uses the Apache 2.0 license](https://github.com/streamingfast/substreams/blob/develop/LICENSE/README.md).
