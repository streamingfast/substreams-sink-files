# Substreams Sink Files

`substreams-sink-files` is a tool that allows developers to pipe data extracted from a blockchain into various file base format like CSV, JSONL & Parquet.

## Usage

Install `substreams-sink-files` by using the pre-built binary release [available in the official GitHub repository](https://github.com/streamingfast/substreams-sink-files/releases) or install using brew by doing `brew install streamingfast/tap/substreams-sink-files`.


The sink supports different Substreams output module's type, here a jump list of today's supported formats:

- [Parquet](#parquet)
- [Line-based CSV](#jsonl-csv-and-any-other-line-based-format)
- [Line-based JSONL](#jsonl-csv-and-any-other-line-based-format)
- [Arbitrary Protobuf to JSONL](#arbitrary-protobuf-to-jsonl-protojsonjq-like-expression-encoder)

### Parquet

The `substreams-sink-files` tool includes a powerful Parquet encoder designed to work seamlessly with any Protobuf message output from your Substreams module. This encoder automatically converts your Protobuf schema into an optimized Parquet schema, enabling efficient columnar storage and analytics.

The Parquet encoder is the default encoder when no `--encoder` flag is specified. It intelligently infers table structures from your Protobuf messages or uses explicit annotations for fine-grained control over the output schema.

**Basic Usage:**

```bash
substreams-sink-files run <endpoint> <manifest_or_spkg> <module_name> <output_path> <start_block>:<stop_block>
```

**Example with Ethereum USDT Events:**

```bash
substreams-sink-files run mainnet.eth.streamingfast.io:443 substreams_ethereum_usdt@v0.1.0 map_events \
    ./parquet_output \
    20_000_000:+10000 \
    --file-block-count=1000
```

> [!NOTE]
> If you have your own `.spkg`, simply replace `substreams_ethereum_usdt@v0.1.0` by it to use your own Substreams.

This will process 10,000 blocks starting from block 20,000,000 and create Parquet files in the `./parquet_output` directory, with each file containing data from 1,000 blocks.

**Inspecting the Schema:**

To see what Parquet schema would be derived from your Substreams module without running the full extraction, use the `inspect` command:

```bash
substreams-sink-files tools parquet schema substreams_ethereum_usdt@v0.1.0 map_events
```

This will show you:
- The inferred table names and structures
- Column types and their Parquet mappings
- Any detected annotations or custom column types
- The complete Parquet schema that would be generated

For the Uniswap V3 example above, you might see output like:
```
----------------------- Table from contract.v1.Transfer -----------------------
message transfers {
    required binary evt_tx_hash (STRING);
    required int32 evt_index (INT(32,false));
    required int64 evt_block_time (TIMESTAMP(isAdjustedToUTC=true,unit=NANOS));
    required int64 evt_block_number (INT(64,false));
    required binary from;
    required binary to;
    required binary value (STRING);
}
-------------------------------------------------------------------------------
...
```

#### Default Table Inference

When your Protobuf message doesn't use explicit Parquet annotations, the sink automatically infers table structures using the following rules:

**1. Single Table from Root Message**

If your output message contains no repeated fields of message types, the entire message becomes a single table:

```proto
message PoolCreated {
    string pool_address = 1;
    string token0 = 2;
    string token1 = 3;
    uint64 block_number = 4;
}
```

This creates a table named `pool_created` where each Substreams output message becomes one row.

**2. Multiple Tables from Repeated Fields**

If your output message contains repeated fields of message types, each repeated field becomes a separate table:

```proto
message BlockEvents {
    repeated SwapEvent swaps = 1;
    repeated MintEvent mints = 2;
    repeated BurnEvent burns = 3;
}

message SwapEvent {
    string pool = 1;
    string amount0 = 2;
    string amount1 = 3;
}

message MintEvent {
    string pool = 1;
    string liquidity = 2;
}
```

This creates three tables: `swaps`, `mints`, and `burns`. All swap events from all processed blocks are collected into the `swaps` table, and so on.

#### Parquet Table Annotation

For explicit control over table creation, use the `(parquet.table_name)` option to designate specific messages as tables:

```proto
import "sf/substreams/sink/files/v1/parquet.proto";

message TransferEvent {
    option (parquet.table_name) = "erc20_transfers";

    string transaction_hash = 1;
    string from_address = 2;
    string to_address = 3;
    string amount = 4;
    uint64 block_number = 5;
}

message SwapEvent {
    option (parquet.table_name) = "dex_swaps";

    string pool_address = 1;
    string token_in = 2;
    string token_out = 3;
    string amount_in = 4;
    string amount_out = 5;
}

message AllEvents {
    repeated TransferEvent transfers = 1;
    repeated SwapEvent swaps = 2;
    // These nested messages in other_data won't become tables
    SomeOtherData other_data = 3;
}
```

When processing `AllEvents`, the sink creates two tables: `erc20_transfers` and `dex_swaps`, while ignoring the `other_data` field for table creation.

#### Parquet Column Annotation

Fine-tune individual columns using the `(parquet.column)` option for custom data types and compression:

```proto
import "sf/substreams/sink/files/v1/parquet.proto";

message Transaction {
    option (parquet.table_name) = "transactions";

    string hash = 1;
    string from_address = 2;
    string to_address = 3;

    // Store as 256-bit integer in 32-byte fixed array
    string value = 4 [(parquet.column) = {type: UINT256}];

    // Compress this column with ZSTD
    bytes input_data = 5 [(parquet.column) = {compression: ZSTD}];

    // Ignore this field in Parquet output
    string debug_info = 6 [(parquet.ignored) = true];

    // Optional field (properly handles null values)
    optional string contract_address = 7;

    // Repeated primitive fields
    repeated string logs = 8;
}
```

**Available Column Types:**
- `UINT256`: Stores string representations of 256-bit unsigned integers as 32-byte fixed arrays

  > [!IMPORTANT]
  > **UINT256 Engine Compatibility**: The interpretation of UINT256 values may differ depending on the analytics engine or tool reading the Parquet files. The raw 32-byte value is stored consistently, but different engines may interpret the byte order differently:
  > - **ClickHouse**: With Physical type `FixedByte(32)` and Logical type `Decimal(N, 0)` expects big-endian format
  > - **ClickHouse**: With Physical type `FixedByte(32)` and Logical type `None` expects little-endian format
  > - **Other engines**: May have their own interpretation rules
  >
  > Currently, the sink stores UINT256 values in big-endian format. If you encounter issues with a specific analytics engine, please file an issue with details about your use case.

**Available Compression Options:**
- `UNCOMPRESSED` (default)
- `SNAPPY`
- `GZIP`
- `LZ4_RAW`
- `BROTLI`
- `ZSTD`

You can also set default compression for all columns:

```bash
substreams-sink-files run ... --parquet-default-compression=snappy
```

Column-specific compression annotations override the default compression setting.

### JSONL, CSV and any other line based format

The sink supports an output type [sf.substreams.sink.files.v1.Lines](./proto/sf/substreams/sink/files/v1/files.proto) that can handle any line format, the Substreams being responsible of transforming blocks into lines of the format of your choice. The [sf.substreams.sink.files.v1.Lines](./proto/sf/substreams/sink/files/v1/files.proto) [documentation found on this link](https://github.com/streamingfast/substreams-sink-files/blob/feature/parquet/proto/sf/substreams/sink/files/v1/files.proto#L13-L26) gives further details about the format.

The [Substreams Ethereum Token Transfers example](https://github.com/streamingfast/substreams-eth-token-transfers/blob/develop/src/lib.rs#L31-L46) can be used as an example, it showcases both JSONL and CSV output format:

> [!NOTE]
> Change output module `jsonl_out` below to `csv_out` to test CSV output

```bash
substreams-sink-files run mainnet.eth.streamingfast.io:443 \
    https://github.com/streamingfast/substreams-eth-token-transfers/releases/download/v0.4.0/substreams-eth-token-transfers-v0.4.0.spkg \
    jsonl_out \
    ./out \
    10_000_000:+20_000 \
    --encoder=lines \
    --file-block-count=10000
```

This will run the Substreams, processes 100 000 blocks and at each bundle of 10 000 blocks, will produce a file containing the lines output by the module, resulting in the files on disk:

```bash
./out
├── 0010000000-0010010000.jsonl
└── 0010010000-0010020000.jsonl
```

With example of file content:

```json
$ cat out/0010000000-0010010000.jsonl | head -n1
{"schema":"erc20","trx_hash":"1f17943d5dd7053959f1dc092dfad60a7caa084224212b1adbecaf3137efdfdd","log_index":0,"from":"876eabf441b2ee5b5b0554fd502a8e0600950cfa","to":"566021352eb2f882538bf8d59e5d2ba741b9ec7a","quantity":"95073600000000000000","operator":"","token_id":""}
```

### Arbitrary Protobuf to JSONL (`protojson:<jq like expression>` encoder)

When using 'protojson:<jq like expression>', the output module must be a Protobuf message of any kind. The encoder will extract
rows by using the `jq like expression` to retrieve rows from a repeated field on the message and will write the data
to a JSONL file, one element retrieved per row.

> [!NOTE]
> The JSON output format used is the Protobuf JSON output format from https://protobuf.dev/programming-guides/json/ and
> we specifically the Golang [protojson](https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson) package to achieve
> that.

The `jq like expression` that will be used to extract rows from the Protobuf message only supports a single query form
today and it's `.<repeated_field_name>[]`. For example, if your output module is a Protobuf message:

```proto
message Output {
    repeated Entity entities = 1;
}

message Entity {
    ...
}
```

Running the sink with the `--encoder="protojson:.entities[]"`:

```bash
substreams-sink-files run mainnet.eth.streamingfast.io:443 \
    https://github.com/streamingfast/substreams-eth-token-transfers/releases/download/v0.4.0/substreams-eth-token-transfers-v0.4.0.spkg \
    map_transfers \
    ./out \
    10_000_000:+20_000 \
    --encoder="protojson:.transfers[]" \
    --file-block-count=10000
```

This will run the Substreams, processes 100 000 blocks and at each bundle of 10 000 blocks,

Would yield all entity from the `entities` list as individual rows in the
JSONL file:

 will produce a file containing the lines output by the module, resulting in the files on disk:

```bash
./out
├── 0010000000-0010010000.jsonl
└── 0010010000-0010020000.jsonl
```

With example of file content:

```json
$ cat out/0010000000-0010010000.jsonl | head -n1
{"schema":"erc20","trxHash":"1f17943d5dd7053959f1dc092dfad60a7caa084224212b1adbecaf3137efdfdd","from":"876eabf441b2ee5b5b0554fd502a8e0600950cfa","to":"566021352eb2f882538bf8d59e5d2ba741b9ec7a","quantity":"95073600000000000000"}
```

This mode is a little bit less performant that the 'lines' encoder, as the JSON encoding is done on the fly, but is more generic and can adapt to more Substreams.

## Documentation

### Cursors

When you use Substreams, it sends back a block to a consumer using an opaque cursor. This cursor points to the exact location within the blockchain where the block is. In case your connection terminates or the process restarts, upon re-connection, Substreams sends back the cursor of the last written bundle in the request so that the stream of data can be resumed exactly where it left off and data integrity is maintained.

You will find that the cursor is saved in a file on disk. The location of this file is specified by the flag `--state-store` which points to a local folder. You must ensure that this file is properly saved to a persistent location. If the file is lost, the `substreams-sink-files` tool will restart from the beginning of the chain, redoing all the previous processing.

Therefore, It is crucial that this file is properly persisted and follows your deployment of `substreams-sink-files` to avoid any data loss.

### High Performance

If you are looking for the fastest performance possible, we suggest that your destination source is able to handle heavy traffic. Also, to speed up things, you can allocate a lot of RAM to the process and increase the flag `--buffer-max-size` to a point where you are able to hold a full batch of N blocks in memory (checking the size of the final file is a good indicator of the size to keep stuff in memory).

A lot of I/O operations is avoid if the buffer can hold everything in memory greatly speeding up the process of writing blocks bundle to its final destination.

### Cloud-based storage

You can use the `substreams-sink-files` tool to route data to files on your local file system and cloud-based storage solutions. To use a cloud-based solution such as Google Cloud Storage bucket, S3 compatible bucket, or Azure bucket, you need to make sure it is set up properly. Then, instead of referencing a local file in the `substreams-sink-files run` command, use the path to the bucket. The paths resemble `gs://<bucket>/<path>`, `s3://<bucket>/<path>`, and `az://<bucket>/<path>` respectively. Be sure to update the values according to your account and provider.

### Limitations

When you use the `substreams-sink-files` tool, you will find that it syncs up to the most recent "final" block of the chain. This means it is not real-time. Additionally, the tool writes bundles to disk when it has seen 10,000 blocks. As a result, the latency of the last available bundle can be delayed by around 10,000 blocks. How many blocks per batch can be controlled by changing the flag `--file-block-count`

## Contributing

For additional information, [refer to the general StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md).

## License

The `substreams-sink-files` tool [uses the Apache 2.0 license](https://github.com/streamingfast/substreams/blob/develop/LICENSE/README.md).
