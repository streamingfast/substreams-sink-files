mod pb;

use pb::sinkfiles::Lines;
use serde_json::json;
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;

#[substreams::handlers::map]
fn jsonl_out(block: eth::Block) -> Result<Lines, substreams::errors::Error> {
    let header = block.header.as_ref().unwrap();

    Ok(pb::sinkfiles::Lines {
        // Although we return a single line, you are free in your own code to return multiple lines, one per entity usually
        lines: vec![json!({
            "number": block.number,
            "hash": Hex(&block.hash).to_string(),
            "parent_hash": Hex(&header.parent_hash).to_string(),
            "timestamp": header.timestamp.as_ref().unwrap().to_string()
        })
        .to_string()],
    })
}
