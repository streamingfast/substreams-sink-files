.PHONY: build
build:
	cargo build --target wasm32-unknown-unknown --release

.PHONY: stream
stream: build
	substreams run -e mainnet.eth.streamingfast.io:443 substreams.yaml jsonl_out -s 12292922 -t +10

.PHONY: protogen
protogen:
	substreams protogen ./substreams.yaml --exclude-paths="sf/substreams/v1,google"
