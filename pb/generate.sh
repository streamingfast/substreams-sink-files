#!/bin/bash -u
# Copyright 2019 dfuse Platform Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"

# Protobuf definitions
PROTO=${1:-"$ROOT/proto"}

function main() {
  checks

  set -e
  pushd "$ROOT" >/dev/null

  generate_proto

  popd >/dev/null

  echo "generate.sh - `date` - `whoami`" > $ROOT/pb/last_generate.txt
  echo "streamingfast/substreams-sink-files revision: `GIT_DIR=$ROOT/.git git rev-parse HEAD`" >> $ROOT/pb/last_generate.txt

  if ! command -v protoc-gen-go &> /dev/null; then
      go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  fi

  pushd "$ROOT/internal"

  proto_files=$(cd proto && find tests -name "*.proto")
  protoc -I=./proto -I=../proto --go_out=paths=source_relative:./pb $proto_files

  echo "Done"
}

function generate_proto() {
  echo "Generating Substreams Sink Files Protobuf bindings via 'buf'"
  buf generate proto
}

function checks() {
  result=`printf "" | buf --version 2>&1 | grep -Eo '1\.(1[0-9]+|[2-9][0-9]+)\.'`
  if [[ "$result" == "" ]]; then
    echo "The 'buf' binary is either missing or is not recent enough (at `which buf || echo N/A`)."
    echo ""
    echo "To fix your problem, on Mac OS, perform this command:"
    echo ""
    echo "  brew install bufbuild/buf/buf"
    echo ""
    echo "On other system, refers to https://docs.buf.build/installation"
    echo ""
    echo "If everything is working as expetcted, the command:"
    echo ""
    echo "  buf --version"
    echo ""
    echo "Should print '1.11.0' (or newer)"
    exit 1
  fi

  # The old `protoc-gen-go` did not accept any flags. Just using `protoc-gen-go --version` in this
  # version waits forever. So we pipe some wrong input to make it exit fast. This in the new version
  # which supports `--version` correctly print the version anyway and discard the standard input
  # so it's good with both version.
  result=`printf "" | protoc-gen-go --version 2>&1 | grep -Eo 'v1.(2[7-9]|[3-9][0-9]+)\.'`
  if [[ "$result" == "" ]]; then
    echo "Plugin 'protoc-gen-go' is either missing or is not recent enough (at `which protoc-gen-go || echo N/A`)."
    echo ""
    echo "To fix your problem, perform this command:"
    echo ""
    echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.0"
    echo ""
    echo "If everything is working as expetcted, the command:"
    echo ""
    echo "  protoc-gen-go --version"
    echo ""
    echo "Should print 'protoc-gen-go v1.27.0' (if it just hangs, you don't have the correct version)"
    exit 1
  fi
}

main "$@"
