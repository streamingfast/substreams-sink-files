#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

project="$ROOT/../.."
sink="$ROOT/../substreams-sink-files"

main() {
  cd "$ROOT" &> /dev/null

  while getopts "hc" opt; do
    case $opt in
      h) usage && exit 0;;
      c) clean=true;;
      \?) usage_error "Invalid option: -$OPTARG";;
    esac
  done
  shift $((OPTIND-1))
  [[ $1 = "--" ]] && shift

  set -e

  output_dir="$ROOT/localdata"
  if [[ "$clean" == true ]]; then
    rm -rf "$output_dir" 2> /dev/null || true
  fi

  exec $sink run \
    "--encoder=lines" \
    "--state-store=$output_dir/working/state.yaml" \
    "${SUBSTREAMS_ENDPOINT:-"mainnet.eth.streamingfast.io:443"}" \
    "${project}/docs/tutorial/substreams.yaml" \
    "${SUBSTREAMS_MODULE:-"jsonl_out"}" \
    "$output_dir/out" \
    "$@"
}

usage_error() {
  message="$1"
  exit_code="$2"

  echo "ERROR: $message"
  echo ""
  usage
  exit ${exit_code:-1}
}

usage() {
  echo "usage: start.sh [-c]"
  echo ""
  echo "Start Substreams Sink Files."
  echo ""
  echo "Options"
  echo "    -c             Clean actual data directory first"
  echo ""
}



main "$@"
