#!/usr/bin/env bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
sink="$ROOT/../substreams-sink-files"

finish() {
    kill -s TERM $active_pid &> /dev/null || true
}

main() {
  trap "finish" EXIT
  pushd "$ROOT" &> /dev/null

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

    localfolder="`pwd`/localdata"

    $sink run \
      "api-dev.streamingfast.io:443" \
      "gs://staging.dfuseio-global.appspot.com/lidar/spkgs/lidar-yuga-retention-v0.0.2.spkg" \
      "map_transfers" \
      ".transfers[]" \
      "$localfolder" \
      "12292922:+10" \
      "$@"

  popd &> /dev/null
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
