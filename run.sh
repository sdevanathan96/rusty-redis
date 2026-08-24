#!/bin/sh
#
# Builds and runs the server in release mode. Extra arguments are passed through.

set -e # Exit early if any commands fail

cd "$(dirname "$0")"
cargo build --release --quiet
exec ./target/release/rusty-redis "$@"
