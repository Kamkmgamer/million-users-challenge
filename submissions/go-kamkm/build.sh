#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PATH=/usr/local/go/bin:$PATH
export CGO_ENABLED=0
mkdir -p bin
go mod download
go build -trimpath -ldflags='-s -w' -o bin/feed-go-luna .
echo "built bin/feed-go-luna ($(go version | awk '{print $3}'), cgo disabled)"
