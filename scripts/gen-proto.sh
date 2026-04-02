#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

protoc \
  --proto_path="${ROOT}/proto" \
  --go_out="${ROOT}" \
  --go_opt=module=github.com/puper/klock \
  --go-grpc_out="${ROOT}" \
  --go-grpc_opt=module=github.com/puper/klock \
  --plugin=protoc-gen-go="${HOME}/go/bin/protoc-gen-go" \
  --plugin=protoc-gen-go-grpc="${HOME}/go/bin/protoc-gen-go-grpc" \
  "${ROOT}/proto/lock.proto"

echo "generated: pkg/lockrpcpb/lock.pb.go pkg/lockrpcpb/lock_grpc.pb.go"
