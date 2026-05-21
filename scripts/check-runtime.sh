#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${1:-$APP_DIR/.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${NATS_URL:?NATS_URL is required}"
: "${INTEL_L1_OUTPUT_S3_BUCKET:?INTEL_L1_OUTPUT_S3_BUCKET is required}"
: "${INTEL_L1_MARKET_L1_BUCKET:?INTEL_L1_MARKET_L1_BUCKET is required}"
: "${AWS_REGION:=ap-northeast-2}"

echo "[1/4] NATS RAW_INTEL stream"
docker run --rm natsio/nats-box:0.17.0 \
  nats --server "$NATS_URL" stream info "${INTEL_L1_RAW_NATS_STREAM:-RAW_INTEL}" >/dev/null

echo "[2/4] Market-L1 bucket"
aws s3api head-bucket \
  --bucket "$INTEL_L1_MARKET_L1_BUCKET" \
  --region "${INTEL_L1_MARKET_S3_REGION:-$AWS_REGION}" >/dev/null

echo "[3/4] INTEL-L1 output bucket"
aws s3api head-bucket \
  --bucket "$INTEL_L1_OUTPUT_S3_BUCKET" \
  --region "${INTEL_L1_OUTPUT_S3_REGION:-$AWS_REGION}" >/dev/null

echo "[4/4] Bedrock inference profiles"
profile_count="$(aws bedrock list-inference-profiles \
  --region "${BEDROCK_REGION:-$AWS_REGION}" \
  --query 'length(inferenceProfileSummaries[?inferenceProfileId==`global.anthropic.claude-haiku-4-5-20251001-v1:0` || inferenceProfileId==`global.anthropic.claude-sonnet-4-6`])' \
  --output text)"
if [[ "$profile_count" != "2" ]]; then
  aws bedrock list-inference-profiles \
    --region "${BEDROCK_REGION:-$AWS_REGION}" \
    --query 'inferenceProfileSummaries[?contains(inferenceProfileId, `claude-haiku`) || contains(inferenceProfileId, `claude-sonnet`)].[inferenceProfileId,status]' \
    --output table
  echo "required Bedrock global inference profiles were not both found" >&2
  exit 1
fi

echo "runtime checks passed"
