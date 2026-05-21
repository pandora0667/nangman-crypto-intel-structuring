# intel-structuring-app

`intel-structuring-app` is the INTEL-L1 stateless worker.

Repository: `git@github.com:pandora0667/nangman-crypto-intel-structuring.git`

It consumes `RAW_INTEL` pointer messages from NATS JetStream, recovers raw evidence from RustFS, reads Market-L1 only through the `l1_index -> manifest -> report -> output_object_keys` contract path, structures the event, writes INTEL-L1 objects to S3, publishes structured pointers to NATS, and only then acknowledges the original RAW_INTEL message.

## Runtime contract

```text
RAW_INTEL durable pull consumer
  -> RustFS raw recovery and sha verification
  -> Market-L1 admission
  -> L0 source/content quality admission
  -> rule/NLP/NLI
  -> deterministic evidence_pack with stable evidence IDs
  -> Claude Haiku 4.5 Global structured-output extraction
  -> Haiku repair for evidence/schema gate misses
  -> Claude Sonnet 4.6 Global for hard/high-risk/high-impact cases
  -> immutable story_member write
  -> refreshed story_cluster merge
  -> S3 JSONL outputs, manifest, and prepared index
  -> NATS structured pointer publish ack
  -> success index write
  -> RAW_INTEL double ack
```

The app is stateless. Local disk is used only for temporary parquet scanning and can be lost at any time.
Story state is reconstructed from S3 `story-members/` objects, so ECS Spot restart does not depend on container memory or local files.

Market-L1 context lookup checks exact/radius windows first, then scans every
hour in the configured latest-before lookback range. It must not jump from the
current hour to only the oldest lookback hour, because that can attach stale L1
manifests and empty universe snapshots even when fresher success pointers exist
in the middle of the lookback window.

## Semantic routing

L0 quality metadata from `intel-crawl-app` is part of the L1 decision input:

```text
content_kind
content_quality
content_quality_score
source_quality
source_relevance_scope
```

Rule-only output is blocked when the raw item is community reaction, market snapshot, title-only, or metadata fallback evidence. Direct, high-quality community reaction can finish on Haiku. Weak global symbol-scan evidence can still call Haiku first, but any structured claim is escalated to Sonnet only when the Sonnet admission contract allows it.

Single numeric derivatives snapshots are never allowed to use Sonnet. `stale_but_usable` market context is preserved in the packet as audit context, but it is not strong enough to open expensive model escalation for numeric snapshots. Non-critical safety escalation also respects `INTEL_L1_SONNET_BUDGET_RATIO` as a hard budget.

## Local run

```bash
git clone git@github.com:pandora0667/nangman-crypto-intel-structuring.git
cd nangman-crypto-intel-structuring
cp .env.example .env
sudo docker compose -f compose.yml --env-file .env up --build
```

Set `INTEL_L1_ENABLE_BEDROCK=true` only after the ECS task role or local AWS credentials can invoke the configured Bedrock inference profiles.

## Quality gate

```bash
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
docker buildx build --platform linux/arm64 -t intel-structuring-app:local --load .
```

The GitHub Actions workflow runs formatting, tests, clippy, coverage generation, SonarQube scan, and SonarQube Quality Gate on `main`.

## Runtime prerequisites

```text
NATS:
- RAW_INTEL stream exists
- app can create or access STRUCTURED_INTEL

RustFS:
- INTEL_L1_L0_RUSTFS_ACCESS_KEY_ID
- INTEL_L1_L0_RUSTFS_SECRET_ACCESS_KEY
- read access to intel-crawl-app-l0

AWS:
- read access to Market-L1 bucket
- write/read access to INTEL-L1 output bucket
- Bedrock invoke permission when INTEL_L1_ENABLE_BEDROCK=true
```

The worker handles both Ctrl-C and Linux `SIGTERM`. If ECS Spot stops the task, the current message is only acknowledged after the complete success path; otherwise JetStream redelivers it.

The app emits CloudWatch Embedded Metric Format JSON to stdout. ECS should route stdout to CloudWatch Logs; alarms belong to the ECS/service infrastructure layer.
