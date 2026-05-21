# INTEL-L1 Enterprise DoD

The app is not complete until all items pass:

- RAW_INTEL durable consumer reads pointer messages.
- RustFS raw record is recovered by pointer range and `content_sha256` is verified.
- Market-L1 is read only through pointer admission.
- Market-L1 missing state becomes `pending` or `unavailable`, not a crash.
- L0 `content_quality`, `source_quality`, and `source_relevance_scope` are preserved into model prompts and source summaries.
- Market-L1 compact symbol summaries are included in model prompts when available.
- Rule-only output is blocked for weak raw evidence such as community reaction, market snapshot, title-only, or metadata fallback inputs.
- Rule/NLP/NLI runs before any model call.
- Claude Haiku 4.5 Global is the primary online model when enabled.
- Haiku receives bounded evidence IDs instead of unconstrained full-body prompts.
- Haiku repair is attempted for local evidence/schema gate misses before Sonnet escalation.
- Claude Sonnet 4.6 Global is the final escalation model when enabled.
- Sonnet is reserved for hard, high-risk, high-impact, conflicting, weak global symbol-scan, or low quality-score claims and produces a terminal decision except broken input quarantine cases.
- Low-confidence numeric market snapshots without available Market-L1 context must stop at Haiku instead of escalating to Sonnet.
- Context flags are emitted only for terminal high-confidence or general-market outputs, and funding flags require available Market-L1 context.
- Story members are written as immutable S3 objects and refreshed story clusters merge prior sources.
- Structured packet, context flag, story cluster, health event, manifest, and prepared index are written before NATS publish.
- NATS structured pointer publish receives JetStream ack.
- Success index is written after NATS publish ack and before RAW_INTEL ack.
- RAW_INTEL double ack occurs only after the complete success path.
- SIGTERM does not acknowledge an incomplete message.
- Duplicate redelivery only skips when the success index exists; prepared-only runs are retried.
- Duplicate redelivery reuses deterministic keys and does not create duplicate S3 output.
- Forbidden trade output terms are blocked.
- Unit, integration, failure, duplicate, schema, and Docker build checks pass.
