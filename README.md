# Lenso Data Export / Retention Plugin

This repository contains a removable vNext data-governance coordinator. It does
not read another Plugin's private tables. App composition binds every source of
truth through one of two many-provider contracts:

- `lenso.data-export-source@1` contributes a bounded, sensitive inline export;
- `lenso.retention-participant@1` applies an idempotent delete or anonymize
  action and returns a durable receipt.

The coordinator provides `lenso.data-export@1` and `lenso.data-retention@1` to
exact configured caller instances. Each artifact or action is owned by the
exact caller Instance that first persisted its globally unique ID. A different
allowlisted caller cannot read, purge, or continue that owner's state, and a
same-ID retry from that caller is an idempotency conflict. Stable IDs make
same-owner export creation and retention execution retry-safe. Retention
receipts are persisted after each participant so a runtime failure can be
resumed without repeating completed participants.

The four Capability crates are public because other removable Plugins provide
or consume these roles. The PostgreSQL coordinator remains unpublished. See
[`docs/release-process.md`](docs/release-process.md) for the gated Trusted
Publishing workflow.

Export sources must treat `collect_export` as a side-effect-free, repeatable
read. Concurrent attempts with the same export ID can collect before one result
wins persistence. Retention participants must treat `action_id` as an
at-least-once idempotency key; once a completed receipt is stored, later racing
rejections cannot downgrade it.

## Deliberate first-slice limit

The workspace does not yet expose a modern blob/content-vault Capability or a
durable audit Capability. Consequently, v1 stores only a strictly configured,
size-bounded inline export in its private PostgreSQL schema. The total bound is
checked against both contributed payload bytes and the serialized JSON artifact
including its envelope. A source payload containing Unicode NUL is a protocol
violation because PostgreSQL `jsonb` cannot preserve it. Payload fields are
marked sensitive and their owning callers must purge them explicitly. This is a
real small artifact path, not a claim of large-file delivery, long-term
archival, or regulatory audit completeness.
