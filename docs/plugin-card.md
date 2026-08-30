# vNext Data Export / Retention Plugin card

## Owner and deletion boundary

`lenso-data-export-retention-postgres-plugin` owns only bounded export artifacts,
retention action identities, the frozen participant set, and participant
receipts. Source Plugins own and mutate their business data. Removing this
Plugin deletes coordination state but never reaches into another private schema.

## Roles and authority

- Provides `lenso.data-export@1` to exact configured export callers.
- Provides `lenso.data-retention@1` to exact configured retention callers.
- Requires every explicitly bound `lenso.data-export-source@1` provider through
  a `many` Port.
- Requires every explicitly bound `lenso.retention-participant@1` provider
  through a separate `many` Port.
- Requires zero or more explicitly bound `lenso.retention-guard@1` providers.
  All guards must allow before any participant can run.
- Requires `lenso.secrets@1` only for its private PostgreSQL URL.

Each export or retention action is bound to the exact configured caller
Instance that first persists its globally unique ID. Other allowlisted callers
are peers, not ambient cross-owner administrators: they cannot read, purge, or
continue that state, and their same-ID create/execute retry conflicts.

The trusted caller remains responsible for authenticating the human and
authorizing the subject/scope request. Every source remains final authority for
disclosing its own data, and every participant remains final authority over its
own mutation.

## First observable behavior

An export caller supplies a stable export ID. All bound sources contribute in
resolved Plan order, payload and serialized-envelope bounds are enforced before
persistence, and an exact same-owner retry returns the existing artifact. A
retention caller supplies a stable action ID. The coordinator first asks every
guard about the exact action, scope, subject, mode, and reason. A deny returns
`blocked_by_guard`; a Domain or Runtime failure fails closed. It then freezes the
caller owner and participant Instance list, stores each completion or rejection,
skips completed participants on same-owner retry, and exposes pending state when
runtime failure interrupted delivery.

Retention participants must make `action_id` idempotent because concurrent
retries may invoke the same participant at least once. Rejected participants
are retried; a stored completion is sticky and cannot be downgraded by a racing
rejection. Export sources must make collection a side-effect-free repeatable
read because concurrent attempts can collect before the stable export ID is
persisted.

## Explicitly deferred

Large downloadable archives, encryption-at-rest keys distinct from PostgreSQL,
automatic artifact expiry, legal-hold case policy, immutable Audit outbox, scheduled
retention scans, and background Jobs are deferred until their own modern
Capabilities exist. Legal Hold policy is supplied by a removable Guard provider,
not owned here. The current inline artifact is limited to 16 MiB by schema
configuration and defaults to no ambient caller authority.
