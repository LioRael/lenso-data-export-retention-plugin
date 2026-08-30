# Release process

The five public Capability crates are reusable contracts for data-governance
participants. The PostgreSQL coordinator remains application-internal and is
not published.

Publish `lenso-capability-retention-guard` before a Legal Hold provider that
implements it. `lenso.data-retention@1` Descriptor 1.1 adds the stable
`blocked_by_guard` outcome while retaining the existing major contract.

Trusted Publisher coordinates for each public crate are:

- repository owner: `LioRael`
- repository name: `lenso-data-export-retention-plugin`
- workflow filename: `release-plz.yml`
- environment: unset

The live workflow is manual and requires both `live=true` and
`confirm=publish` on `main`. Pushes to `main` may create a release PR but never
publish directly.
