# Release process

The four public Capability crates are reusable contracts for data-governance
participants. The PostgreSQL coordinator remains application-internal and is
not published.

Trusted Publisher coordinates for each public crate are:

- repository owner: `LioRael`
- repository name: `lenso-data-export-retention-plugin`
- workflow filename: `release-plz.yml`
- environment: unset

The live workflow is manual and requires both `live=true` and
`confirm=publish` on `main`. Pushes to `main` may create a release PR but never
publish directly.
