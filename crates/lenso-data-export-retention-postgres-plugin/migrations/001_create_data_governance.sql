CREATE TABLE data_exports (
    export_id text PRIMARY KEY,
    requester_instance text NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    subject text NOT NULL,
    source_count bigint NOT NULL CHECK (source_count > 0),
    item_count bigint NOT NULL CHECK (item_count >= 0),
    total_bytes bigint NOT NULL CHECK (total_bytes >= 0),
    items jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE retention_actions (
    action_id text PRIMARY KEY,
    requester_instance text NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    subject text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('delete', 'anonymize')),
    reason text NOT NULL,
    participant_instances text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CHECK (cardinality(participant_instances) > 0)
);

CREATE TABLE retention_results (
    action_id text NOT NULL REFERENCES retention_actions(action_id) ON DELETE CASCADE,
    provider_instance text NOT NULL,
    status text NOT NULL CHECK (status IN ('completed', 'rejected')),
    receipt text,
    attempted_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (action_id, provider_instance),
    CHECK (
        (status = 'completed' AND receipt IS NOT NULL)
        OR (status = 'rejected' AND receipt IS NULL)
    )
);
