CREATE TABLE IF NOT EXISTS entitlements (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id    text NOT NULL CHECK (length(team_id) > 0 AND length(team_id) <= 256),
    type       text NOT NULL CHECK (type IN ('seat', 'quota')),
    total      bigint NOT NULL CHECK (total > 0),
    used       bigint NOT NULL DEFAULT 0 CHECK (used >= 0),
    status     text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cancelled')),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    version    bigint NOT NULL DEFAULT 1,
    UNIQUE (team_id, type, expires_at)
);

CREATE TABLE IF NOT EXISTS allocations (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entitlement_id   uuid NOT NULL REFERENCES entitlements(id) ON DELETE CASCADE,
    member_id        text NOT NULL CHECK (length(member_id) > 0 AND length(member_id) <= 256),
    amount           bigint NOT NULL CHECK (amount > 0),
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (entitlement_id, member_id)
);

CREATE TABLE IF NOT EXISTS usages (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entitlement_id   uuid NOT NULL REFERENCES entitlements(id) ON DELETE CASCADE,
    member_id        text NOT NULL CHECK (length(member_id) > 0 AND length(member_id) <= 256),
    usage_key        text NOT NULL CHECK (length(usage_key) > 0 AND length(usage_key) <= 256),
    amount           bigint NOT NULL CHECK (amount > 0),
    -- Snapshot of the entitlement record returned by the first (accepted)
    -- request, so replays return the first result verbatim.
    result_used      bigint NOT NULL,
    result_version   bigint NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (entitlement_id, usage_key)
);

CREATE INDEX IF NOT EXISTS idx_usages_entitlement_member ON usages (entitlement_id, member_id);
