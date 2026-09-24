-- Grant-based team service entitlements.
-- A grant is valid for the half-open interval [effective_at, expires_at).
CREATE TABLE IF NOT EXISTS grants (
    tenant_id    text                     NOT NULL,
    grant_id     text                     NOT NULL,
    feature      text                     NOT NULL,
    amount       bigint                   NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    expires_at   timestamp with time zone NOT NULL,
    state        text                     NOT NULL DEFAULT 'active',
    cancelled_at timestamp with time zone,
    created_at   timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT grants_pkey PRIMARY KEY (tenant_id, grant_id),
    CONSTRAINT grants_feature_check CHECK (feature IN ('seats', 'quota')),
    CONSTRAINT grants_amount_check CHECK (amount > 0),
    CONSTRAINT grants_interval_check CHECK (expires_at > effective_at),
    CONSTRAINT grants_state_check CHECK (state IN ('active', 'cancelled'))
);
