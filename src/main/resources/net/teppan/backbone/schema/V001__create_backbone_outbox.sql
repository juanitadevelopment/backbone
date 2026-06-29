-- Transactional outbox: events are inserted in the SAME transaction as the
-- business change that produced them, then delivered after commit by a poller.
CREATE TABLE backbone_outbox (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_type   VARCHAR(500) NOT NULL,
    payload      BYTEA        NOT NULL,
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP    NULL
);

CREATE INDEX idx_backbone_outbox_pending
    ON backbone_outbox (processed_at, id);
