-- Dead-letter support for the transactional outbox.
--
-- Without this, a delivery that always fails (a "poison" event) either retries
-- forever or, on decode failure, was silently dropped. These columns let the
-- poller count attempts, record the last error, and move an event to a terminal
-- DEAD state after a maximum number of attempts, so it can be inspected,
-- retried, or discarded rather than blocking or vanishing.
ALTER TABLE backbone_outbox ADD COLUMN status     VARCHAR(16) DEFAULT 'PENDING' NOT NULL;
ALTER TABLE backbone_outbox ADD COLUMN attempts   INT         DEFAULT 0         NOT NULL;
ALTER TABLE backbone_outbox ADD COLUMN last_error VARCHAR(2000);
ALTER TABLE backbone_outbox ADD COLUMN failed_at  TIMESTAMP   NULL;

-- Backfill: rows already delivered before this migration are terminal.
UPDATE backbone_outbox SET status = 'PROCESSED' WHERE processed_at IS NOT NULL;

-- The poller now selects by status; index it for the pending scan.
CREATE INDEX idx_backbone_outbox_status ON backbone_outbox (status, id);
