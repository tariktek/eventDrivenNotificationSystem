-- Retention deletes notifications that reached a final status long ago.
-- updated_at is when they reached it: a final notification is not
-- updated again (a redrive moves it back to pending first). The partial
-- index keeps finding them a range scan over final rows only, oldest
-- first, however large the table grows.
CREATE INDEX notifications_final_updated_idx
    ON notifications (updated_at)
    WHERE status IN ('sent', 'failed', 'cancelled');
