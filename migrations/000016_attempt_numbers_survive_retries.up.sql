-- Attempt numbers that keep counting across manual retries and redrives.
--
-- notifications.attempts is the attempt budget: a retry or redrive resets
-- it to 0 so the notification gets max_attempts fresh tries. It also
-- numbered the attempt rows, so after a reset the new attempts reused
-- numbers 1, 2, ... and the unique (notification_id, attempt_no) key
-- silently dropped them: a notification could be "sent" with only its
-- old failed attempts in its history.
--
-- total_attempts is never reset. Each claim increments it with attempts,
-- and it numbers the attempt rows, so the history is complete and in
-- order whatever happened in between.
ALTER TABLE notifications
    ADD COLUMN total_attempts integer NOT NULL DEFAULT 0 CHECK (total_attempts >= 0);

UPDATE notifications n
SET total_attempts = greatest(n.attempts, coalesce(a.last_no, 0))
FROM (
    SELECT n2.id, (SELECT max(attempt_no) FROM notification_attempts x WHERE x.notification_id = n2.id) AS last_no
    FROM notifications n2
) a
WHERE a.id = n.id AND (n.attempts > 0 OR a.last_no IS NOT NULL);
