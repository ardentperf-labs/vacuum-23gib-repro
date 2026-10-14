-- This single statement is pgbench's transaction. client_id is its stable
-- per-session identifier (0..9), so concurrent clients update distinct rows.
UPDATE public.pgbench_client_rows
SET update_count = update_count + 1
WHERE client_id = :client_id;
