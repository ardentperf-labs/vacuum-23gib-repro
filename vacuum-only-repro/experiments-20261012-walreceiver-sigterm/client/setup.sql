-- One logged row per pgbench client. The table is intentionally tiny, has no
-- indexes (so row updates can be HOT), and disables autovacuum to avoid adding
-- background work to the VACUUM measurement.
CREATE TABLE public.pgbench_client_rows (
    client_id integer NOT NULL,
    update_count bigint NOT NULL
) WITH (autovacuum_enabled = false);

INSERT INTO public.pgbench_client_rows (client_id, update_count)
SELECT g.client_id, 0
FROM generate_series(0, 9) AS g(client_id);

GRANT SELECT, UPDATE ON public.pgbench_client_rows TO app;
