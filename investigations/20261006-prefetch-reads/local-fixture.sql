\set ON_ERROR_STOP on
CREATE TABLE vacuum_fixture (
  id bigint NOT NULL,
  payload bytea NOT NULL
) WITH (fillfactor = 100);
ALTER TABLE vacuum_fixture ALTER COLUMN payload SET STORAGE PLAIN;
INSERT INTO vacuum_fixture
SELECT g, decode(repeat('5a', 1536), 'hex')
FROM generate_series(1, 1000) AS g;
CREATE INDEX vacuum_fixture_id_idx ON vacuum_fixture (id);
WITH page_counts AS (
  SELECT split_part(trim(both '()' FROM ctid::text), ',', 1)::int AS blockno,
         count(*) AS n
  FROM vacuum_fixture
  GROUP BY 1
)
SELECT 'before-delete' AS phase,
       pg_relation_size('vacuum_fixture') AS heap_bytes,
       pg_relation_size('vacuum_fixture_id_idx') AS index_bytes,
       (SELECT count(*) FROM vacuum_fixture) AS rows,
       count(*) AS heap_pages,
       min(n) AS min_tuples_per_page,
       max(n) AS max_tuples_per_page
FROM page_counts;
DELETE FROM vacuum_fixture WHERE id % 2 = 0;
-- Do not scan the heap after DELETE before the checkpoint. A SELECT can
-- opportunistically prune committed dead tuples during heap_page_prune_opt.
SELECT pg_relation_size('vacuum_fixture') AS heap_bytes_after_delete,
       pg_relation_size('vacuum_fixture_id_idx') AS index_bytes_after_delete,
       pg_relation_filepath('vacuum_fixture') AS heap_path,
       pg_relation_filepath('vacuum_fixture_id_idx') AS index_path,
       pg_relation_size('vacuum_fixture') / 8192 AS heap_blocks,
       pg_relation_size('vacuum_fixture_id_idx') / 8192 AS index_blocks;
CHECKPOINT;
