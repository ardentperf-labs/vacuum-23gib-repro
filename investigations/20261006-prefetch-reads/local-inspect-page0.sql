SELECT 'relation' AS item,
       pg_relation_size('vacuum_fixture') AS heap_bytes,
       pg_relation_size('vacuum_fixture_id_idx') AS index_bytes,
       relpages, relallvisible, relfrozenxid
FROM pg_class WHERE oid = 'vacuum_fixture'::regclass;
SELECT 'header' AS item, * FROM page_header(get_raw_page('vacuum_fixture', 0));
SELECT 'tuple' AS item, lp, lp_off, lp_flags, lp_len, t_xmin, t_xmax,
       to_hex(t_infomask) AS infomask, to_hex(t_infomask2) AS infomask2
FROM heap_page_items(get_raw_page('vacuum_fixture', 0))
ORDER BY lp;
