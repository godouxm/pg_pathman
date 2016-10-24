\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

/* Init tables with range and hash partitioning */
CREATE TABLE test.range_partitioned(id integer not null, val real);
INSERT INTO test.range_partitioned SELECT generate_series(1, 10000), random();
SELECT pathman.create_range_partitions('test.range_partitioned', 'id', 1, 1000);
CREATE TABLE test.hash_partitioned(id integer not null, val real);
INSERT INTO test.hash_partitioned SELECT generate_series(1, 10000), random();
SELECT pathman.create_hash_partitions('test.hash_partitioned', 'id', 6);

/* Test select ... where id in (1, 2, 3, 4) */
EXPLAIN (COSTS OFF) SELECT * FROM test.range_partitioned WHERE id IN (1, 2, 3, 4);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_partitioned WHERE id IN (1, 2, 3, 4);

DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
