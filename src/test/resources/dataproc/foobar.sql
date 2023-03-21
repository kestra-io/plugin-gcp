CREATE EXTERNAL TABLE foo(bar int) LOCATION 'gs://spark-jobs-kestra/';
INSERT INTO foo values (1);
INSERT INTO foo values (2);
INSERT INTO foo values (3);
INSERT INTO foo values (4);
SELECT * FROM foo WHERE bar > 2;