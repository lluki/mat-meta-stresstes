A materialize stress test designed to find memory leaks in the meta data architecture.

This script will send a fixed number of SQL commands, which are undone afterwards. For example a `CREATE VIEW` is always followed by a corresponding `DROP VIEW`.


As of now, it tests 

 * INSERT into / DELETE from table
 * CREATE / DROP table
 * CREATE / DROP view
 * CREATE / DROP materialized view
 * CREATE / DROP replica
 * SELECT
 * SUBSCRIBE


The created objects are very simple.
