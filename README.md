## watch.py

Log mem and cpu utilization of running materialize processes (environmentd/storaged/computed):

Do `sudo apt install python3-pandas` first.

`./watch.py -c` to capture memory usage (writes a csv file to `/tmp/matmem.csv`)
after a while use `./watch.py -p` to plot the mem usage. You might need a X connection
for the pyplot window to popup.

## stresstest.py
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

## repro18.py

Try to reproduce incident #18 by creating a source, dropping a replica, waiting until
data accumulates and restarting that replica.

