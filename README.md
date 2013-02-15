hadoop-sync
===========

hadoop-sync is a Java application that synchronizes block metadata from the HDFS
NameNode to the CitusDB master node. The application also creates a colocated
foreign table on CitusDB worker nodes for each block in the Hadoop File System
(HDFS). Optionally, the application also collects statistics about the underlying
HDFS blocks.

hadoop-sync is idempotent. You can run it multiple times; it calculates metadata
differences between the HDFS NameNode and CitusDB master node, and only syncs
metadata for newly added or removed HDFS blocks. If no such blocks exist, the
application does nothing.

hadoop-sync also leaves metadata on the CitusDB master node in a consistent
state. When you run the application, it either completes by propagating enough
metadata that makes recent HDFS blocks queriable; or it errors out by reverting
recent metadata updates and leaves CitusDB in the queriable state it was in
before hadoop-sync was run.

hadoop-sync assumes that you are already running a Hadoop cluster and you have
your CitusDB databases colocated on this cluster. For step by step instructions
on how to get started, please see our documentation page at
http://citusdata.com/docs/sql-on-hadoop

Usage
-----

Our [documentation page](http://citusdata.com/docs/sql-on-hadoop) explains in
detail running the hadoop-sync application. The following section talks about a
few details that relate to hadoop-sync's internals. Please note that these
details may change over time as hadoop-sync is still in Beta form.

Now, assuming that you already have a Hadoop cluster with data loaded into it,
CitusDB deployed on the Hadoop cluster, and a distributed foreign table created
on the CitusDB master node, you synchronize metadata like the following:

    java -jar target/hadoop-sync-0.1.jar table_name [--fetch-min-max]

The distributed foreign table name is a required argument, and hadoop-sync will
only synchronize block metadata for that particular table. The fetch-min-max is
an optional argument that triggers the collection of min/max statistics for each
_new_ HDFS block. Since this requires going over all records in the HDFS block,
passing this argument notably increases synchronization times. On the other
hand, with these statistics, CitusDB can perform optimizations such as partition
or join pruning that dramatically reduce query execution times.

It is worth mentioning that these min/max statistics are only relevant in the
context of distributed queries. For local queries, the worker nodes themselves
already use PostgreSQL's statistics collector to optimize queries, if the
foreign data wrapper supports data sampling.

It is also worth noting that hadoop-sync relies on a properties file to
determine node names and port numbers that Hadoop and CitusDB clusters use.
This sync.properties file is bundled with the JAR file; and you can change the
default settings by using emacs or vi on the JAR bundle, or by copying the file
from `target/classes/sync.properties` to the current directory and editing it.
