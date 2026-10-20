=========================================
Acknowledged writes and losing power
=========================================

A database acknowledges a write before or after it is on disk, as it is
configured. A write acknowledged before is lost when the instance, or the
host it runs on, loses power before the database wrote it. Trove keeps
each datastore's own default; where the default may lose acknowledged
writes, a configuration group (:doc:`manage-db-config`) asks for writes
to be on disk first, at a cost to write latency and throughput.

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Datastore
     - By default, on power loss
     - On disk before acknowledged
   * - MySQL, Percona Server, MariaDB, Percona XtraDB Cluster,
       PostgreSQL, CouchDB, Db2
     - Nothing acknowledged is lost.
     -
   * - Redis, Valkey, KeyDB
     - Up to the last second (``appendfsync everysec`` of the append
       only file Trove turns on).
     - ``appendfsync`` ``always``.
   * - MongoDB (a single instance)
     - Up to the journal commit interval (100 ms), unless the client
       asks for ``j: true``.
     - The client's write concern ``{j: true}``; or a shorter interval,
       ``storage.journal.commitIntervalMs`` (1 to 500).
   * - Cassandra
     - Up to ``commitlog_sync_period`` (10 s, ``commitlog_sync``
       ``periodic``).
     - ``commitlog_sync`` ``batch``, or ``group`` (synced together every
       ``commitlog_sync_group_window``, 15 ms unless set); or a shorter
       ``commitlog_sync_period``.
   * - Couchbase
     - Whatever was acknowledged and not yet written, with no bound: the
       server acknowledges a write once it is in memory.
     - ``durability_min_level`` ``persistToMajority`` or
       ``majorityAndPersistActive`` (on a single node the majority is
       the node), kept on every bucket, those there already included.
   * - Vertica
     - A single instance may lose recent commits or not start again.
     - The extended property ``sync_writes=true`` at creation.

The members of a MongoDB replica set acknowledge a write once a majority
of them journaled it (the default write concern ``w: majority``), so a
member that loses power alone loses nothing acknowledged.

A Cassandra ``commitlog_sync`` other than ``periodic`` is written without
``commitlog_sync_period``, which the server refuses with it. The server
takes only ``periodic``, ``batch`` and ``group``: an instance given
another value does not start until the configuration group is detached.

A Couchbase bucket keeps the least durability it was last given when the
configuration group is detached. A value the server does not know is
an error when the group is applied, as a memory quota it refuses is, and
the buckets keep the level they had.
