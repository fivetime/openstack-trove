===================================================
Cassandra and MongoDB clusters: repairs and resyncs
===================================================

Two kinds of upkeep that a Cassandra or MongoDB cluster needs, and that
the members of a cluster created by Trove do by themselves. Neither needs
the task manager: each member decides for itself, from what its database
tells it.

Cassandra: repairs
~~~~~~~~~~~~~~~~~~

The replicas of a Cassandra table drift apart when a node is down for
longer than the hint window (3 hours by default): the writes it missed
are not replayed to it. A deleted row can come back when a node that
still has it was not repaired within ``gc_grace_seconds`` of the delete
(10 days by default). Repairs fix both, and have to run regularly.

Cassandra 5.0.8 and later schedule repairs themselves (the repair
scheduler of CEP-37, in 6.0 and backported to 5.0.8): the nodes take turns
repairing their ranges, a replica that shares ranges with one being
repaired waits for its turn, and the history is in
``system_distributed.auto_repair_history``. The guest agent of a member of
a cluster turns it on, for full repairs:

- ``auto_repair`` in ``cassandra.yaml``, with the interval of
  ``[cassandra] auto_repair_interval``;
- the start option ``-Dcassandra.autorepair.enable=true``, which creates
  the tables of the scheduler. Once given it is not taken away: with
  ``[cassandra] auto_repair = false`` the member still starts with it, and
  the scheduler is off in ``cassandra.yaml``.

A single instance has nothing to repair. A server older than 5.0.8 has no
scheduler, and refuses to start with ``auto_repair`` in its
configuration: the guest agent reads the version from the
``CASSANDRA_VERSION`` of the image and leaves such a cluster as it was;
repair it with ``nodetool repair`` yourself.

Options, in ``[cassandra]`` of the guest agent:

``auto_repair`` (``true``)
  Whether the members of a cluster repair it themselves.
``auto_repair_interval`` (``86400``)
  Seconds between two full repairs of a node. Keep it well below the
  ``gc_grace_seconds`` of the tables.

Check it on a member with ``nodetool getautorepairconfig`` and
``nodetool autorepairstatus -t full``.

MongoDB: members that fell behind
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A replica set member (of a shard, or a config server) that was down for
longer than the oplog of the other members covers can never catch up: it
stays ``RECOVERING``, with nothing in ``replSetGetStatus`` to say why. The way back MongoDB documents
is an initial sync from an empty data directory.

Two options keep a cluster out of this, and get it out:

``oplog_min_retention_hours`` (``0``)
  Hours of writes the oplog of a member keeps at least
  (``storage.oplogMinRetentionHours``): a member down for less than that
  catches up by itself. The oplog grows past its size when it has to, on
  the data volume of every member, so size the volumes for the writes of
  that many hours. ``0`` keeps the oplog of a fixed size, as MongoDB does.
  The guest agent writes it into the configuration of a member when it
  starts, and sets it on the running server (``replSetResizeOplog``) when
  it differs, so a change of the option needs no restart.
``resync_stale_members`` (``true``)
  Whether a member that is too stale to catch up resyncs by itself: it
  stops, empties its data directory and starts again, and MongoDB copies
  the data from the others. A member counts as too stale when it is
  ``RECOVERING``, the set has a primary, and its server turned down every
  member it could sync from within the last five minutes because the
  oplog of that member starts after the last write it fetched. The
  server compares this itself and logs each candidate it turns down (log
  id 5579708, about once a minute each); the guest agent reads those
  lines from the log the server keeps in memory (``getLog``). It never
  connects to another member, which with network isolation only the
  database container could. When anything cannot be asked, the member
  does not count as stale. Its data is behind the others' and has
  nothing they lack.
``resync_stale_after`` (``300``)
  Seconds a member has been too stale before it resyncs.

A replica set that lost the majority of its members for good, and a
member that is the only one with some data, are not handled: they need a
decision that loses or rolls back writes.
