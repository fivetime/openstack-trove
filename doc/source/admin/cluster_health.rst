==============
Cluster health
==============

``openstack database cluster show`` and ``cluster list`` show the health
of a cluster, as the task manager last found it::

    health              DEGRADED
    health_members_out  mycluster-member-3
    health_reason       not answering: mycluster-member-3
    health_since        2026-10-19T18:02:11

``HEALTHY``
  Every member answers and none sees a member out.
``DEGRADED``
  The cluster serves, short of members: a member does not answer or its
  heartbeat is stale, a member sees another one out, or a part of the
  cluster says it is degraded (a MongoDB config server replica set
  without a primary).
``DOWN``
  No member answers, or the cluster or a part of it no longer serves: a
  Redis cluster with hash slots no master serves, a MongoDB shard without
  a primary, a Cassandra ring with most of its nodes down, a PostgreSQL
  cluster without a writable primary, a Galera cluster without a primary
  component.
``UNKNOWN``
  Not looked at yet: a new cluster, until the task manager's next scan.

How it is found
~~~~~~~~~~~~~~~

Redis, Valkey, KeyDB, MongoDB, Cassandra and Vertica clusters heal
themselves (the datastore fails over and catches up on its own); the
guest agent of each member runs a watch that asks its own database what
it sees of the cluster: ``CLUSTER INFO`` and ``CLUSTER NODES``,
``replSetGetStatus``, ``nodetool status``, the ``nodes`` and ``system``
tables. With network isolation a guest agent cannot reach the other
members; the database knows them through its own gossip or heartbeats.
When the member sees its cluster short of members or not serving, it
tells the task manager through the conductor, again every
``cluster_event_interval`` seconds while it lasts, and once more when the
cluster is whole again. The task manager then asks every member
(``get_cluster_health``), counts as out the members that do not answer
within ``cluster_health_timeout`` or whose heartbeat is older than
``agent_heartbeat_expiry``, and records the health; a cluster that is
not healthy is looked at again every ``cluster_health_interval`` seconds
until it is. Members named by their address in what the others see are
shown by their Trove names once the task manager has learnt them.

PostgreSQL and Galera clusters get their health from the views their
recovery asks for anyway: a member out of the cluster (a PostgreSQL
member that is neither the primary nor a standby streaming from it, a
Galera member out of the primary component) is out.

The task manager's scan (``cluster_recovery_scan_interval``) also looks
at the clusters whose health is not recorded yet, and at those with a
member whose heartbeat is stale: a cluster whose every member is down
tells nothing by itself.

A member whose guest agent is from before the watch counts as up while
its heartbeat is fresh.

Options
~~~~~~~

In the section of each of these datastores (``[redis]``, ``[valkey]``,
``[keydb]``, ``[mongodb]``, ``[cassandra]``, ``[vertica]``):

``cluster_probe_interval`` (``30``)
  Seconds between two looks of the guest agent; ``0`` turns the watch
  off.
``cluster_probe_timeout``
  Seconds a look may take (``10`` for Redis and MongoDB, ``30`` for
  Cassandra, whose ``nodetool`` starts a JVM, ``20`` for Vertica).
``cluster_events`` (``true``), ``cluster_event_interval`` (``300``), ``cluster_event_min_interval`` (``5``)
  Whether and how often a member tells.
``cluster_health_timeout`` (``30``), ``cluster_health_interval`` (``120``)
  How long the task manager waits for a member, and how often it looks
  at a cluster that is not healthy.

The health is kept in its own table (``cluster_health``), written only
when it changes.
