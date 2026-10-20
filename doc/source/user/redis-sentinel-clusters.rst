==============================================
Redis, Valkey and KeyDB clusters with sentinels
==============================================

A Redis family cluster is one of two topologies, chosen when it is
created with the extended property ``topology``:

``cluster`` (the default)
  A Redis Cluster: hash slots spread over masters, each with its
  replicas (``replicas_per_master``). Its clients reach every node and
  must speak Redis Cluster (no ``SELECT``; the keys of a command, a
  transaction or a script in one slot; ``MOVED``/``ASK`` redirections).
``sentinel``
  A primary with replicas and a sentinel beside each, which elect a new
  primary when the primary is gone (in a few seconds). Every command an
  application uses with a single server works. At least three members:
  a majority of the sentinels has to agree a primary is gone.

.. code-block:: console

   $ openstack database cluster create cache valkey 9 \
       --extended-properties topology=sentinel,replication_mode=sync \
       --instance flavor=m1.small,volume=2 \
       --instance flavor=m1.small,volume=2 \
       --instance flavor=m1.small,volume=2

``replication_mode``
  ``sync`` (the default): a primary that no replica follows refuses
  writes (``min-replicas-to-write 1``), so that a primary cut off from
  the others takes no write the cluster then loses; while every replica
  is down, the cluster takes no writes either. ``async``: the primary
  takes writes regardless; a primary cut off may take writes for a few
  seconds that the cluster loses when it rejoins as a replica.

Connecting
~~~~~~~~~~

``openstack database cluster show`` gives three endpoints, all on one
load balancer, and the name the sentinels know the primary by
(``sentinel_master_name``, ``trove``):

``endpoint`` (port 6379)
  The primary, whichever it is now. For an application that knows
  nothing of sentinels.
``read_endpoint`` (port 6380)
  The replicas that keep up with the primary, for reads.
``sentinel_endpoint`` (port 26379)
  The sentinels, for a client that speaks sentinel and asks them where
  the primary is (``SENTINEL GET-MASTER-ADDR-BY-NAME trove``). They
  answer such questions without a password and refuse to be
  reconfigured.

``openstack database cluster list instances`` shows each member's role
now, ``master`` or ``replica``. Root (``openstack database root enable``)
is enabled on every member.

Growing, shrinking, restarting and upgrading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``openstack database cluster grow`` adds members as replicas of the
primary there is, each with its sentinel; the sentinels then agree by a
majority of them all, and root, if it is enabled, is enabled on the new
members with the same password. A member that cannot join is failed
(``GROWING_ERROR``) and the cluster is left as it was.

``openstack database cluster shrink`` removes members, keeping at least
three. The members that go are never elected again; if the primary is
one of them, the sentinels elect another first (writes stop for a few
seconds, as for any failover). The sentinels that stay then agree by a
majority of them, and learn the members again.

``openstack database cluster restart`` and ``upgrade`` take the members
one after another, the replicas first; the primary last, and only once
the sentinels elected another, so that writes stop once, for a
failover, and not for the primary's restart.
