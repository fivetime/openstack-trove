==================================================
Vertica clusters: storage mode and synced writes
==================================================

A Vertica cluster takes two choices when it is created, as extended
properties. They are independent of each other and cannot be changed later.

``storage_mode``
  ``enterprise`` (the default): the data is on the volumes of the members.
  ``eon``: the data is in a bucket of your project's object storage, and the
  volumes of the members are its cache. Only where the cloud offers it (see
  :doc:`/admin/object_storage`).

``sync_writes``
  ``false`` (the default) or ``true``: whether the data volumes of the members
  are mounted so that every write reaches the disk before it returns.

.. code-block:: console

   $ openstack database cluster create mycluster vertica 25.4 \
       --extended-properties storage_mode=eon,sync_writes=true \
       --instance flavor=<flavor>,volume=<size>,nic=net-id=<net> \
       --instance flavor=<flavor>,volume=<size>,nic=net-id=<net> \
       --instance flavor=<flavor>,volume=<size>,nic=net-id=<net>

   $ openstack database cluster show mycluster -c extended_properties
   {'storage_mode': 'eon', 'sync_writes': True,
    'communal_bucket': 'trove-vertica-<cluster id>'}

An extended property other than these two, or a value that is not one of the
above, is refused.

Why it matters
~~~~~~~~~~~~~~

Vertica does not sync what it commits to disk: it relies on the other members
holding the same data. A member that fails alone loses nothing. When every
member loses power at once, for example in a power failure of the whole site,
the commits of the last moments are not on any disk:

- In Enterprise Mode the database does not start again by itself. An
  administrator has to recover it to an earlier epoch, which loses everything
  committed after it, and after a long run of writes there may be no epoch it
  can be recovered to at all.
- In Eon Mode the data of every commit is in object storage, and the database
  starts again by itself; the commits of the last seconds are lost, because
  the catalog on the members' volumes was not synced either.
- With ``sync_writes`` nothing that was committed is lost and the database
  starts again by itself, in either mode, at a cost to write speed.

Measured on one cloud (three members, a client committing a row about every
0.2 s for ten minutes, then all three powered off at once):

.. list-table::
   :header-rows: 1

   * - Storage mode
     - sync_writes
     - Bulk load of 1M rows
     - Single row commit
     - After the power failure
   * - enterprise
     - false
     - 0.45 s
     - 37 ms
     - does not start by itself; seconds to minutes of commits lost, or not
       recoverable
   * - enterprise
     - true
     - 7.5 s
     - 790 ms
     - starts by itself, nothing lost
   * - eon
     - false
     - 0.55 s
     - 60 ms
     - starts by itself, the last 3 to 15 s lost
   * - eon
     - true
     - 3.0 s
     - 475 ms
     - starts by itself, nothing lost

With ``sync_writes`` the volumes are mounted with sync only while the
database answers: it starts on volumes without it, as it would not start in
time otherwise, and they get sync again within the status interval of the
guest agent (30 s by default). Writes in between are not synced.

Eon Mode: your bucket and your EC2 credential
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The communal storage of an Eon Mode cluster is the bucket shown as
``communal_bucket``, in your project, and counts against your project's
object storage quota. Trove reaches it as a user of your project it makes for
the cluster, ``trove-vertica-<cluster id>``, which can use the project's object
storage and nothing else. The database's administrators (``root`` among them)
can read the EC2 credential of that user: it opens all the object storage of
the project. Give it a new one with
``openstack database cluster reset communal credential <cluster>`` when it may
be known to others.

.. warning::

   Removing the bucket, any object in it, the user or its EC2 credential
   destroys the database.

Deleting the cluster empties and removes the bucket, and then the credential
and the user.

Reviving a cluster from its bucket
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When the members of an Eon Mode cluster are gone, create a new cluster from
its bucket:

.. code-block:: console

   $ openstack database cluster create newcluster vertica 25.4 \
       --extended-properties revive_from=trove-vertica-<old cluster id> \
       --instance ... --instance ... --instance ...

The database comes back as it was the last time its catalog reached the
bucket, with the users of the old cluster, and the old cluster's admin
password replaced by the new one's. The old database must not run: the
revive is refused until about 15 minutes after it stopped. The new cluster
takes the bucket over; the old one, shown with ``communal_moved_to``, can be
deleted without touching it.

Licenses
~~~~~~~~

The Community Edition license of the Vertica image allows three nodes and
1 TB of raw data, in either mode; Eon Mode needs three nodes at least, so an
Eon Mode cluster with it has exactly three. From Vertica 26.1 the server
refuses the Community Edition license of the image: create the members with a
module of type ``vertica_license``.
