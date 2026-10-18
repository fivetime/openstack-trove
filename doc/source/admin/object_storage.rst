===============================================
Object storage of the tenant for datastores
===============================================

Some datastores keep data in object storage, in a bucket of the project of
the tenant: the communal storage of a Vertica cluster in Eon Mode
(:doc:`/user/vertica-cluster-storage`). The bucket is the project's own: it is
in the project's namespace, the project sees it through its object storage
API, and it counts against the project's quota.

Trove uses two APIs for it, and nothing that depends on how the cloud is
deployed:

- the Identity API, with Trove's service account, to create a user of the
  project for the database alone, with only the role ``s3_credential_role``,
  and an EC2 credential of it, and to remove both again;
- the S3 API, with that credential, to create, empty and remove the bucket.
  The database instances reach the bucket with the same credential.

The code is ``trove.common.object_storage`` for datastores to use.

Requirements
~~~~~~~~~~~~

An S3 service that takes Keystone EC2 credentials:

- Ceph RGW with ``rgw_s3_auth_use_keystone = true`` (and the other
  ``rgw_keystone_*`` options of the Keystone integration). Set
  ``rgw_keystone_implicit_tenants = true`` (or ``s3``) so that the bucket is
  in the project's own tenant; with ``true`` the project sees the same buckets
  through Swift and S3. RGW reads these options when it starts: restart it
  after changing them.
- Or Swift with the ``s3api`` and ``s3token`` middleware.

Check it with a user of a project before enabling a datastore on it:

.. code-block:: console

   $ openstack ec2 credentials create
   # sign an S3 request with the access and secret key: create a bucket,
   # write, read and delete an object; the bucket shows in
   # "openstack container list" of the same project only.

The database's administrator can read the EC2 credential, and an EC2
credential gets a Keystone token of its user in the project: the user Trove
makes has a role the S3 service accepts and no other service does, so the
credential opens the project's object storage and nothing else. Create such
a role and let the S3 service accept it:

.. code-block:: console

   $ openstack role create objectstore-only
   # Ceph RGW: rgw_keystone_accepted_roles = member,admin,objectstore-only
   # (restart RGW); do not give the role any rule in other policies.

Trove's service account (``[service_credentials]``) needs to be allowed to
create and delete users in the domains of the projects, grant roles on the
projects, and manage the EC2 credentials of those users. With the default
policy of Keystone that is ``admin``. A domain whose users come from LDAP
cannot have users made in it: Eon Mode clusters cannot be created in its
projects.

The task manager and the database instances need to reach the S3 service:
the instances over their management network, unless their tenant network
reaches it.

Configuration
~~~~~~~~~~~~~

The S3 service, for every datastore, in ``[DEFAULT]``:

``s3_endpoint_url``
  URL of the S3 service. Not needed when the service catalog has it.
``s3_service_type`` (``s3``), ``s3_endpoint_type`` (``public``)
  The S3 service in the service catalog, for example::

    openstack service create --name s3 s3
    openstack endpoint create --region RegionOne s3 public https://s3.example.net

``s3_region`` (``us-east-1``)
  The region S3 requests are signed for.
``s3_credential_role``
  The role of the users Trove makes for the object storage of projects
  (``objectstore-only`` above). Datastores that keep data in object storage
  are not available until it is set.

Vertica, in ``[vertica]``:

``eon_enabled`` (``false``)
  Whether clusters can be created with ``storage_mode=eon``.
``eon_communal_endpoint``, ``eon_communal_https``
  ``host:port`` of the S3 service and whether it is HTTPS, as the database
  instances reach it, when that is not the address of ``s3_endpoint_url`` or
  the catalog.

The API and the task manager need ``boto3``.

Operations
~~~~~~~~~~

- A database in Eon Mode uploads the data of every commit to its bucket: while
  the S3 service is down it starts and answers from the volumes of its
  members, but cannot commit writes.
- Deleting the cluster empties and removes the bucket and then the EC2
  credential. When that fails, or when the credential is gone already (its
  user was deleted), the task manager logs a warning with the project, the
  bucket and the access key, and the bucket is left to be removed by hand.
- The users Trove makes are named ``trove-vertica-<cluster id>``; deleting one,
  or its EC2 credential, breaks the database. ``reset_communal_credential``
  gives a cluster's user a new credential, when the old one may be known to
  others.
