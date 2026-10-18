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

- the Identity API, with Trove's service account, to create, read and delete
  an EC2 credential of the user who creates the database, in the user's
  project;
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

Trove's service account (``[service_credentials]``) needs a role that may
manage the EC2 credentials of other users. With the default policy of
Keystone that is ``admin`` (``identity:ec2_create_credential``,
``identity:ec2_get_credential``, ``identity:ec2_delete_credential``).

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

- A database in Eon Mode needs its bucket when it starts: maintenance of the
  S3 service while such databases start, after a power failure for instance,
  holds them up.
- Deleting the cluster empties and removes the bucket and then the EC2
  credential. When that fails, or when the credential is gone already (its
  user was deleted), the task manager logs a warning with the project, the
  bucket and the access key, and the bucket is left to be removed by hand.
- The EC2 credential belongs to the user who created the cluster. When that
  user is deleted or leaves the project, the database cannot reach its bucket
  any more.
