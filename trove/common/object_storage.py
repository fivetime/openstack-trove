# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Buckets of a tenant's project on an S3 service, for a datastore that
keeps data in object storage (the communal storage of a Vertica Eon Mode
cluster, for one).

The S3 service is one that takes Keystone EC2 credentials: Ceph RGW with
rgw_s3_auth_use_keystone (with rgw_keystone_implicit_tenants the project
sees the same buckets through Swift), or Swift with s3api and s3token. A
bucket made with the EC2 credential of a user of the project is the
project's: it is in the project's namespace and counts against its quota.
Trove, with its service account, creates a user of the project for its
object storage alone (storage_user, with s3_credential_role), an EC2
credential of it, and with that the bucket, and removes them all again.
Only the Identity API and the S3 API are used.
"""

import secrets
from urllib import parse

import boto3
from botocore import config as boto_config
from botocore import exceptions as boto_exceptions
from keystoneauth1 import exceptions as ks_exceptions
from keystoneclient.v3 import client as ks_client
from oslo_log import log as logging

from trove.common import cfg
from trove.common import clients_admin
from trove.common import exception
from trove.common.i18n import _

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class ProjectObjectStorage(object):

    def __init__(self, session=None):
        self.session = session or clients_admin.get_keystone_session()

    # Where the S3 service is.

    def url(self):
        if CONF.s3_endpoint_url:
            return CONF.s3_endpoint_url
        try:
            url = self.session.get_endpoint(
                service_type=CONF.s3_service_type,
                interface=CONF.s3_endpoint_type,
                region_name=CONF.service_credentials.region_name)
        except ks_exceptions.EndpointNotFound:
            url = None
        if not url:
            raise exception.TroveError(_(
                "No %s endpoint in the service catalog.")
                % CONF.s3_service_type)
        return url

    def address(self):
        """host:port of the S3 service and whether it is HTTPS."""
        url = parse.urlsplit(self.url())
        port = url.port or (443 if url.scheme == 'https' else 80)
        return '%s:%d' % (url.hostname, port), url.scheme == 'https'

    # The EC2 credential of a user in a project.

    def _keystone(self):
        return ks_client.Client(session=self.session)

    def credential(self, user_id, project_id, access=None):
        """The access and secret key of the user's EC2 credential: the one
        named by access, or a new one when there is none yet.
        """
        keystone = self._keystone()
        if access:
            try:
                found = keystone.ec2.get(user_id, access)
                return found.access, found.secret
            except ks_exceptions.NotFound:
                LOG.warning("The EC2 credential %(access)s of user "
                            "%(user)s is gone, creating another.",
                            {'access': access, 'user': user_id})
        created = keystone.ec2.create(user_id, project_id)
        return created.access, created.secret

    def secret(self, user_id, access):
        """The secret key of the user's EC2 credential, None when the
        credential is gone.
        """
        try:
            return self._keystone().ec2.get(user_id, access).secret
        except ks_exceptions.NotFound:
            return None

    def delete_credential(self, user_id, access):
        try:
            self._keystone().ec2.delete(user_id, access)
        except ks_exceptions.NotFound:
            pass

    # A user of the project for its object storage alone.

    def storage_user(self, name, project_id, description):
        """The id of the user of this name in the domain of the project,
        made if need be, with only the s3_credential_role on the project.
        An EC2 credential of it reaches the project's object storage, and
        a Keystone token got with it carries that role alone, which no
        other service takes: unlike a credential of a person, it is no
        key to the rest of the project.
        """
        keystone = self._keystone()
        role = self._storage_role(keystone)
        domain = keystone.projects.get(project_id).domain_id
        found = keystone.users.list(name=name, domain=domain)
        user = found[0] if found else keystone.users.create(
            name=name, domain=domain, password=secrets.token_urlsafe(32),
            description=description, enabled=True)
        keystone.roles.grant(role, user=user.id, project=project_id)
        return user.id

    def delete_user(self, user_id):
        try:
            self._keystone().users.delete(user_id)
        except ks_exceptions.NotFound:
            pass

    def _storage_role(self, keystone):
        if not CONF.s3_credential_role:
            raise exception.TroveError(_(
                "s3_credential_role is not set: there is no role to give "
                "the users of the object storage of projects."))
        roles = keystone.roles.list(name=CONF.s3_credential_role)
        if not roles:
            raise exception.TroveError(_("There is no role %s.")
                                       % CONF.s3_credential_role)
        return roles[0].id

    # Buckets.

    def client(self, access, secret):
        return boto3.client(
            's3', endpoint_url=self.url(),
            aws_access_key_id=access, aws_secret_access_key=secret,
            region_name=CONF.s3_region, config=client_config())

    def create_bucket(self, s3, bucket):
        """Make the bucket; one made already by an earlier try is fine."""
        try:
            s3.create_bucket(Bucket=bucket)
        except boto_exceptions.ClientError as e:
            if _code(e) != 'BucketAlreadyOwnedByYou':
                raise

    def bucket_exists(self, s3, bucket):
        """Whether the bucket is there and the credential reaches it."""
        try:
            s3.head_bucket(Bucket=bucket)
            return True
        except boto_exceptions.ClientError as e:
            if _code(e) in ('404', 'NoSuchBucket', 'NotFound'):
                return False
            raise

    def purge_bucket(self, s3, bucket):
        """Delete every object of the bucket and the bucket; a bucket
        that is not there is purged already.
        """
        try:
            pages = s3.get_paginator('list_objects_v2').paginate(
                Bucket=bucket)
            for page in pages:
                keys = [{'Key': o['Key']} for o in page.get('Contents', [])]
                if keys:
                    s3.delete_objects(Bucket=bucket,
                                      Delete={'Objects': keys,
                                              'Quiet': True})
            s3.delete_bucket(Bucket=bucket)
        except boto_exceptions.ClientError as e:
            if _code(e) != 'NoSuchBucket':
                raise


def client_config():
    options = dict(signature_version='s3v4',
                   s3={'addressing_style': 'path'},
                   retries={'max_attempts': 3, 'mode': 'standard'})
    try:
        # Checksums only where S3 requires them: object stores other than
        # AWS do not all take the trailers botocore 1.36 sends by default.
        return boto_config.Config(
            request_checksum_calculation='when_required',
            response_checksum_validation='when_required', **options)
    except TypeError:
        # Before botocore 1.36, which sends no such checksums.
        return boto_config.Config(**options)


def _code(error):
    return error.response.get('Error', {}).get('Code')
