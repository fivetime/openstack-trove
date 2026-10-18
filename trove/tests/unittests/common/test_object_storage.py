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

import json
from unittest import mock
from urllib import parse

from botocore import exceptions as boto_exceptions
from botocore import stub
from keystoneauth1 import exceptions as ks_exceptions
from keystoneauth1 import session as ks_session
from keystoneauth1 import token_endpoint
import requests
from requests import adapters

from trove.common import cfg
from trove.common import exception
from trove.common import object_storage
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class FakeKeystone(adapters.BaseAdapter):
    """The calls of the Identity API Trove makes for object storage
    (EC2 credentials, users, roles), answered in the
    process: the real keystoneauth session and keystoneclient build the
    requests and read the replies, no socket or thread is involved (the
    unit tests run under eventlet).
    """

    url = 'http://keystone.example.net/v3'

    def __init__(self):
        super(FakeKeystone, self).__init__()
        self.credentials = {}   # access: (user, project, secret)
        self.seen = []
        self.created = 0
        self.projects = {'p1': 'd1'}   # project: its domain
        self.roles = ['objectstore']
        self.users = {}   # id: (name, domain)
        self.passwords = []
        self.grants = set()   # (project, user, role)

    def close(self):
        pass

    def _cred(self, access):
        user, project, secret = self.credentials[access]
        return {'credential': {'user_id': user, 'tenant_id': project,
                               'access': access, 'secret': secret,
                               'links': {}}}

    def send(self, request, **kwargs):
        url = parse.urlsplit(request.url)
        path = url.path
        parts = path.split('/')
        query = dict(parse.parse_qsl(url.query))
        self.seen.append((request.method, path,
                          request.headers.get('X-Auth-Token')))
        if parts[2] == 'projects' and len(parts) == 4:
            project = self.projects.get(parts[3])
            if not project:
                return self._reply(request, 404, {'error': {'code': 404}})
            return self._reply(request, 200, {'project': {
                'id': parts[3], 'domain_id': project, 'links': {}}})
        if parts[2] == 'projects':
            # PUT /v3/projects/<p>/users/<u>/roles/<r>
            self.grants.add((parts[3], parts[5], parts[7]))
            return self._reply(request, 204)
        if parts[2] == 'roles':
            return self._reply(request, 200, {'roles': [
                {'id': 'role-' + name, 'name': name, 'links': {}}
                for name in self.roles if name == query.get('name')]})
        if parts[2] == 'users' and len(parts) <= 4:
            return self._users(request, parts, query)
        return self._credentials(request, parts)

    def _users(self, request, parts, query):
        if request.method == 'GET':
            return self._reply(request, 200, {'users': [
                {'id': i, 'name': n, 'domain_id': d, 'links': {}}
                for i, (n, d) in self.users.items()
                if n == query.get('name') and d == query.get('domain_id')]})
        if request.method == 'POST':
            user = json.loads(request.body)['user']
            self.passwords.append(user['password'])
            user_id = 'user-%d' % (len(self.users) + 1)
            self.users[user_id] = (user['name'], user['domain_id'])
            return self._reply(request, 201, {'user': {
                'id': user_id, 'name': user['name'],
                'domain_id': user['domain_id'], 'links': {}}})
        if self.users.pop(parts[3], None) is None:
            return self._reply(request, 404, {'error': {'code': 404}})
        return self._reply(request, 204)

    def _credentials(self, request, parts):
        # /v3/users/<user>/credentials/OS-EC2[/<access>]
        user = parts[3]
        access = parts[6] if len(parts) > 6 else None
        if request.method == 'POST':
            body = json.loads(request.body)
            self.created += 1
            access = 'access-%d' % self.created
            self.credentials[access] = (user, body['tenant_id'],
                                        'secret-%d' % self.created)
            return self._reply(request, 201, self._cred(access))
        if access not in self.credentials:
            return self._reply(request, 404, {'error': {'code': 404}})
        if request.method == 'DELETE':
            del self.credentials[access]
            return self._reply(request, 204)
        return self._reply(request, 200, self._cred(access))

    def _reply(self, request, code, body=None):
        response = requests.Response()
        response.status_code = code
        response.request = request
        response.url = request.url
        response.headers['Content-Type'] = 'application/json'
        response._content = (json.dumps(body).encode()
                             if body is not None else b'')
        return response


class ObjectStorageTestCase(trove_testtools.TestCase):

    def setUp(self):
        super(ObjectStorageTestCase, self).setUp()
        self.keystone = FakeKeystone()
        transport = requests.Session()
        transport.mount('http://keystone.example.net/', self.keystone)
        self.session = ks_session.Session(
            auth=token_endpoint.Token(self.keystone.url, 'service-token'),
            session=transport)
        self._conf(s3_endpoint_url='http://s3.example.net:7480')

    def _conf(self, **values):
        for name, value in values.items():
            CONF.set_override(name, value)
            self.addCleanup(CONF.clear_override, name)

    def _storage(self):
        return object_storage.ProjectObjectStorage(session=self.session)


class TestEndpoints(ObjectStorageTestCase):

    def test_from_the_configuration(self):
        storage = self._storage()
        self.assertEqual('http://s3.example.net:7480', storage.url())
        self.assertEqual(('s3.example.net:7480', False), storage.address())

    def test_from_the_catalog(self):
        self._conf(s3_endpoint_url=None)
        session = mock.Mock()
        session.get_endpoint.return_value = 'https://s3.example.net'
        storage = object_storage.ProjectObjectStorage(session=session)
        self.assertEqual('https://s3.example.net', storage.url())
        session.get_endpoint.assert_called_once_with(
            service_type='s3', interface='public',
            region_name=CONF.service_credentials.region_name)
        # The default port of the scheme.
        self.assertEqual(('s3.example.net:443', True), storage.address())

    def test_no_endpoint(self):
        self._conf(s3_endpoint_url=None)
        for found in (None, ks_exceptions.EndpointNotFound()):
            session = mock.Mock()
            if isinstance(found, Exception):
                session.get_endpoint.side_effect = found
            else:
                session.get_endpoint.return_value = found
            storage = object_storage.ProjectObjectStorage(session=session)
            self.assertRaises(exception.TroveError, storage.url)


class TestCredential(ObjectStorageTestCase):

    def test_a_new_credential_for_the_user_in_the_project(self):
        storage = self._storage()
        self.assertEqual(('access-1', 'secret-1'),
                         storage.credential('u1', 'p1'))
        self.assertEqual(('u1', 'p1', 'secret-1'),
                         self.keystone.credentials['access-1'])
        # As the service, for the user.
        self.assertEqual(
            [('POST', '/v3/users/u1/credentials/OS-EC2', 'service-token')],
            self.keystone.seen)

    def test_the_recorded_credential_is_used_again(self):
        storage = self._storage()
        storage.credential('u1', 'p1')
        self.assertEqual(('access-1', 'secret-1'),
                         storage.credential('u1', 'p1', 'access-1'))
        self.assertEqual(1, self.keystone.created)

    def test_a_credential_that_is_gone_is_made_again(self):
        self.assertEqual(('access-1', 'secret-1'),
                         self._storage().credential('u1', 'p1', 'gone'))

    def test_secret_and_delete(self):
        storage = self._storage()
        storage.credential('u1', 'p1')
        self.assertEqual('secret-1', storage.secret('u1', 'access-1'))
        storage.delete_credential('u1', 'access-1')
        self.assertEqual({}, self.keystone.credentials)
        # Gone already: nothing to do.
        self.assertIsNone(storage.secret('u1', 'access-1'))
        storage.delete_credential('u1', 'access-1')


class TestBucket(ObjectStorageTestCase):
    """The S3 calls as botocore checks them against the S3 model."""

    def setUp(self):
        super(TestBucket, self).setUp()
        self.s3 = self._storage().client('AK', 'SK')
        self.stubber = stub.Stubber(self.s3)
        self.stubber.activate()
        self.addCleanup(self.stubber.deactivate)

    def test_the_client(self):
        self.assertEqual('http://s3.example.net:7480',
                         self.s3.meta.endpoint_url)
        self.assertEqual('us-east-1', self.s3.meta.region_name)
        config = self.s3.meta.config
        self.assertEqual('s3v4', config.signature_version)
        self.assertEqual('path', config.s3['addressing_style'])

    def test_create(self):
        self.stubber.add_response('create_bucket', {},
                                  {'Bucket': 'trove-vertica-c1'})
        self.stubber.add_client_error('create_bucket',
                                      'BucketAlreadyOwnedByYou')
        self.stubber.add_client_error('create_bucket', 'AccessDenied')
        storage = self._storage()
        storage.create_bucket(self.s3, 'trove-vertica-c1')
        # Made already by an earlier try.
        storage.create_bucket(self.s3, 'trove-vertica-c1')
        self.assertRaises(boto_exceptions.ClientError,
                          storage.create_bucket, self.s3, 'trove-vertica-c1')
        self.stubber.assert_no_pending_responses()

    def test_purge(self):
        bucket = 'trove-vertica-c1'
        self.stubber.add_response(
            'list_objects_v2',
            {'Contents': [{'Key': 'db/a'}, {'Key': 'db/b'}],
             'IsTruncated': True, 'NextContinuationToken': 'next'},
            {'Bucket': bucket})
        self.stubber.add_response(
            'delete_objects', {},
            {'Bucket': bucket, 'Delete': {'Objects': [{'Key': 'db/a'},
                                                      {'Key': 'db/b'}],
                                          'Quiet': True}})
        self.stubber.add_response(
            'list_objects_v2',
            {'Contents': [{'Key': 'db/c'}], 'IsTruncated': False},
            {'Bucket': bucket, 'ContinuationToken': 'next'})
        self.stubber.add_response(
            'delete_objects', {},
            {'Bucket': bucket, 'Delete': {'Objects': [{'Key': 'db/c'}],
                                          'Quiet': True}})
        self.stubber.add_response('delete_bucket', {}, {'Bucket': bucket})
        self._storage().purge_bucket(self.s3, bucket)
        self.stubber.assert_no_pending_responses()

    def test_purge_an_empty_or_missing_bucket(self):
        bucket = 'trove-vertica-c1'
        self.stubber.add_response('list_objects_v2', {'IsTruncated': False},
                                  {'Bucket': bucket})
        self.stubber.add_response('delete_bucket', {}, {'Bucket': bucket})
        self.stubber.add_client_error('list_objects_v2', 'NoSuchBucket')
        self.stubber.add_client_error('list_objects_v2', 'AccessDenied')
        storage = self._storage()
        storage.purge_bucket(self.s3, bucket)
        storage.purge_bucket(self.s3, bucket)
        self.assertRaises(boto_exceptions.ClientError,
                          storage.purge_bucket, self.s3, bucket)
        self.stubber.assert_no_pending_responses()

    def test_bucket_exists(self):
        bucket = 'trove-vertica-c1'
        self.stubber.add_response('head_bucket', {}, {'Bucket': bucket})
        self.stubber.add_client_error('head_bucket', '404',
                                      http_status_code=404)
        self.stubber.add_client_error('head_bucket', '403',
                                      http_status_code=403)
        storage = self._storage()
        self.assertTrue(storage.bucket_exists(self.s3, bucket))
        self.assertFalse(storage.bucket_exists(self.s3, bucket))
        # Not the credential's: an error, not a missing bucket.
        self.assertRaises(boto_exceptions.ClientError,
                          storage.bucket_exists, self.s3, bucket)
        self.stubber.assert_no_pending_responses()


class TestStorageUser(ObjectStorageTestCase):

    def setUp(self):
        super(TestStorageUser, self).setUp()
        self._conf(s3_credential_role='objectstore')

    def test_a_user_of_the_project_for_its_storage_alone(self):
        storage = self._storage()
        user = storage.storage_user('trove-vertica-c1', 'p1', 'the cluster')
        # In the domain of the project, with the role on the project only.
        self.assertEqual({user: ('trove-vertica-c1', 'd1')},
                         self.keystone.users)
        self.assertEqual({('p1', user, 'role-objectstore')},
                         self.keystone.grants)
        # A long random password nobody keeps.
        self.assertGreaterEqual(len(self.keystone.passwords[0]), 32)
        # A retry finds it.
        self.assertEqual(user, storage.storage_user('trove-vertica-c1', 'p1',
                                                    'the cluster'))
        self.assertEqual(1, len(self.keystone.users))
        storage.delete_user(user)
        storage.delete_user(user)
        self.assertEqual({}, self.keystone.users)

    def test_no_role_no_user(self):
        storage = self._storage()
        self._conf(s3_credential_role='nosuchrole')
        self.assertRaises(exception.TroveError, storage.storage_user,
                          'trove-vertica-c1', 'p1', 'the cluster')
        self._conf(s3_credential_role=None)
        self.assertRaises(exception.TroveError, storage.storage_user,
                          'trove-vertica-c1', 'p1', 'the cluster')
        self.assertEqual({}, self.keystone.users)
