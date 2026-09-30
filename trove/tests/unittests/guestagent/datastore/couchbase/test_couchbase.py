#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy
from unittest import mock

from oslo_config import cfg as oslo_cfg
from oslo_utils import importutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.couchbase import models
from trove.common import exception
from trove.common import stream_codecs
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.common import configuration
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.couchbase import manager as couchbase_manager
from trove.guestagent.datastore.couchbase import service as couchbase_service
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
CODEC = stream_codecs.KeyValueCodec(line_terminator='\n')


def _render(ram=2048):
    ds_version = mock.Mock()
    ds_version.datastore_name = 'couchbase'
    ds_version.manager = 'couchbase'
    # The name is the tag of the image; the number is registered with it.
    ds_version.name = 'community-7.6.2'
    ds_version.version = '7.6.2'
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': ram, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()


class CouchbaseGuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a couchbase instance sees it."""

    def setUp(self):
        super(CouchbaseGuestTestCase, self).setUp()
        self.patch_datastore_manager('couchbase')
        # Registered by the guest agent process, not by trove.common.cfg.
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass

    def _app(self, settings=None):
        # The status is the real class behind a mock: a call the container
        # based status does not have must fail here, not on a guest.
        status = mock.create_autospec(base_service.BaseDbStatus,
                                      instance=True)
        app = couchbase_service.CouchbaseApp(status, mock.Mock())
        settings = dict(settings or {})
        app._configuration_manager = mock.Mock()
        app._configuration_manager.get_value.side_effect = \
            lambda name, section=None, default=None: settings.get(name)
        return app


class FakeServer(object):
    """The part of the REST API the guest agent uses, in memory."""

    def __init__(self, initialized=True):
        self.initialized = initialized
        self.admin = None
        self.quotas = {'memoryQuota': 256, 'indexMemoryQuota': 256}
        self.buckets = {}
        self.users = {}
        self.calls = []

    def request(self, method, path, data=None, files=None,
                expected=(200, 201, 202), local=False):
        self.calls.append((method, path, copy.deepcopy(data), files, local))
        code, result = self._handle(method, path, data or {}, files or {})
        if expected is not None and code not in expected:
            raise exception.TroveError('%s %s returned %s: %s'
                                       % (method, path, code, result))
        return code, result

    def _handle(self, method, path, data, files):
        if path == '/pools':
            return 200, {'isEnterprise': False}
        if path == '/pools/default':
            if method == 'GET' and not self.initialized:
                return 404, 'unknown pool'
            if method == 'POST':
                for name, value in data.items():
                    if int(value) < 256:
                        return 400, {'errors': {name: 'below 256MB'}}
                    self.quotas[name] = int(value)
            return 200, dict(self.quotas)
        if path == '/node/controller/setupServices':
            return 200, None
        if path == '/settings/indexes':
            return 200, None
        if path == '/settings/web':
            self.initialized = True
            self.admin = (data['username'], files['password'])
            return 200, None
        if path == '/controller/resetAdminPassword':
            self.admin = ('os_admin', files['password'])
            return 200, None
        if path == '/controller/setAutoCompaction':
            if 'parallelDBAndViewCompaction' not in data:
                return 400, {'errors': {'parallelDBAndViewCompaction':
                                        'is missing'}}
            return 200, None
        if path == '/pools/default/buckets':
            if method == 'POST':
                if data['name'] in self.buckets:
                    return 400, {'errors': {'name': 'exists'}}
                self.buckets[data['name']] = dict(data)
                return 202, None
            return 200, [{'name': name} for name in sorted(self.buckets)]
        if path.startswith('/pools/default/buckets/'):
            name = path.rsplit('/', 1)[1]
            if name not in self.buckets:
                return 404, 'not found'
            del self.buckets[name]
            for user in self.users.values():
                user['roles'] = [role for role in user['roles']
                                 if role.get('bucket_name') != name]
            return 200, None
        if path == '/settings/rbac/users':
            return 200, [copy.deepcopy(user) for user in self.users.values()]
        if path.startswith('/settings/rbac/users/local/'):
            name = path.rsplit('/', 1)[1]
            if method == 'GET':
                if name not in self.users:
                    return 404, 'Unknown user.'
                return 200, copy.deepcopy(self.users[name])
            if method == 'DELETE':
                if name not in self.users:
                    return 404, 'Unknown user.'
                del self.users[name]
                return 200, None
            if method == 'PUT':
                roles = []
                for role in filter(None, data.get('roles', '').split(',')):
                    if '[' in role:
                        role, bucket = role[:-1].split('[')
                        if bucket not in self.buckets:
                            return 400, {'errors': {'roles': 'unknown'}}
                        roles.append({'role': role, 'bucket_name': bucket})
                    else:
                        roles.append({'role': role})
                if 'password' in data and len(data['password']) < 6:
                    return 400, {'errors': {'password': 'too short'}}
                user = self.users.setdefault(name, {'id': name,
                                                    'domain': 'local'})
                if 'password' in data:
                    user['password'] = data['password']
                elif 'password' not in user:
                    return 400, {'errors': {'password': 'is missing'}}
                # The roles are taken as a whole, with every request.
                user['roles'] = roles
                return 200, None
        raise AssertionError('unexpected %s %s' % (method, path))


class TestCouchbaseDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['couchbase'])
        self.assertIs(couchbase_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, base_manager.Manager))

    def test_options(self):
        self.assertEqual('couchbase', CONF.couchbase.docker_image)
        self.assertEqual('couchbasebackup', CONF.couchbase.backup_strategy)
        self.assertEqual('/var/lib/couchbase', CONF.couchbase.mount_point)
        # The image runs as the couchbase user.
        self.assertEqual('1000', CONF.couchbase.database_service_uid)
        self.assertEqual('1000', CONF.couchbase.database_service_gid)
        ports = {port for port_range in CONF.couchbase.tcp_ports
                 for port in port_range}
        # REST, views and query, the data service, and the same over TLS.
        for port in (8091, 8092, 8093, 11210, 11207, 18091, 18093):
            self.assertIn(port, ports)
        # The server requires six characters of a password.
        self.assertGreaterEqual(CONF.couchbase.default_password_length, 6)

    def test_the_admins_are_hidden(self):
        self.assertEqual({'os_admin', 'root'},
                         set(CONF.couchbase.ignore_users))

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('couchbase',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'couchbase',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_template_quotas_leave_the_server_its_gigabyte(self):
        # The server refuses quotas above the memory less 1024 MB, and
        # the memory it sees is a little below that of the flavor.
        for ram in (1024, 2048, 4096, 8192):
            config = CODEC.deserialize(_render(ram))
            data = int(config['memory_quota'])
            index = int(config['index_memory_quota'])
            self.assertGreaterEqual(data, 256)
            self.assertGreaterEqual(index, 256)
            if ram >= 2048:
                self.assertLessEqual(data + index, ram * 9 // 10 - 1024)
        config = CODEC.deserialize(_render(4096))
        self.assertEqual('2048', config['memory_quota'])
        self.assertEqual('100', config['bucket_ramsize'])
        self.assertEqual('0', config['bucket_replicas'])


class TestCouchbaseApp(CouchbaseGuestTestCase):

    def test_configuration_manager(self):
        app = couchbase_service.CouchbaseApp(mock.Mock(), mock.Mock())
        with mock.patch.object(configuration.OneFileOverrideStrategy,
                               'configure'):
            manager = app.configuration_manager
        self.assertIsInstance(manager._codec, stream_codecs.KeyValueCodec)
        self.assertEqual('/etc/couchbase/couchbase.conf',
                         manager._base_config_path)
        self.assertIsInstance(manager._override_strategy,
                              configuration.OneFileOverrideStrategy)
        # The template and the overrides round-trip through the codec.
        config = CODEC.deserialize(_render())
        self.assertEqual(config, CODEC.deserialize(CODEC.serialize(config)))

    @mock.patch.object(couchbase_service.CouchbaseApp, 'save_password')
    @mock.patch.object(couchbase_service, 'operating_system')
    def test_secure_writes_the_password_for_curl_and_for_the_server(
            self, mock_os, mock_save):
        app = self._app()
        app.secure('pw')

        written = {call[0][0]: call[0][1]
                   for call in mock_os.write_file.call_args_list}
        self.assertEqual('machine 127.0.0.1 login os_admin password pw\n',
                         written['/etc/couchbase/trove.netrc'])
        self.assertEqual('pw', written['/etc/couchbase/trove.secret'])
        # Both belong to the database user alone: they hold a password.
        self.assertEqual(
            {('/etc/couchbase/trove.netrc', '1000', '1000'),
             ('/etc/couchbase/trove.secret', '1000', '1000')},
            {call[0] for call in mock_os.chown.call_args_list})
        self.assertEqual(
            {FileMode.SET_USR_RW},
            {call[0][1] for call in mock_os.chmod.call_args_list})
        mock_save.assert_called_once_with('os_admin', 'pw')

    @mock.patch.object(couchbase_service, 'docker_util')
    @mock.patch.object(couchbase_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.return_value = True
        with mock.patch.object(couchbase_service.CouchbaseApp,
                               'initialize') as initialize:
            app.start_db(ds_version='community-7.6.2')

        args, kwargs = mock_docker.start_container.call_args
        self.assertEqual('couchbase:community-7.6.2', args[1])
        self.assertEqual('couchbase-server', kwargs['command'])
        self.assertEqual('1000:1000', kwargs['user'])
        # The volume is the var directory of the server: data,
        # configuration, users and logs.
        self.assertEqual('/opt/couchbase/var',
                         kwargs['volumes']['/var/lib/couchbase']['bind'])
        self.assertEqual('/etc/couchbase',
                         kwargs['volumes']['/etc/couchbase']['bind'])
        self.assertIn('8091/tcp', kwargs['ports'])
        self.assertIn('11210/tcp', kwargs['ports'])
        # The health check has the token of the node, not the password:
        # restored data brings the password of another instance.
        self.assertIn('@localtoken', kwargs['healthcheck']['test'][1])
        self.assertNotIn('netrc', kwargs['healthcheck']['test'][1])
        # The server is made Trove's once it answers.
        initialize.assert_called_once_with()

    def test_a_fresh_server_is_initialized_with_the_quotas(self):
        app = self._app({'memory_quota': '563', 'index_memory_quota': '256',
                         'compaction_database_threshold': '30'})
        server = FakeServer(initialized=False)
        app.adm.request = server.request

        app.initialize()

        self.assertTrue(server.initialized)
        # The password goes in from the file, not the command line.
        self.assertEqual(('os_admin', '/etc/couchbase/trove.secret'),
                         server.admin)
        self.assertEqual({'memoryQuota': 563, 'indexMemoryQuota': 256},
                         server.quotas)
        paths = [call[1] for call in server.calls]
        self.assertLess(paths.index('/node/controller/setupServices'),
                        paths.index('/settings/web'))
        self.assertIn('/controller/setAutoCompaction', paths)
        # Everything up to the request that creates the admin uses the
        # token of the node; the settings after it, the admin.
        admin_at = paths.index('/settings/web')
        self.assertTrue(all(call[4] for call in server.calls[:admin_at + 1]))
        self.assertFalse(any(call[4] for call in server.calls[admin_at + 1:]))

    def test_a_restored_server_gets_this_instances_password(self):
        app = self._app({'memory_quota': '563'})
        server = FakeServer(initialized=True)
        server.admin = ('os_admin', 'of-the-backup')
        server.quotas['memoryQuota'] = 700
        app.adm.request = server.request

        app.initialize()

        self.assertEqual(('os_admin', '/etc/couchbase/trove.secret'),
                         server.admin)
        self.assertNotIn('/settings/web', [call[1] for call in server.calls])
        # And the settings of this instance, not of the backup.
        self.assertEqual(563, server.quotas['memoryQuota'])

    def test_settings_the_server_refuses_are_an_error(self):
        app = self._app({'memory_quota': '100'})
        app.adm.request = FakeServer().request
        self.assertRaisesRegex(exception.TroveError, 'below 256MB',
                               app.apply_settings)

    def test_bucket_settings_have_defaults(self):
        app = self._app({'bucket_ramsize': '200'})
        self.assertEqual(
            {'bucket_ramsize': '200', 'bucket_replicas': 0,
             'bucket_eviction_policy': 'valueOnly'},
            app.bucket_settings())

    def test_execute(self):
        app = self._app()
        container = app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, (b'out', None))
        self.assertEqual('out', app.execute(['curl', '-s']))

        container.exec_run.return_value = (7, (None, b'refused'))
        self.assertRaisesRegex(exception.TroveError, 'refused',
                               app.execute, ['curl', '-s'])

    @mock.patch.object(base_service.BaseDbApp, 'create_backup')
    def test_backup_copies_the_volume(self, mock_base):
        app = self._app()
        app.create_backup(mock.Mock(), {'id': 'b1'})
        kwargs = mock_base.call_args[1]
        self.assertFalse(kwargs['need_dbuser'])
        self.assertEqual('--db-datadir=/var/lib/couchbase',
                         kwargs['extra_params'])
        self.assertEqual(
            {'/var/lib/couchbase': {'bind': '/var/lib/couchbase',
                                    'mode': 'rw'}},
            kwargs['volumes_mapping'])


class TestCouchbaseAdmin(CouchbaseGuestTestCase):

    def _adm(self):
        app = self._app({'bucket_ramsize': '100'})
        server = FakeServer()
        app.adm.request = server.request
        return app.adm, server

    def _user(self, name, password='secret', databases=()):
        user = models.CouchbaseUser(name, password)
        for database in databases:
            user.databases = database
        return user.serialize()

    def test_request(self):
        app = self._app()
        adm = couchbase_service.CouchbaseAdmin(app)
        with mock.patch.object(couchbase_service.CouchbaseApp, 'execute',
                               return_value='[{"name": "a"}]\n200') \
                as execute:
            self.assertEqual(
                (200, [{'name': 'a'}]),
                adm.request('PUT', '/x', data={'roles': "it's[a]"},
                            files={'password': '/etc/couchbase/trove.secret'}))
        command = execute.call_args[0][0]
        self.assertEqual('curl', command[0])
        # The credentials come from a file, not from the command line, and
        # there is no shell between the values and curl.
        self.assertIn('/etc/couchbase/trove.netrc', command)
        self.assertFalse([arg for arg in command if 'os_admin' in arg])
        self.assertNotIn('sh', command)
        self.assertIn("roles=it's[a]", command)
        self.assertIn('password@/etc/couchbase/trove.secret', command)
        self.assertEqual('http://127.0.0.1:8091/x', command[-1])

        with mock.patch.object(couchbase_service.CouchbaseApp, 'execute',
                               return_value='"Unknown user."\n404'):
            self.assertRaises(exception.TroveError, adm.request, 'GET', '/x')
            self.assertEqual(404, adm.request('GET', '/x',
                                              expected=(200, 404))[0])
        # An answer that is not JSON is returned as it is.
        with mock.patch.object(couchbase_service.CouchbaseApp, 'execute',
                               return_value='unknown pool\n404'):
            self.assertEqual((404, 'unknown pool'),
                             adm.request('GET', '/x', expected=None))

    def test_request_with_the_token_of_the_node(self):
        app = self._app()
        adm = couchbase_service.CouchbaseAdmin(app)
        with mock.patch.object(couchbase_service.CouchbaseApp, 'execute',
                               return_value='\n200') as execute, \
                mock.patch.object(couchbase_service.operating_system,
                                  'read_file', return_value='tok\n') as read:
            adm.request('GET', '/pools/default', local=True)
        read.assert_called_once_with(
            '/var/lib/couchbase/lib/couchbase/localtoken', as_root=True)
        command = execute.call_args[0][0]
        self.assertIn('@localtoken:tok', command)
        self.assertNotIn('--netrc-file', command)

    def test_names_are_one_segment_of_the_path(self):
        adm, _ = self._adm()
        self.assertEqual('/pools/default/buckets/a%25b',
                         adm._bucket_path('a%b'))
        self.assertEqual('/settings/rbac/users/local/jo%40e',
                         adm._user_path('jo@e'))

    def test_databases_are_buckets(self):
        adm, server = self._adm()
        adm.create_database([models.CouchbaseSchema('appdb').serialize()])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in adm.list_databases()[0]])
        self.assertEqual(
            {'name': 'appdb', 'bucketType': 'couchbase', 'ramQuota': '100',
             'replicaNumber': 0, 'evictionPolicy': 'valueOnly',
             'flushEnabled': 1},
            server.buckets['appdb'])
        adm.delete_database(models.CouchbaseSchema('appdb').serialize())
        self.assertEqual([], adm.list_databases()[0])

    def test_invalid_names(self):
        # Refused before the server sees them.
        for name in ('a b', 'a/b', 'a:b', 'a' * 101):
            self.assertRaises(ValueError, models.CouchbaseSchema, name)
        for name in ('a b', 'a/b', 'a:b', 'a[b]', 'a' * 129):
            self.assertRaises(ValueError, models.CouchbaseUser, name, 'pw')
        models.CouchbaseSchema('a.b-c_d%e')
        models.CouchbaseUser('a@b', 'pw')

    def test_users_and_their_buckets(self):
        adm, server = self._adm()
        adm.create_database([models.CouchbaseSchema('appdb').serialize(),
                             models.CouchbaseSchema('otherdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb']),
                         self._user('nobucket')])
        self.assertEqual([{'role': 'bucket_full_access',
                           'bucket_name': 'appdb'}],
                         server.users['appuser']['roles'])
        self.assertEqual([], server.users['nobucket']['roles'])

        users = adm.list_users()[0]
        self.assertEqual(['appuser', 'nobucket'],
                         [u['_name'] for u in users])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in users[0]['_databases']])

        adm.grant_access('appuser', None, ['otherdb'])
        self.assertEqual(['appdb', 'otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        adm.revoke_access('appuser', None, 'appdb')
        self.assertEqual(['otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        self.assertEqual('appuser', adm.get_user('appuser')['_name'])
        self.assertIsNone(adm.get_user('nobody'))
        self.assertIsNone(adm.get_user('os_admin'))

    def test_a_grant_keeps_the_other_grants(self):
        # The server takes the roles as a whole: a grant that sent only
        # the new one would take the others away.
        adm, server = self._adm()
        adm.create_database([models.CouchbaseSchema('a').serialize(),
                             models.CouchbaseSchema('b').serialize(),
                             models.CouchbaseSchema('c').serialize()])
        adm.create_user([self._user('u', databases=['a'])])
        adm.grant_access('u', None, ['b'])
        adm.grant_access('u', None, ['c', 'b'])
        self.assertEqual(['a', 'b', 'c'],
                         [d['_name'] for d in adm.list_access('u')])

    def test_a_password_change_keeps_the_roles(self):
        # A request with the password alone leaves the user with no
        # roles: the roles are sent again.
        adm, server = self._adm()
        adm.create_database([models.CouchbaseSchema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])

        adm.change_passwords([self._user('appuser', 'newsecret')])

        self.assertEqual('newsecret', server.users['appuser']['password'])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        put = [call for call in server.calls
               if call[0] == 'PUT' and call[2].get('password') == 'newsecret']
        self.assertEqual('bucket_full_access[appdb]', put[-1][2]['roles'])

        adm.update_attributes('appuser', None, {'password': 'third-one'})
        self.assertEqual('third-one', server.users['appuser']['password'])

    def test_a_user_cannot_be_renamed(self):
        adm, _ = self._adm()
        adm.create_user([self._user('appuser')])
        self.assertRaises(exception.UnprocessableEntity,
                          adm.update_attributes, 'appuser', None,
                          {'name': 'renamed'})

    def test_delete_user(self):
        adm, server = self._adm()
        adm.create_user([self._user('appuser')])
        adm.delete_user(self._user('appuser'))
        self.assertNotIn('appuser', server.users)
        # Deleting what is not there is not an error.
        adm.delete_user(self._user('appuser'))

    def test_reserved_and_missing_users(self):
        adm, server = self._adm()
        for name in ('os_admin', 'root'):
            self.assertRaises(exception.BadRequest, adm.create_user,
                              [self._user(name)])
            self.assertRaises(exception.BadRequest, adm.delete_user,
                              self._user(name))
        self.assertEqual({}, server.users)
        self.assertRaises(exception.UserNotFound, adm.grant_access,
                          'nobody', None, ['appdb'])
        self.assertRaises(exception.UserNotFound, adm.change_passwords,
                          [self._user('nobody')])

    def test_what_the_server_refuses_is_an_error(self):
        adm, server = self._adm()
        adm.create_database([models.CouchbaseSchema('appdb').serialize()])
        self.assertRaisesRegex(
            exception.TroveError, 'exists', adm.create_database,
            [models.CouchbaseSchema('appdb').serialize()])
        adm.create_user([self._user('appuser')])
        self.assertRaisesRegex(
            exception.TroveError, 'unknown', adm.grant_access,
            'appuser', None, ['nope'])
        self.assertRaisesRegex(
            exception.TroveError, 'too short', adm.change_passwords,
            [self._user('appuser', 'abc')])

    def test_root_is_a_local_user_with_the_admin_role(self):
        adm, server = self._adm()
        self.assertFalse(adm.is_root_enabled())

        root = adm.enable_root('rootsecret')

        self.assertEqual('root', root['_name'])
        self.assertEqual('rootsecret', root['_password'])
        self.assertEqual([{'role': 'admin'}], server.users['root']['roles'])
        self.assertTrue(adm.is_root_enabled())
        # Not among the users of the instance.
        self.assertEqual([], adm.list_users()[0])

        adm.disable_root()
        self.assertFalse(adm.is_root_enabled())
        adm.disable_root()


class TestCouchbaseManager(CouchbaseGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = couchbase_manager.Manager()
        manager.app = mock.Mock()
        manager.app.mount_point = '/var/lib/couchbase'
        manager.app.has_admin.return_value = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=2048, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/couchbase', backup_info=None,
                    config_contents='memory_quota=563\n',
                    root_password=None, overrides=None, cluster_config=None,
                    snapshot=None, ds_version='community-7.6.2')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_the_admin_is_decided_before_the_first_start(self):
        manager = self._manager()
        order = mock.Mock()
        order.attach_mock(manager.app.secure, 'secure')
        order.attach_mock(manager.app.start_db, 'start_db')

        self._prepare(manager)

        self.assertEqual(['secure', 'start_db'],
                         [call[0] for call in order.mock_calls])
        manager.app.start_db.assert_called_once_with(
            ds_version='community-7.6.2')

    def test_an_existing_admin_is_kept(self):
        manager = self._manager()
        manager.app.has_admin.return_value = True
        self._prepare(manager)
        manager.app.secure.assert_not_called()

    @mock.patch.object(couchbase_manager.Manager, 'perform_restore')
    def test_restore_unpacks_before_the_first_start(self, mock_restore):
        manager = self._manager()
        order = mock.Mock()
        order.attach_mock(manager.app.secure, 'secure')
        order.attach_mock(mock_restore, 'restore')
        order.attach_mock(manager.app.start_db, 'start_db')

        self._prepare(manager, backup_info={'id': 'b1'})

        self.assertEqual(['secure', 'restore', 'start_db'],
                         [call[0] for call in order.mock_calls])
        # Onto the whole volume: the archive is the node.
        self.assertEqual('/var/lib/couchbase', mock_restore.call_args[0][1])

    def test_configuration_changes_reach_the_running_server(self):
        manager = self._manager()
        manager.apply_overrides(mock.Mock(), {'memory_quota': 700})
        manager.app.apply_settings.assert_called_once_with()
