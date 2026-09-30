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
import json
from unittest import mock

from oslo_config import cfg as oslo_cfg
from oslo_utils import importutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.couchdb import models
from trove.common import exception
from trove.common import stream_codecs
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.common import configuration
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.couchdb import manager as couchdb_manager
from trove.guestagent.datastore.couchdb import service as couchdb_service
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
INI = stream_codecs.IniCodec()


def _render(version='3.5'):
    ds_version = mock.Mock()
    ds_version.datastore_name = 'couchdb'
    ds_version.manager = 'couchdb'
    ds_version.name = version
    ds_version.version = version
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': 2048, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()


class CouchDBGuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a couchdb instance sees it."""

    def setUp(self):
        super(CouchDBGuestTestCase, self).setUp()
        self.patch_datastore_manager('couchdb')
        # Registered by the guest agent process, not by trove.common.cfg.
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass

    def _app(self):
        # The status is the real class behind a mock: a call the container
        # based status does not have must fail here, not on a guest.
        status = mock.create_autospec(base_service.BaseDbStatus,
                                      instance=True)
        app = couchdb_service.CouchDBApp(status, mock.Mock())
        app._configuration_manager = mock.Mock()
        return app


class FakeServer(object):
    """The part of the HTTP API the guest agent uses, in memory."""

    def __init__(self):
        self.databases = {'_users': {}, '_replicator': {}}
        self.users = {}
        self.admins = {'os_admin': '-pbkdf2-hash'}
        self.calls = []

    def request(self, method, path, body=None, expected=(200, 201, 202)):
        self.calls.append((method, path, copy.deepcopy(body)))
        code, result = self._handle(method, path, body)
        if expected is not None and code not in expected:
            raise exception.TroveError('%s %s returned %s' % (method, path,
                                                              code))
        return code, result

    def _handle(self, method, path, body):
        if path == '/_all_dbs':
            return 200, sorted(self.databases)
        if path == '/_users/_all_docs':
            return 200, {'rows': [{'id': '_design/_auth'}] + [
                {'id': 'org.couchdb.user:' + name}
                for name in sorted(self.users)]}
        if path.startswith('/_users/org.couchdb.user%3A'):
            name = path.split('%3A', 1)[1].split('?')[0]
            if method == 'GET':
                if name not in self.users:
                    return 404, {'error': 'not_found'}
                return 200, copy.deepcopy(self.users[name])
            if method == 'PUT':
                if name in self.users and \
                        body.get('_rev') != self.users[name]['_rev']:
                    return 409, {'error': 'conflict'}
                self.users[name] = dict(
                    body, _id='org.couchdb.user:' + name,
                    _rev='%d-x' % (len(self.calls)))
                return 201, {'ok': True}
            if method == 'DELETE':
                del self.users[name]
                return 200, {'ok': True}
        if path.startswith('/_node/_local/_config/admins'):
            name = path[len('/_node/_local/_config/admins/'):]
            if method == 'GET':
                return 200, dict(self.admins)
            if method == 'PUT':
                self.admins[name] = '-pbkdf2-' + body
                return 200, ''
            if method == 'DELETE':
                return 200, self.admins.pop(name)
        name, _sep, rest = path[1:].partition('/')
        if rest == '_security':
            if method == 'GET':
                return 200, copy.deepcopy(self.databases[name])
            self.databases[name] = body
            return 200, {'ok': True}
        if method == 'PUT':
            if name in self.databases:
                return 412, {'error': 'file_exists'}
            # What a new database reports: admins only.
            self.databases[name] = {'members': {'roles': ['_admin']},
                                    'admins': {'roles': ['_admin']}}
            return 201, {'ok': True}
        if method == 'DELETE':
            del self.databases[name]
            return 200, {'ok': True}
        raise AssertionError('unexpected request %s %s' % (method, path))


class TestCouchDBDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['couchdb'])
        self.assertIs(couchdb_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, base_manager.Manager))

    def test_options(self):
        self.assertEqual('couchdb', CONF.couchdb.docker_image)
        self.assertEqual('couchdbbackup', CONF.couchdb.backup_strategy)
        self.assertEqual('/var/lib/couchdb', CONF.couchdb.mount_point)
        # The image runs as the couchdb user.
        self.assertEqual('5984', CONF.couchdb.database_service_uid)
        self.assertEqual('5984', CONF.couchdb.database_service_gid)
        ports = {port for port_range in CONF.couchdb.tcp_ports
                 for port in port_range}
        self.assertEqual({5984}, ports)

    def test_the_admins_and_the_system_databases_are_hidden(self):
        self.assertEqual({'os_admin', 'root'},
                         set(CONF.couchdb.ignore_users))
        for database in ('_users', '_replicator', '_global_changes'):
            self.assertIn(database, CONF.couchdb.ignore_dbs)

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('couchdb',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'couchdb',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_template_is_an_ini_file_without_what_the_guest_decides(self):
        config = INI.deserialize(_render())
        self.assertEqual(['couchdb'], list(config))
        for name in ('single_node', 'database_dir', 'view_index_dir'):
            self.assertNotIn(name, config['couchdb'])


class TestCouchDBApp(CouchDBGuestTestCase):

    def test_files_sort_the_way_the_server_reads_them(self):
        # The template first, the overrides of the configuration manager
        # after it, the file the server writes itself last.
        manager = configuration.ConfigurationManager
        names = [
            couchdb_service.CONFIG_FILE.rsplit('/', 1)[1],
            '%s-001-common.ini' % manager.SYSTEM_PRE_USER_GROUP,
            '%s-001-common.ini' % manager.USER_GROUP,
            '%s-001-common.ini' % manager.SYSTEM_POST_USER_GROUP,
            couchdb_service.SERVER_FILE.rsplit('/', 1)[1],
        ]
        self.assertEqual(names, sorted(names))
        # Only *.ini is configuration to the server.
        self.assertFalse(couchdb_service.NETRC_FILE.endswith('.ini'))

    def test_configuration_manager(self):
        app = couchdb_service.CouchDBApp(mock.Mock(), mock.Mock())
        with mock.patch.object(configuration.ImportOverrideStrategy,
                               'configure'):
            manager = app.configuration_manager
        self.assertIsInstance(manager._codec, stream_codecs.IniCodec)
        strategy = manager._override_strategy
        self.assertIsInstance(strategy, configuration.ImportOverrideStrategy)
        self.assertEqual('/etc/couchdb', strategy._revision_dir)
        self.assertEqual('ini', strategy._revision_ext)

    def test_initial_configuration(self):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        overrides = app.configuration_manager.apply_system_override.\
            call_args[0][0]
        self.assertEqual(
            {'couchdb': {'single_node': 'true',
                         'database_dir': '/var/lib/couchdb/data',
                         'view_index_dir': '/var/lib/couchdb/data'},
             'chttpd': {'bind_address': '0.0.0.0', 'port': 5984}},
            overrides)
        # And it is an ini file the server reads, as the codec writes it.
        lines = INI.serialize(overrides).splitlines()
        for line in ('[couchdb]', 'single_node = true',
                     'database_dir = /var/lib/couchdb/data', '[chttpd]',
                     'bind_address = 0.0.0.0', 'port = 5984'):
            self.assertIn(line, lines)

    @mock.patch.object(couchdb_service.CouchDBApp, 'save_password')
    @mock.patch.object(couchdb_service, 'operating_system')
    def test_secure_writes_the_admin_where_the_server_hashes_it(
            self, mock_os, mock_save):
        app = self._app()
        app.secure('pw')

        written = {call[0][0]: call[0][1]
                   for call in mock_os.write_file.call_args_list}
        self.assertEqual('[admins]\nos_admin = pw\n',
                         written['/etc/couchdb/docker.ini'])
        # The entrypoint of the image looks for exactly this shape.
        self.assertRegex(written['/etc/couchdb/docker.ini'],
                         r'\[admins\]\n[^;]\w+')
        self.assertEqual('machine 127.0.0.1 login os_admin password pw\n',
                         written['/etc/couchdb/trove.netrc'])
        # Both belong to the database user alone: the server rewrites the
        # one and the other holds a password.
        self.assertEqual(
            {('/etc/couchdb/docker.ini', '5984', '5984'),
             ('/etc/couchdb/trove.netrc', '5984', '5984')},
            {call[0] for call in mock_os.chown.call_args_list})
        self.assertEqual(
            {FileMode.SET_USR_RW},
            {call[0][1] for call in mock_os.chmod.call_args_list})
        mock_save.assert_called_once_with('os_admin', 'pw')

    @mock.patch.object(couchdb_service, 'docker_util')
    @mock.patch.object(couchdb_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.return_value = True

        app.start_db(ds_version='3.5')

        args, kwargs = mock_docker.start_container.call_args
        self.assertEqual('couchdb:3.5', args[1])
        self.assertEqual('/opt/couchdb/bin/couchdb', kwargs['command'])
        self.assertEqual('5984:5984', kwargs['user'])
        # The files of the guest are the local settings of the server;
        # the settings the image ships stay where they are.
        self.assertEqual('/opt/couchdb/etc/local.d',
                         kwargs['volumes']['/etc/couchdb']['bind'])
        self.assertEqual('/var/lib/couchdb',
                         kwargs['volumes']['/var/lib/couchdb']['bind'])
        self.assertEqual({'5984/tcp': 5984}, kwargs['ports'])
        self.assertIn('/_up', kwargs['healthcheck']['test'][1])

    def test_execute(self):
        app = self._app()
        container = app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, (b'out', None))
        self.assertEqual('out', app.execute(['curl', '-s']))

        container.exec_run.return_value = (7, (None, b'refused'))
        self.assertRaisesRegex(exception.TroveError, 'refused',
                               app.execute, ['curl', '-s'])

    @mock.patch.object(base_service.BaseDbApp, 'create_backup')
    def test_backup_copies_the_data_directory(self, mock_base):
        app = self._app()
        app.create_backup(mock.Mock(), {'id': 'b1'})
        kwargs = mock_base.call_args[1]
        self.assertFalse(kwargs['need_dbuser'])
        self.assertEqual('--db-datadir=/var/lib/couchdb/data',
                         kwargs['extra_params'])
        self.assertEqual(
            {'/var/lib/couchdb/data': {'bind': '/var/lib/couchdb/data',
                                       'mode': 'rw'}},
            kwargs['volumes_mapping'])


class TestCouchDBAdmin(CouchDBGuestTestCase):

    def _adm(self):
        adm = couchdb_service.CouchDBAdmin(self._app())
        server = FakeServer()
        adm.request = server.request
        return adm, server

    def _user(self, name, password='pw', databases=()):
        user = models.CouchDBUser(name, password)
        for database in databases:
            user.databases = database
        return user.serialize()

    def test_request(self):
        app = self._app()
        adm = couchdb_service.CouchDBAdmin(app)
        with mock.patch.object(couchdb_service.CouchDBApp, 'execute',
                               return_value='{"ok": true}\n201') as execute:
            self.assertEqual(
                (201, {'ok': True}),
                adm.request('PUT', '/appdb', body={'a': "it's"}))
        command = execute.call_args[0][0]
        self.assertEqual('curl', command[0])
        # The credentials come from a file, not from the command line.
        self.assertIn('/opt/couchdb/etc/local.d/trove.netrc', command)
        self.assertFalse([arg for arg in command if 'os_admin' in arg])
        # The body is one argument, whatever is in it.
        self.assertIn(json.dumps({'a': "it's"}), command)
        self.assertEqual('http://127.0.0.1:5984/appdb', command[-1])

        with mock.patch.object(couchdb_service.CouchDBApp, 'execute',
                               return_value='{"error":"x"}\n404'):
            self.assertRaises(exception.TroveError, adm.request, 'GET', '/x')
            self.assertEqual(404, adm.request('GET', '/x',
                                              expected=(200, 404))[0])

    def test_names_are_one_segment_of_the_path(self):
        adm, _ = self._adm()
        self.assertEqual('/a%2Fb', adm._database_path('a/b'))
        self.assertEqual('/_users/org.couchdb.user%3Ajo%2Fe',
                         adm._user_path('jo/e'))

    def test_databases(self):
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize()])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in adm.list_databases()[0]])
        adm.delete_database(models.CouchDBSchema('appdb').serialize())
        self.assertEqual([], adm.list_databases()[0])

    def test_a_second_grant_keeps_the_first(self):
        # Writing the members anew for every grant dropped everybody who
        # had been granted before.
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize()])
        adm.create_user([self._user('first', databases=['appdb']),
                         self._user('second', databases=['appdb'])])
        self.assertEqual(['first', 'second'],
                         server.databases['appdb']['members']['names'])
        # The rest of the security object is left as it was.
        self.assertEqual({'roles': ['_admin']},
                         server.databases['appdb']['admins'])

    def test_a_database_without_members_stays_closed(self):
        # No member names and no member roles means everybody.
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])
        server.databases['appdb']['members']['roles'] = []

        adm.revoke_access('appuser', None, 'appdb')

        self.assertEqual({'names': [], 'roles': ['_admin']},
                         server.databases['appdb']['members'])

    def test_grant_that_changes_nothing_writes_nothing(self):
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])
        server.calls.clear()
        adm.grant_access('appuser', None, ['appdb'])
        self.assertFalse([call for call in server.calls if call[0] == 'PUT'])

    def test_users_and_their_databases(self):
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize(),
                             models.CouchDBSchema('otherdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])
        self.assertEqual({'name': 'appuser', 'password': 'pw', 'roles': [],
                          'type': 'user'},
                         {k: v for k, v in server.users['appuser'].items()
                          if not k.startswith('_')})

        users = adm.list_users()[0]
        self.assertEqual(['appuser'], [u['_name'] for u in users])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in users[0]['_databases']])

        adm.grant_access('appuser', None, ['otherdb'])
        self.assertEqual(['appdb', 'otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        self.assertEqual('appuser', adm.get_user('appuser')['_name'])
        self.assertIsNone(adm.get_user('nobody'))
        self.assertIsNone(adm.get_user('os_admin'))

    def test_delete_user_leaves_no_membership_behind(self):
        adm, server = self._adm()
        adm.create_database([models.CouchDBSchema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb']),
                         self._user('other', databases=['appdb'])])
        revision = server.users['appuser']['_rev']

        adm.delete_user(self._user('appuser'))

        self.assertNotIn('appuser', server.users)
        self.assertEqual(['other'],
                         server.databases['appdb']['members']['names'])
        self.assertIn(('DELETE', '/_users/org.couchdb.user%3Aappuser?rev=' +
                       revision, None), server.calls)

    def test_change_password_rewrites_the_document(self):
        adm, server = self._adm()
        adm.create_user([self._user('appuser')])
        server.users['appuser'].update(
            {'derived_key': 'k', 'salt': 's', 'iterations': 10,
             'password_scheme': 'pbkdf2'})
        server.users['appuser'].pop('password')

        adm.change_passwords([self._user('appuser', 'new')])

        doc = server.users['appuser']
        self.assertEqual('new', doc['password'])
        for derived in ('derived_key', 'salt', 'iterations'):
            self.assertNotIn(derived, doc)

    def test_a_user_cannot_be_renamed(self):
        adm, _ = self._adm()
        adm.create_user([self._user('appuser')])
        self.assertRaises(exception.UnprocessableEntity,
                          adm.update_attributes, 'appuser', None,
                          {'name': 'renamed'})

    def test_reserved_and_missing_users(self):
        adm, server = self._adm()
        for name in ('os_admin', 'root'):
            self.assertRaises(exception.BadRequest, adm.create_user,
                              [self._user(name)])
        self.assertEqual({}, server.users)
        self.assertRaises(exception.UserNotFound, adm.grant_access,
                          'nobody', None, ['appdb'])
        self.assertRaises(ValueError, adm.create_database,
                          [models.CouchDBSchema('_users').serialize()])

    def test_root_is_a_second_server_admin(self):
        adm, server = self._adm()
        self.assertFalse(adm.is_root_enabled())

        root = adm.enable_root('rootpw')

        self.assertEqual('root', root['_name'])
        self.assertEqual('rootpw', root['_password'])
        self.assertIn(('PUT', '/_node/_local/_config/admins/root', 'rootpw'),
                      server.calls)
        self.assertTrue(adm.is_root_enabled())
        # Not a user: it is in the configuration, not in the database.
        self.assertEqual([], adm.list_users()[0])

        adm.disable_root()
        self.assertFalse(adm.is_root_enabled())

    def test_enable_root_waits_for_the_hash(self):
        adm, server = self._adm()
        answers = iter([{'root': 'rootpw'}, {'root': 'rootpw'},
                        {'root': '-pbkdf2-hash'}])
        with mock.patch.object(couchdb_service.CouchDBAdmin, '_admins',
                               side_effect=lambda: next(answers)) as admins, \
                mock.patch('time.sleep'):
            adm.enable_root('rootpw')
        self.assertEqual(3, admins.call_count)


class TestCouchDBManager(CouchDBGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = couchdb_manager.Manager()
        manager.app = mock.Mock()
        manager.app.datadir = '/var/lib/couchdb/data'
        manager.app.has_admin.return_value = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=2048, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/couchdb', backup_info=None,
                    config_contents='[couchdb]\n', root_password=None,
                    overrides=None, cluster_config=None, snapshot=None,
                    ds_version='3.5')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_the_admin_is_there_before_the_first_start(self):
        # The server does not start without one.
        manager = self._manager()
        order = mock.Mock()
        order.attach_mock(manager.app.secure, 'secure')
        order.attach_mock(manager.app.start_db, 'start_db')

        self._prepare(manager)

        self.assertEqual(['secure', 'start_db'],
                         [call[0] for call in order.mock_calls])
        manager.app.start_db.assert_called_once_with(ds_version='3.5')

    def test_an_existing_admin_is_kept(self):
        manager = self._manager()
        manager.app.has_admin.return_value = True
        self._prepare(manager)
        manager.app.secure.assert_not_called()

    @mock.patch.object(couchdb_manager.Manager, 'perform_restore')
    def test_restore_unpacks_before_the_first_start(self, mock_restore):
        manager = self._manager()
        order = mock.Mock()
        order.attach_mock(manager.app.secure, 'secure')
        order.attach_mock(mock_restore, 'restore')
        order.attach_mock(manager.app.start_db, 'start_db')

        self._prepare(manager, backup_info={'id': 'b1'})

        # The admin is this instance's own: it is not in the data.
        self.assertEqual(['secure', 'restore', 'start_db'],
                         [call[0] for call in order.mock_calls])
        self.assertEqual('/var/lib/couchdb/data',
                         mock_restore.call_args[0][1])
