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
import inspect
import json
import os
from unittest import mock

from oslo_config import cfg as oslo_cfg
from oslo_utils import importutils

import trove
from trove.common import cfg
from trove.common import constants
from trove.common.db.mongodb import models
from trove.common import exception
from trove.common.strategies.cluster.experimental.mongodb import (
    guestagent as mongodb_guest_api)
from trove.common import stream_codecs
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent import api as guest_api
from trove.guestagent.common import configuration
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore.mongodb import manager as mongodb_manager
from trove.guestagent.datastore.mongodb import service as mongodb_service
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
YAML = stream_codecs.SafeYamlCodec(default_flow_style=False)


def _render(version='8.2'):
    ds_version = mock.Mock()
    ds_version.datastore_name = 'mongodb'
    ds_version.manager = 'mongodb'
    ds_version.name = version
    ds_version.version = version
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': 2048, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()


class FakeConfigurationManager(object):
    """Keeps the configuration as a nested dict, the way the file does,
    merging overrides the way the real manager does.
    """

    def __init__(self):
        self.config = {}
        self.reset_calls = []

    def reset_configuration(self, options, remove_overrides=False):
        if not isinstance(options, dict):
            options = YAML.deserialize(options) or {}
        self.config = options
        self.reset_calls.append((copy.deepcopy(options), remove_overrides))

    def apply_system_override(self, options, change_id='overrides',
                              pre_user=False):
        self._merge(options, self.config)

    def apply_user_override(self, options, change_id='overrides'):
        self._merge(options, self.config)

    def parse_configuration(self):
        return self.config

    @classmethod
    def _merge(cls, updates, target):
        for key, value in updates.items():
            if isinstance(value, dict):
                cls._merge(value, target.setdefault(key, {}))
            else:
                target[key] = value


class MongoDBGuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a mongodb instance sees it."""

    def setUp(self):
        super(MongoDBGuestTestCase, self).setUp()
        self.patch_datastore_manager('mongodb')
        # Registered by the guest agent process, not by trove.common.cfg.
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass


def _app(config=None):
    app = mongodb_service.MongoDBApp(mock.Mock(), mock.Mock())
    app._configuration_manager = FakeConfigurationManager()
    if config is not None:
        app.configuration_manager.reset_configuration(config)
    return app


class TestMongoDBDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['mongodb'])
        self.assertIs(mongodb_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, base_manager.Manager))

    def test_manager_has_every_call_the_task_manager_makes(self):
        generic = {name for name, _ in inspect.getmembers(
            guest_api.API, inspect.isfunction)}
        cluster_calls = {
            name for name, _ in inspect.getmembers(
                mongodb_guest_api.MongoDbGuestAgentAPI, inspect.isfunction)
            if name not in generic and not name.startswith('_')}

        self.assertTrue(cluster_calls)
        for name in cluster_calls:
            self.assertTrue(
                callable(getattr(mongodb_manager.Manager, name, None)),
                'the MongoDB manager does not implement %s' % name)

    def test_options(self):
        self.assertEqual('mongo', CONF.mongodb.docker_image)
        self.assertEqual('mongodump', CONF.mongodb.backup_strategy)
        self.assertEqual('/var/lib/mongodb', CONF.mongodb.mount_point)
        # The image runs as the mongodb user.
        self.assertEqual('999', CONF.mongodb.database_service_uid)
        self.assertEqual('999', CONF.mongodb.database_service_gid)
        self.assertTrue(CONF.mongodb.cluster_support)
        self.assertTrue(CONF.mongodb.cluster_secure)

    def test_ports_are_the_ones_of_mongod_and_the_config_servers(self):
        ports = {port for port_range in CONF.mongodb.tcp_ports
                 for port in port_range}
        self.assertEqual({27017, 27019}, ports)
        self.assertEqual(27017, CONF.mongodb.mongodb_port)
        self.assertEqual(27019, CONF.mongodb.configsvr_port)

    def test_internal_accounts_and_databases_are_hidden(self):
        self.assertIn('admin.os_admin', CONF.mongodb.ignore_users)
        for db in ('admin', 'local', 'config'):
            self.assertIn(db, CONF.mongodb.ignore_dbs)

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('mongodb',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'mongodb',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)


class TestMongoDBConfigTemplate(trove_testtools.TestCase):

    def test_is_nested_yaml_the_server_accepts(self):
        rendered = _render()
        config = YAML.deserialize(rendered)
        self.assertEqual({'systemLog': {'verbosity': 0}}, config)

    def test_carries_nothing_the_guest_agent_decides(self):
        config = YAML.deserialize(_render())
        for section in ('net', 'storage', 'security', 'replication',
                        'sharding'):
            self.assertNotIn(section, config)

    def test_options_the_server_no_longer_has(self):
        # storage.mmapv1 went with 4.2, storage.journal.enabled with 6.1.
        rendered = _render()
        self.assertNotIn('mmapv1', rendered)
        self.assertNotIn('journal', rendered)

    def test_user_configuration_cannot_touch_security_or_roles(self):
        rules_file = os.path.join(os.path.dirname(trove.__file__),
                                  'templates', 'mongodb',
                                  'validation-rules.json')
        with open(rules_file) as f:
            rules = json.load(f)
        names = {rule['name'] for rule in rules['configuration-parameters']}
        self.assertIn('systemLog.verbosity', names)
        self.assertIn('storage.wiredTiger.engineConfig.cacheSizeGB', names)
        for name in ('security.authorization', 'sharding.clusterRole',
                     'setParameter', 'net.ipv6', 'storage.engine',
                     'storage.mmapv1.smallFiles', 'storage.journal.enabled',
                     'net.http.enabled'):
            self.assertNotIn(name, names)


class TestMongoDBAppConfiguration(MongoDBGuestTestCase):

    def test_single_instance(self):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(None)

        config = app.configuration_manager.config
        self.assertEqual('0.0.0.0', config['net']['bindIp'])
        self.assertEqual(27017, config['net']['port'])
        self.assertEqual(mongodb_service.SOCKET_DIR,
                         config['net']['unixDomainSocket']['pathPrefix'])
        self.assertEqual('/var/lib/mongodb/data', config['storage']['dbPath'])
        self.assertEqual('enabled', config['security']['authorization'])
        self.assertNotIn('replication', config)
        self.assertNotIn('sharding', config)
        self.assertFalse(app.is_query_router)
        self.assertFalse(app.is_config_server)
        self.assertFalse(app.is_cluster_member)
        self.assertEqual(27017, app.port)

    @mock.patch.object(mongodb_service.MongoDBApp, 'store_key')
    def test_cluster_member(self, mock_store_key):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'member', 'replica_set_name': 'rs1',
             'key': 'secret'})

        config = app.configuration_manager.config
        self.assertEqual('rs1', config['replication']['replSetName'])
        # addShard refuses a replica set that does not run as one.
        self.assertEqual('shardsvr', config['sharding']['clusterRole'])
        self.assertEqual(mongodb_service.KEY_FILE,
                         config['security']['keyFile'])
        self.assertEqual('enabled', config['security']['authorization'])
        mock_store_key.assert_called_once_with('secret')
        self.assertTrue(app.is_cluster_member)
        self.assertFalse(app.is_config_server)
        self.assertEqual('rs1', app.replica_set_name)
        self.assertEqual(27017, app.port)

    @mock.patch.object(mongodb_service.MongoDBApp, 'store_key')
    def test_config_server(self, mock_store_key):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'config_server', 'key': 'secret'})

        config = app.configuration_manager.config
        self.assertEqual('configsvr', config['sharding']['clusterRole'])
        # A config server has to be a replica set since 3.4.
        self.assertEqual(mongodb_service.CONFIGSVR_REPLICA_SET,
                         config['replication']['replSetName'])
        self.assertEqual(27019, config['net']['port'])
        self.assertTrue(app.is_config_server)
        self.assertFalse(app.is_cluster_member)
        self.assertEqual(27019, app.port)

    @mock.patch.object(mongodb_service.MongoDBApp, 'store_key')
    def test_query_router(self, mock_store_key):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'query_router', 'key': 'secret'})

        config = app.configuration_manager.config
        # mongos refuses storage and authorization settings; the template
        # is dropped, with every override.
        self.assertIn(({}, True), app.configuration_manager.reset_calls)
        self.assertNotIn('storage', config)
        self.assertNotIn('authorization', config['security'])
        self.assertEqual(mongodb_service.KEY_FILE,
                         config['security']['keyFile'])
        self.assertEqual('', config['sharding']['configDB'])
        self.assertTrue(app.is_query_router)

        app.set_config_servers(['10.0.0.1', '10.0.0.2', '10.0.0.3'])
        self.assertEqual(
            'configsvr/10.0.0.1:27019,10.0.0.2:27019,10.0.0.3:27019',
            app.configuration_manager.config['sharding']['configDB'])

    def test_unknown_instance_type(self):
        app = _app(_render())
        self.assertRaises(exception.TroveError,
                          app.apply_initial_guestagent_configuration,
                          {'instance_type': 'arbiter'})

    def test_user_overrides_are_expanded(self):
        app = _app(_render())
        app.update_overrides({'systemLog.verbosity': 2,
                              'net.maxIncomingConnections': 500})
        config = app.configuration_manager.config
        self.assertEqual(2, config['systemLog']['verbosity'])
        self.assertEqual(500, config['net']['maxIncomingConnections'])
        self.assertNotIn('systemLog.verbosity', config)

    def test_real_configuration_manager_writes_yaml(self):
        app = mongodb_service.MongoDBApp(mock.Mock(), mock.Mock())
        with mock.patch.object(configuration.OneFileOverrideStrategy,
                               'configure'):
            manager = app.configuration_manager
        self.assertIsInstance(manager._codec, stream_codecs.SafeYamlCodec)
        self.assertIsInstance(manager._override_strategy,
                              configuration.OneFileOverrideStrategy)


class TestMongoDBAppLifecycle(MongoDBGuestTestCase):

    def test_healthcheck_pings_over_the_socket(self):
        app = _app(_render())
        healthcheck = app._healthcheck(27019)
        self.assertEqual('CMD-SHELL', healthcheck['test'][0])
        command = healthcheck['test'][1]
        self.assertIn('mongodb://%2Fvar%2Frun%2Fmongodb%2Fmongodb-27019.sock',
                      command)
        self.assertIn('ping', command)
        # A server that is not the one being checked never answers here.
        self.assertNotIn('localhost', command)
        self.assertNotIn('127.0.0.1', command)

    @mock.patch.object(mongodb_service, 'docker_util')
    @mock.patch.object(mongodb_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(None)
        app.status.wait_for_status.return_value = True

        app.start_db(ds_version='8.2')

        kwargs = mock_docker.start_container.call_args[1]
        self.assertEqual('mongo:8.2',
                         mock_docker.start_container.call_args[0][1])
        self.assertEqual('mongod --config /etc/mongodb/mongod.conf',
                         kwargs['command'])
        self.assertEqual('999:999', kwargs['user'])
        for path in (mongodb_service.CONFIG_DIR, mongodb_service.SOCKET_DIR,
                     '/var/lib/mongodb/data'):
            self.assertEqual(path, kwargs['volumes'][path]['bind'])
        self.assertEqual({'27017/tcp': 27017, '27019/tcp': 27019},
                         kwargs['ports'])
        self.assertIn('27017.sock', kwargs['healthcheck']['test'][1])

    @mock.patch.object(mongodb_service, 'docker_util')
    @mock.patch.object(mongodb_service, 'operating_system')
    @mock.patch.object(mongodb_service.MongoDBApp, 'store_key')
    def test_query_router_runs_mongos(self, mock_key, mock_os, mock_docker):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'query_router', 'key': 'k'})
        app.set_config_servers(['10.0.0.1'])
        app.status.wait_for_status.return_value = True

        app.start_db()

        kwargs = mock_docker.start_container.call_args[1]
        self.assertEqual('mongos --config /etc/mongodb/mongod.conf',
                         kwargs['command'])

    @mock.patch.object(mongodb_service, 'docker_util')
    @mock.patch.object(mongodb_service, 'operating_system')
    def test_temporary_db_has_no_access_control_and_no_network(
            self, mock_os, mock_docker):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(None)
        app.status.wait_for_status.return_value = True

        app.start_temporary_db(ds_version='8.2')

        command = mock_docker.start_container.call_args[1]['command']
        self.assertTrue(command.startswith('mongod '))
        self.assertNotIn('--config', command)
        self.assertIn('--bind_ip 127.0.0.1', command)
        self.assertIn('--dbpath /var/lib/mongodb/data', command)
        self.assertIn(f'--unixSocketPrefix {mongodb_service.SOCKET_DIR}',
                      command)

        with mock.patch.object(mongodb_service.MongoDBApp, 'stop_db') as s:
            app.stop_temporary_db()
        # The next start must not reuse the container of the temporary
        # server: an existing container is started with its own command.
        s.assert_called_once_with()
        mock_docker.remove_container.assert_called_once()

    @mock.patch.object(mongodb_service.MongoDBApp, 'save_password')
    def test_secure_creates_the_admin_user_and_keeps_its_password(
            self, mock_save):
        app = _app(_render())
        app.adm = mock.Mock()

        app.secure()

        password = app.adm.create_admin_user.call_args[0][0]
        self.assertTrue(password)
        mock_save.assert_called_once_with('os_admin', password)

    @mock.patch.object(mongodb_service.MongoDBApp, 'save_password')
    def test_prep_primary_initiates_then_creates_the_admin_user(
            self, mock_save):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'member', 'replica_set_name': 'rs1'})
        app.adm = mock.Mock()
        app.adm.is_primary.return_value = True
        order = mock.Mock()
        order.attach_mock(app.adm.rs_initiate, 'rs_initiate')
        order.attach_mock(app.adm.create_admin_user, 'create_admin_user')

        with mock.patch.object(mongodb_service.MongoDBApp, 'instance_ip',
                               new_callable=mock.PropertyMock,
                               return_value='10.0.0.5'):
            app.prep_primary()

        self.assertEqual('rs_initiate', order.mock_calls[0][0])
        self.assertEqual(('rs1', '10.0.0.5:27017'),
                         order.mock_calls[0][1])
        self.assertEqual('create_admin_user', order.mock_calls[1][0])

    def test_add_members_waits_for_all_of_them(self):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'member', 'replica_set_name': 'rs1'})
        app.adm = mock.Mock()
        app.adm.count_healthy_members.side_effect = [1, 2, 3]

        with mock.patch.object(mongodb_service.utils, 'poll_until',
                               side_effect=lambda fn, **kw: [fn() for _ in
                                                             range(3)]):
            app.add_members(['10.0.0.6', '10.0.0.7'])

        app.adm.rs_add_members.assert_called_once_with(
            ['10.0.0.6:27017', '10.0.0.7:27017'])
        self.assertEqual(3, app.adm.count_healthy_members.call_count)

    @mock.patch.object(mongodb_service.MongoDBApp, 'start_db')
    @mock.patch.object(mongodb_service.MongoDBApp, 'store_key')
    def test_add_config_servers_starts_the_router(self, mock_key,
                                                  mock_start):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(
            {'instance_type': 'query_router', 'key': 'k'})

        app.add_config_servers(['10.0.0.1', '10.0.0.2'])

        self.assertEqual(
            'configsvr/10.0.0.1:27019,10.0.0.2:27019',
            app.configuration_manager.config['sharding']['configDB'])
        mock_start.assert_called_once_with(update_db=True)

    def test_add_shard(self):
        app = _app(_render())
        app.adm = mock.Mock()
        app.add_shard('rs1', '10.0.0.5')
        app.adm.add_shard.assert_called_once_with('rs1/10.0.0.5:27017')

        app.adm.list_active_shard_names.return_value = ['rs1']
        self.assertTrue(app.is_shard_active('rs1'))
        self.assertFalse(app.is_shard_active('rs2'))

    @mock.patch.object(mongodb_service.MongoDBApp, 'admin_password',
                       new_callable=mock.PropertyMock, return_value='pw')
    def test_backup_container_gets_the_socket_and_the_admin_user(
            self, mock_pw):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(None)

        self.assertEqual(
            {mongodb_service.SOCKET_DIR: {
                'bind': mongodb_service.SOCKET_DIR, 'mode': 'rw'}},
            app._backup_volumes())
        self.assertEqual(
            '--db-host=/var/run/mongodb/mongodb-27017.sock '
            '--db-user=os_admin --db-password=pw',
            app._backup_params())
        # The temporary server a backup is restored into has no users.
        self.assertEqual('--db-host=/var/run/mongodb/mongodb-27017.sock',
                         app._backup_params(authenticate=False))


class TestMongoDBAdmin(MongoDBGuestTestCase):

    def _adm(self):
        app = _app(_render())
        app.apply_initial_guestagent_configuration(None)
        adm = mongodb_service.MongoDBAdmin(app)
        client = mock.MagicMock()
        client.__enter__.return_value = client
        return adm, client

    @mock.patch.object(mongodb_service.pymongo, 'MongoClient')
    def test_client_uses_the_socket(self, mock_client):
        adm, _ = self._adm()
        with mock.patch.object(mongodb_service.MongoDBApp, 'admin_password',
                               new_callable=mock.PropertyMock,
                               return_value='pw'):
            adm._client()
        kwargs = mock_client.call_args[1]
        self.assertEqual('/var/run/mongodb/mongodb-27017.sock',
                         kwargs['host'])
        self.assertEqual('os_admin', kwargs['username'])
        self.assertEqual('admin', kwargs['authSource'])
        self.assertTrue(kwargs['directConnection'])

        mock_client.reset_mock()
        adm._client(authenticate=False)
        self.assertNotIn('username', mock_client.call_args[1])

    def test_create_admin_user_needs_no_authentication(self):
        adm, client = self._adm()
        with mock.patch.object(adm, '_client', return_value=client) as c:
            adm.create_admin_user('pw')
        c.assert_called_once_with(False)
        client['admin'].command.assert_called_once_with(
            'createUser', 'os_admin', pwd='pw',
            roles=mongodb_service.ADMIN_ROLES)

    def test_set_admin_password_creates_or_updates(self):
        adm, client = self._adm()
        with mock.patch.object(adm, '_client', return_value=client):
            client.admin.system.users.find_one.return_value = None
            adm.set_admin_password('pw')
            client.admin.command.assert_called_with(
                'createUser', 'os_admin', pwd='pw',
                roles=mongodb_service.ADMIN_ROLES)

            client.admin.system.users.find_one.return_value = {'_id': 'x'}
            adm.set_admin_password('pw2')
            client.admin.command.assert_called_with(
                'updateUser', 'os_admin', pwd='pw2',
                roles=mongodb_service.ADMIN_ROLES)

    def test_users(self):
        adm, client = self._adm()
        client.admin.system.users.find_one.return_value = None
        client.admin.system.users.find.return_value = [
            {'_id': 'admin.os_admin', 'user': 'os_admin', 'db': 'admin',
             'roles': [{'role': 'root', 'db': 'admin'}]},
            {'_id': 'appdb.appuser', 'user': 'appuser', 'db': 'appdb',
             'roles': [{'role': 'readWrite', 'db': 'appdb'}]},
        ]
        with mock.patch.object(adm, '_client', return_value=client):
            appuser = models.MongoDBUser(name='appdb.appuser',
                                         password='pw',
                                         databases=['appdb'])
            adm.create_users([appuser.serialize()])
            client['appdb'].command.assert_called_once_with(
                'createUser', 'appuser', pwd='pw',
                roles=[{'db': 'appdb', 'role': 'readWrite'}])

            users = adm.list_users()
        # The admin user is not shown.
        self.assertEqual(['appdb.appuser'], [u['_name'] for u in users[0]])

    def test_root(self):
        adm, client = self._adm()
        client.admin.system.users.find_one.return_value = None
        with mock.patch.object(adm, '_client', return_value=client):
            root = adm.enable_root('rootpw')
            client['admin'].command.assert_called_once_with(
                'createUser', 'root', pwd='rootpw',
                roles=[{'db': 'admin', 'role': 'root'}])
            self.assertEqual('admin.root', root['_name'])

            self.assertFalse(adm.is_root_enabled())
            client.admin.system.users.find_one.return_value = {'_id': 'r'}
            self.assertTrue(adm.is_root_enabled())
            client.admin.system.users.find_one.assert_called_with(
                {'user': 'root', 'db': 'admin'})

    def test_databases(self):
        adm, client = self._adm()
        client.list_database_names.return_value = ['admin', 'config',
                                                   'local', 'appdb']
        client['appdb'].list_collection_names.return_value = []
        with mock.patch.object(adm, '_client', return_value=client):
            adm.create_database([{'_name': 'appdb'}])
            client['appdb'].create_collection.assert_called_once_with(
                mongodb_service.PLACEHOLDER_COLLECTION)

            databases = adm.list_databases()
            adm.delete_database({'_name': 'appdb'})
        self.assertEqual(['appdb'], [d['_name'] for d in databases[0]])
        client.drop_database.assert_called_once_with('appdb')

    def test_replica_set(self):
        adm, client = self._adm()
        client['admin'].command.side_effect = [
            {'ok': 1},  # replSetInitiate
            {'config': {'version': 1, 'members': [
                {'_id': 0, 'host': '10.0.0.5:27017'}]}},
            {'ok': 1},  # replSetReconfig
            {'myState': 1, 'members': [
                {'state': 1, 'health': 1}, {'state': 2, 'health': 1},
                {'state': 0, 'health': 1}]},
        ]
        with mock.patch.object(adm, '_client', return_value=client):
            adm.rs_initiate('rs1', '10.0.0.5:27017')
            adm.rs_add_members(['10.0.0.6:27017'])
            healthy = adm.count_healthy_members()

        calls = client['admin'].command.call_args_list
        self.assertEqual(('replSetInitiate',
                          {'_id': 'rs1', 'members': [
                              {'_id': 0, 'host': '10.0.0.5:27017'}]}),
                         calls[0][0])
        self.assertEqual(('replSetReconfig',
                          {'version': 2, 'members': [
                              {'_id': 0, 'host': '10.0.0.5:27017'},
                              {'_id': 1, 'host': '10.0.0.6:27017'}]}),
                         calls[2][0])
        # The member still starting up does not count.
        self.assertEqual(2, healthy)


class TestMongoDBManager(MongoDBGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = mongodb_manager.Manager()
        manager.app = mock.Mock()
        manager.app.is_query_router = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=2048, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/mongodb', backup_info=None,
                    config_contents='systemLog:\n  verbosity: 0\n',
                    root_password=None, overrides=None,
                    cluster_config=None, snapshot=None, ds_version='8.2')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_single_instance_gets_its_admin_user(self):
        manager = self._manager()
        self._prepare(manager)
        manager.app.apply_initial_guestagent_configuration.\
            assert_called_once_with(None)
        manager.app.start_db.assert_called_once_with(ds_version='8.2')
        manager.app.secure.assert_called_once_with()
        manager.app.start_temporary_db.assert_not_called()

    def test_cluster_member_starts_without_an_admin_user(self):
        manager = self._manager()
        self._prepare(manager, cluster_config={'instance_type': 'member',
                                               'replica_set_name': 'rs1'})
        manager.app.start_db.assert_called_once_with(ds_version='8.2')
        manager.app.secure.assert_not_called()

    def test_query_router_is_not_started(self):
        manager = self._manager()
        manager.app.is_query_router = True
        self._prepare(manager,
                      cluster_config={'instance_type': 'query_router'})
        manager.app.start_db.assert_not_called()
        manager.app.secure.assert_not_called()

    @mock.patch.object(mongodb_manager.Manager, 'perform_restore')
    def test_restore_runs_on_the_temporary_server(self, mock_restore):
        manager = self._manager()
        manager.adm.is_root_enabled.return_value = True
        order = mock.Mock()
        order.attach_mock(manager.app.start_temporary_db, 'start_temp')
        order.attach_mock(mock_restore, 'restore')
        order.attach_mock(manager.app.reset_admin_password, 'reset')
        order.attach_mock(manager.app.stop_temporary_db, 'stop_temp')
        order.attach_mock(manager.app.start_db, 'start_db')
        backup_info = {'id': 'b1', 'location': 'x', 'checksum': 'y'}

        self._prepare(manager, backup_info=backup_info)

        self.assertEqual(['start_temp', 'restore', 'reset', 'stop_temp',
                          'start_db'],
                         [call[0] for call in order.mock_calls])
        # The users of the backup came with it.
        manager.app.secure.assert_not_called()
        manager.status.report_root.assert_called_once()

    @mock.patch.object(mongodb_manager.Manager, 'perform_restore',
                       side_effect=Exception('boom'))
    def test_temporary_server_is_stopped_when_the_restore_fails(
            self, mock_restore):
        manager = self._manager()
        self.assertRaisesRegex(Exception, 'boom', self._prepare, manager,
                               backup_info={'id': 'b1'})
        manager.app.stop_temporary_db.assert_called_once()
        manager.app.start_db.assert_not_called()

    def test_cluster_actions_fail_the_instance(self):
        manager = self._manager()
        manager.app.add_members.side_effect = Exception('boom')
        self.assertRaisesRegex(Exception, 'boom', manager.add_members,
                               mock.Mock(), ['10.0.0.6'])
        manager.status.set_status.assert_called_once()

    def test_cluster_answers(self):
        manager = self._manager()
        manager.app.replica_set_name = 'rs1'
        manager.app.admin_password = 'pw'
        manager.app.get_key.return_value = 'k'
        context = mock.Mock()
        self.assertEqual('rs1', manager.get_replica_set_name(context))
        self.assertEqual('pw', manager.get_admin_password(context))
        self.assertEqual('k', manager.get_key(context))
        manager.store_admin_password(context, 'pw2')
        manager.app.store_admin_password.assert_called_once_with('pw2')
        manager.create_admin_user(context, 'pw3')
        manager.app.secure.assert_called_once_with('pw3')
