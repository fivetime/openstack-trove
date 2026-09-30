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

import bcrypt
from oslo_config import cfg as oslo_cfg
from oslo_utils import importutils

import trove
from trove.common import cfg
from trove.common import constants
from trove.common.db.cassandra import models
from trove.common import exception
from trove.common.strategies.cluster.experimental.cassandra import (
    guestagent as cassandra_guest_api)
from trove.common import stream_codecs
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent import api as guest_api
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.cassandra import manager as cassandra_manager
from trove.guestagent.datastore.cassandra import service as cassandra_service
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.instance import service_status
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
YAML = stream_codecs.SafeYamlCodec(default_flow_style=False)


def _render(version='5.0'):
    ds_version = mock.Mock()
    ds_version.datastore_name = 'cassandra'
    ds_version.manager = 'cassandra'
    ds_version.name = version
    ds_version.version = version
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': 2048, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()


class FakeConfigurationManager(object):
    """Keeps the configuration as a dict, the way the file does. A change
    set applied later wins, and can be removed again.
    """

    def __init__(self):
        self.base = {}
        self.changes = []

    def reset_configuration(self, options, remove_overrides=False):
        if not isinstance(options, dict):
            options = YAML.deserialize(options) or {}
        self.base = options

    def apply_system_override(self, options, change_id='overrides',
                              pre_user=False):
        self.changes.append((change_id, copy.deepcopy(options)))

    def apply_user_override(self, options, change_id='overrides'):
        self.changes.append(('user', copy.deepcopy(options)))

    def remove_system_override(self, change_id='overrides'):
        self.changes = [c for c in self.changes if c[0] != change_id]

    def parse_configuration(self):
        config = copy.deepcopy(self.base)
        for _change_id, options in self.changes:
            config.update(copy.deepcopy(options))
        return config


class CassandraGuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a cassandra instance sees it."""

    def setUp(self):
        super(CassandraGuestTestCase, self).setUp()
        self.patch_datastore_manager('cassandra')
        self.patch_conf_property('guest_id', 'guest-0001')
        # Registered by the guest agent process, not by trove.common.cfg.
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass
        patcher = mock.patch.object(
            cassandra_service.CassandraApp, 'address',
            new_callable=mock.PropertyMock, return_value='10.0.0.5')
        patcher.start()
        self.addCleanup(patcher.stop)

    def _app(self, config=None):
        # The status is the real class behind a mock: a call the container
        # based status does not have must fail here, not on a guest.
        status = mock.create_autospec(base_service.BaseDbStatus,
                                      instance=True)
        app = cassandra_service.CassandraApp(status, mock.Mock())
        app._configuration_manager = FakeConfigurationManager()
        app.configuration_manager.reset_configuration(
            _render() if config is None else config)
        return app


class TestCassandraDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['cassandra'])
        self.assertIs(cassandra_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, base_manager.Manager))

    def test_manager_has_every_call_the_task_manager_makes(self):
        generic = {name for name, _ in inspect.getmembers(
            guest_api.API, inspect.isfunction)}
        cluster_calls = {
            name for name, _ in inspect.getmembers(
                cassandra_guest_api.CassandraGuestAgentAPI,
                inspect.isfunction)
            if name not in generic and not name.startswith('_')}

        self.assertTrue(cluster_calls)
        for name in cluster_calls:
            self.assertTrue(
                callable(getattr(cassandra_manager.Manager, name, None)),
                'the Cassandra manager does not implement %s' % name)

    def test_options(self):
        self.assertEqual('cassandra', CONF.cassandra.docker_image)
        self.assertEqual('nodetoolsnapshot', CONF.cassandra.backup_strategy)
        self.assertEqual('/var/lib/cassandra', CONF.cassandra.mount_point)
        # The image runs as the cassandra user.
        self.assertEqual('999', CONF.cassandra.database_service_uid)
        self.assertTrue(CONF.cassandra.cluster_support)

    def test_ports_leave_out_jmx_and_thrift(self):
        ports = {port for port_range in CONF.cassandra.tcp_ports
                 for port in port_range}
        # Storage, encrypted storage and the native transport. JMX needs
        # no credentials and thrift went with 4.0.
        self.assertEqual({7000, 7001, 9042}, ports)

    def test_system_keyspaces_and_the_admin_are_hidden(self):
        self.assertIn('os_admin', CONF.cassandra.ignore_users)
        for keyspace in ('system', 'system_auth', 'system_schema',
                         'system_traces', 'system_distributed',
                         'system_views', 'system_virtual_schema'):
            self.assertIn(keyspace, CONF.cassandra.ignore_dbs)

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('cassandra',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'cassandra',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)


class TestCassandraConfigTemplate(trove_testtools.TestCase):

    def test_names_only_what_trove_decides(self):
        config = YAML.deserialize(_render())
        self.assertEqual(
            {'num_tokens', 'partitioner', 'role_manager', 'commitlog_sync',
             'commitlog_sync_period'}, set(config))

    def test_user_configuration_cannot_touch_identity_or_security(self):
        rules_file = os.path.join(os.path.dirname(trove.__file__),
                                  'templates', 'cassandra',
                                  'validation-rules.json')
        with open(rules_file) as f:
            rules = json.load(f)
        names = [rule['name'] for rule in rules['configuration-parameters']]
        self.assertEqual(len(names), len(set(names)))
        self.assertIn('concurrent_reads', names)
        for name in ('cluster_name', 'listen_address', 'seed_provider',
                     'endpoint_snitch', 'authenticator', 'authorizer',
                     'auto_bootstrap', 'num_tokens', 'partitioner',
                     'broadcast_rpc_address', 'initial_token',
                     # options the server no longer has
                     'rpc_server_type', 'thrift_framed_transport_size_in_mb',
                     'read_request_timeout_in_ms'):
            self.assertNotIn(name, names)


class TestCassandraAppConfiguration(CassandraGuestTestCase):

    def test_single_instance(self):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        config = app.configuration_manager.parse_configuration()

        # A cluster of its own, named after the instance.
        self.assertEqual('guest-0001', config['cluster_name'])
        self.assertEqual('SimpleSnitch', config['endpoint_snitch'])
        self.assertEqual(cassandra_service.PASSWORD_AUTHENTICATOR,
                         config['authenticator'])
        self.assertEqual(cassandra_service.CASSANDRA_AUTHORIZER,
                         config['authorizer'])
        self.assertEqual('0.0.0.0', config['rpc_address'])
        self.assertEqual('10.0.0.5', config['listen_address'])
        self.assertEqual('10.0.0.5', config['broadcast_rpc_address'])
        self.assertEqual(['/var/lib/cassandra/data'],
                         config['data_file_directories'])
        for name in ('commitlog_directory', 'saved_caches_directory',
                     'hints_directory', 'cdc_raw_directory'):
            self.assertTrue(config[name].startswith('/var/lib/cassandra/'))
        self.assertEqual(['10.0.0.5'], app.get_seeds())
        self.assertFalse(app.is_cluster_member)

    def test_cluster_member(self):
        app = self._app()
        app.apply_initial_guestagent_configuration(cluster_name='cluster-1')
        config = app.configuration_manager.parse_configuration()

        self.assertEqual('cluster-1', config['cluster_name'])
        self.assertEqual('GossipingPropertyFileSnitch',
                         config['endpoint_snitch'])
        self.assertTrue(app.is_cluster_member)

    def test_seeds(self):
        app = self._app()
        app.apply_initial_guestagent_configuration(cluster_name='cluster-1')
        app.set_seeds({'10.0.0.7', '10.0.0.6'})

        config = app.configuration_manager.parse_configuration()
        self.assertEqual(
            [{'class_name': cassandra_service.SEED_PROVIDER,
              'parameters': [{'seeds': '10.0.0.6,10.0.0.7'}]}],
            config['seed_provider'])
        self.assertEqual(['10.0.0.6', '10.0.0.7'], app.get_seeds())

    def test_auto_bootstrap(self):
        app = self._app()
        app.set_auto_bootstrap(False)
        self.assertIs(
            False,
            app.configuration_manager.parse_configuration()['auto_bootstrap'])

    def test_rendered_configuration_is_yaml_the_codec_round_trips(self):
        app = self._app()
        app.apply_initial_guestagent_configuration(cluster_name='cluster-1')
        config = app.configuration_manager.parse_configuration()
        self.assertEqual(config, YAML.deserialize(YAML.serialize(config)))

    @mock.patch.object(cassandra_service, 'operating_system')
    def test_topology_file(self, mock_os):
        app = self._app()
        app.write_cluster_topology('dc1', 'rack2')

        path, content = mock_os.write_file.call_args[0][:2]
        self.assertEqual('/etc/cassandra/cassandra-rackdc.properties', path)
        self.assertEqual({'dc': 'dc1', 'rack': 'rack2', 'prefer_local': True},
                         content)
        self.assertEqual(
            ['dc=dc1', 'rack=rack2', 'prefer_local=true'],
            app._TOPOLOGY_CODEC.serialize(content).splitlines())


class TestCassandraAddress(trove_testtools.TestCase):

    def setUp(self):
        super(TestCassandraAddress, self).setUp()
        self.patch_datastore_manager('cassandra')
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass

    def test_address_is_the_one_of_the_users_interface(self):
        # The guest agent sees the management network; the address the
        # clients and the other nodes use is on record for the container.
        app = cassandra_service.CassandraApp(mock.Mock(), mock.Mock())
        eth1 = json.dumps({'ipv4_address': '192.168.1.20',
                           'mac_address': 'fa:16:3e:00:00:01'})
        self.patch_conf_property('network_isolation', True)
        with mock.patch('os.path.exists', return_value=True), \
                mock.patch('builtins.open', mock.mock_open(read_data=eth1)):
            self.assertEqual('192.168.1.20', app.address)

    def test_address_without_network_isolation(self):
        app = cassandra_service.CassandraApp(mock.Mock(), mock.Mock())
        self.patch_conf_property('network_isolation', False)
        with mock.patch.object(cassandra_service.netutils, 'get_my_ipv4',
                               return_value='172.31.240.9'):
            self.assertEqual('172.31.240.9', app.address)


class TestCassandraAppLifecycle(CassandraGuestTestCase):

    @mock.patch.object(cassandra_service, 'docker_util')
    @mock.patch.object(cassandra_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.return_value = True

        app.start_db(ds_version='5.0')

        args, kwargs = mock_docker.start_container.call_args
        self.assertEqual('cassandra:5.0', args[1])
        command = kwargs['command']
        # The entrypoint rewrites the configuration for the command
        # 'cassandra'; a path gets past it.
        self.assertTrue(command.startswith('/opt/cassandra/bin/cassandra -f'))
        self.assertIn('-Dcassandra.config=file:///etc/cassandra-trove/'
                      'cassandra.yaml', command)
        self.assertIn('-Dcassandra-rackdc.properties=file:///etc/'
                      'cassandra-trove/cassandra-rackdc.properties', command)
        self.assertEqual('999:999', kwargs['user'])
        # The configuration directory of the image is not mounted over.
        self.assertEqual('/etc/cassandra-trove',
                         kwargs['volumes']['/etc/cassandra']['bind'])
        self.assertEqual('/var/lib/cassandra',
                         kwargs['volumes']['/var/lib/cassandra']['bind'])
        self.assertEqual({'7000/tcp': 7000, '7001/tcp': 7001,
                          '9042/tcp': 9042}, kwargs['ports'])
        self.assertIn('statusbinary', kwargs['healthcheck']['test'][1])

    @mock.patch.object(cassandra_service, 'docker_util')
    @mock.patch.object(cassandra_service, 'operating_system')
    def test_a_slow_start_is_waited_for_while_the_container_runs(
            self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.side_effect = [False, True]
        mock_docker.get_container_status.return_value = 'running'

        app.start_db(ds_version='5.0')

        self.assertEqual(CONF.restore_usage_timeout,
                         app.status.wait_for_status.call_args[0][1])

        app.status.wait_for_status.side_effect = [False]
        mock_docker.get_container_status.return_value = 'exited'
        self.assertRaises(exception.TroveError, app.start_db,
                          ds_version='5.0')

    @mock.patch.object(cassandra_service.CassandraApp, 'start_db')
    @mock.patch.object(cassandra_service, 'docker_util')
    @mock.patch.object(cassandra_service, 'operating_system')
    def test_restart_starts_a_member_that_never_ran(self, mock_os,
                                                    mock_docker, mock_start):
        app = self._app()
        app.docker_client.containers.get.side_effect = Exception('NotFound')

        app.restart()

        mock_start.assert_called_once_with(update_db=True)
        mock_docker.restart_container.assert_not_called()

    def test_execute_reports_any_failure(self):
        app = self._app()
        container = app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, (b'out\n', None))
        self.assertEqual('out\n', app.execute(['nodetool', 'status']))
        self.assertEqual({'HOME': '/tmp'},
                         container.exec_run.call_args[1]['environment'])

        # The shell exits with 2 for a statement the server refused.
        container.exec_run.return_value = (2, (b'', b'Unauthorized'))
        self.assertRaisesRegex(exception.TroveError, 'Unauthorized',
                               app.execute, ['cqlsh', '-e', 'x'])

    @mock.patch.object(cassandra_service.CassandraApp, 'save_password')
    @mock.patch.object(cassandra_service, 'operating_system')
    def test_credentials_file_is_the_database_users_alone(self, mock_os,
                                                          mock_save):
        app = self._app()
        app._write_credentials('os_admin', 'pw')

        mock_save.assert_called_once_with('os_admin', 'pw')
        path, content = mock_os.write_file.call_args[0][:2]
        self.assertEqual('/etc/cassandra/credentials', path)
        self.assertEqual('[PlainTextAuthProvider]\nusername = os_admin\n'
                         'password = pw\n', content)
        # The shell ignores a file that is not its user's own or that
        # others can read.
        self.assertEqual(('/etc/cassandra/credentials', '999', '999'),
                         mock_os.chown.call_args[0])
        self.assertEqual(FileMode.SET_USR_RW, mock_os.chmod.call_args[0][1])

    @mock.patch.object(cassandra_service.CassandraApp, '_write_credentials')
    def test_secure_replaces_the_default_superuser(self, mock_write):
        app = self._app()
        app.adm = mock.Mock()
        app.adm.is_available.return_value = True
        order = mock.Mock()
        order.attach_mock(mock_write, 'write')
        order.attach_mock(app.adm.create_superuser, 'create')
        order.attach_mock(app.adm.drop_role, 'drop')

        password = app.secure()

        self.assertEqual(
            [mock.call.write('cassandra', 'cassandra'),
             mock.call.create('os_admin', password),
             mock.call.write('os_admin', password),
             mock.call.drop('cassandra')],
            order.mock_calls)

    @mock.patch.object(cassandra_service.CassandraApp, 'get_data_center',
                       return_value='dc1')
    @mock.patch.object(cassandra_service.CassandraApp, 'secure')
    @mock.patch.object(cassandra_service.CassandraApp, 'admin_password',
                       new_callable=mock.PropertyMock, return_value='pw')
    def test_cluster_secure_keeps_the_roles_on_several_nodes(
            self, mock_pw, mock_secure, mock_dc):
        app = self._app()
        app.adm = mock.Mock()
        status = ('Datacenter: dc1\n==\n'
                  '--  Address    Load  Tokens  Owns  Host ID  Rack\n'
                  'UN  10.0.0.5  1 KiB  16  ?  a  rack1\n'
                  'UN  10.0.0.6  1 KiB  16  ?  b  rack1\n')
        with mock.patch.object(cassandra_service.CassandraApp, 'nodetool',
                               return_value=status) as nodetool:
            credentials = app.cluster_secure('pw')

        mock_secure.assert_called_once_with('pw')
        # Two nodes: two replicas. Never more than three.
        app.adm.set_replication.assert_called_once_with(
            'system_auth', 'dc1', 2)
        nodetool.assert_any_call('repair', '--full', 'system_auth')
        self.assertEqual('os_admin', credentials['_name'])
        self.assertEqual('pw', credentials['_password'])

    def test_count_nodes(self):
        app = self._app()
        status = ('Datacenter: dc1\n===============\n'
                  'Status=Up/Down\n|/ State=Normal/Leaving/Joining/Moving\n'
                  '--  Address    Load  Tokens  Owns  Host ID  Rack\n'
                  'UN  10.0.0.5  1 KiB  16  ?  a  rack1\n'
                  'DN  10.0.0.6  1 KiB  16  ?  b  rack1\n'
                  'UJ  10.0.0.7  1 KiB  16  ?  c  rack1\n\n')
        with mock.patch.object(cassandra_service.CassandraApp, 'nodetool',
                               return_value=status):
            self.assertEqual(3, app.count_nodes())

    @mock.patch.object(cassandra_service.CassandraApp, 'stop_db')
    def test_decommission_stops_the_node(self, mock_stop):
        app = self._app()
        with mock.patch.object(cassandra_service.CassandraApp,
                               'nodetool') as nodetool:
            app.node_decommission()
            nodetool.assert_called_once_with('decommission')
        mock_stop.assert_called_once_with(update_db=True)

        mock_stop.reset_mock()
        with mock.patch.object(cassandra_service.CassandraApp, 'nodetool',
                               side_effect=exception.TroveError('boom')):
            app.node_decommission()
        mock_stop.assert_not_called()
        app.status.set_status.assert_called_once()

    def test_cleanup_reports_the_state_it_ends_in(self):
        app = self._app()
        app.node_cleanup_begin()
        app.status.set_status.assert_called_once_with(
            service_status.ServiceStatuses.BLOCKED)

        app.status.get_actual_db_status.return_value = (
            service_status.ServiceStatuses.HEALTHY)
        with mock.patch.object(cassandra_service.CassandraApp, 'nodetool',
                               side_effect=exception.TroveError('boom')):
            app.node_cleanup()
        app.status.set_status.assert_called_with(
            service_status.ServiceStatuses.HEALTHY)

    @mock.patch.object(cassandra_service.service.BaseDbApp, 'create_backup')
    def test_backup_is_a_snapshot_named_after_it(self, mock_base):
        app = self._app()
        order = mock.Mock()
        order.attach_mock(mock_base, 'backup')
        with mock.patch.object(cassandra_service.CassandraApp,
                               'nodetool') as nodetool:
            order.attach_mock(nodetool, 'nodetool')
            app.create_backup(mock.Mock(), {'id': 'b1'})

        self.assertEqual(
            ['nodetool', 'nodetool', 'backup', 'nodetool'],
            [call[0] for call in order.mock_calls])
        self.assertEqual(('snapshot', '-t', 'b1'), order.mock_calls[1][1])
        self.assertEqual(('clearsnapshot', '-t', 'b1'),
                         order.mock_calls[3][1])
        kwargs = mock_base.call_args[1]
        self.assertFalse(kwargs['need_dbuser'])
        self.assertEqual('--db-datadir=/var/lib/cassandra/data',
                         kwargs['extra_params'])
        self.assertIn('/var/lib/cassandra/data', kwargs['volumes_mapping'])

    @mock.patch.object(cassandra_service.CassandraApp, '_write_credentials')
    @mock.patch.object(cassandra_service.CassandraApp, 'stop_db')
    @mock.patch.object(cassandra_service.CassandraApp, 'start_db')
    def test_post_restore_updates(self, mock_start, mock_stop, mock_write):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        app.adm = mock.Mock()
        app.adm.is_available.return_value = True
        seen = {}
        mock_start.side_effect = lambda **kw: seen.update(
            app.configuration_manager.parse_configuration())

        with mock.patch.object(cassandra_service.CassandraApp, 'nodetool'):
            app.apply_post_restore_updates({'instance_id': 'source-id'})

        # Started once as the instance the backup was taken from, without
        # authentication, reachable from the instance only.
        self.assertEqual('source-id', seen['cluster_name'])
        self.assertEqual(cassandra_service.ALLOW_ALL_AUTHENTICATOR,
                         seen['authenticator'])
        self.assertEqual('127.0.0.1', seen['rpc_address'])
        self.assertEqual('127.0.0.1', seen['listen_address'])

        app.adm.set_cluster_name.assert_called_once_with('guest-0001')
        name, salted_hash = app.adm.reset_superuser.call_args[0]
        password = mock_write.call_args[0][1]
        self.assertEqual('os_admin', name)
        self.assertTrue(salted_hash.startswith('$2a$10$'))
        self.assertTrue(bcrypt.checkpw(password.encode(),
                                       salted_hash.encode()))

        # And is itself again afterwards.
        mock_stop.assert_called_once()
        config = app.configuration_manager.parse_configuration()
        self.assertEqual('guest-0001', config['cluster_name'])
        self.assertEqual(cassandra_service.PASSWORD_AUTHENTICATOR,
                         config['authenticator'])
        self.assertEqual('0.0.0.0', config['rpc_address'])
        self.assertEqual('10.0.0.5', config['listen_address'])

    @mock.patch.object(cassandra_service.CassandraApp, '_write_credentials')
    @mock.patch.object(cassandra_service.CassandraApp, 'stop_db')
    @mock.patch.object(cassandra_service.CassandraApp, 'start_db',
                       side_effect=exception.TroveError('boom'))
    def test_restore_settings_do_not_outlive_a_failure(
            self, mock_start, mock_stop, mock_write):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        self.assertRaises(exception.TroveError,
                          app.apply_post_restore_updates,
                          {'instance_id': 'source-id'})
        config = app.configuration_manager.parse_configuration()
        self.assertEqual(cassandra_service.PASSWORD_AUTHENTICATOR,
                         config['authenticator'])
        mock_write.assert_not_called()


class TestCassandraAdmin(CassandraGuestTestCase):

    def _adm(self, clustered=False):
        app = self._app()
        app.apply_initial_guestagent_configuration(
            cluster_name='cluster-1' if clustered else None)
        adm = cassandra_service.CassandraAdmin(app)
        statements = []
        adm.execute = mock.Mock(
            side_effect=lambda statement, authenticate=True:
            statements.append(statement) or '')
        return adm, statements

    def test_shell_command(self):
        app = self._app()
        adm = cassandra_service.CassandraAdmin(app)
        with mock.patch.object(cassandra_service.CassandraApp, 'execute',
                               return_value='') as execute:
            adm.execute('SELECT key FROM system.local')
            command = execute.call_args[0][0]
            self.assertEqual('cqlsh', command[0])
            self.assertIn('--credentials=/etc/cassandra-trove/credentials',
                          command)
            self.assertEqual(['-e', 'SELECT key FROM system.local'],
                             command[-2:])

            adm.execute('x', authenticate=False)
            self.assertFalse([arg for arg in execute.call_args[0][0]
                              if arg.startswith('--credentials')])

    def test_query_reads_the_json_rows(self):
        app = self._app()
        adm = cassandra_service.CassandraAdmin(app)
        output = ('\n [json]\n----------------------\n'
                  '        {"role": "os_admin", "is_superuser": true}\n'
                  ' {"role": "appuser", "is_superuser": false}\n'
                  '\n(2 rows)\n')
        with mock.patch.object(cassandra_service.CassandraApp, 'execute',
                               return_value=output):
            self.assertEqual(
                [{'role': 'os_admin', 'is_superuser': True},
                 {'role': 'appuser', 'is_superuser': False}],
                adm.query('SELECT JSON role FROM system_auth.roles'))

    def test_literals_are_quoted(self):
        adm, statements = self._adm()
        adm.create_superuser('os_admin', "it's")
        self.assertEqual(
            "CREATE ROLE 'os_admin' WITH PASSWORD = 'it''s' AND "
            "SUPERUSER = true AND LOGIN = true", statements[0])

    def test_users_get_no_permission_to_pass_on(self):
        adm, statements = self._adm()
        user = models.CassandraUser('appuser', 'pw')
        user.databases.append(models.CassandraSchema('appdb').serialize())

        adm.create_user([user.serialize()])

        self.assertIn('SUPERUSER = false', statements[0])
        grants = statements[1:]
        self.assertEqual(
            ['GRANT %s ON KEYSPACE "appdb" TO \'appuser\'' % permission
             for permission in ('ALTER', 'CREATE', 'DROP', 'MODIFY',
                                'SELECT')], grants)
        self.assertFalse([g for g in grants if 'AUTHORIZE' in g or
                          'ALL PERMISSIONS' in g])

    def test_reserved_names_are_refused(self):
        adm, statements = self._adm()
        self.assertRaises(
            ValueError, adm.create_user,
            [models.CassandraUser('os_admin', 'pw').serialize()])
        self.assertRaises(
            ValueError, adm.create_database,
            [models.CassandraSchema('system_auth').serialize()])
        self.assertEqual([], statements)

    def _with_tables(self, adm, roles, permissions, keyspaces=()):
        tables = {
            'system_auth.roles': roles,
            'system_auth.role_permissions': permissions,
            'system_schema.keyspaces': [{'keyspace_name': k}
                                        for k in keyspaces],
        }
        adm.query = mock.Mock(side_effect=lambda statement, *a, **k: next(
            rows for table, rows in tables.items() if table in statement))

    def test_list_users_and_their_databases(self):
        adm, _ = self._adm()
        self._with_tables(
            adm,
            roles=[
                {'role': 'os_admin', 'is_superuser': True, 'can_login': True},
                {'role': 'cassandra', 'is_superuser': True,
                 'can_login': True},
                {'role': 'appuser', 'is_superuser': False, 'can_login': True},
                {'role': 'group', 'is_superuser': False, 'can_login': False},
            ],
            permissions=[
                {'role': 'appuser', 'resource': 'data/appdb',
                 'permissions': ['SELECT', 'MODIFY']},
                # a table, not the keyspace
                {'role': 'appuser', 'resource': 'data/other/t',
                 'permissions': ['SELECT']},
                {'role': 'appuser', 'resource': 'roles/appuser',
                 'permissions': ['ALTER']},
                {'role': 'appuser', 'resource': 'data/system_auth',
                 'permissions': ['SELECT']},
            ])

        users = adm.list_users()[0]

        # Superusers, the root user among them, and roles that cannot log
        # in are not users.
        self.assertEqual(['appuser'], [u['_name'] for u in users])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in users[0]['_databases']])
        self.assertEqual(['cassandra', 'os_admin'],
                         sorted(u.name for u in adm.list_superusers()))
        self.assertTrue(adm.is_root_enabled())

    def test_permission_on_all_keyspaces(self):
        adm, _ = self._adm()
        self._with_tables(
            adm,
            roles=[{'role': 'appuser', 'is_superuser': False,
                    'can_login': True}],
            permissions=[{'role': 'appuser', 'resource': 'data',
                          'permissions': ['SELECT']}],
            keyspaces=['system', 'appdb', 'otherdb'])
        self.assertEqual(['appdb', 'otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])

    def test_root_is_not_enabled_with_the_admin_alone(self):
        adm, _ = self._adm()
        self._with_tables(
            adm, roles=[{'role': 'os_admin', 'is_superuser': True,
                         'can_login': True}], permissions=[])
        self.assertFalse(adm.is_root_enabled())

    def test_keyspace_of_a_single_instance_has_one_replica(self):
        adm, statements = self._adm()
        adm.create_database([models.CassandraSchema('appdb').serialize()])
        self.assertEqual(
            'CREATE KEYSPACE "appdb" WITH REPLICATION = '
            "{'class': 'SimpleStrategy', 'replication_factor': 1}",
            statements[0])

    def test_keyspace_of_a_cluster_is_on_every_member_up_to_three(self):
        adm, statements = self._adm(clustered=True)
        with mock.patch.object(cassandra_service.CassandraApp,
                               'get_data_center', return_value='dc1'), \
                mock.patch.object(cassandra_service.CassandraApp,
                                  'count_nodes', return_value=5):
            adm.create_database(
                [models.CassandraSchema('appdb').serialize()])
        self.assertEqual(
            'CREATE KEYSPACE "appdb" WITH REPLICATION = '
            "{'class': 'NetworkTopologyStrategy', 'dc1': 3}", statements[0])

    def test_rename_needs_a_password_and_keeps_the_permissions(self):
        adm, statements = self._adm()
        user = models.CassandraUser('appuser')
        user.databases.append(models.CassandraSchema('appdb').serialize())
        adm._find_user = mock.Mock(return_value=user)

        self.assertRaises(exception.UnprocessableEntity,
                          adm.update_attributes, 'appuser', None,
                          {'name': 'renamed'})

        adm.update_attributes('appuser', None,
                              {'name': 'renamed', 'password': 'pw'})
        self.assertIn("CREATE ROLE 'renamed'", statements[0])
        self.assertIn('ON KEYSPACE "appdb" TO \'renamed\'', statements[1])
        self.assertEqual("DROP ROLE 'appuser'", statements[-1])

    def test_restore_writes_the_role_without_authentication(self):
        app = self._app()
        adm = cassandra_service.CassandraAdmin(app)
        adm.execute = mock.Mock(return_value='')
        adm.reset_superuser('os_admin', '$2a$10$hash')
        statement, kwargs = (adm.execute.call_args[0][0],
                             adm.execute.call_args[1])
        self.assertIn('system_auth.roles', statement)
        self.assertIn("'$2a$10$hash'", statement)
        self.assertFalse(kwargs['authenticate'])

        adm.set_cluster_name('guest-0001')
        self.assertEqual(
            "UPDATE system.local SET cluster_name = 'guest-0001' "
            "WHERE key = 'local'", adm.execute.call_args[0][0])


class TestCassandraManager(CassandraGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = cassandra_manager.Manager()
        manager.app = mock.Mock()
        manager.app.datadir = '/var/lib/cassandra/data'
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=2048, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/cassandra', backup_info=None,
                    config_contents='num_tokens: 16\n',
                    root_password=None, overrides=None,
                    cluster_config=None, snapshot=None, ds_version='5.0')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_single_instance(self):
        manager = self._manager()
        self._prepare(manager)
        manager.app.apply_initial_guestagent_configuration.\
            assert_called_once_with(cluster_name=None)
        manager.app.start_db.assert_called_once_with(ds_version='5.0')
        manager.app.secure.assert_called_once_with()

    def test_cluster_member_is_configured_and_not_started(self):
        # A server records its cluster on its first start; a member must
        # not have one before it has the seeds.
        manager = self._manager()
        self._prepare(manager, cluster_config={
            'id': 'cluster-1', 'instance_type': 'member', 'dc': 'dc1',
            'rack': 'az1'})
        manager.app.apply_initial_guestagent_configuration.\
            assert_called_once_with(cluster_name='cluster-1')
        manager.app.write_cluster_topology.assert_called_once_with(
            'dc1', 'az1')
        manager.app.start_db.assert_not_called()
        manager.app.secure.assert_not_called()

    @mock.patch.object(cassandra_manager.Manager, 'perform_restore')
    def test_restore(self, mock_restore):
        manager = self._manager()
        manager.adm.is_root_enabled.return_value = True
        order = mock.Mock()
        order.attach_mock(mock_restore, 'restore')
        order.attach_mock(manager.app.apply_post_restore_updates, 'updates')
        order.attach_mock(manager.app.start_db, 'start_db')
        backup_info = {'id': 'b1', 'instance_id': 'source-id',
                       'location': 'x', 'checksum': 'y'}

        self._prepare(manager, backup_info=backup_info)

        self.assertEqual(['restore', 'updates', 'start_db'],
                         [call[0] for call in order.mock_calls])
        # The superuser came with the backup and was given a new password.
        manager.app.secure.assert_not_called()
        manager.status.report_root.assert_called_once()

    def test_cluster_complete_repairs_the_roles_first(self):
        manager = self._manager()
        with mock.patch.object(base_manager.Manager,
                               'cluster_complete') as base_complete:
            manager.cluster_complete(mock.Mock())
        manager.app.repair_auth.assert_called_once_with()
        base_complete.assert_called_once()
