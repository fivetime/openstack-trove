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

import grp
import os
import pwd
import shutil
import tempfile
from unittest import mock

from oslo_utils import importutils
from valkey._parsers import helpers as parsers

from trove.common import cfg
from trove.common import cluster_health as ch
from trove.common import configurations
from trove.common import constants
from trove.common import exception
from trove.common import loadbalancer
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.datastore.redis_common import manager as common_manager
from trove.guestagent.datastore.redis_common import sentinel
from trove.guestagent.datastore.redis_common import service as common_service
from trove.guestagent.strategies.replication import redis_base
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class TestRedisDatastoreWiring(trove_testtools.TestCase):
    """The redis datastore resolves through every lookup the services use.

    Nothing here needs a guest: these are the name-to-class lookups that the
    API, the taskmanager and the guest agent each do from configuration, and
    a name that resolves to nothing only shows up when an instance is built.
    """

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['redis'])

        self.assertTrue(
            issubclass(manager_cls, common_manager.RedisManager))
        self.assertEqual('redis', manager_cls.DATASTORE_NAME)
        self.assertTrue(
            issubclass(manager_cls.APP_CLASS, common_service.RedisApp))
        self.assertEqual('redis', manager_cls.APP_CLASS.DATASTORE_NAME)

    def test_app_uses_redis_names(self):
        app_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['redis']).APP_CLASS

        self.assertEqual('redis-server', app_cls.SERVER_BINARY)
        self.assertEqual('redis-cli', app_cls.CLI_BINARY)
        self.assertEqual('/etc/redis/redis.conf', app_cls.CONFIG_FILE)
        # The health check runs inside the container, so it has to use the
        # socket path as the container sees it.
        self.assertIn(app_cls.CONTAINER_SOCKET,
                      app_cls.HEALTHCHECK['test'][1])
        self.assertIn(app_cls.CLI_BINARY, app_cls.HEALTHCHECK['test'][1])

    def test_replication_strategy_resolves(self):
        strategy_cls = importutils.import_class('%s.%s' % (
            CONF.redis.replication_namespace,
            CONF.redis.replication_strategy))

        self.assertTrue(
            issubclass(strategy_cls, redis_base.RedisReplicationBase))
        self.assertEqual('redis', strategy_cls.DATASTORE_NAME)

    def test_options(self):
        self.assertEqual('redis', CONF.redis.docker_image)
        self.assertEqual('redisbackup', CONF.redis.backup_strategy)
        self.assertEqual('/var/lib/redis', CONF.redis.mount_point)
        self.assertIsNotNone(
            importutils.import_class(CONF.redis.root_controller))

    def test_cluster_support_and_strategies(self):
        self.assertTrue(CONF.redis.cluster_support)
        for option in ('api_strategy', 'taskmanager_strategy',
                       'guestagent_strategy'):
            self.assertIsNotNone(
                importutils.import_class(CONF.redis.get(option)))
        # The members talk on the cluster bus, client port + 10000, which a
        # single instance keeps closed.
        self.assertTrue(any(16379 in ports
                            for ports in CONF.redis.cluster_tcp_ports))
        self.assertFalse(any(16379 in ports for ports in CONF.redis.tcp_ports))
        self.assertIn('clusteradmin', CONF.redis.ignore_users)
        # Root on a cluster goes to every member: the Redis controller
        # does that, the default one refuses clusters.
        self.assertEqual(
            'trove.extensions.redis.service.RedisRootController',
            CONF.redis.root_controller)

    def test_user_api_is_enabled(self):
        self.assertIn('redis',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertNotIn(
            'redis',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_configuration_parser(self):
        self.assertIs(configurations.RedisConfParser,
                      template.SERVICE_PARSERS['redis'])


class TestRedisConfigTemplate(trove_testtools.TestCase):

    def _render(self, manager):
        version = mock.Mock()
        version.datastore_name = manager
        version.manager = manager
        version.name = '7.2'
        version.version = '7.2'
        flavor = {'ram': 2048, 'vcpus': 2}
        return template.SingleInstanceConfigTemplate(
            version, flavor, 'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()

    def test_runs_in_foreground(self):
        rendered = self._render('redis')

        # The server is the container's main process. If it daemonizes or
        # waits for systemd, the container exits as soon as it starts.
        self.assertNotIn('daemonize', rendered)
        self.assertNotIn('supervised', rendered)
        self.assertNotIn('pidfile', rendered)

    def test_paths_match_the_app(self):
        rendered = self._render('redis').splitlines()
        app_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['redis']).APP_CLASS

        self.assertIn('unixsocket %s' % app_cls.CONTAINER_SOCKET, rendered)
        self.assertIn('dir %s' % CONF.redis.mount_point, rendered)
        self.assertIn('include %s/index.conf' % app_cls.CNF_INCLUDE_DIR,
                      rendered)
        self.assertIn('port 6379', rendered)

    def test_append_only_file_on(self):
        # On by default and before the include, so that a configuration
        # group can still turn it off.
        for manager in ('redis', 'valkey', 'keydb'):
            lines = self._render(manager).splitlines()
            self.assertIn('appendonly yes', lines, manager)
            self.assertIn('appendfsync everysec', lines, manager)
            include = [i for i, line in enumerate(lines)
                       if line.startswith('include ')][0]
            self.assertLess(lines.index('appendonly yes'), include, manager)

    def test_family_members_do_not_share_paths(self):
        # Each member keeps its files under its own name; a template
        # copied from a sibling without renaming would point the server at
        # a directory the guest agent never creates.
        for manager in ('redis', 'valkey', 'keydb'):
            rendered = self._render(manager)
            for other in {'redis', 'valkey', 'keydb'} - {manager}:
                self.assertNotIn('/%s' % other, rendered,
                                 '%s template mentions %s' % (manager, other))


class TestRedisCluster(trove_testtools.TestCase):
    """The guest side of a Redis Cluster, on a mocked server."""

    def setUp(self):
        super(TestRedisCluster, self).setUp()
        patcher = mock.patch.object(common_manager.RedisManager,
                                    '__init__', return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.manager = common_manager.RedisManager()
        self.manager.app = mock.MagicMock()
        self.manager.adm = mock.MagicMock()

    def test_rebalance_arguments(self):
        self.manager.cluster_rebalance(None, use_empty_masters=True)
        self.manager.app.run_cluster_cli.assert_called_with(
            'rebalance', '--cluster-use-empty-masters')
        self.manager.cluster_rebalance(None, weights={'n1': 0, 'n2': 0})
        self.manager.app.run_cluster_cli.assert_called_with(
            'rebalance', '--cluster-weight', 'n1=0',
            '--cluster-weight', 'n2=0')

    CHECK_OK = ('[OK] All nodes agree about slots configuration.\n'
                '>>> Check for open slots...\n'
                '>>> Check slots coverage...\n'
                '[OK] All 16384 slots covered.\n')

    def _cli_fails_on_rebalance(self, check_output):
        def cli(*args):
            if args[0] == 'rebalance':
                raise exception.TroveError('rebalance failed')
            return check_output
        self.manager.app.run_cluster_cli.side_effect = cli

    def test_rebalance_failure_with_the_slots_moved(self):
        # KeyDB's CLI exits with 1 once the last slot is off a master
        # without replicas; the slots did move.
        self._cli_fails_on_rebalance(self.CHECK_OK)
        self.manager.adm.cluster_nodes.return_value = [
            {'id': 'n1', 'has_slots': False},
            {'id': 'n2', 'has_slots': True}]
        self.manager.cluster_rebalance(None, weights={'n1': 0})
        self.manager.app.run_cluster_cli.assert_called_with('check')

    def test_rebalance_failure_with_slots_left(self):
        self._cli_fails_on_rebalance(self.CHECK_OK)
        self.manager.adm.cluster_nodes.return_value = [
            {'id': 'n1', 'has_slots': True}]
        self.assertRaises(exception.TroveError,
                          self.manager.cluster_rebalance, None,
                          weights={'n1': 0})

    def test_rebalance_failure_with_open_slots(self):
        self._cli_fails_on_rebalance(
            self.CHECK_OK + '[WARNING] The following slots are open: 12.\n')
        self.manager.adm.cluster_nodes.return_value = [
            {'id': 'n1', 'has_slots': False}]
        self.assertRaises(exception.TroveError,
                          self.manager.cluster_rebalance, None,
                          weights={'n1': 0})

    def test_rebalance_failure_when_spreading(self):
        # Nothing was drained, so nothing tells a failure from a success.
        self._cli_fails_on_rebalance(self.CHECK_OK)
        self.assertRaises(exception.TroveError,
                          self.manager.cluster_rebalance, None,
                          use_empty_masters=True)

    def test_root_password_only_when_enabled(self):
        self.manager.adm.is_root_enabled.return_value = False
        self.assertIsNone(self.manager.get_root_password(None))
        self.manager.adm.is_root_enabled.return_value = True
        self.manager.app.get_auth_password.return_value = 'rootpw'
        self.assertEqual('rootpw', self.manager.get_root_password(None))
        self.manager.app.get_auth_password.assert_called_once_with(
            file='root.cnf')

    def test_del_node(self):
        self.manager.cluster_del_node(None, 'n1')
        self.manager.app.run_cluster_cli.assert_called_once_with(
            'del-node', 'n1')

    def test_forget(self):
        # A node that is gone: told to this node, not through the CLI
        # (del-node loads the cluster from the nodes that answer).
        self.manager.cluster_forget(None, 'n1')
        self.manager.adm.cluster_forget.assert_called_once_with('n1')
        self.manager.app.run_cluster_cli.assert_not_called()


class TestRedisClusterApp(trove_testtools.TestCase):

    def setUp(self):
        super(TestRedisClusterApp, self).setUp()
        patcher = mock.patch.object(common_service.RedisApp, '__init__',
                                    return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.app = common_service.RedisApp()
        self.app.docker_client = mock.MagicMock()
        self.app.adm = mock.MagicMock()
        self.container = self.app.docker_client.containers.get.return_value
        for name, value in (('get_node_ip', ['10.0.0.5', '6379']),
                            ('get_cluster_admin_password', 's3cret')):
            p = mock.patch.object(common_service.RedisApp, name,
                                  return_value=value)
            p.start()
            self.addCleanup(p.stop)

    def test_cluster_nodes(self):
        # As valkey-py parses CLUSTER NODES (seen on Redis 7.2).
        admin = common_service.RedisAdmin()
        admin._connection = mock.Mock()
        admin._connection.cluster.return_value = {
            '172.31.79.12:6379': {
                'node_id': 'r1', 'flags': 'slave', 'master_id': 'm1',
                'slots': [], 'connected': True},
            '172.31.79.11:6379': {
                'node_id': 'm1', 'flags': 'myself,master', 'master_id': '-',
                'slots': [['0', '16383']], 'connected': True},
        }
        nodes = sorted(admin.cluster_nodes(), key=lambda n: n['id'])
        self.assertEqual(
            [{'id': 'm1', 'address': '172.31.79.11', 'role': 'master',
              'master_id': None, 'has_slots': True,
              'flags': ['myself', 'master']},
             {'id': 'r1', 'address': '172.31.79.12', 'role': 'replica',
              'master_id': 'm1', 'has_slots': False, 'flags': ['slave']}],
            nodes)

    def test_cluster_forget(self):
        admin = common_service.RedisAdmin()
        admin._connection = mock.Mock()
        admin.cluster_forget('m1')
        admin._connection.cluster.assert_called_once_with('FORGET', 'm1')

    def _seen(self, state):
        # CLUSTER INFO and CLUSTER NODES of a Valkey 9 cluster of three
        # masters with a replica each, as one node printed them (2026-10-19):
        # all up; a master stopped and its replica failed over; then that
        # replica, now a master without one, stopped too. Parsed as
        # valkey-py parses them.
        here = os.path.join(os.path.dirname(__file__), '..', 'redis_common',
                            'cluster')
        with open(os.path.join(here, 'info-%s.txt' % state)) as f:
            info = parsers.parse_cluster_info(f.read())
        with open(os.path.join(here, 'nodes-%s.txt' % state)) as f:
            nodes = parsers.parse_cluster_nodes(f.read())
        admin = common_service.RedisAdmin()
        admin._connection = mock.Mock()
        admin._connection.cluster.side_effect = (
            lambda what: {'INFO': info, 'NODES': nodes}[what])
        self.app.adm = admin

    def test_a_healthy_cluster(self):
        self._seen('healthy')
        health = self.app.cluster_health()
        self.assertEqual('HEALTHY', health['status'])
        self.assertEqual('172.31.250.51', health['me'])
        view = self.app.member_view()
        self.assertEqual(('ok', 'master', True, False),
                         (view.state, view.role, view.writable, view.down))

    def test_a_master_failed_over(self):
        self._seen('failover')
        health = self.app.cluster_health()
        self.assertEqual('DEGRADED', health['status'])
        self.assertEqual(['172.31.250.53'], health['out'])
        self.assertEqual('failing: 172.31.250.53', health['reason'])

    def test_slots_without_a_master(self):
        self._seen('down')
        health = self.app.cluster_health()
        self.assertEqual('DOWN', health['status'])
        self.assertEqual(['172.31.250.53', '172.31.250.54'], health['out'])
        self.assertEqual('cluster_state fail, 10923 of 16384 slots served',
                         health['reason'])

    def test_cluster_cli_password_in_environment(self):
        self.container.exec_run.return_value = (0, b'[OK] All good')
        self.app.run_cluster_cli('del-node', 'n1')
        command = self.container.exec_run.call_args[0][0]
        self.assertEqual(['redis-cli', '--user', 'clusteradmin', '--cluster',
                          'del-node', '10.0.0.5:6379', 'n1'], command)
        self.assertNotIn('s3cret', ' '.join(command))
        self.assertEqual(
            {'REDISCLI_AUTH': 's3cret'},
            self.container.exec_run.call_args[1]['environment'])

    def test_cluster_cli_failures(self):
        # redis-cli --cluster reports some failures only in its output.
        for ret, output in ((1, b'boom'),
                            (0, b'>>> Removing\n[ERR] Node is not empty!'),
                            (0, b'Moving\n*** Please fix your cluster')):
            self.container.exec_run.return_value = (ret, output)
            self.assertRaises(exception.TroveError, self.app.run_cluster_cli,
                              'rebalance')

    def test_wait_for_cluster(self):
        self.app.adm.cluster_info.side_effect = [
            {'cluster_state': 'fail', 'cluster_known_nodes': '1'},
            {'cluster_state': 'ok', 'cluster_known_nodes': '2'},
            {'cluster_state': 'ok', 'cluster_known_nodes': '3'}]
        with mock.patch.object(common_service.time, 'sleep'):
            self.app.wait_for_cluster(3)
        self.assertEqual(3, self.app.adm.cluster_info.call_count)


class TestRedisAppendOnlyFile(trove_testtools.TestCase):
    """The data of an RDB-only data directory when the configuration turns
    the append only file on: with appendonly yes Redis, Valkey and KeyDB
    load that file and nothing else (seen on redis:7.2, valkey:9 and
    keydb:6.3.3 with a directory holding only dump.rdb: 0 keys).

    The configuration is the guest agent's own manager, codec and override
    directory on real files (a rendered template), not a stub: a stub of
    get_configuration_property hid that it lost its default.
    """

    def setUp(self):
        super(TestRedisAppendOnlyFile, self).setUp()
        patcher = mock.patch.object(common_service.RedisApp, '__init__',
                                    return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        self.datadir = os.path.join(root, 'data')
        os.mkdir(self.datadir)
        conf_dir = os.path.join(root, 'etc')
        os.mkdir(conf_dir)
        for name, value in (
                ('CONFIG_FILE', os.path.join(conf_dir, 'redis.conf')),
                ('CNF_INCLUDE_DIR', os.path.join(conf_dir, 'conf.d')),
                ('database_service_uid', pwd.getpwuid(os.getuid()).pw_name),
                ('database_service_gid', grp.getgrgid(os.getgid()).gr_name)):
            p = mock.patch.object(common_service.RedisApp, name, value)
            p.start()
            self.addCleanup(p.stop)
        # Files the test user owns: no sudo.
        manager_cls = common_service.configuration.ConfigurationManager
        p = mock.patch.object(manager_cls, '__init__', autospec=True,
                              side_effect=self._manager_init)
        p.start()
        self.addCleanup(p.stop)
        self.app = common_service.RedisApp()
        self.app._configuration_manager = None
        self.app.datadir = self.datadir
        self.app.adm = mock.MagicMock()
        self.app.docker_client = mock.MagicMock()
        version = mock.Mock(datastore_name='redis', manager='redis',
                            version='7.2')
        version.name = '7.2'
        self.manager = self.app.configuration_manager
        self.manager.reset_configuration(template.SingleInstanceConfigTemplate(
            version, {'ram': 2048, 'vcpus': 2},
            'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render())
        # The real check, as the guest agent's user: a directory is not a
        # file (Redis 7 and Valkey keep a directory, KeyDB one file).
        real_exists = common_service.operating_system.exists
        p = mock.patch.object(
            common_service.operating_system, 'exists',
            side_effect=lambda path, is_directory=False, as_root=False:
            real_exists(path, is_directory))
        p.start()
        self.addCleanup(p.stop)
        p = mock.patch.object(common_service.time, 'sleep')
        p.start()
        self.addCleanup(p.stop)

    _real_init = common_service.configuration.ConfigurationManager.__init__

    def _manager_init(self_, manager, base_config_path, owner, group, codec,
                      requires_root=False, override_strategy=None):
        TestRedisAppendOnlyFile._real_init(
            manager, base_config_path, owner, group, codec,
            requires_root=False, override_strategy=override_strategy)

    def _touch(self, name):
        open(os.path.join(self.datadir, name), 'w').close()

    def _overridden(self):
        # The override is there and the server would start with the file off.
        return (self.manager.has_system_override(
            common_service.AOF_CHANGE_ID) and
            self.app.get_configuration_property('appendonly') is False)

    def _info(self, *answers):
        base = {'aof_enabled': 1, 'aof_rewrite_in_progress': 0,
                'aof_rewrite_scheduled': 0,
                'aof_last_bgrewrite_status': 'ok'}
        self.app.adm.connection.info.side_effect = [
            dict(base, **a) for a in answers]

    def test_stopped_rdb_only_starts_with_the_file_off(self):
        self._touch('dump.rdb')

        self.assertTrue(self.app.prepare_aof(running=False))
        self.assertTrue(self._overridden())
        self.app.adm.connection.config_set.assert_not_called()

    def test_finish_writes_the_file_then_drops_the_override(self):
        self._info({'aof_rewrite_in_progress': 1}, {})

        self._touch('dump.rdb')
        self.assertTrue(self.app.prepare_aof(running=False))

        self.app.finish_aof()

        self.app.adm.connection.config_set.assert_called_once_with(
            'appendonly', 'yes')
        self.assertFalse(self._overridden())
        self.assertIs(True, self.app.get_configuration_property('appendonly'))

    def test_finish_failure_keeps_the_file_off(self):
        self._info({'aof_last_bgrewrite_status': 'err'})

        self._touch('dump.rdb')
        self.assertTrue(self.app.prepare_aof(running=False))

        self.app.finish_aof()

        self.assertTrue(self._overridden())

    def test_directory_of_redis_7_counts(self):
        self._touch('dump.rdb')
        os.mkdir(os.path.join(self.datadir, 'appendonlydir'))

        self.assertFalse(self.app.prepare_aof(running=False))
        self.assertFalse(self._overridden())

    def test_single_file_of_keydb_counts(self):
        self._touch('dump.rdb')
        self._touch('appendonly.aof')

        self.assertFalse(self.app.prepare_aof(running=False))
        self.assertFalse(self._overridden())

    def test_defaults_when_the_names_are_not_configured(self):
        # The template names neither file: the defaults must hold.
        self.assertEqual('dump.rdb', self.app.get_configuration_property(
            'dbfilename', 'dump.rdb'))
        self.assertIsNone(self.app.get_configuration_property('dbfilename'))

    def test_configured_names(self):
        self.manager.apply_user_override({'dbfilename': 'data.rdb',
                                          'appenddirname': 'aofs'})
        self._touch('data.rdb')

        self.assertTrue(self.app.prepare_aof(running=False))
        os.mkdir(os.path.join(self.datadir, 'aofs'))
        self.assertFalse(self.app.prepare_aof(running=False))

    def test_nothing_without_the_file_or_without_data(self):
        self._touch('dump.rdb')
        self.manager.apply_user_override({'appendonly': 'no'})
        self.assertFalse(self.app.prepare_aof(running=False))
        self.assertFalse(self.manager.has_system_override(
            common_service.AOF_CHANGE_ID))

        os.remove(os.path.join(self.datadir, 'dump.rdb'))
        self.manager.remove_user_override()
        self.assertFalse(self.app.prepare_aof(running=False))
        self.assertFalse(self._overridden())

    def test_leftover_override_is_dropped_first(self):
        # A start that could not write the file left it off; the next one
        # tries again (and with no data left, drops it).
        self.manager.apply_system_override(
            {'appendonly': 'no'}, change_id=common_service.AOF_CHANGE_ID)
        self.assertFalse(self.app.prepare_aof(running=False))
        self.assertFalse(self.manager.has_system_override(
            common_service.AOF_CHANGE_ID))
        self.assertIs(True, self.app.get_configuration_property('appendonly'))

    def test_running_server_writes_the_file_before_restart(self):
        self._touch('dump.rdb')
        self._info({})

        self.assertFalse(self.app.prepare_aof(running=True))
        self.app.adm.connection.config_set.assert_called_once_with(
            'appendonly', 'yes')
        self.assertFalse(self._overridden())

    def test_running_server_failure_restarts_with_the_file_off(self):
        self._touch('dump.rdb')
        self.app.adm.connection.config_set.side_effect = Exception('down')

        self.assertFalse(self.app.prepare_aof(running=True))
        self.assertTrue(self._overridden())

    def test_turn_on_times_out(self):
        self.app.adm.connection.info.return_value = {
            'aof_enabled': 1, 'aof_rewrite_in_progress': 1}
        with mock.patch.object(common_service.time, 'monotonic',
                               side_effect=[0, 1, 5]):
            self.assertRaises(exception.TroveError, self.app.turn_on_aof,
                              timeout=2)

    def test_configuration_group_restart(self):
        self._touch('dump.rdb')
        self._info({})
        with mock.patch.object(common_service.docker_util,
                               'restart_container') as restart:
            self.app.apply_overrides({'appendonly': 'yes'})
        self.app.adm.connection.config_set.assert_called_once_with(
            'appendonly', 'yes')
        restart.assert_called_once()

    def test_start_db_order(self):
        self._touch('dump.rdb')
        self._info({})
        calls = []
        self.app.status = mock.Mock()
        self.app.status.wait_for_status.side_effect = (
            lambda *a: calls.append('healthy') or True)
        real_apply = self.manager.apply_system_override
        real_remove = self.manager.remove_system_override
        self.manager.apply_system_override = (
            lambda *a, **k: calls.append('file off') or real_apply(*a, **k))
        self.manager.remove_system_override = (
            lambda **k: calls.append('override gone') or real_remove(**k))
        self.app.adm.connection.config_set.side_effect = (
            lambda *a: calls.append('file on'))
        conf = mock.Mock(network_isolation=False, state_change_wait_time=5)
        conf.get.return_value = mock.Mock(docker_image='redis')
        with mock.patch.object(common_service, 'CONF', conf), \
                mock.patch.object(common_service.cfg,
                                  'get_configuration_property',
                                  return_value=[]), \
                mock.patch.object(common_service.operating_system,
                                  'ensure_directory'), \
                mock.patch.object(common_service.docker_util,
                                  'start_container',
                                  side_effect=lambda *a, **k:
                                  calls.append('container')), \
                mock.patch.object(common_service.RedisApp,
                                  'database_service_uid', 999), \
                mock.patch.object(common_service.RedisApp,
                                  'database_service_gid', 999):
            self.app.start_db(ds_version='7.2')
        self.assertEqual(['override gone', 'file off', 'container',
                          'healthy', 'file on', 'override gone'], calls)
        self.assertFalse(self._overridden())

    def _start_image(self, **kwargs):
        """The image start_db starts the container of."""
        self._info({})
        self.app.status = mock.Mock()
        self.app.status.wait_for_status.return_value = True
        conf = mock.Mock(network_isolation=False, state_change_wait_time=5)
        conf.get.return_value = mock.Mock(
            docker_image='harbor.tue.jp/cache-dockerhub/valkey/valkey')
        with mock.patch.object(common_service, 'CONF', conf), \
                mock.patch.object(common_service.cfg,
                                  'get_configuration_property',
                                  return_value=[]), \
                mock.patch.object(common_service.operating_system,
                                  'ensure_directory'), \
                mock.patch.object(common_service.docker_util,
                                  'start_container') as start, \
                mock.patch.object(common_service.RedisApp,
                                  'database_service_uid', 999), \
                mock.patch.object(common_service.RedisApp,
                                  'database_service_gid', 999):
            self.app.start_db(**kwargs)
        return start.call_args[0][1]

    def test_start_db_image(self):
        self.assertEqual('harbor.tue.jp/cache-dockerhub/valkey/valkey:8.1',
                         self._start_image(ds_version='8.1'))
        self.assertEqual('harbor.tue.jp/cache-dockerhub/valkey/valkey:latest',
                         self._start_image())
        # The image of the container it replaces, whatever the version.
        self.assertEqual(
            'harbor.tue.jp/cache-dockerhub/valkey/valkey:8.1',
            self._start_image(
                image='harbor.tue.jp/cache-dockerhub/valkey/valkey:8.1'))


class TestRedisReplicationPosition(trove_testtools.TestCase):
    """Where a server stands in its replication stream, for promote and
    eject.
    """

    def setUp(self):
        super(TestRedisReplicationPosition, self).setUp()
        patcher = mock.patch.object(common_service.RedisApp, '__init__',
                                    return_value=None)
        patcher.start()
        self.addCleanup(mock.patch.stopall)
        self.app = common_service.RedisApp()
        self.app.adm = mock.MagicMock()

    def _seen(self, *names):
        # INFO replication of a Valkey 9 primary and its replica with 1000
        # keys written, and of the replica once the primary was killed
        # (2026-10-20), as valkey-py parses it.
        here = os.path.join(os.path.dirname(__file__), '..', 'redis_common',
                            'replication')
        infos = []
        for name in names:
            with open(os.path.join(here, name + '.txt')) as f:
                infos.append(parsers.parse_info(f.read()))
        self.app.adm.connection.info.side_effect = infos

    def test_the_position_of_a_primary_and_a_replica(self):
        self._seen('primary', 'replica', 'replica-primary-gone')
        self.assertEqual((None, 31838), self.app.replication_position())
        self.assertEqual(('172.31.250.178', 31838),
                         self.app.replication_position())
        # Its primary gone, the replica still tells what it had.
        self.assertEqual(('172.31.250.178', 31838),
                         self.app.replication_position())

    def test_the_manager_tells_it_to_the_task_manager(self):
        mgr = common_manager.RedisManager.__new__(common_manager.RedisManager)
        mgr.app = self.app
        self._seen('replica', 'primary')
        self.assertEqual(['172.31.250.178', 31838],
                         mgr.get_last_txn(None))
        self.assertEqual(31838, mgr.get_latest_txn_id(None))

    def test_a_replica_waits_for_the_primarys_offset(self):
        positions = iter([('p', 100), ('p', 31000), ('p', 31838)])
        self.app.replication_position = lambda: next(positions)

        def poll(caught_up, sleep_time, time_out):
            while not caught_up():
                pass
        with mock.patch.object(common_service.utils, 'poll_until',
                               side_effect=poll):
            self.app.wait_for_offset(31838, 60)
        self.assertRaises(StopIteration, next, positions)

    def test_a_replica_that_does_not_catch_up(self):
        self.app.replication_position = lambda: ('p', 100)
        with mock.patch.object(common_service.utils, 'poll_until',
                               side_effect=exception.PollTimeOut):
            self.assertRaisesRegex(exception.TroveError, 'offset 31838',
                                   self.app.wait_for_offset, 31838, 60)


def _sentinel_state(name):
    """What the sentinel of 172.31.250.195 answered (SENTINEL MASTER,
    REPLICAS and SENTINELS of a Valkey 9 primary .194 with replicas .195
    and .196, 2026-10-20): all up; .196 killed; .196 back and the primary
    .194 killed, .196 elected. Parsed as valkey-py parses them.
    """
    import json
    here = os.path.join(os.path.dirname(__file__), '..', 'redis_common',
                        'sentinel')
    with open(os.path.join(here, name + '.json')) as f:
        raw = json.load(f)
    return {'master': parsers.parse_sentinel_master(raw['master']),
            'replicas': parsers.parse_sentinel_slaves_and_sentinels(
                raw['replicas']),
            'sentinels': parsers.parse_sentinel_slaves_and_sentinels(
                raw['sentinels'])}


class TestRedisSentinelMember(trove_testtools.TestCase):
    """A member of a cluster of the sentinel topology."""

    def setUp(self):
        super(TestRedisSentinelMember, self).setUp()
        patcher = mock.patch.object(common_service.RedisApp, '__init__',
                                    return_value=None)
        patcher.start()
        self.addCleanup(mock.patch.stopall)
        self.app = common_service.RedisApp()
        self.app.adm = mock.MagicMock()
        self.app.datadir = '/var/lib/redis'
        self.app.docker_client = mock.MagicMock()
        self.cm = mock.MagicMock()
        mock.patch.object(common_service.RedisApp, 'configuration_manager',
                          new_callable=mock.PropertyMock,
                          return_value=self.cm).start()
        mock.patch.object(common_service.RedisApp, 'get_node_ip',
                          return_value=['172.31.250.195', '6379']).start()
        for name in ('database_service_uid', 'database_service_gid'):
            mock.patch.object(common_service.RedisApp, name,
                              new_callable=mock.PropertyMock,
                              return_value='999').start()
        self.os = mock.patch.object(sentinel, 'operating_system').start()

    def test_the_sentinel_configuration(self):
        # As the containers of all three datastores took it (2026-10-20):
        # clients without a password ask where the primary is; the
        # sentinels log in to each other as trove-sentinel.
        text = sentinel.sentinel_config('10.0.0.2', '10.0.0.1', 2, 'adm',
                                        'sen', '/d/sentinel.sock')
        lines = text.splitlines()
        self.assertIn('sentinel monitor trove 10.0.0.1 6379 2', lines)
        self.assertIn('sentinel announce-ip 10.0.0.2', lines)
        self.assertIn('unixsocket /d/sentinel.sock', lines)
        # As the server's socket: the guest agent cannot connect to one
        # only the database user may (EACCES on production, 2026-10-20).
        self.assertIn('unixsocketperm 777', lines)
        self.assertIn('sentinel sentinel-user trove-sentinel', lines)
        self.assertIn('sentinel auth-user trove sentinel', lines)
        self.assertIn('sentinel auth-pass trove sen', lines)
        default = [ln for ln in lines if ln.startswith('user default ')][0]
        self.assertIn('-@all', default)
        self.assertIn('+sentinel|get-master-addr-by-name', default)
        for forbidden in ('+sentinel|set', '+sentinel|failover',
                          '+sentinel|remove', '+@all'):
            self.assertNotIn(forbidden + ' ', default + ' ')
        self.assertEqual(2, sentinel.quorum(['a', 'b', 'c']))
        self.assertEqual(3, sentinel.quorum(['a', 'b', 'c', 'd', 'e']))

    def _install(self, primary, mode='sync'):
        self.app.set_cluster_admin = mock.Mock()
        self.app.save_password = mock.Mock()
        self.app.restart_with_sentinel = mock.Mock()
        self.app.wait_for_primary_link = mock.Mock()
        self.app.install_sentinel({
            'primary': primary, 'mode': mode,
            'members': ['172.31.250.194', '172.31.250.195',
                        '172.31.250.196'],
            'admin_password': 'adm', 'sentinel_password': 'sen',
            'sentinel_admin_password': 'tsen'})

    def test_install_a_replica(self):
        self._install('172.31.250.194')
        self.app.set_cluster_admin.assert_called_once_with('adm')
        acl = self.app.adm.connection.acl_setuser.call_args[1]
        self.assertEqual(('sentinel', ['+sen'], ['*']),
                         (acl['username'], acl['passwords'],
                          acl['channels']))
        self.assertIn('+config|rewrite', acl['commands'])
        self.app.adm.connection.acl_save.assert_called_once_with()
        self.cm.apply_system_override.assert_called_once_with(
            {'replica-announce-ip': '172.31.250.195',
             'min-replicas-to-write': 1, 'min-replicas-max-lag': 10},
            'sentinel')
        written = self.os.write_file.call_args_list[0][0]
        self.assertEqual('/var/lib/redis/sentinel/sentinel.conf',
                         written[0])
        self.assertIn('sentinel monitor trove 172.31.250.194 6379 2',
                      written[1])
        self.app.restart_with_sentinel.assert_called_once_with()
        self.app.adm.connection.replicaof.assert_called_once_with(
            '172.31.250.194', 6379)
        self.app.wait_for_primary_link.assert_called_once_with()

    def test_install_the_primary_in_async_mode(self):
        self._install('172.31.250.195', mode='async')
        self.assertEqual(
            0, self.cm.apply_system_override.call_args[0][0][
                'min-replicas-to-write'])
        self.app.adm.connection.replicaof.assert_not_called()

    def _view(self, info, elected='172.31.250.195', mode='sync'):
        self.app.adm.connection.info.return_value = info
        self.app.sentinel_primary = mock.Mock(return_value=elected)
        self.app.sentinel_mode = mock.Mock(return_value=mode)
        return self.app.sentinel_member_view()

    def test_the_primary_takes_writes_as_its_sentinel_elects_it(self):
        primary = {'role': 'master', 'connected_slaves': 2}
        self.assertEqual(('master', 'master', True, False, False),
                         tuple(self._view(primary)))
        # Started again, a primary still, but the sentinels elected
        # another meanwhile: no writes from the endpoint.
        self.assertFalse(self._view(primary, elected='10.9.9.9').writable)
        # Its sentinel does not answer: no writes either.
        self.app.adm.connection.info.return_value = primary
        self.app.sentinel_primary = mock.Mock(side_effect=OSError('gone'))
        self.assertFalse(self.app.sentinel_member_view().writable)

    def test_a_replaced_primary_is_not_where_it_should_be(self):
        # The old primary started again after a power off, a primary
        # still, the sentinels having elected another (production,
        # 2026-10-20): abnormal until it is a replica, which it then tells.
        self.app.is_sentinel_member = mock.Mock(return_value=True)
        primary = {'role': 'master', 'connected_slaves': 0}
        view = self._view(primary, elected='172.31.250.194')
        signal = self.app.cluster_signal(view)
        self.assertTrue(signal.abnormal)
        self.assertEqual('abnormal', signal.kind)
        self.assertEqual('a primary the sentinels replaced', signal.reason)
        # A replica now: the probe's own word again.
        replica = {'role': 'slave', 'master_link_status': 'up',
                   'master_last_io_seconds_ago': 1}
        self.assertIsNone(self.app.cluster_signal(self._view(replica)))
        # The primary its sentinel elects, or one whose sentinel does not
        # answer: nothing of its own to say either.
        self.assertIsNone(self.app.cluster_signal(self._view(primary)))
        self.app.adm.connection.info.return_value = primary
        self.app.sentinel_primary = mock.Mock(side_effect=OSError('gone'))
        self.assertIsNone(self.app.cluster_signal(
            self.app.sentinel_member_view()))

    def test_a_node_of_the_cluster_topology_has_no_word_of_its_own(self):
        self.app.is_sentinel_member = mock.Mock(return_value=False)
        self.assertIsNone(self.app.cluster_signal(mock.Mock()))

    def test_the_sentinel_comes_beside_the_server_of_the_same_image(self):
        # As docker-py gives a container's attributes (docker inspect): the
        # image as it was named when the container was made.
        container = mock.Mock(attrs={'Config': {
            'Image': 'harbor.tue.jp/cache-dockerhub/valkey/valkey:8.1',
            'Cmd': ['valkey-server', '/etc/valkey/valkey.conf']}})
        self.app.docker_client.containers.get.return_value = container
        self.app.stop_db = mock.Mock()
        self.app.start_db = mock.Mock()
        self.app.sentinel_command = mock.Mock(return_value='/data/start.sh')
        self.app.restart_with_sentinel()
        container.remove.assert_called_once_with(force=True)
        self.app.start_db.assert_called_once_with(
            update_db=False, command='/data/start.sh',
            image='harbor.tue.jp/cache-dockerhub/valkey/valkey:8.1')

    def _sentinel_client(self):
        client = mock.MagicMock()
        self.app._sentinel = mock.Mock(return_value=client)
        return client

    def test_a_member_that_joins_is_given_what_the_others_keep(self):
        self._install('172.31.250.194')
        saved = {c[0][0]: c[0][1]
                 for c in self.app.save_password.call_args_list}
        self.assertEqual({'trove-sentinel': 'tsen', 'sentinel': 'sen'},
                         saved)
        self.app.get_cluster_admin_password = mock.Mock(return_value='adm')
        self.app.get_auth_password = mock.Mock(
            side_effect=lambda file: {'sentinel.cnf': 'sen',
                                      'trove-sentinel.cnf': 'tsen'}[file])
        self.app.sentinel_mode = mock.Mock(return_value='sync')
        self.assertEqual({'admin_password': 'adm', 'sentinel_password': 'sen',
                          'sentinel_admin_password': 'tsen', 'mode': 'sync'},
                         self.app.joining_config())

    def _hand_over(self, promoted, caught_up=True, state='online'):
        """The primary 172.31.250.195 hands over, its replicas' states
        and offsets as INFO REPLICATION gives them (valkey-py parses each
        slaveN line into a dict).
        """
        events = []
        conn = self.app.adm.connection
        conn.client_pause.side_effect = (
            lambda ms, all: events.append(('pause', all)))
        conn.client_unpause.side_effect = lambda: events.append('unpause')
        conn.replicaof.side_effect = (
            lambda ip, port: events.append(('replicaof', ip, port)))
        conn.info.return_value = {
            'role': 'master', 'master_repl_offset': 100,
            'slave0': {'ip': '172.31.250.194', 'port': 6379,
                       'state': 'online', 'offset': 100, 'lag': 0},
            'slave1': {'ip': '172.31.250.196', 'port': 6379, 'state': state,
                       'offset': 100 if caught_up else 90, 'lag': 0}}
        client = self._sentinel_client()
        client.sentinel_failover.side_effect = (
            lambda name: events.append(('failover', name)))
        # As SENTINEL REPLICAS answers: the promoted one reports master
        # while the sentinels still tell the old primary.
        answers = iter([[{'ip': '172.31.250.194',
                          'role-reported': 'slave'}]])
        client.sentinel_slaves.side_effect = lambda name: next(
            answers, [{'ip': '172.31.250.194', 'role-reported': promoted}])
        told = iter(['172.31.250.195', '172.31.250.195', '172.31.250.194'])
        self.app.sentinel_primary = mock.Mock(side_effect=lambda: next(told))

        def poll(f, **kw):
            for _ in range(3):
                value = f()
                if value:
                    return value
            raise sentinel.exception.PollTimeOut()
        # For the whole test: the caller calls sentinel_failover after.
        mock.patch.object(sentinel.utils, 'poll_until',
                          side_effect=poll).start()
        return events, self.app.sentinel_failover

    def test_the_primary_hands_over_without_losing_a_write(self):
        events, failover = self._hand_over('master')
        self.assertEqual('172.31.250.194', failover())
        # Writes held (not refused) until this member follows the new
        # primary: those held then get READONLY.
        self.assertEqual([('pause', False), ('failover', 'trove'),
                          ('replicaof', '172.31.250.194', 6379),
                          'unpause'], events)

    def test_no_failover_while_a_replica_is_behind(self):
        events, failover = self._hand_over('master', caught_up=False)
        self.assertRaisesRegex(sentinel.exception.TroveError, 'catch up',
                               failover)
        self.assertEqual([('pause', False), 'unpause'], events)

    def test_no_pause_while_a_replica_resynchronizes(self):
        # A replica just started again (a rolling restart): in a full
        # resynchronization, the sentinels may pick it and its
        # resynchronization undoes the promotion (Redis 8.10, 2026-10-20).
        events, failover = self._hand_over('master', state='send_bulk')
        self.assertRaisesRegex(sentinel.exception.TroveError, 'in sync',
                               failover)
        self.assertEqual([], events)

    def test_writes_go_on_when_no_replica_is_promoted(self):
        events, failover = self._hand_over('slave')
        self.assertRaisesRegex(sentinel.exception.TroveError, 'promote',
                               failover)
        self.assertEqual([('pause', False), ('failover', 'trove'),
                          'unpause'], events)

    def test_only_the_primary_hands_over(self):
        self.app.sentinel_primary = mock.Mock(return_value='172.31.250.194')
        self.assertRaisesRegex(sentinel.exception.TroveError,
                               'not the primary', self.app.sentinel_failover)
        self.app.adm.connection.client_pause.assert_not_called()

    def test_leave_quorum_and_reset(self):
        client = self._sentinel_client()
        self.app.sentinel_leave()
        self.app.adm.connection.config_set.assert_called_once_with(
            'replica-priority', 0)
        self.app.sentinel_set_quorum(2)
        client.sentinel_set.assert_called_once_with('trove', 'quorum', 2)
        states = iter([{'num-other-sentinels': 1, 'num-slaves': 1},
                       {'num-other-sentinels': 2, 'num-slaves': 2}])
        self.app.sentinel_state = mock.Mock(
            side_effect=lambda: {'master': next(states)})
        with mock.patch.object(sentinel.utils, 'poll_until',
                               side_effect=lambda f, **kw: [
                                   f() for _ in range(2)]):
            self.app.sentinel_reset(3)
        client.sentinel_reset.assert_called_once_with('trove')
        self.assertEqual(2, client.close.call_count)

    def test_sync_needs_a_replica(self):
        alone = {'role': 'master', 'connected_slaves': 0}
        self.assertFalse(self._view(alone).writable)
        self.assertTrue(self._view(alone, mode='async').writable)

    def test_a_replica_serves_reads_while_it_keeps_up(self):
        replica = {'role': 'slave', 'master_link_status': 'up',
                   'master_last_io_seconds_ago': 1}
        self.assertEqual(('replica', 'replica', False, False, True),
                         tuple(self._view(replica)))
        self.assertFalse(self._view(dict(
            replica, master_last_io_seconds_ago=30)).readable)
        self.assertFalse(self._view(dict(
            replica, master_link_status='down')).readable)

    def test_the_health_as_the_sentinel_sees_it(self):
        for name, health in (
                ('all-up', ch.view(ch.HEALTHY, me='172.31.250.195')),
                ('replica-down', ch.view(
                    ch.DEGRADED, ['172.31.250.196'],
                    'down as the sentinel sees it', '172.31.250.195')),
                # .196 elected; the old primary .194 held down.
                ('failed-over', ch.view(
                    ch.DEGRADED, ['172.31.250.194'],
                    'down as the sentinel sees it', '172.31.250.195'))):
            self.app.sentinel_state = mock.Mock(
                return_value=_sentinel_state(name))
            self.assertEqual(health, self.app.sentinel_health(), name)

    def test_no_primary_is_down(self):
        state = _sentinel_state('all-up')
        state['master']['is_odown'] = True
        self.app.sentinel_state = mock.Mock(return_value=state)
        self.assertEqual(ch.DOWN, self.app.sentinel_health()['status'])

    def test_sentinels_that_failed_to_log_in_are_dropped(self):
        # CLIENT LIST of a member whose sentinels connected before their
        # user was there (2026-10-20): they stay as default.
        self.app.adm.connection.client_list.return_value = [
            {'id': '7', 'user': 'default', 'cmd': 'publish'},
            {'id': '8', 'user': 'default', 'cmd': 'ping'},
            {'id': '9', 'user': 'os_admin', 'cmd': 'client|list'},
            {'id': '10', 'user': 'sentinel', 'cmd': 'publish'}]
        self.assertEqual(1, self.app.drop_failed_sentinel_links())
        self.app.adm.connection.client_kill_filter.assert_called_once_with(
            _id='7')

    def test_the_container_runs_the_sentinel_too(self):
        self.cm.has_system_override.return_value = True
        self.app.SERVER_BINARY = 'valkey-server'
        self.app.DATASTORE_NAME = 'valkey'
        self.assertEqual(['/bin/sh', '/etc/valkey/trove-start.sh'],
                         self.app.sentinel_command())
        self.app._write_start_script()
        script = self.os.write_file.call_args[0][1]
        self.assertIn('valkey-sentinel /var/lib/redis/sentinel/sentinel.conf'
                      ' &', script)
        self.assertIn('valkey-server /etc/redis/redis.conf &', script)


class TestRedisSentinelManager(trove_testtools.TestCase):

    def _manager(self, sentinel_member):
        mgr = common_manager.RedisManager.__new__(common_manager.RedisManager)
        mgr.app = mock.Mock()
        mgr.app.is_sentinel_member.return_value = sentinel_member
        mgr.app.get_node_ip.return_value = ['10.0.0.2', '6379']
        mgr._docker_client = mock.Mock()
        mgr.cluster_watch = None
        return mgr

    def test_a_sentinel_member_keeps_the_endpoint_ports(self):
        self.patch_datastore_manager('valkey')
        with mock.patch.object(common_manager.cluster_probe,
                               'ClusterEntry') as entry, \
                mock.patch.object(common_manager.cluster_member.cluster_probe,
                                  'ClusterWatch') as watch:
            self.assertIs(entry.return_value,
                          self._manager(True).new_cluster_watch())
            self.assertIs(watch.return_value,
                          self._manager(False).new_cluster_watch())

    def test_joining_restarts_the_watch_as_an_entry(self):
        mgr = self._manager(True)
        old = mock.Mock()
        mgr.cluster_watch = old
        with mock.patch.object(common_manager.cluster_probe,
                               'ClusterEntry') as entry:
            self.assertEqual({'ip': '10.0.0.2'},
                             mgr.sentinel_install(None, {'primary': 'x'}))
        mgr.app.install_sentinel.assert_called_once_with({'primary': 'x'})
        old.stop.assert_called_once_with()
        entry.return_value.start.assert_called_once_with()
        self.assertIs(entry.return_value, mgr.cluster_watch)

    def test_the_options_of_the_endpoints(self):
        for ds in ('redis', 'valkey', 'keydb'):
            conf = cfg.CONF.get(ds)
            self.assertEqual(
                (6380, 6381, 6379, 6380, {'sentinel_endpoint': 26379}, 3),
                (conf.cluster_ready_port, conf.cluster_read_port,
                 conf.cluster_load_balancer_port,
                 conf.cluster_read_load_balancer_port,
                 loadbalancer.extra_ports(conf),
                 conf.cluster_probe_interval), ds)
            for port in (16379, 26379, 6380, 6381):
                self.assertTrue(any(port in ports
                                    for ports in conf.cluster_tcp_ports),
                                '%s %s' % (ds, port))


class TestRedisPrepareTopology(trove_testtools.TestCase):
    """A member of a Redis Cluster runs in cluster mode; one of the
    sentinel topology does not (REPLICAOF is refused in cluster mode: the
    first sentinel cluster on 2026-10-20 failed so).
    """

    def _prepare(self, cluster_config):
        mgr = common_manager.RedisManager.__new__(common_manager.RedisManager)
        mgr.app = mock.Mock()
        mgr.app.ACL_FILE = '/var/lib/valkey/conf.d/users.acl'
        mgr.update_overrides = mock.Mock()
        with mock.patch.object(common_manager, 'operating_system') as os_:
            os_.exists.return_value = True
            mgr.do_prepare(None, None, None, 512, None, None, None, None,
                           'conf', None, None, cluster_config, None)
        return mgr.app

    def test_cluster_mode_for_a_redis_cluster_only(self):
        for cluster_config, enabled in (
                (None, False),
                ({'id': 'c1', 'instance_type': 'member'}, True),
                ({'id': 'c1', 'topology': 'cluster'}, True),
                ({'id': 'c1', 'topology': 'sentinel'}, False)):
            app = self._prepare(cluster_config)
            self.assertEqual(enabled, app.enable_cluster.called,
                             cluster_config)
            app.start_db.assert_called_once_with(ds_version=None,
                                                 memory_mb=512)
