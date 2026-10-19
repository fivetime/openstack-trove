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
from trove.common import configurations
from trove.common import constants
from trove.common import exception
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.datastore.redis_common import manager as common_manager
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
