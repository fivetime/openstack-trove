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

from unittest import mock

from oslo_utils import importutils

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
              'master_id': None, 'has_slots': True},
             {'id': 'r1', 'address': '172.31.79.12', 'role': 'replica',
              'master_id': 'm1', 'has_slots': False}], nodes)

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
