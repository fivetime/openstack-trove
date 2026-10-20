# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
from unittest import mock

from trove.cluster import models as cluster_models
from trove.cluster import views as cluster_views
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.common.strategies.cluster.experimental.redis import (
    api as redis_api)
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_api)
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_taskmanager)
from trove.common.strategies.cluster.experimental.redis import (
    taskmanager as redis_taskmanager)
from trove.tests.unittests import trove_testtools


class _DBInfo(object):
    """A cluster's row, keeping its properties as the real one does."""

    def __init__(self, properties=None, id='c1'):
        self.id = id
        self.name = 'c'
        self.properties = (json.dumps(properties) if properties else None)
        self.calls = []

    def get_properties(self):
        return json.loads(self.properties) if self.properties else {}

    def save_properties(self, properties):
        self.calls.append(('save_properties', dict(properties)))
        self.properties = json.dumps(properties)


class SentinelApiTest(trove_testtools.TestCase):

    def setUp(self):
        super(SentinelApiTest, self).setUp()
        self.addCleanup(mock.patch.stopall)
        self.db_info = _DBInfo()
        mock.patch.object(cluster_models.DBCluster, 'create',
                          return_value=self.db_info).start()
        mock.patch.object(common_api, 'validate_cluster_instances').start()
        self.created = mock.patch.object(
            common_api.HomogeneousCluster, '_create_instances').start()
        self.task_api = mock.patch.object(common_api.task_api,
                                          'load').start()
        self.task_api.return_value.create_cluster.side_effect = (
            lambda cid: self.db_info.calls.append(('create_cluster', cid)))

        def init(cluster, context, db_info, *args, **kwargs):
            cluster.db_info = db_info
        mock.patch.object(cluster_models.Cluster, '__init__',
                          new=init).start()
        ds_version = mock.Mock(manager='valkey', id='v1')
        self.ctx = mock.Mock(project_id='p1')
        self.args = (self.ctx, 'c', mock.Mock(), ds_version)

    def _create(self, props):
        instances = [{'flavor_id': 'f', 'volume_size': 2}] * 3
        return redis_api.CLUSTERS.create(*self.args, instances, props, None,
                                         None)

    def test_the_topology_and_the_mode_are_kept_before_the_build(self):
        self._create({'topology': 'sentinel'})
        # The task manager loads the cluster as soon as it is asked: by
        # then it is of the sentinel topology, in sync mode by default.
        self.assertEqual(
            [('save_properties', {'topology': 'sentinel',
                                  'replication_mode': 'sync'}),
             ('create_cluster', 'c1')], self.db_info.calls)
        member_config = sentinel_api.RedisSentinelCluster._member_config(
            self.db_info, {'replication_mode': 'async',
                           'topology': 'sentinel'})
        self.assertEqual('async', member_config['replication_mode'])
        # The member is not to run in cluster mode.
        self.assertEqual('sentinel', member_config['topology'])
        self.assertEqual(
            'sentinel', self.created.call_args[0][5]['topology'])

    def test_async_mode_and_a_mode_that_is_none(self):
        self._create({'topology': 'sentinel', 'replication_mode': 'async'})
        self.assertEqual('async',
                         self.db_info.get_properties()['replication_mode'])
        self.assertRaisesRegex(exception.BadRequest, 'replication_mode',
                               self._create, {'topology': 'sentinel',
                                              'replication_mode': 'quorum'})

    def test_a_redis_cluster_as_before(self):
        with mock.patch.object(redis_api.RedisCluster, 'create') as create:
            redis_api.CLUSTERS.create(*self.args, [], {'replicas_per_master':
                                                       '1'}, None, None)
        self.assertEqual({'replicas_per_master': '1'},
                         create.call_args[0][5])
        self.assertEqual([], self.db_info.calls)

    def test_the_strategy_dispatches(self):
        strategy = redis_api.RedisAPIStrategy()
        with mock.patch.object(redis_api.RedisCluster, '__init__',
                               return_value=None):
            self.assertIsInstance(
                strategy.cluster_class(self.ctx, _DBInfo()),
                redis_api.RedisCluster)
            self.assertIsInstance(
                strategy.cluster_class(self.ctx,
                                       _DBInfo({'topology': 'sentinel'})),
                sentinel_api.RedisSentinelCluster)
        self.assertIs(redis_taskmanager.CLUSTER_TASKS,
                      redis_taskmanager.RedisTaskManagerStrategy()
                      .task_manager_cluster_tasks_class)

    def test_no_grow_or_shrink_yet(self):
        cluster = sentinel_api.RedisSentinelCluster.__new__(
            sentinel_api.RedisSentinelCluster)
        self.assertRaises(exception.BadRequest, cluster.grow, [])
        self.assertRaises(exception.BadRequest, cluster.shrink, [])


class SentinelViewTest(trove_testtools.TestCase):

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(common_api.loadbalancer, 'OctaviaClient')
    @mock.patch.object(common_api.loadbalancer, 'find_endpoint')
    def test_the_endpoints_the_mode_and_the_name(self, find_endpoint,
                                                 client, data):
        find_endpoint.return_value = {'address': '10.0.0.50', 'port': 6379,
                                      'status': 'ONLINE'}
        cluster = mock.Mock(id='c1', db_info=_DBInfo(
            {'topology': 'sentinel', 'replication_mode': 'async'}))
        cluster.datastore_version.manager = 'valkey'
        view = redis_api.VIEWS(cluster, load_servers=True)
        self.assertIsInstance(view, sentinel_api.RedisSentinelClusterView)
        result = view.data()['cluster']
        self.assertEqual(
            {'endpoint': 6379, 'read_endpoint': 6380,
             'sentinel_endpoint': 26379},
            {k: result[k]['port'] for k in ('endpoint', 'read_endpoint',
                                            'sentinel_endpoint')})
        self.assertEqual(('sentinel', 'async', 'trove'),
                         (result['topology'], result['replication_mode'],
                          result['sentinel_master_name']))
        self.assertIsInstance(
            redis_api.MGMT_VIEWS(cluster, load_servers=False),
            sentinel_api.RedisSentinelMgmtClusterView)

    @mock.patch.object(common_api.loadbalancer, 'find_endpoint')
    def test_a_redis_cluster_has_no_load_balancer(self, find_endpoint):
        cluster = mock.Mock(id='c1', db_info=_DBInfo())
        cluster.datastore_version.manager = 'valkey'
        view = redis_api.VIEWS(cluster, load_servers=True)
        self.assertIsInstance(view, redis_api.RedisClusterView)
        self.assertEqual({}, view._endpoints())
        find_endpoint.assert_not_called()


class SentinelTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(SentinelTasksTest, self).setUp()
        self.addCleanup(mock.patch.stopall)
        self.tasks = sentinel_taskmanager.RedisSentinelClusterTasks.__new__(
            sentinel_taskmanager.RedisSentinelClusterTasks)
        self.tasks.db_info = _DBInfo({'topology': 'sentinel',
                                      'replication_mode': 'sync'})
        self.tasks.ds_version = mock.Mock(manager='valkey')
        self.calls = []
        members = [mock.Mock(id='i%d' % n) for n in (1, 2, 3)]
        mock.patch.object(sentinel_taskmanager.DBInstance, 'find_all'
                          ).start().return_value.all.return_value = members
        mock.patch.object(sentinel_taskmanager.Instance, 'load',
                          side_effect=lambda ctx, i: mock.Mock(id=i)).start()
        self.tasks._all_instances_ready = mock.Mock(return_value=True)
        self.guests = {}

        def guest(instance):
            g = mock.Mock()
            g.get_node_ip.return_value = ['10.0.0.%s' % instance.id[-1],
                                          '6379']
            g.sentinel_install.side_effect = (
                lambda config, _i=instance.id: self.calls.append(
                    ('install', _i, config)))
            g.cluster_complete.side_effect = (
                lambda _i=instance.id: self.calls.append(('complete', _i)))
            self.guests[instance.id] = g
            return g
        self.tasks.get_guest = guest
        self.tasks._load_balancer_enabled = mock.Mock(return_value=True)
        self.tasks._sync_load_balancer = mock.Mock(
            side_effect=lambda ctx, cid, instances: self.calls.append(
                ('lb', cid, [i.id for i in instances])))
        self.tasks.reset_task = mock.Mock(
            side_effect=lambda: self.calls.append(('reset',)))
        self.tasks.update_statuses_on_failure = mock.Mock(
            side_effect=lambda cid: self.calls.append(('failed', cid)))

    def test_the_primary_first_then_the_replicas_then_the_endpoint(self):
        self.tasks.create_cluster('ctx', 'c1')
        self.assertEqual(
            ['install i1', 'install i2', 'install i3', 'complete i1',
             'complete i2', 'complete i3', 'lb c1', 'reset'],
            [' '.join(c[:2]) for c in self.calls])
        config = self.calls[0][2]
        self.assertEqual(('10.0.0.1', ['10.0.0.1', '10.0.0.2', '10.0.0.3'],
                          'sync'),
                         (config['primary'], config['members'],
                          config['mode']))
        # Every member gets the same passwords, one for each purpose.
        passwords = [config[k] for k in ('admin_password',
                                         'sentinel_password',
                                         'sentinel_admin_password')]
        self.assertEqual(3, len(set(passwords)))
        for call in self.calls[1:3]:
            self.assertEqual(config, call[2])

    def test_a_cluster_without_its_endpoint_fails(self):
        self.tasks._sync_load_balancer.side_effect = Exception('octavia')
        self.tasks.create_cluster('ctx', 'c1')
        self.assertEqual(('failed', 'c1'), self.calls[-1])
        self.assertNotIn(('reset',), self.calls)

    def test_a_member_that_cannot_join_fails_the_cluster(self):
        def guest(instance):
            g = mock.Mock()
            g.get_node_ip.return_value = ['10.0.0.1', '6379']
            g.sentinel_install.side_effect = exception.GuestError(
                original_message='no')
            return g
        self.tasks.get_guest = guest
        self.tasks.create_cluster('ctx', 'c1')
        self.assertEqual([('failed', 'c1')], self.calls)
