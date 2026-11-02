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

"""The placement of a cluster's members by group
(cluster_placement_per_shard): a server group for each set of members that
must not share a host. On the Incus migration matrix (2026-11-01) a soft
group of the whole cluster put a Redis master with its replica, and two of
a MongoDB replica set's three members, on one host.
"""

import json
from unittest import mock

from trove.cluster import models as cluster_models
from trove.common import exception
from trove.common import server_group as srv_grp
from trove.common.strategies.cluster.experimental.common import placement
from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)
from trove.common.strategies.cluster.experimental.mongodb import (
    api as mongodb_api)
from trove.common.strategies.cluster.experimental.redis import (
    api as redis_api)
from trove.common.strategies.cluster.experimental.redis import (
    taskmanager as redis_tm)
from trove.tests.unittests import trove_testtools


class FakeDBInfo(object):
    """A cluster record's properties, as DBCluster keeps them (JSON)."""

    def __init__(self, name='c', properties=None):
        self.id = 'cluster-id'
        self.name = name
        self.properties = json.dumps(properties) if properties else None

    def get_properties(self):
        return json.loads(self.properties) if self.properties else {}

    def save_properties(self, properties):
        self.properties = json.dumps(properties) if properties else None


def _limit(name, value):
    # Mock(name=...) names the mock; the attribute is set after.
    limit = mock.Mock(value=value)
    limit.name = name
    return limit


def _limits(allowed, used):
    return mock.Mock(absolute=[_limit('maxServerGroups', allowed),
                               _limit('totalServerGroupsUsed', used)])


class PlacementTest(trove_testtools.TestCase):

    def setUp(self):
        super(PlacementTest, self).setUp()
        self.patch_conf_property('cluster_placement_per_shard', True)
        self.nova = mock.patch.object(placement, 'create_nova_client').start()
        self.nova.return_value.limits.get.return_value = _limits(10, 0)
        self.made = []

        def create(context, policy, name):
            group = mock.Mock(id='group-%d' % len(self.made),
                              policy=policy)
            group.name = 'locality_' + name
            self.made.append(group)
            return group
        self.create = mock.patch.object(srv_grp.ServerGroup, 'create',
                                        side_effect=create).start()
        self.addCleanup(mock.patch.stopall)

    def test_wanted_only_when_on_and_the_class_asks(self):
        # A Redis family cluster of the topology cluster (the default)
        # and a MongoDB cluster ask; the sentinel topology and a cluster
        # of fewer members than hosts do not.
        self.assertTrue(placement.wanted(redis_api.CLUSTERS, {}))
        self.assertTrue(placement.wanted(redis_api.CLUSTERS,
                                         {'topology': 'cluster'}))
        self.assertFalse(placement.wanted(redis_api.CLUSTERS,
                                          {'topology': 'sentinel'}))
        self.assertTrue(placement.wanted(mongodb_api.MongoDbCluster, None))
        self.assertFalse(placement.wanted(
            galera_api.GaleraCommonCluster, {}))
        self.patch_conf_property('cluster_placement_per_shard', False)
        self.assertFalse(placement.wanted(redis_api.CLUSTERS, {}))
        self.assertFalse(placement.wanted(mongodb_api.MongoDbCluster, {}))

    def test_the_policy_of_the_groups(self):
        self.assertEqual('soft-anti-affinity', placement.ShardPlacement(
            'soft-anti-affinity').policy)
        self.patch_conf_property('cluster_placement_shard_policy', 'strict')
        shard = placement.ShardPlacement('soft-anti-affinity')
        self.assertEqual('anti-affinity', shard.policy)
        self.assertEqual('soft-anti-affinity', shard.locality)

    def test_start_keeps_the_placement_and_gives_no_hint(self):
        db_info = FakeDBInfo(properties={'topology': 'cluster'})
        hint = placement.start(mock.Mock(), db_info, placement.ShardPlacement(
            'anti-affinity'), 3)
        self.assertIsNone(hint)
        self.assertEqual({'topology': 'cluster',
                          placement.GROUPS: {},
                          placement.POLICY: 'anti-affinity',
                          placement.LOCALITY: 'anti-affinity'},
                         db_info.get_properties())
        self.assertTrue(placement.placed(db_info))
        self.assertEqual('anti-affinity', placement.locality(db_info))

    def test_start_of_a_cluster_with_its_one_group(self):
        db_info = FakeDBInfo()
        hint = {'group': 'the-group'}
        self.assertIs(hint, placement.start(mock.Mock(), db_info, hint, 3))
        self.assertIsNone(placement.start(mock.Mock(), db_info, None, 3))
        self.assertFalse(placement.placed(db_info))
        self.nova.assert_not_called()

    def test_start_refused_when_the_quota_is_short(self):
        self.nova.return_value.limits.get.return_value = _limits(10, 8)
        db_info = FakeDBInfo()
        self.assertRaises(exception.BadRequest, placement.start,
                          mock.Mock(), db_info,
                          placement.ShardPlacement('anti-affinity'), 3)
        self.assertFalse(placement.placed(db_info))
        self.create.assert_not_called()

    def test_an_unread_quota_does_not_refuse(self):
        self.nova.return_value.limits.get.side_effect = RuntimeError('nova')
        placement.check_quota(mock.Mock(), 100)
        self.nova.return_value.limits.get.side_effect = None
        self.nova.return_value.limits.get.return_value = _limits(-1, 50)
        placement.check_quota(mock.Mock(), 100)

    def test_a_group_for_each_key_made_once(self):
        db_info = FakeDBInfo(name='rc', properties={
            placement.GROUPS: {}, placement.POLICY: 'anti-affinity'})
        fallback = {'group': 'cluster-group'}
        shard_a = {'shard_id': 'aaaaaaaa-1', 'instance_type': 'member'}
        replica_a = {'shard_id': 'aaaaaaaa-1', 'instance_type': 'replica'}
        shard_b = {'shard_id': 'bbbbbbbb-2', 'instance_type': 'member'}
        config = {'instance_type': 'config_server'}
        hints = [placement.locality_for(mock.Mock(), db_info, c, fallback)
                 for c in (shard_a, replica_a, shard_b, config, config)]
        self.assertEqual([{'group': 'group-0'}, {'group': 'group-0'},
                          {'group': 'group-1'}, {'group': 'group-2'},
                          {'group': 'group-2'}], hints)
        self.assertEqual(3, self.create.call_count)
        self.assertEqual(['anti-affinity'] * 3,
                         [c.args[1] for c in self.create.call_args_list])
        self.assertEqual(['rc_aaaaaaaa', 'rc_bbbbbbbb', 'rc_config_s'],
                         [c.args[2] for c in self.create.call_args_list])
        self.assertEqual({'aaaaaaaa-1': 'group-0', 'bbbbbbbb-2': 'group-1',
                          'config_server': 'group-2'},
                         db_info.get_properties()[placement.GROUPS])

    def test_a_cluster_with_its_one_group_keeps_it(self):
        db_info = FakeDBInfo(properties={'topology': 'cluster'})
        fallback = {'group': 'cluster-group'}
        self.assertIs(fallback, placement.locality_for(
            mock.Mock(), db_info, {'shard_id': 's'}, fallback))
        self.create.assert_not_called()

    def test_delete_every_group(self):
        db_info = FakeDBInfo(properties={
            placement.GROUPS: {'a': 'group-a', 'b': 'group-b'}})
        delete = self.nova.return_value.server_groups.delete
        delete.side_effect = [RuntimeError('gone'), None]
        placement.delete(mock.Mock(), db_info)
        self.assertEqual([mock.call('group-a'), mock.call('group-b')],
                         delete.call_args_list)
        delete.reset_mock()
        placement.delete(mock.Mock(), FakeDBInfo())
        delete.assert_not_called()


class ClusterCreateTest(trove_testtools.TestCase):
    """The cluster's create: placed by group, it makes no group of the
    whole cluster and hands the strategy a ShardPlacement.
    """

    def setUp(self):
        super(ClusterCreateTest, self).setUp()
        self.strategy = mock.patch.object(
            cluster_models.strategy, 'load_api_strategy').start()
        self.cluster_class = mock.Mock(SHARD_PLACEMENT=True, spec=[
            'SHARD_PLACEMENT', 'create'])
        self.strategy.return_value.cluster_class = self.cluster_class
        self.hint = mock.patch.object(
            srv_grp.ServerGroup, 'build_scheduler_hint',
            return_value={'group': 'cluster-group'}).start()
        self.addCleanup(mock.patch.stopall)

    def _create(self, locality):
        cluster_models.Cluster.create(
            mock.Mock(), 'c', mock.Mock(), mock.Mock(manager='redis'), [],
            {}, locality, None)
        return self.cluster_class.create.call_args.args[6]

    def test_placed_by_group(self):
        self.patch_conf_property('cluster_placement_per_shard', True)
        locality = self._create('soft-anti-affinity')
        self.assertIsInstance(locality, placement.ShardPlacement)
        self.assertEqual('soft-anti-affinity', locality.locality)
        self.hint.assert_not_called()

    def test_one_group_when_off_or_without_a_locality(self):
        self.assertEqual({'group': 'cluster-group'},
                         self._create('anti-affinity'))
        self.patch_conf_property('cluster_placement_per_shard', True)
        self.assertEqual({'group': 'cluster-group'}, self._create(None))


class RedisClusterPlacementTest(trove_testtools.TestCase):
    """A Redis Cluster's members get the group of their master with its
    replicas.
    """

    def setUp(self):
        super(RedisClusterPlacementTest, self).setUp()
        self.patch_conf_property('cluster_placement_per_shard', True)
        self.patch_conf_property('cluster_placement_shard_policy', 'strict')
        self.db_info = FakeDBInfo(name='rc')
        nova = mock.patch.object(placement, 'create_nova_client').start()
        nova.return_value.limits.get.return_value = _limits(10, 0)
        self.groups = []

        def create(context, policy, name):
            self.groups.append((policy, name))
            return mock.Mock(id='group-%d' % len(self.groups))
        mock.patch.object(srv_grp.ServerGroup, 'create',
                          side_effect=create).start()
        for target, name in ((redis_api.models, 'validate_instance_flavors'),
                             (redis_api.models, 'get_required_volume_size'),
                             (redis_api.models, 'assert_homogeneous_cluster'),
                             (redis_api.models, 'validate_instance_nics'),
                             (redis_api, 'check_quotas')):
            mock.patch.object(target, name).start()
        mock.patch.object(redis_api.inst_models.DBInstance, 'find_all',
                          return_value=mock.Mock(
                              all=mock.Mock(return_value=[]))).start()
        self.create = mock.patch.object(redis_api.inst_models.Instance,
                                        'create').start()
        self.addCleanup(mock.patch.stopall)

    def test_each_master_with_its_replica_in_a_group(self):
        hint = placement.start(mock.Mock(), self.db_info,
                               placement.ShardPlacement('soft-anti-affinity'),
                               3)
        redis_api.RedisCluster._create_instances(
            mock.Mock(), self.db_info, mock.Mock(),
            mock.Mock(manager='redis'), [{'flavor_id': '1'}] * 6, {}, hint,
            replicas_per_master=1)
        calls = self.create.call_args_list
        configs = [c.kwargs['cluster_config'] for c in calls]
        hints = [c.kwargs['locality'] for c in calls]
        # Members come as master, replica, master, replica...
        self.assertEqual(['member', 'replica'] * 3,
                         [c['instance_type'] for c in configs])
        self.assertEqual([{'group': 'group-1'}] * 2 +
                         [{'group': 'group-2'}] * 2 +
                         [{'group': 'group-3'}] * 2, hints)
        self.assertEqual(['anti-affinity'] * 3, [g[0] for g in self.groups])
        for master, replica in zip(configs[::2], configs[1::2]):
            self.assertEqual(master['shard_id'], replica['shard_id'])


class RedisAddNodeTest(trove_testtools.TestCase):
    """A node in place of a lost one follows the master of its own group,
    the one its server group keeps it apart from.
    """

    def setUp(self):
        super(RedisAddNodeTest, self).setUp()
        self.tasks = redis_tm.RedisClusterTasks.__new__(
            redis_tm.RedisClusterTasks)
        self.node_ids = {'a-master': 'node-a', 'a-replica': 'node-a2',
                         'b-master': 'node-b'}
        mock.patch.object(redis_tm.Instance, 'load',
                          side_effect=lambda ctx, i: mock.Mock(id=i)).start()
        mock.patch.object(
            redis_tm.RedisClusterTasks, 'get_guest',
            mock.MagicMock(side_effect=lambda inst: mock.Mock(
                get_node_id=mock.Mock(
                    return_value=self.node_ids[inst.id])))).start()
        self.addCleanup(mock.patch.stopall)

    def _db(self, i, shard):
        return mock.Mock(id=i, shard_id=shard)

    def test_the_master_of_its_group(self):
        db_instances = [self._db('a-master', 'A'), self._db('b-master', 'B'),
                        self._db('a-new', 'A')]
        new = mock.Mock(db_info=mock.Mock(shard_id='A'))
        # node-b has fewer replicas, but node-a is the new node's master.
        self.assertEqual('node-a', self.tasks._master_of_group(
            db_instances, new, 'a-new', {'node-a': 1, 'node-b': 0}))

    def test_a_replica_promoted_is_its_group_s_master(self):
        db_instances = [self._db('a-master', 'A'), self._db('a-replica', 'A'),
                        self._db('a-new', 'A')]
        new = mock.Mock(db_info=mock.Mock(shard_id='A'))
        self.assertEqual('node-a2', self.tasks._master_of_group(
            db_instances, new, 'a-new', {'node-a2': 0, 'node-b': 0}))

    def test_none_without_a_master_of_its_group(self):
        db_instances = [self._db('b-master', 'B'), self._db('a-new', 'A')]
        new = mock.Mock(db_info=mock.Mock(shard_id='A'))
        self.assertIsNone(self.tasks._master_of_group(
            db_instances, new, 'a-new', {'node-b': 0}))
        new = mock.Mock(db_info=mock.Mock(shard_id=None))
        self.assertIsNone(self.tasks._master_of_group(
            db_instances, new, 'a-new', {'node-b': 0}))


class MongoDbClusterPlacementTest(trove_testtools.TestCase):
    """A MongoDB cluster's replica set, config servers and query routers
    each get a group.
    """

    def setUp(self):
        super(MongoDbClusterPlacementTest, self).setUp()
        self.patch_conf_property('cluster_placement_per_shard', True)
        self.db_info = FakeDBInfo(name='mc')
        mock.patch.object(mongodb_api.models.DBCluster, 'create',
                          return_value=self.db_info).start()
        nova = mock.patch.object(placement, 'create_nova_client').start()
        self.limits = nova.return_value.limits.get
        self.groups = []

        def create(context, policy, name):
            self.groups.append(name)
            return mock.Mock(id='group-%d' % len(self.groups))
        self.group_create = mock.patch.object(
            srv_grp.ServerGroup, 'create', side_effect=create).start()
        for target, name in (
                (mongodb_api.models, 'validate_instance_flavors'),
                (mongodb_api.models, 'get_required_volume_size'),
                (mongodb_api.models, 'assert_homogeneous_cluster'),
                (mongodb_api.models, 'validate_instance_nics'),
                (mongodb_api, 'check_quotas'),
                (mongodb_api, 'task_api')):
            mock.patch.object(target, name).start()
        # The cluster object loads its members: not wanted here.
        mock.patch.object(mongodb_api.MongoDbCluster, '__init__',
                          return_value=None).start()
        self.create = mock.patch.object(mongodb_api.inst_models.Instance,
                                        'create').start()
        self.addCleanup(mock.patch.stopall)

    def _create(self, used=0):
        self.limits.return_value = _limits(10, used)
        mongodb_api.MongoDbCluster.create(
            mock.Mock(), 'mc', mock.Mock(), mock.Mock(manager='mongodb'),
            [{'flavor_id': '1', 'volume_size': 1}] * 3,
            {'num_configsvr': 3, 'num_mongos': 1}, placement.ShardPlacement(
                'soft-anti-affinity'), None)

    def test_each_set_of_members_in_a_group(self):
        self._create()
        calls = self.create.call_args_list
        kinds = [c.kwargs['cluster_config']['instance_type'] for c in calls]
        hints = [c.kwargs['locality'] for c in calls]
        self.assertEqual(['member'] * 3 + ['config_server'] * 3 +
                         ['query_router'], kinds)
        self.assertEqual([{'group': 'group-1'}] * 3 +
                         [{'group': 'group-2'}] * 3 +
                         [{'group': 'group-3'}], hints)
        self.assertEqual(3, len(self.groups))

    def test_refused_when_the_quota_is_short(self):
        self.assertRaises(exception.BadRequest, self._create, used=8)
        self.create.assert_not_called()
        self.group_create.assert_not_called()
