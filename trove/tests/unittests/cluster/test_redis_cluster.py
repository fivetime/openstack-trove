# Copyright [2015] Hewlett-Packard Development Company, L.P.
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


from unittest.mock import Mock
from unittest.mock import patch
import uuid

from novaclient import exceptions as nova_exceptions

from trove.cluster.models import Cluster
from trove.cluster.models import ClusterTasks
from trove.cluster.models import DBCluster
from trove.common import clients
from trove.common import exception
from trove.common.strategies.cluster.experimental.redis import api as redis_api
from trove.instance import models as inst_models
from trove.instance.models import DBInstance
from trove.instance.models import InstanceTasks
from trove.quota.quota import QUOTAS
from trove.taskmanager import api as task_api
from trove.tests.unittests import trove_testtools


class FakeOptGroup(object):
    def __init__(self, cluster_member_count=3,
                 volume_support=True, device_path='/dev/vdb'):
        self.cluster_member_count = cluster_member_count
        self.volume_support = volume_support
        self.device_path = device_path


class ClusterTest(trove_testtools.TestCase):
    def setUp(self):
        super(ClusterTest, self).setUp()

        self.cluster_id = str(uuid.uuid4())
        self.cluster_name = "Cluster" + self.cluster_id
        self.tenant_id = "23423432"
        self.dv_id = "1"
        self.db_info = DBCluster(ClusterTasks.NONE,
                                 id=self.cluster_id,
                                 name=self.cluster_name,
                                 tenant_id=self.tenant_id,
                                 datastore_version_id=self.dv_id,
                                 task_id=ClusterTasks.NONE._code)

        self.get_client_patch = patch.object(task_api.API, 'get_client')
        self.get_client_mock = self.get_client_patch.start()
        self.addCleanup(self.get_client_patch.stop)
        self.dbcreate_patch = patch.object(DBCluster, 'create',
                                           return_value=self.db_info)
        self.dbcreate_mock = self.dbcreate_patch.start()
        self.addCleanup(self.dbcreate_patch.stop)

        self.context = trove_testtools.TroveTestContext(self)
        self.datastore = Mock()
        self.dv = Mock()
        self.dv.manager = "redis"
        self.datastore_version = self.dv
        self.cluster = redis_api.RedisCluster(self.context, self.db_info,
                                              self.datastore,
                                              self.datastore_version)
        self.cluster._server_group_loaded = True
        self.instances_w_volumes = [{'volume_size': 1,
                                     'flavor_id': '1234'}] * 3
        self.instances_no_volumes = [{'flavor_id': '1234'}] * 3

    def tearDown(self):
        super(ClusterTest, self).tearDown()

    @patch.object(clients, 'create_nova_client')
    def test_create_invalid_flavor_specified(self,
                                             mock_client):
        (mock_client.return_value.flavors.get) = Mock(
            side_effect=nova_exceptions.NotFound(
                404, "Flavor id not found %s" % id))

        self.assertRaises(exception.FlavorNotFound,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_w_volumes,
                          {}, None, None)

    @patch.object(clients, 'create_nova_client')
    @patch.object(redis_api, 'CONF')
    def test_create_volume_no_specified(self, mock_conf, mock_client):
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=True))
        self.assertRaises(exception.ClusterVolumeSizeRequired,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_no_volumes,
                          {}, None, None)

    @patch.object(clients, 'create_nova_client')
    @patch.object(redis_api, 'CONF')
    def test_create_storage_specified_with_no_volume_support(self,
                                                             mock_conf,
                                                             mock_client):
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=False))
        mock_client.return_value.flavors = Mock()
        self.assertRaises(exception.VolumeNotSupported,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_w_volumes,
                          {}, None, None)

    @patch.object(clients, 'create_nova_client')
    @patch.object(redis_api, 'CONF')
    def test_create_storage_not_specified_and_no_ephemeral_flavor(self,
                                                                  mock_conf,
                                                                  mock_client):
        class FakeFlavor(object):
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 0
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=False))
        (mock_client.return_value.
         flavors.get.return_value) = FakeFlavor('1234')
        self.assertRaises(exception.LocalStorageNotSpecified,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          self.instances_no_volumes,
                          {}, None, None)

    @patch.object(redis_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    def test_create(self, mock_client, mock_check_quotas, mock_task_api,
                    mock_ins_create, mock_conf):
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=True))
        mock_client.return_value.flavors = Mock()
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            self.instances_w_volumes, {}, None, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch.object(redis_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    def test_create_with_ephemeral_flavor(self, mock_client, mock_check_quotas,
                                          mock_task_api, mock_ins_create,
                                          mock_conf):
        class FakeFlavor(object):
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 1
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=False))
        (mock_client.return_value.
         flavors.get.return_value) = FakeFlavor('1234')
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            self.instances_no_volumes, {}, None, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch.object(DBCluster, 'update')
    @patch.object(redis_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    def test_grow(self, mock_client, mock_check_quotas, mock_task_api,
                  mock_ins_create, mock_conf, mock_update):
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=True))
        mock_client.return_value.flavors = Mock()
        self.cluster.grow(self.instances_w_volumes)
        mock_task_api.return_value.grow_cluster.assert_called_with(
            self.dbcreate_mock.return_value.id,
            [mock_ins_create.return_value.id] * 3)
        self.assertEqual(3, mock_ins_create.call_count)

    def _members(self, *ids):
        return [DBInstance(InstanceTasks.NONE, id=i, name=i,
                           compute_instance_id="compute-" + i,
                           task_id=InstanceTasks.NONE._code,
                           task_description=InstanceTasks.NONE._db_text,
                           volume_id="volume-" + i,
                           datastore_version_id="1",
                           cluster_id=self.cluster_id,
                           type="member") for i in ids]

    @patch.object(DBCluster, 'update')
    @patch.object(redis_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    @patch.object(DBInstance, 'find_all')
    def test_grow_names_new_members_after_the_existing(
            self, mock_find_all, mock_client, mock_check_quotas,
            mock_task_api, mock_ins_create, mock_conf, mock_update):
        # member-2 left the cluster: the next one is member-4, not one
        # of the names there are.
        mock_find_all.return_value.all.return_value = self._members(
            'c-member-1', 'c-member-3')
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=True))
        mock_client.return_value.flavors = Mock()
        self.cluster.grow([dict(i) for i in self.instances_w_volumes[:2]])
        self.assertEqual([self.cluster_name + '-member-4',
                          self.cluster_name + '-member-5'],
                         [c[0][1] for c in mock_ins_create.call_args_list])

    def test_replica_groups(self):
        self.assertEqual((0, 3), redis_api.replica_groups(3, None))
        self.assertEqual((1, 3), redis_api.replica_groups(6, '1'))
        self.assertEqual((2, 3), redis_api.replica_groups(9, 2))
        for count, replicas in ((5, 1),    # not whole groups
                                (4, 1),    # 2 masters cannot fail over
                                (3, -1), (3, 'x')):
            self.assertRaises(exception.BadRequest,
                              redis_api.replica_groups, count, replicas)
        # Growing a cluster of 3 masters by one group is fine.
        self.assertEqual((1, 1), redis_api.replica_groups(
            2, 1, existing_masters=3))

    @patch.object(redis_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    @patch.object(DBInstance, 'find_all')
    def test_create_instances_in_groups(self, mock_find_all, mock_client,
                                        mock_check_quotas, mock_ins_create,
                                        mock_conf):
        mock_find_all.return_value.all.return_value = []
        mock_conf.get = Mock(return_value=FakeOptGroup(volume_support=True))
        mock_client.return_value.flavors = Mock()
        db_info = Mock(id='cid')
        db_info.name = 'rc'
        redis_api.RedisCluster._create_instances(
            Mock(), db_info, Mock(), Mock(),
            [dict(i) for i in self.instances_w_volumes * 2], None, None,
            replicas_per_master=1)
        calls = mock_ins_create.call_args_list
        self.assertEqual(['rc-member-1', 'rc-member-1-replica-1',
                          'rc-member-2', 'rc-member-2-replica-1',
                          'rc-member-3', 'rc-member-3-replica-1'],
                         [c[0][1] for c in calls])
        configs = [c[1]['cluster_config'] for c in calls]
        self.assertEqual(['member', 'replica'] * 3,
                         [c['instance_type'] for c in configs])
        # A master and its replica share a shard_id, the groups do not.
        shards = [c['shard_id'] for c in configs]
        self.assertEqual(shards[0], shards[1])
        self.assertEqual(3, len(set(shards)))

    @patch.object(DBCluster, 'update')
    @patch.object(DBInstance, 'find_all')
    def test_grow_by_whole_groups(self, mock_find_all, mock_update):
        rows = self._members('m1', 'm2', 'm3', 'r1', 'r2', 'r3')
        for row in rows[3:]:
            row.type = 'replica'
        mock_find_all.return_value.all.return_value = rows
        self.assertRaises(exception.BadRequest, self.cluster.grow,
                          [dict(self.instances_w_volumes[0])])
        mock_update.assert_not_called()

    @patch.object(task_api, 'load')
    @patch.object(DBInstance, 'find_all')
    @patch.object(DBCluster, 'update')
    @patch.object(inst_models.Instance, 'delete')
    def test_shrink(self, mock_ins_delete, mock_update, mock_find_all,
                    mock_task_api):
        # The taskmanager moves the slots off and deletes the members;
        # the request itself only hands the work over.
        mock_find_all.return_value.all.return_value = self._members(
            'id1', 'id2', 'id3')
        self.cluster.shrink(['id1'])
        mock_task_api.return_value.shrink_cluster.assert_called_once_with(
            self.dbcreate_mock.return_value.id, ['id1'])
        mock_ins_delete.assert_not_called()

    @patch.object(task_api, 'load')
    @patch.object(DBInstance, 'find_all')
    @patch.object(DBCluster, 'update')
    def test_shrink_must_leave_a_member(self, mock_update, mock_find_all,
                                        mock_task_api):
        mock_find_all.return_value.all.return_value = self._members(
            'id1', 'id2')
        self.assertRaises(exception.ClusterShrinkMustNotLeaveClusterEmpty,
                          self.cluster.shrink, ['id1', 'id2'])
        mock_task_api.return_value.shrink_cluster.assert_not_called()

    @patch('trove.cluster.models.LOG')
    def test_delete_bad_task_status(self, mock_logging):
        self.cluster.db_info.task_status = ClusterTasks.BUILDING_INITIAL
        self.assertRaises(exception.UnprocessableEntity,
                          self.cluster.delete)

    @patch.object(task_api.API, 'delete_cluster')
    @patch.object(Cluster, 'update_db')
    @patch.object(inst_models.DBInstance, 'find_all')
    def test_delete_task_status_none(self,
                                     mock_find_all,
                                     mock_update_db,
                                     mock_delete_cluster):
        self.cluster.db_info.task_status = ClusterTasks.NONE
        self.cluster.delete()
        mock_update_db.assert_called_with(task_status=ClusterTasks.DELETING)

    @patch.object(task_api.API, 'delete_cluster')
    @patch.object(Cluster, 'update_db')
    @patch.object(inst_models.DBInstance, 'find_all')
    def test_delete_task_status_deleting(self,
                                         mock_find_all,
                                         mock_update_db,
                                         mock_delete_cluster):
        self.cluster.db_info.task_status = ClusterTasks.DELETING
        self.cluster.delete()
        mock_update_db.assert_called_with(task_status=ClusterTasks.DELETING)
