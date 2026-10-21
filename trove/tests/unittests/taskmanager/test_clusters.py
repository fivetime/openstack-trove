# Copyright 2014 eBay Software Foundation
# All Rights Reserved.
#
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

import datetime

from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

from trove.cluster.models import ClusterTasks as ClusterTaskStatus
from trove.cluster.models import DBCluster
from trove.common.strategies.cluster.experimental.mongodb.taskmanager import (
    MongoDbClusterTasks as ClusterTasks)
from trove.common import utils
from trove.datastore import models as datastore_models
from trove.instance.models import BaseInstance
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.instance.models import InstanceServiceStatus
from trove.instance.models import InstanceTasks
from trove.instance.service_status import ServiceStatuses
from trove.tests.unittests import trove_testtools


class CassandraClusterTasksTest(trove_testtools.TestCase):
    def setUp(self):
        super(CassandraClusterTasksTest, self).setUp()
        self.cluster_id = "1234"
        self.cluster_name = "test1"
        self.tenant_id = "2345"
        self.db_cluster = DBCluster(ClusterTaskStatus.NONE,
                                    id=self.cluster_id,
                                    created=str(datetime.date),
                                    updated=str(datetime.date),
                                    name=self.cluster_name,
                                    task_id=ClusterTaskStatus.NONE._code,
                                    tenant_id=self.tenant_id,
                                    datastore_version_id="1",
                                    deleted=False)
        self.dbinst1 = DBInstance(InstanceTasks.NONE, id="1", name="member1",
                                  compute_instance_id="compute-1",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-1",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="member")
        self.dbinst2 = DBInstance(InstanceTasks.NONE, id="2", name="member2",
                                  compute_instance_id="compute-2",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-2",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="member")
        mock_ds1 = Mock()
        mock_ds1.name = 'cassandra'
        mock_dv1 = Mock()
        mock_dv1.name = '4.0.0'
        self.clustertasks = ClusterTasks(Mock(),
                                         self.db_cluster,
                                         datastore=mock_ds1,
                                         datastore_version=mock_dv1)

    @patch.object(DBInstance, 'find_by')
    @patch.object(InstanceServiceStatus, 'find_by')
    def test_all_instances_healthy(self, mock_find, mock_db_find):
        (mock_find.return_value.
         get_status.return_value) = ServiceStatuses.HEALTHY
        (mock_db_find.return_value.
         get_task_status.return_value) = InstanceTasks.NONE
        ret_val = self.clustertasks._all_instances_healthy(["1", "2"],
                                                           self.cluster_id)
        self.assertTrue(ret_val)


class MongoDbClusterTasksTest(trove_testtools.TestCase):
    def setUp(self):
        super(MongoDbClusterTasksTest, self).setUp()
        self.cluster_id = "1232"
        self.cluster_name = "Cluster-1234"
        self.tenant_id = "6789"
        self.db_cluster = DBCluster(ClusterTaskStatus.NONE,
                                    id=self.cluster_id,
                                    created=str(datetime.date),
                                    updated=str(datetime.date),
                                    name=self.cluster_name,
                                    task_id=ClusterTaskStatus.NONE._code,
                                    tenant_id=self.tenant_id,
                                    datastore_version_id="1",
                                    deleted=False)
        self.dbinst1 = DBInstance(InstanceTasks.NONE, id="1", name="member1",
                                  compute_instance_id="compute-1",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-1",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="member")
        self.dbinst2 = DBInstance(InstanceTasks.NONE, id="2", name="member2",
                                  compute_instance_id="compute-2",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-2",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="member")
        self.dbinst3 = DBInstance(InstanceTasks.NONE, id="3", name="mongos",
                                  compute_instance_id="compute-3",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-3",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="query_router")
        self.dbinst4 = DBInstance(InstanceTasks.NONE, id="4",
                                  name="configserver",
                                  compute_instance_id="compute-4",
                                  task_id=InstanceTasks.NONE._code,
                                  task_description=InstanceTasks.NONE._db_text,
                                  volume_id="volume-4",
                                  datastore_version_id="1",
                                  cluster_id=self.cluster_id,
                                  shard_id="shard-1",
                                  type="config_server")
        mock_ds1 = Mock()
        mock_ds1.name = 'mongodb'
        mock_dv1 = Mock()
        mock_dv1.name = '2.0.4'
        self.clustertasks = ClusterTasks(Mock(),
                                         self.db_cluster,
                                         datastore=mock_ds1,
                                         datastore_version=mock_dv1)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(DBInstance, 'find_by')
    @patch.object(InstanceServiceStatus, 'find_by')
    @patch('trove.taskmanager.models.LOG')
    def test_all_instances_ready_with_server_error(self,
                                                   mock_logging, mock_find,
                                                   mock_db_find, mock_update):
        (mock_find.return_value.
         get_status.return_value) = ServiceStatuses.NEW
        (mock_db_find.return_value.
         get_task_status.return_value) = InstanceTasks.BUILDING_ERROR_SERVER
        ret_val = self.clustertasks._all_instances_ready(["1", "2", "3", "4"],
                                                         self.cluster_id)
        mock_update.assert_called_with(
            self.cluster_id, None, instance_ids=["1", "2", "3", "4"])
        self.assertFalse(ret_val)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(DBInstance, 'find_by')
    @patch.object(InstanceServiceStatus, 'find_by')
    @patch('trove.taskmanager.models.LOG')
    @patch('trove.common.utils.poll_until')
    def test_all_instances_ready_with_any_build_error(
            self, mock_poll, mock_logging, mock_find, mock_db_find,
            mock_update):
        # An instance whose security group, port or volume could not be
        # created has no server and never reports; waiting for it used to
        # end only with the timeout.
        mock_poll.side_effect = (
            lambda retriever, condition, **kw: self.assertTrue(
                condition(retriever())))
        (mock_find.return_value.
         get_status.return_value) = ServiceStatuses.NEW
        for task in (InstanceTasks.BUILDING_ERROR_SEC_GROUP,
                     InstanceTasks.BUILDING_ERROR_PORT,
                     InstanceTasks.BUILDING_ERROR_VOLUME,
                     InstanceTasks.BUILDING_ERROR_DNS):
            mock_update.reset_mock()
            (mock_db_find.return_value.
             get_task_status.return_value) = task
            ret_val = self.clustertasks._all_instances_ready(
                ["1", "2", "3", "4"], self.cluster_id)
            mock_update.assert_called_with(
                self.cluster_id, None, instance_ids=["1", "2", "3", "4"])
            self.assertFalse(ret_val)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(DBInstance, 'find_by')
    @patch.object(InstanceServiceStatus, 'find_by')
    @patch('trove.taskmanager.models.LOG')
    def test_all_instances_ready_bad_status(self, mock_logging,
                                            mock_find, mock_db_find,
                                            mock_update):
        (mock_find.return_value.
         get_status.return_value) = ServiceStatuses.FAILED
        (mock_db_find.return_value.
         get_task_status.return_value) = InstanceTasks.NONE
        ret_val = self.clustertasks._all_instances_ready(["1", "2", "3", "4"],
                                                         self.cluster_id)
        mock_update.assert_called_with(
            self.cluster_id, None, instance_ids=["1", "2", "3", "4"])
        self.assertFalse(ret_val)

    @patch.object(DBInstance, 'find_by')
    @patch.object(InstanceServiceStatus, 'find_by')
    def test_all_instances_ready(self, mock_find, mock_db_find):
        (mock_find.return_value.
         get_status.return_value) = ServiceStatuses.INSTANCE_READY
        (mock_db_find.return_value.
         get_task_status.return_value) = InstanceTasks.NONE
        ret_val = self.clustertasks._all_instances_ready(["1", "2", "3", "4"],
                                                         self.cluster_id)
        self.assertTrue(ret_val)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    @patch(
        'trove.common.strategies.cluster.experimental.mongodb.taskmanager.LOG')
    def test_init_replica_set_failure(self, mock_logging, mock_dv, mock_ds,
                                      mock_ip, mock_guest,
                                      mock_update):
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        primary_member = member1
        other_members = [member2]
        mock_ip.side_effect = ["10.0.0.3", "10.0.0.2"]
        mock_guest().prep_primary.return_value = Mock()
        mock_guest().add_members.return_value = Mock()

        mock_guest.return_value.add_members = Mock(
            side_effect=Exception("Boom!"))

        ret_val = self.clustertasks._init_replica_set(primary_member,
                                                      other_members)

        mock_update.assert_called_with(self.cluster_id, shard_id='shard-1')
        self.assertFalse(ret_val)

    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_init_replica_set(self, mock_dv, mock_ds,
                              mock_ip, mock_guest):
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        primary_member = member1
        other_members = [member2]
        mock_ip.side_effect = ["10.0.0.3", "10.0.0.2"]
        mock_guest().prep_primary.return_value = Mock()
        mock_guest().add_members.return_value = Mock()
        mock_guest().get_admin_password.return_value = "pwd"

        ret_val = self.clustertasks._init_replica_set(primary_member,
                                                      other_members)
        # The primary is told the address the others reach it at.
        mock_guest.return_value.prep_primary.assert_called_with("10.0.0.2")
        mock_guest.return_value.add_members.assert_called_with(
            ["10.0.0.3"]
        )
        # The other members took the users of the primary with its data.
        mock_guest.return_value.store_admin_password.assert_called_with(
            "pwd"
        )
        self.assertTrue(ret_val)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, '_init_replica_set')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    @patch(
        'trove.common.strategies.cluster.experimental.mongodb.taskmanager.LOG')
    def test_create_shard_failure(self, mock_logging, mock_dv, mock_ds,
                                  mock_ip, mock_guest, mock_init_rs,
                                  mock_update):
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        members = [member1, member2]
        mock_ip.side_effect = ["10.0.0.2"]

        query_router = [
            BaseInstance(Mock(), self.dbinst3, Mock(),
                         InstanceServiceStatus(ServiceStatuses.NEW))
        ]
        mock_guest().get_replica_set_name.return_value = 'testrs'
        mock_add_shard = Mock(side_effect=Exception("Boom!"))
        mock_guest().add_shard = mock_add_shard

        ret_val = self.clustertasks._create_shard(query_router, members)

        mock_init_rs.assert_called_with(member1, [member2])
        mock_update.assert_called_with(self.cluster_id, shard_id="shard-1")
        self.assertFalse(ret_val)

    @patch.object(ClusterTasks, '_init_replica_set')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_create_shard(self, mock_dv, mock_ds,
                          mock_ip, mock_guest, mock_init_rs):
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        members = [member1, member2]
        mock_ip.side_effect = ["10.0.0.2"]

        query_router = [
            BaseInstance(Mock(), self.dbinst3, Mock(),
                         InstanceServiceStatus(ServiceStatuses.NEW))
        ]
        mock_guest().get_replica_set_name.return_value = 'testrs'
        mock_add_shard = Mock()
        mock_guest().add_shard = mock_add_shard

        ret_val = self.clustertasks._create_shard(query_router, members)

        mock_init_rs.assert_called_with(member1, [member2])
        mock_add_shard.assert_called_with("testrs", "10.0.0.2")
        self.assertTrue(ret_val)

    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, '_create_shard')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    @patch.object(Instance, 'load')
    @patch.object(ClusterTasks, '_all_instances_ready')
    @patch.object(DBInstance, 'find_all')
    def test_add_shard_cluster(self, mock_find_all,
                               mock_all_instances_ready,
                               mock_load,
                               mock_dv,
                               mock_ds,
                               mock_add_shard,
                               mock_guest,
                               mock_reset_task):
        mock_find_all.return_value.all.return_value = [self.dbinst1,
                                                       self.dbinst2,
                                                       self.dbinst3,
                                                       self.dbinst4]
        mock_load.return_value = BaseInstance(Mock(),
                                              self.dbinst1, Mock(),
                                              InstanceServiceStatus(
                                                  ServiceStatuses.NEW))
        mock_all_instances_ready.return_value = True
        mock_add_shard.return_value = True
        mock_guest.return_value.cluster_complete.return_value = Mock()
        self.clustertasks.add_shard_cluster(Mock(),
                                            self.cluster_id,
                                            "shard-1", "rs1")
        mock_guest.return_value.cluster_complete.assert_called_with()
        mock_reset_task.assert_called_with()

    def _member(self, instance_id, name):
        return DBInstance(InstanceTasks.NONE, id=instance_id, name=name,
                          compute_instance_id="compute-%s" % instance_id,
                          task_id=InstanceTasks.NONE._code,
                          task_description=InstanceTasks.NONE._db_text,
                          volume_id="volume-%s" % instance_id,
                          datastore_version_id="1",
                          cluster_id=self.cluster_id,
                          shard_id="shard-1", type="member")

    def _guests(self, mock_guest, roles):
        guests = {}

        def guest(instance):
            g = guests.setdefault(instance.id, Mock())
            g.get_member_role.return_value = roles.get(instance.id)
            return g
        mock_guest.side_effect = guest
        return guests

    @patch('trove.common.strategies.cluster.experimental.mongodb.'
           'taskmanager.utils.poll_until')
    @patch('trove.common.strategies.cluster.experimental.mongodb.'
           'taskmanager.load_any_instance')
    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(Instance, 'delete')
    @patch.object(Instance, 'load')
    @patch.object(DBInstance, 'find_by')
    @patch.object(DBInstance, 'find_all')
    def test_remove_member(self, mock_find_all, mock_find_by, mock_load,
                           mock_delete, mock_guest, mock_reset_task,
                           mock_update, mock_load_any, mock_poll):
        # The repair: a member of the shard lost for good is dropped from
        # the set by its primary, asked among the members that stay, and
        # deleted (without its server: loaded as any instance is).
        lost = self._member("5", "member3")
        mock_find_by.return_value = lost
        mock_find_all.return_value.all.return_value = [
            self.dbinst1, self.dbinst2, lost]
        mock_load.side_effect = lambda ctx, i: Mock(id=i)
        guests = self._guests(mock_guest, {'1': {'role': 'SECONDARY'},
                                           '2': {'role': 'PRIMARY'}})
        self.clustertasks.remove_member(Mock(), self.cluster_id, "5",
                                        "10.0.0.15:27017")
        guests['2'].remove_members.assert_called_once_with(
            ['10.0.0.15:27017'])
        self.assertNotIn('5', guests)
        mock_load_any.assert_called_once_with(ANY, "5")
        mock_delete.assert_called_once_with(mock_load_any.return_value)
        mock_reset_task.assert_called_with()
        mock_update.assert_not_called()

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(Instance, 'delete')
    @patch.object(Instance, 'load')
    @patch.object(DBInstance, 'find_by')
    @patch.object(DBInstance, 'find_all')
    def test_remove_member_without_a_primary(self, mock_find_all,
                                             mock_find_by, mock_load,
                                             mock_delete, mock_guest,
                                             mock_reset_task, mock_update):
        # Nothing is dropped or deleted; the lost member alone is marked.
        lost = self._member("5", "member3")
        mock_find_by.return_value = lost
        mock_find_all.return_value.all.return_value = [
            self.dbinst1, self.dbinst2, lost]
        mock_load.side_effect = lambda ctx, i: Mock(id=i)
        guests = self._guests(mock_guest, {'1': {'role': 'SECONDARY'},
                                           '2': {'role': 'SECONDARY'}})
        self.clustertasks.remove_member(Mock(), self.cluster_id, "5",
                                        "10.0.0.15:27017")
        for g in guests.values():
            g.remove_members.assert_not_called()
        mock_delete.assert_not_called()
        mock_update.assert_called_once_with(
            self.cluster_id, status=InstanceTasks.SHRINKING_ERROR,
            instance_ids=["5"])

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(ClusterTasks, '_all_instances_ready')
    @patch.object(Instance, 'load')
    @patch.object(DBInstance, 'find_by')
    @patch.object(DBInstance, 'find_all')
    def test_add_member(self, mock_find_all, mock_find_by, mock_load,
                        mock_ready, mock_get_ip, mock_guest,
                        mock_reset_task, mock_update):
        # The repair: the member in place of a lost one, once ready, is
        # added by the set's primary, given its admin password, complete.
        new = self._member("6", "member-new")
        mock_find_by.return_value = new
        mock_find_all.return_value.all.return_value = [
            self.dbinst1, self.dbinst2, new]
        mock_load.side_effect = lambda ctx, i: Mock(id=i)
        mock_ready.return_value = True
        mock_get_ip.return_value = '10.0.0.16'
        guests = self._guests(mock_guest, {'1': {'role': 'SECONDARY'},
                                           '2': {'role': 'PRIMARY'}})
        guests.setdefault('6', Mock())
        guests.setdefault('2', Mock()).get_admin_password.return_value = 'pw'
        self.clustertasks.add_member(Mock(), self.cluster_id, "6")
        mock_ready.assert_called_once_with(["6"], self.cluster_id, "shard-1")
        guests['2'].add_members.assert_called_once_with(['10.0.0.16'])
        guests['6'].store_admin_password.assert_called_once_with('pw')
        guests['6'].cluster_complete.assert_called_once_with()
        mock_reset_task.assert_called_with()
        mock_update.assert_not_called()

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, '_all_instances_ready')
    @patch.object(DBInstance, 'find_by')
    def test_add_member_that_is_not_ready(self, mock_find_by, mock_ready,
                                          mock_guest, mock_reset_task,
                                          mock_update):
        mock_find_by.return_value = self._member("6", "member-new")
        mock_ready.return_value = False
        self.clustertasks.add_member(Mock(), self.cluster_id, "6")
        mock_guest.assert_not_called()
        mock_reset_task.assert_called_with()

    @patch.object(DBCluster, 'save')
    @patch.object(DBCluster, 'find_by')
    @patch.object(DBInstance, 'find_all')
    def test_delete_cluster(self, mock_find_all, mock_find_by, mock_save):
        mock_find_all.return_value.all.return_value = []
        mock_find_by.return_value = self.db_cluster
        self.clustertasks.delete_cluster(Mock(), self.cluster_id)
        self.assertEqual(ClusterTaskStatus.NONE, self.db_cluster.task_status)
        mock_save.assert_called_with()

    def test_rolling_upgrade_cluster_without_order_specified(self):
        self._assert_rolling_upgrade_cluster(None, None)

    def test_rolling_upgrade_cluster_with_order_specified(self):
        ordering = {
            1: 1,
            2: 2,
            3: 3,
            4: 4,
            5: 5
        }

        def ordering_function(instance):
            return ordering[instance.id]

        self._assert_rolling_upgrade_cluster(ordering_function, ordering)

    @patch('trove.taskmanager.models.DBaaSInstanceUpgrade')
    @patch('trove.taskmanager.models.BuiltInstanceTasks')
    @patch('trove.taskmanager.models.EndNotification')
    @patch('trove.taskmanager.models.StartNotification')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(DBInstance, 'find_all')
    def test_rolling_upgrade_cluster_before_each_member(
            self, mock_find_all, mock_reset_task, mock_start, mock_end,
            mock_instance_task, mock_upgrade):
        events = []
        db_instances = [Mock(id=i) for i in (1, 2)]
        mock_find_all.return_value.all.return_value = db_instances

        def load(_, instance_id):
            instance = Mock(id=instance_id)
            instance.upgrade.side_effect = (
                lambda _v: events.append(('upgrade', instance_id)))
            return instance
        mock_instance_task.load.side_effect = load
        self.clustertasks.rolling_upgrade_cluster(
            MagicMock(), Mock(), Mock(),
            before_member=lambda i: events.append(('before', i.id)))
        self.assertEqual([('before', 1), ('upgrade', 1), ('before', 2),
                          ('upgrade', 2)], events)

    @patch('trove.taskmanager.models.DBaaSInstanceRestart')
    @patch('trove.taskmanager.models.BuiltInstanceTasks')
    @patch('trove.taskmanager.models.EndNotification')
    @patch('trove.taskmanager.models.StartNotification')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(DBInstance, 'find_all')
    def test_rolling_restart_cluster_before_each_member(
            self, mock_find_all, mock_reset_task, mock_start, mock_end,
            mock_instance_task, mock_restart):
        events = []
        mock_find_all.return_value.all.return_value = [Mock(id=2),
                                                       Mock(id=1)]

        def load(_, instance_id):
            instance = Mock(id=instance_id)
            instance.restart.side_effect = (
                lambda: events.append(('restart', instance_id)))
            return instance
        mock_instance_task.load.side_effect = load
        with patch('trove.taskmanager.models.utils.poll_until'):
            self.clustertasks.rolling_restart_cluster(
                MagicMock(), Mock(), ordering_function=lambda i: i.id,
                before_member=lambda i: events.append(('before', i.id)))
        self.assertEqual([('before', 1), ('restart', 1), ('before', 2),
                          ('restart', 2)], events)

    @patch('trove.taskmanager.models.DBaaSInstanceUpgrade')
    @patch('trove.taskmanager.models.BuiltInstanceTasks')
    @patch('trove.taskmanager.models.EndNotification')
    @patch('trove.taskmanager.models.StartNotification')
    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(DBInstance, 'find_all')
    def _assert_rolling_upgrade_cluster(self,
                                        ordering_function,
                                        ordering,
                                        mock_find_all,
                                        mock_reset_task,
                                        mock_start,
                                        mock_end,
                                        mock_instance_task,
                                        mock_upgrade):
        class MockInstance(Mock):
            upgrade_counter = 0

            def upgrade(self, _):
                MockInstance.upgrade_counter += 1
                self.upgrade_number = MockInstance.upgrade_counter

        db_instances = [Mock() for _ in range(5)]
        for i in range(5):
            db_instances[i].id = i + 1

        mock_find_all.return_value.all.return_value = db_instances
        instances = []

        def load_side_effect(_, instance_id):
            return_value = MockInstance()
            return_value.id = instance_id
            instances.append(return_value)
            return return_value

        mock_instance_task.load.side_effect = load_side_effect
        if ordering is None:
            ordering = {
                1: 1,
                2: 2,
                3: 3,
                4: 4,
                5: 5
            }
        self.clustertasks.rolling_upgrade_cluster(MagicMock(),
                                                  Mock(),
                                                  Mock(),
                                                  ordering_function)
        order_result = {inst.id: inst.upgrade_number for inst in instances}

        self.assertEqual(ClusterTaskStatus.NONE, self.db_cluster.task_status)
        self.assertDictEqual(ordering, order_result)

    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, '_create_shard')
    @patch.object(ClusterTasks, '_init_replica_set')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(Instance, 'load')
    @patch.object(ClusterTasks, '_all_instances_ready')
    @patch.object(DBInstance, 'find_all')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_create_cluster(self,
                            mock_dv,
                            mock_ds,
                            mock_find_all,
                            mock_all_instances_ready,
                            mock_load,
                            mock_ip,
                            mock_guest,
                            mock_init_replica_set,
                            mock_create_shard,
                            mock_reset_task):
        mock_find_all.return_value.all.return_value = [self.dbinst1,
                                                       self.dbinst2,
                                                       self.dbinst3,
                                                       self.dbinst4]
        mock_all_instances_ready.return_value = True
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        config_server = BaseInstance(
            Mock(), self.dbinst4, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        mock_load.side_effect = [member1, member2, query_router, config_server]
        mock_ip.side_effect = ["10.0.0.5"]
        mock_init_replica_set.return_value = True
        mock_create_shard.return_value = True
        mock_guest().get_admin_password.return_value = "pwd"

        self.clustertasks.create_cluster(Mock(), self.cluster_id)

        # The config servers are a replica set and hold the users of the
        # cluster; the query routers get the admin user of its primary.
        mock_init_replica_set.assert_called_with(config_server, [])
        mock_guest().add_config_servers.assert_called_with(["10.0.0.5"])
        mock_guest().create_admin_user.assert_not_called()
        mock_guest().store_admin_password.assert_called_with("pwd")
        mock_create_shard.assert_called_with(
            query_router, [member1, member2]
        )
        self.assertEqual(4, mock_guest().cluster_complete.call_count)
        mock_reset_task.assert_called_with()

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, 'get_cluster_admin_password')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    @patch(
        'trove.common.strategies.cluster.experimental.mongodb.taskmanager.LOG')
    def test_add_query_routers_failure(self,
                                       mock_logging, mock_dv, mock_ds,
                                       mock_password, mock_guest, mock_update):
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        mock_guest.side_effect = Exception("Boom!")

        ret_val = self.clustertasks._add_query_routers([query_router],
                                                       ['10.0.0.5'])
        mock_update.assert_called_with(self.cluster_id)
        self.assertFalse(ret_val)

    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_add_query_routers(self,
                               mock_dv,
                               mock_ds,
                               mock_guest):
        password = 'pwd'
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )

        ret_val = self.clustertasks._add_query_routers([query_router],
                                                       ['10.0.0.5'],
                                                       admin_password=password)
        mock_guest.assert_called_with(query_router)
        mock_guest().add_config_servers.assert_called_with(['10.0.0.5'])
        mock_guest().store_admin_password.assert_called_with(password)
        self.assertTrue(ret_val)

    @patch.object(ClusterTasks, 'update_statuses_on_failure')
    @patch.object(InstanceServiceStatus, 'find_by')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_get_running_query_router_id(self, mock_dv, mock_ds,
                                         mock_find_by, mock_update):
        self.clustertasks._db_instances = [self.dbinst1, self.dbinst2,
                                           self.dbinst3, self.dbinst4]
        # The container based guest agent reports HEALTHY, not RUNNING.
        for status in (ServiceStatuses.HEALTHY, ServiceStatuses.RUNNING):
            mock_find_by.return_value.get_status.return_value = status
            self.assertEqual(
                self.dbinst3.id,
                self.clustertasks._get_running_query_router_id())
            mock_find_by.assert_called_with(instance_id=self.dbinst3.id)

        mock_find_by.return_value.get_status.return_value = (
            ServiceStatuses.SHUTDOWN)
        self.assertFalse(self.clustertasks._get_running_query_router_id())
        mock_update.assert_called_with(self.cluster_id)

    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(utils, 'generate_random_password')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_add_query_routers_new_cluster(self,
                                           mock_dv,
                                           mock_ds,
                                           mock_gen_password,
                                           mock_guest):
        password = 'pwd'
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        mock_gen_password.return_value = password

        ret_val = self.clustertasks._add_query_routers([query_router],
                                                       ['10.0.0.5'])
        mock_guest.assert_called_with(query_router)
        mock_guest().add_config_servers.assert_called_with(['10.0.0.5'])
        mock_guest().create_admin_user.assert_called_with(password)
        self.assertTrue(ret_val)

    @patch.object(ClusterTasks, 'reset_task')
    @patch.object(ClusterTasks, 'get_guest')
    @patch.object(ClusterTasks, '_all_instances_ready')
    @patch.object(DBInstance, 'find_all')
    def _run_grow_cluster(self,
                          mock_find_all,
                          mock_all_instances_ready,
                          mock_guest,
                          mock_reset_task,
                          new_instances_ids=None):
        mock_find_all().all.return_value = [self.dbinst1,
                                            self.dbinst2,
                                            self.dbinst3,
                                            self.dbinst4]
        mock_all_instances_ready.return_value = True

        self.clustertasks.grow_cluster(Mock(), self.cluster_id,
                                       new_instances_ids)

        self.assertEqual(len(new_instances_ids),
                         mock_guest().cluster_complete.call_count)
        mock_reset_task.assert_called_with()

    @patch.object(ClusterTasks, '_add_query_routers')
    @patch.object(ClusterTasks, 'get_cluster_admin_password')
    @patch.object(ClusterTasks, 'get_ip')
    @patch.object(Instance, 'load')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_grow_cluster_query_router(self,
                                       mock_dv,
                                       mock_ds,
                                       mock_load,
                                       mock_ip,
                                       mock_get_password,
                                       mock_add_query_router):
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        config_server = BaseInstance(
            Mock(), self.dbinst4, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        mock_load.side_effect = [query_router, config_server]
        mock_ip.return_value = '10.0.0.5'
        mock_add_query_router.return_value = True

        self._run_grow_cluster(new_instances_ids=[query_router.id])

        mock_add_query_router.assert_called_with(
            [query_router], ['10.0.0.5'], admin_password=mock_get_password()
        )

    @patch.object(ClusterTasks, '_create_shard')
    @patch.object(Instance, 'load')
    @patch.object(ClusterTasks, '_get_running_query_router_id')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    def test_grow_cluster_shard(self,
                                mock_dv,
                                mock_ds,
                                mock_running_qr_id,
                                mock_load,
                                mock_create_shard):
        mock_running_qr_id.return_value = '3'
        member1 = BaseInstance(Mock(), self.dbinst1, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        member2 = BaseInstance(Mock(), self.dbinst2, Mock(),
                               InstanceServiceStatus(ServiceStatuses.NEW))
        query_router = BaseInstance(
            Mock(), self.dbinst3, Mock(),
            InstanceServiceStatus(ServiceStatuses.NEW)
        )
        mock_load.side_effect = [member1, member2, query_router]
        mock_create_shard.return_value = True

        self._run_grow_cluster(new_instances_ids=[member1.id, member2.id])

        mock_create_shard.assert_called_with(
            query_router, [member1, member2]
        )
