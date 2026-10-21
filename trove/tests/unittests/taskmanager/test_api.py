# Copyright 2014 eBay Software Foundation
# Copyright [2015] Hewlett-Packard Development Company, L.P.
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

from unittest import mock
from unittest.mock import Mock
from unittest.mock import patch

from trove.common import context
from trove.common import exception
from trove.common.rpc.version import RPC_API_VERSION
from trove.common.strategies.cluster.experimental.mongodb.taskmanager import (
    MongoDbTaskManagerAPI)
from trove.guestagent import models as agent_models
from trove.taskmanager import api as task_api
from trove.taskmanager.manager import Manager
from trove.tests.unittests import trove_testtools


class ApiTest(trove_testtools.TestCase):
    @patch.object(task_api.API, 'get_client')
    def setUp(self, *args):
        super(ApiTest, self).setUp()
        self.context = context.TroveContext()
        self.api = task_api.API(self.context)
        self._mock_rpc_client()

    def _verify_rpc_prepare_before_cast(self):
        self.api.client.prepare.assert_called_once_with(
            version=RPC_API_VERSION)

    def _verify_cast(self, *args, **kwargs):
        self.call_context.cast.assert_called_once_with(self.context, *args,
                                                       **kwargs)

    def _mock_rpc_client(self):
        self.call_context = trove_testtools.TroveTestContext(self)
        self.api.client.prepare = Mock(return_value=self.call_context)
        self.call_context.cast = Mock()

    @patch.object(task_api.API, '_transform_obj', Mock(return_value='flv-id'))
    def test_create_instance(self):
        self.api.create_instance(
            'inst-id', 'inst-name', mock.ANY, 'img-id', {'name': 'db1'},
            {'name': 'usr1'}, 'mysql', None, 1, backup_id='bk-id',
            availability_zone='az', root_password='pwd', nics=['nic-id'],
            overrides={}, slave_of_id='slv-id', cluster_config={},
            volume_type='type', modules=['mod-id'], locality='affinity')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast(
            'create_instance', availability_zone='az', backup_id='bk-id',
            cluster_config={}, databases={'name': 'db1'},
            datastore_manager='mysql', flavor='flv-id', image_id='img-id',
            instance_id='inst-id', locality='affinity', modules=['mod-id'],
            name='inst-name', nics=['nic-id'], overrides={}, packages=None,
            root_password='pwd', slave_of_id='slv-id', users={'name': 'usr1'},
            volume_size=1, volume_type='type', access=None, ds_version=None)

    def test_cluster_event(self):
        exp_resp = None
        self.call_context.cast.return_value = exp_resp
        self.api.cluster_event('c1', 'i1', 'abnormal',
                               payload={'reason': 'x'}, sent=3.0)
        self.api.client.prepare.assert_called_once_with(
            version=self.api.API_BASE_VERSION, fanout=True)
        self._verify_cast('cluster_event', cluster_id='c1', instance_id='i1',
                          kind='abnormal', payload={'reason': 'x'}, sent=3.0)

    def test_cluster_event_to_the_watching_replicas(self):
        self.api.cluster_event('c1', 'i1', 'abnormal', payload={}, sent=3.0,
                               servers=['recovery-0', 'recovery-2'])
        # On the slots' own topic: a cast on the task manager's topic with
        # a server would reach the server queue of the main RPC server.
        self.assertEqual(
            [mock.call(version=self.api.API_BASE_VERSION,
                       topic='taskmanager-slots', server='recovery-0'),
             mock.call(version=self.api.API_BASE_VERSION,
                       topic='taskmanager-slots', server='recovery-2')],
            self.api.client.prepare.call_args_list)
        self.assertEqual(2, self.call_context.cast.call_count)

    def test_detach_replica(self):
        self.api.detach_replica('some-instance-id')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('detach_replica', instance_id='some-instance-id')

    def test_promote_to_replica_source(self):
        self.api.promote_to_replica_source('some-instance-id')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('promote_to_replica_source',
                          instance_id='some-instance-id')

    def test_eject_replica_source(self):
        self.api.eject_replica_source('some-instance-id')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('eject_replica_source',
                          instance_id='some-instance-id')

    def test_create_cluster(self):
        self.api.create_cluster('some-cluster-id')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('create_cluster', cluster_id='some-cluster-id')

    def test_delete_cluster(self):
        self.api.delete_cluster('some-cluster-id')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('delete_cluster', cluster_id='some-cluster-id')

    @patch.object(agent_models, 'AgentHeartBeat')
    def test_delete_heartbeat(self, mock_agent_heart_beat):
        mock_heartbeat = Mock()
        mock_agent_heart_beat.return_value.find_by_instance_id = Mock(
            return_value=mock_heartbeat)
        self.api._delete_heartbeat('some-cluster-id')
        mock_heartbeat.delete.assert_called_with()

    @patch.object(agent_models, 'AgentHeartBeat')
    @patch('trove.taskmanager.api.LOG')
    def test_exception_delete_heartbeat(self, mock_logging,
                                        mock_agent_heart_beat):
        mock_agent_heart_beat.return_value.find_by_instance_id.side_effect = (
            exception.ModelNotFoundError)
        self.api._delete_heartbeat('some-cluster-id')
        mock_agent_heart_beat.return_value.delete.assert_not_called()

    def test_transform_obj(self):
        flavor = Mock()
        self.assertRaisesRegex(ValueError,
                               ('Could not transform %s' % flavor),
                               self.api._transform_obj, flavor)

    def test_upgrade(self):
        self.api.upgrade('some-instance-id', 'some-datastore-version')

        self._verify_rpc_prepare_before_cast()
        self._verify_cast('upgrade',
                          instance_id='some-instance-id',
                          datastore_version_id='some-datastore-version')


class MongoDbApiTest(ApiTest):
    """The casts of a strategy's API name methods the task manager
    answers: the manager takes the strategy from the first word of the
    method ("mongodb_add_member"), and knows no "add_member".
    """

    @patch.object(task_api.API, 'get_client')
    def setUp(self, *args):
        super(MongoDbApiTest, self).setUp()
        self.api = MongoDbTaskManagerAPI(self.context)
        self._mock_rpc_client()

    def _verify_the_manager_answers(self):
        method = self.call_context.cast.call_args[0][1]
        self.assertTrue(callable(getattr(Manager(), method)), method)

    def test_remove_member(self):
        self.api.mongodb_remove_member('c1', 'i1', '10.0.0.5:27017')
        self._verify_cast('mongodb_remove_member', cluster_id='c1',
                          instance_id='i1', host='10.0.0.5:27017')
        self._verify_the_manager_answers()

    def test_add_member(self):
        self.api.mongodb_add_member('c1', 'i2')
        self._verify_cast('mongodb_add_member', cluster_id='c1',
                          instance_id='i2')
        self._verify_the_manager_answers()

    def test_add_shard_cluster(self):
        self.api.mongodb_add_shard_cluster('c1', 's1', 'rs2')
        self._verify_cast('mongodb_add_shard_cluster', cluster_id='c1',
                          shard_id='s1', replica_set_name='rs2')
        self._verify_the_manager_answers()


class TestAPI(trove_testtools.TestCase):

    @patch.object(task_api.API, 'get_client')
    def test_load_api(self, get_client_mock):
        context = trove_testtools.TroveTestContext(self)
        manager = 'mongodb'

        self.assertIsInstance(task_api.load(context), task_api.API)
        self.assertIsInstance(task_api.load(context, manager),
                              MongoDbTaskManagerAPI)
