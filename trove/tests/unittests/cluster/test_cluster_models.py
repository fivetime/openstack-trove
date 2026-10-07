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

from unittest.mock import Mock, patch

from trove.cluster import models
from trove.common.strategies.cluster.experimental.mongodb.api import (
    MongoDbCluster)
from trove.datastore import models as datastore_models
from trove.instance import models as instance_models
from trove.tests.unittests import trove_testtools


class TestClusterModel(trove_testtools.TestCase):

    @patch.object(models.srv_grp.ServerGroup, 'load_for_cluster')
    @patch.object(datastore_models.Datastore, 'load')
    @patch.object(datastore_models.DatastoreVersion, 'load_by_uuid')
    @patch.object(models.DBCluster, 'find_by')
    @patch.object(instance_models.DBInstance, 'find_all')
    def test_load(self, mock_find_all, mock_find_by,
                  mock_load_dsv_by_uuid, mock_ds_load, mock_load_group):
        context = trove_testtools.TroveTestContext(self)
        id = Mock()
        mock_find_by.return_value.name = 'c1'
        # The group is the cluster's, found through its members' servers.
        mock_find_all.return_value.all.return_value = [
            Mock(compute_instance_id='server-1'),
            Mock(compute_instance_id=None)]
        server_group = Mock()
        mock_load_group.return_value = server_group

        dsv = Mock()
        dsv.manager = 'mongodb'
        mock_load_dsv_by_uuid.return_value = dsv
        cluster = models.Cluster.load(context, id)
        self.assertIsInstance(cluster, MongoDbCluster)
        self.assertEqual(server_group, cluster.server_group,
                         "Unexpected server group")
        mock_load_group.assert_called_once_with(context, 'c1', ['server-1'])
        # Cached, nova is not asked again.
        cluster.server_group
        mock_load_group.assert_called_once()


class TestClusterGrowAction(trove_testtools.TestCase):

    def test_grow_keeps_the_volume_type(self):
        cluster = models.Cluster.__new__(models.Cluster)
        cluster.db_info = Mock(id='cid')
        cluster.ds_version = Mock(image_id='image')
        cluster.grow = Mock()
        context = Mock()
        with patch.object(models, 'StartNotification'), \
                patch.object(models, 'DBaaSClusterGrow'):
            cluster.action(context, Mock(), 'grow', [
                {'flavorRef': 'f1', 'volume': {'size': 2,
                                               'type': 'nvme-rep3'}},
                {'flavorRef': 'f1', 'volume': {'size': 2}}])
        instances, image_id = cluster.grow.call_args[0]
        self.assertEqual('nvme-rep3', instances[0]['volume_type'])
        self.assertEqual(2, instances[0]['volume_size'])
        self.assertNotIn('volume_type', instances[1])
        self.assertEqual('image', image_id)
