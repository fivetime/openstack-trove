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

import json
from unittest.mock import Mock
from unittest.mock import patch
import uuid

from novaclient import exceptions as nova_exceptions

from trove.cluster.models import Cluster
from trove.cluster.models import ClusterTasks
from trove.cluster.models import DBCluster
from trove.common import cfg
from trove.common import clients
from trove.common import exception
from trove.common.strategies.cluster.experimental.vertica import (
    api as vertica_api)
from trove.instance import models as inst_models
from trove.quota.quota import QUOTAS
from trove.taskmanager import api as task_api
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class FakeOptGroup(object):
    def __init__(self, cluster_member_count=3,
                 volume_support=True, device_path='/dev/vdb'):
        self.cluster_member_count = cluster_member_count
        self.volume_support = volume_support
        self.device_path = device_path


class ClusterTest(trove_testtools.TestCase):
    def setUp(self):
        super(ClusterTest, self).setUp()
        self.get_client_patch = patch.object(task_api.API, 'get_client')
        self.get_client_mock = self.get_client_patch.start()
        self.addCleanup(self.get_client_patch.stop)
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
        self.context = trove_testtools.TroveTestContext(self)
        self.datastore = Mock()
        self.dv = Mock()
        self.dv.manager = "vertica"
        self.datastore_version = self.dv
        self.cluster = vertica_api.VerticaCluster(self.context, self.db_info,
                                                  self.datastore,
                                                  self.datastore_version)
        self.instances = [{'volume_size': 1, 'flavor_id': '1234',
                           'instance_type': 'master'},
                          {'volume_size': 1, 'flavor_id': '1234',
                           'instance_type': 'member'},
                          {'volume_size': 1, 'flavor_id': '1234',
                           'instance_type': 'member'}]
        self.db_instances = [1, 2, 3]

    def tearDown(self):
        super(ClusterTest, self).tearDown()

    @patch.object(inst_models.DBInstance, 'find_all')
    def test_create_empty_instances(self, *args):
        self.assertRaises(exception.ClusterNumInstancesNotSupported,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          [], {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    def test_create_flavor_not_specified(self, *args):
        instances = self.instances
        instances[0]['flavor_id'] = None
        self.assertRaises(exception.ClusterFlavorsNotEqual,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          instances,
                          {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    def test_create_invalid_flavor_specified(self, mock_client,
                                             mock_find_all, mock_create):
        instances = [{'flavor_id': '1234'},
                     {'flavor_id': '1234'},
                     {'flavor_id': '1234'}]

        (mock_client.return_value.flavors.get) = Mock(
            side_effect=nova_exceptions.NotFound(
                404, "Flavor id not found %s" % id))

        self.assertRaises(exception.FlavorNotFound,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          instances,
                          {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    def test_create_volume_no_specified(self, mock_client, mock_find_all,
                                        mock_create):
        instances = self.instances
        instances[0]['volume_size'] = None
        flavors = Mock()
        mock_client.return_value.flavors = flavors
        self.assertRaises(exception.ClusterVolumeSizeRequired,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          instances,
                          {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    @patch.object(vertica_api, 'CONF')
    def test_create_storage_specified_with_no_volume_support(self,
                                                             mock_conf,
                                                             mock_client,
                                                             mock_find_all,
                                                             mock_create):
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=False))
        instances = self.instances
        instances[0]['volume_size'] = None
        flavors = Mock()
        mock_client.return_value.flavors = flavors
        self.assertRaises(exception.VolumeNotSupported,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          instances,
                          {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    @patch.object(vertica_api, 'CONF')
    def test_create_storage_not_specified_and_no_ephemeral_flavor(self,
                                                                  mock_conf,
                                                                  mock_client,
                                                                  m_find_all,
                                                                  mock_create):
        class FakeFlavor(object):
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 0
        instances = [{'flavor_id': '1234'},
                     {'flavor_id': '1234'},
                     {'flavor_id': '1234'}]
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
                          instances,
                          {}, None, None)

    @patch.object(DBCluster, 'create')
    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(clients, 'create_nova_client')
    def test_create_volume_not_equal(self, mock_client, mock_find_all,
                                     mock_create):
        instances = self.instances
        instances[0]['volume_size'] = 2
        flavors = Mock()
        mock_client.return_value.flavors = flavors
        self.assertRaises(exception.ClusterVolumeSizesNotEqual,
                          Cluster.create,
                          Mock(),
                          self.cluster_name,
                          self.datastore,
                          self.datastore_version,
                          instances,
                          {}, None, None)

    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(DBCluster, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    def test_create(self, mock_client, mock_check_quotas, mock_task_api,
                    mock_db_create, mock_ins_create, mock_find_all):
        instances = self.instances
        flavors = Mock()
        mock_client.return_value.flavors = flavors
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            instances,
                            {}, None, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            mock_db_create.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

    @patch.object(inst_models.DBInstance, 'find_all')
    @patch.object(vertica_api, 'CONF')
    @patch.object(inst_models.Instance, 'create')
    @patch.object(DBCluster, 'create')
    @patch.object(task_api, 'load')
    @patch.object(QUOTAS, 'check_quotas')
    @patch.object(clients, 'create_nova_client')
    def test_create_with_ephemeral_flavor(self, mock_client, mock_check_quotas,
                                          mock_task_api, mock_db_create,
                                          mock_ins_create, mock_conf,
                                          mock_find_all):
        class FakeFlavor(object):
            def __init__(self, flavor_id):
                self.flavor_id = flavor_id

            @property
            def id(self):
                return self.flavor.id

            @property
            def ephemeral(self):
                return 1
        instances = [{'flavor_id': '1234'},
                     {'flavor_id': '1234'},
                     {'flavor_id': '1234'}]
        mock_conf.get = Mock(
            return_value=FakeOptGroup(volume_support=False))
        (mock_client.return_value.
         flavors.get.return_value) = FakeFlavor('1234')
        self.cluster.create(Mock(),
                            self.cluster_name,
                            self.datastore,
                            self.datastore_version,
                            instances,
                            {}, None, None)
        mock_task_api.return_value.create_cluster.assert_called_with(
            mock_db_create.return_value.id)
        self.assertEqual(3, mock_ins_create.call_count)

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

    # The sync_writes extended property, through the real create.

    def _create(self, extended_properties, context=None):
        with patch.object(inst_models.DBInstance, 'find_all'), \
                patch.object(inst_models.Instance, 'create') as mock_ins, \
                patch.object(DBCluster, 'create') as mock_db, \
                patch.object(task_api, 'load'), \
                patch.object(QUOTAS, 'check_quotas'), \
                patch.object(clients, 'create_nova_client'):
            mock_db.return_value.id = 'cluster-1'
            self.cluster.create(context or Mock(), self.cluster_name,
                                self.datastore, self.datastore_version,
                                self.instances, extended_properties,
                                None, None)
        return mock_db, [c.kwargs['cluster_config']
                         for c in mock_ins.call_args_list]

    def _eon_enabled(self):
        CONF.set_override('eon_enabled', True, group='vertica')
        self.addCleanup(CONF.clear_override, 'eon_enabled', group='vertica')

    def test_the_properties_are_kept(self):
        mock_db, _configs = self._create({})
        mock_db.return_value.save_properties.assert_called_once_with(
            {'storage_mode': 'enterprise', 'sync_writes': False})
        mock_db, _configs = self._create({'sync_writes': 'true',
                                          'storage_mode': 'Enterprise'})
        mock_db.return_value.save_properties.assert_called_once_with(
            {'storage_mode': 'enterprise', 'sync_writes': True})

    def test_eon_is_refused_where_it_is_not_enabled(self):
        with patch.object(DBCluster, 'create') as mock_db:
            error = self.assertRaises(
                exception.BadRequest, self.cluster.create, Mock(),
                self.cluster_name, self.datastore, self.datastore_version,
                self.instances, {'storage_mode': 'eon'}, None, None)
        self.assertIn('Eon Mode is not available', str(error))
        mock_db.assert_not_called()

    def test_a_storage_mode_that_does_not_exist_is_refused(self):
        self._eon_enabled()
        with patch.object(DBCluster, 'create') as mock_db:
            self.assertRaises(
                exception.BadRequest, self.cluster.create, Mock(),
                self.cluster_name, self.datastore, self.datastore_version,
                self.instances, {'storage_mode': 'eon2'}, None, None)
        mock_db.assert_not_called()

    def test_an_eon_cluster_keeps_its_bucket_and_user(self):
        self._eon_enabled()
        context = Mock(user_id='user-1')
        mock_db, configs = self._create(
            {'storage_mode': 'eon', 'sync_writes': 'true'}, context)
        mock_db.return_value.save_properties.assert_called_once_with(
            {'storage_mode': 'eon', 'sync_writes': True,
             'communal_bucket': 'trove-vertica-cluster-1',
             'ec2_user_id': 'user-1'})
        # The members only get what their guest acts on.
        for config in configs:
            self.assertNotIn('storage_mode', config)
            self.assertIs(True, config['sync_writes'])

    def test_what_a_cluster_shows_of_its_properties(self):
        shown = vertica_api.shown_properties
        # A cluster created before the properties were kept.
        self.assertIsNone(shown({}))
        self.assertEqual({'storage_mode': 'enterprise', 'sync_writes': True},
                         shown({'storage_mode': 'enterprise',
                                'sync_writes': True}))
        self.assertEqual(
            {'storage_mode': 'eon', 'sync_writes': False,
             'communal_bucket': 'trove-vertica-c1'},
            shown({'storage_mode': 'eon', 'sync_writes': False,
                   'communal_bucket': 'trove-vertica-c1',
                   'ec2_user_id': 'u1', 'ec2_access': 'AK'}))
        view = vertica_api.VerticaClusterView(Mock())
        view.cluster.db_info.get_properties.return_value = {
            'storage_mode': 'eon', 'communal_bucket': 'b'}
        self.assertEqual('eon', view.get_extended_properties()['storage_mode'])

    def test_every_member_gets_sync_writes(self):
        for value in ('true', 'True', True, '1'):
            _db, configs = self._create({'sync_writes': value})
            self.assertEqual(3, len(configs))
            for config in configs:
                self.assertIs(True, config['sync_writes'])
            self.assertEqual(['master', 'member', 'member'],
                             [c['instance_type'] for c in configs])

    def test_without_it_or_false_the_members_keep_the_default(self):
        for props in ({}, None, {'sync_writes': 'false'}):
            _db, configs = self._create(props)
            for config in configs:
                self.assertNotIn('sync_writes', config)

    def test_an_unknown_property_is_refused_before_anything_is_made(self):
        # A misspelled property must not leave the writes unsynced.
        with patch.object(DBCluster, 'create') as mock_db:
            error = self.assertRaises(
                exception.BadRequest, self.cluster.create, Mock(),
                self.cluster_name, self.datastore, self.datastore_version,
                self.instances, {'sync_write': 'true'}, None, None)
        self.assertIn('sync_write', str(error))
        self.assertIn('sync_writes', str(error))
        mock_db.assert_not_called()

    def test_a_value_that_is_not_a_boolean_is_refused(self):
        with patch.object(DBCluster, 'create') as mock_db:
            self.assertRaises(
                exception.BadRequest, self.cluster.create, Mock(),
                self.cluster_name, self.datastore, self.datastore_version,
                self.instances, {'sync_writes': 'yes please'}, None, None)
        mock_db.assert_not_called()

    # reset_communal_credential.

    def _eon_cluster(self, project=None):
        self.db_info.properties = json.dumps(
            {'storage_mode': 'eon', 'communal_bucket': 'b',
             'ec2_user_id': 'u1', 'ec2_access': 'AK'})
        self.cluster.context.project_id = project or self.tenant_id
        return self.cluster

    @patch.object(task_api, 'load')
    @patch.object(DBCluster, 'update')
    def test_reset_the_communal_credential(self, mock_update, mock_load):
        self._eon_cluster().reset_communal_credential()
        mock_update.assert_called_once_with(
            task_status=ClusterTasks.UPDATING_CLUSTER)
        mock_load.return_value.reset_communal_credential.\
            assert_called_once_with(self.cluster_id)

    @patch.object(task_api, 'load')
    @patch.object(DBCluster, 'update')
    def test_the_task_is_undone_when_the_cast_fails(self, mock_update,
                                                    mock_load):
        mock_load.return_value.reset_communal_credential.side_effect = (
            exception.TroveError('no broker'))
        self.assertRaises(exception.TroveError,
                          self._eon_cluster().reset_communal_credential)
        self.assertEqual(
            [((), {'task_status': ClusterTasks.UPDATING_CLUSTER}),
             ((), {'task_status': ClusterTasks.NONE})],
            [(c[0], c[1]) for c in mock_update.call_args_list])

    @patch.object(task_api, 'load')
    def test_reset_is_refused_without_communal_storage_or_project(
            self, mock_load):
        self.db_info.properties = json.dumps({'storage_mode': 'enterprise'})
        self.cluster.context.project_id = self.tenant_id
        self.assertRaises(exception.BadRequest,
                          self.cluster.reset_communal_credential)
        # An admin of another project would make the credential there.
        self.assertRaises(exception.BadRequest,
                          self._eon_cluster('other').reset_communal_credential)
        mock_load.assert_not_called()

    def test_the_action_reaches_the_cluster(self):
        with patch.object(vertica_api.VerticaCluster,
                          'reset_communal_credential') as reset:
            self.cluster.action(Mock(), Mock(), 'reset_communal_credential',
                                {})
        reset.assert_called_once_with()
        # Another datastore has none.
        base = Cluster(self.context, self.db_info, self.datastore,
                       self.datastore_version)
        self.assertRaises(exception.BadRequest,
                          base.reset_communal_credential)

    # revive_from.

    def test_revive_from_a_bucket_of_the_project(self):
        self._eon_enabled()
        context = Mock(user_id='user-1')
        mock_db, _configs = self._create(
            {'revive_from': 'trove-vertica-old'}, context)
        mock_db.return_value.save_properties.assert_called_once_with(
            {'storage_mode': 'eon', 'sync_writes': False,
             'revive_from': 'trove-vertica-old',
             'communal_bucket': 'trove-vertica-old',
             'ec2_user_id': 'user-1'})

    def test_revive_from_is_refused(self):
        for props, enabled in (
                ({'revive_from': 'trove-vertica-old'}, False),
                ({'revive_from': 'trove-vertica-old',
                  'storage_mode': 'enterprise'}, True),
                ({'revive_from': 'Not_A/Bucket'}, True),
                ({'revive_from': ''}, True)):
            if enabled:
                CONF.set_override('eon_enabled', True, group='vertica')
            else:
                CONF.clear_override('eon_enabled', group='vertica')
            with patch.object(DBCluster, 'create') as mock_db:
                self.assertRaises(
                    exception.BadRequest, self.cluster.create, Mock(),
                    self.cluster_name, self.datastore,
                    self.datastore_version, self.instances, props,
                    None, None)
            mock_db.assert_not_called()
        CONF.clear_override('eon_enabled', group='vertica')

    def test_a_revived_cluster_shows_where_from(self):
        self.assertEqual(
            {'storage_mode': 'eon', 'sync_writes': False,
             'communal_bucket': 'b', 'revive_from': 'b'},
            vertica_api.shown_properties(
                {'storage_mode': 'eon', 'communal_bucket': 'b',
                 'revive_from': 'b', 'communal_owner': True}))
        self.assertEqual(
            'new', vertica_api.shown_properties(
                {'storage_mode': 'eon', 'communal_bucket': 'b',
                 'communal_moved_to': 'new'})['communal_moved_to'])
