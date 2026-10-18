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

from unittest.mock import MagicMock
from unittest.mock import patch

from trove.common import exception
from trove.common.strategies.cluster.experimental.vertica import api
from trove.common.strategies.cluster.experimental.vertica import guestagent
from trove.common.strategies.cluster.experimental.vertica import taskmanager
from trove.tests.unittests import trove_testtools

Tasks = taskmanager.VerticaClusterTasks


class VerticaClusterTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(VerticaClusterTasksTest, self).setUp()
        self.tasks = Tasks.__new__(Tasks)
        self.guests = {}
        for name, mock in (
                ('get_guest', MagicMock(side_effect=self._guest)),
                ('get_ip', MagicMock(side_effect=lambda i: 'ip-' + i.id)),
                ('_all_instances_ready', MagicMock(return_value=True)),
                ('reset_task', MagicMock()),
                ('update_statuses_on_failure', MagicMock())):
            patcher = patch.object(Tasks, name, mock)
            patcher.start()
            self.addCleanup(patcher.stop)
        for target, name, kwargs in (
                (taskmanager.Instance, 'load',
                 {'side_effect': lambda ctx, i: MagicMock(id=i)}),
                (taskmanager.DBInstance, 'find_all', {})):
            patcher = patch.object(target, name, **kwargs)
            setattr(self, name, patcher.start())
            self.addCleanup(patcher.stop)
        self.find_all.return_value.all.return_value = [
            MagicMock(id='b', type='member'),
            MagicMock(id='a', type='master'),
            MagicMock(id='c', type='member')]
        self.properties = {}
        self.tasks.db_info = MagicMock(tenant_id='project', id='cluster')
        self.tasks.db_info.get_properties.side_effect = (
            lambda: dict(self.properties))
        self.tasks.db_info.save_properties.side_effect = (
            lambda p: self.properties.update(p))
        self.tasks.ds_version = MagicMock(manager='vertica')

    def _guest(self, instance):
        return self.guests.setdefault(instance.id, MagicMock(name=instance.id))

    def test_create(self):
        self._guest(MagicMock(id='a')).get_cluster_secrets.return_value = 'S'
        self.guests['a'].install_cluster.return_value = 'CONFIG'
        self.tasks.create_cluster(None, 'cluster')
        master = self.guests['a']
        # The members trust the master's authority before it creates the
        # database on all of them, and then get its configuration.
        master.install_cluster.assert_called_once_with(
            ['ip-b', 'ip-a', 'ip-c'], communal=None)
        master.install_cluster_secrets.assert_not_called()
        master.set_cluster_config.assert_not_called()
        for member in ('b', 'c'):
            guest = self.guests[member]
            guest.install_cluster_secrets.assert_called_once_with('S')
            guest.set_cluster_config.assert_called_once_with('CONFIG')
            guest.install_cluster.assert_not_called()
        for guest in self.guests.values():
            guest.cluster_complete.assert_called_once_with()
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once_with()

    def test_create_failure(self):
        self._guest(MagicMock(id='a')).install_cluster.side_effect = (
            Exception('create_db failed'))
        self.tasks.create_cluster(None, 'cluster')
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster')
        for guest in self.guests.values():
            guest.cluster_complete.assert_not_called()

    def _eon(self):
        self.properties.update({'storage_mode': 'eon', 'sync_writes': False,
                                'communal_bucket': 'trove-vertica-cluster',
                                'ec2_user_id': 'user'})
        storage = MagicMock()
        storage.credential.return_value = ('AK', 'SK')
        storage.address.return_value = ('10.0.0.10:80', False)
        patcher = patch.object(taskmanager.object_storage,
                               'ProjectObjectStorage', return_value=storage)
        patcher.start()
        self.addCleanup(patcher.stop)
        return storage

    def test_create_in_eon_mode(self):
        storage = self._eon()
        order = MagicMock()
        order.attach_mock(storage.credential, 'credential')
        order.attach_mock(storage.create_bucket, 'create_bucket')
        order.attach_mock(self._guest(MagicMock(id='a')).install_cluster,
                          'install_cluster')
        self.tasks.create_cluster(None, 'cluster')
        self.assertEqual(['credential', 'create_bucket', 'install_cluster'],
                         [c[0] for c in order.mock_calls])
        # The user who created the cluster, in its project.
        storage.credential.assert_called_once_with('user', 'project', None)
        storage.create_bucket.assert_called_once_with(
            storage.client.return_value, 'trove-vertica-cluster')
        storage.client.assert_called_once_with('AK', 'SK')
        self.guests['a'].install_cluster.assert_called_once_with(
            ['ip-b', 'ip-a', 'ip-c'], communal={
                'location': 's3://trove-vertica-cluster/db',
                'endpoint': '10.0.0.10:80', 'https': False,
                'region': 'us-east-1', 'access': 'AK', 'secret': 'SK',
                'revive': False})
        # The access key is kept, the secret is not.
        self.assertEqual('AK', self.properties['ec2_access'])
        self.assertNotIn('SK', self.properties.values())
        self.tasks.update_statuses_on_failure.assert_not_called()

    def test_a_retry_takes_the_recorded_credential(self):
        storage = self._eon()
        # An earlier try made the credential and the bucket.
        self.properties.update({'ec2_access': 'AK', 'communal_owner': True})
        self.tasks.create_cluster(None, 'cluster')
        storage.credential.assert_called_once_with('user', 'project', 'AK')
        self.tasks.db_info.save_properties.assert_not_called()

    def test_no_bucket_no_database(self):
        storage = self._eon()
        storage.create_bucket.side_effect = Exception('AccessDenied')
        self.tasks.create_cluster(None, 'cluster')
        self.guests['a'].install_cluster.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster')

    def _other_cluster(self, properties, cluster_id='other'):
        other = MagicMock(id=cluster_id)
        other.get_properties.return_value = dict(properties)
        return other

    def _delete(self, deleted=True, others=()):
        with patch.object(taskmanager.task_models.ClusterTasks,
                          'delete_cluster') as mock_super, \
                patch.object(taskmanager.DBCluster, 'find_by') as mock_find, \
                patch.object(taskmanager.DBCluster,
                             'find_all') as mock_all:
            mock_find.return_value.deleted = deleted
            mock_find.return_value.get_properties.return_value = dict(
                self.properties)
            mock_all.return_value.all.return_value = list(others)
            self.tasks.delete_cluster(None, 'cluster')
        mock_super.assert_called_once_with(None, 'cluster')

    # Reviving from a bucket.

    def _revive(self, others=()):
        storage = self._eon()
        self.properties.update({'revive_from': 'trove-vertica-old',
                                'communal_bucket': 'trove-vertica-old'})
        storage.bucket_exists.return_value = True
        with patch.object(taskmanager.DBCluster, 'find_all') as mock_all:
            mock_all.return_value.all.return_value = list(others)
            self.tasks.create_cluster(None, 'cluster')
        return storage

    def test_create_revives_from_the_bucket(self):
        old = self._other_cluster({'storage_mode': 'eon',
                                   'communal_bucket': 'trove-vertica-old',
                                   'ec2_user_id': 'old-user',
                                   'ec2_access': 'OLD'}, 'old')
        unrelated = self._other_cluster({'communal_bucket': 'another'})
        storage = self._revive(others=[old, unrelated, self.tasks.db_info])
        storage.bucket_exists.assert_called_once_with(
            storage.client.return_value, 'trove-vertica-old')
        storage.create_bucket.assert_not_called()
        communal = self.guests['a'].install_cluster.call_args[1]['communal']
        self.assertIs(True, communal['revive'])
        self.assertEqual('s3://trove-vertica-old/db', communal['location'])
        # The bucket is this cluster's now; the old cluster loses its
        # credential and will not purge the bucket.
        self.assertIs(True, self.properties['communal_owner'])
        storage.delete_credential.assert_called_once_with('old-user', 'OLD')
        theirs = old.save_properties.call_args[0][0]
        self.assertEqual('cluster', theirs['communal_moved_to'])
        self.assertIs(False, theirs['communal_owner'])
        self.assertNotIn('ec2_access', theirs)
        unrelated.save_properties.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_not_called()

    def test_no_bucket_to_revive_from(self):
        storage = self._eon()
        self.properties.update({'revive_from': 'trove-vertica-old',
                                'communal_bucket': 'trove-vertica-old'})
        storage.bucket_exists.return_value = False
        self.tasks.create_cluster(None, 'cluster')
        self.guests['a'].install_cluster.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster')
        self.assertNotIn('communal_owner', self.properties)

    def test_a_failed_revive_leaves_the_bucket_alone(self):
        storage = self._eon()
        self.properties.update({'revive_from': 'trove-vertica-old',
                                'communal_bucket': 'trove-vertica-old',
                                'ec2_access': 'AK'})
        storage.secret.return_value = 'SK'
        self._delete()
        storage.purge_bucket.assert_not_called()
        storage.delete_credential.assert_called_once_with('user', 'AK')

    def test_a_bucket_another_cluster_names_is_not_purged(self):
        storage = self._eon()
        self.properties['ec2_access'] = 'AK'
        storage.secret.return_value = 'SK'
        self._delete(others=[self._other_cluster(
            {'communal_bucket': 'trove-vertica-cluster'})])
        storage.purge_bucket.assert_not_called()
        storage.delete_credential.assert_called_once_with('user', 'AK')

    def test_a_cluster_made_owner_purges(self):
        storage = self._eon()
        self.properties.update({'revive_from': 'trove-vertica-old',
                                'communal_bucket': 'trove-vertica-old',
                                'communal_owner': True, 'ec2_access': 'AK'})
        storage.secret.return_value = 'SK'
        self._delete()
        storage.purge_bucket.assert_called_once_with(
            storage.client.return_value, 'trove-vertica-old')

    def test_create_in_eon_mode_owns_its_bucket(self):
        self._eon()
        self.tasks.create_cluster(None, 'cluster')
        self.assertIs(True, self.properties['communal_owner'])
        self.assertIs(False, self.guests['a'].install_cluster.call_args[1][
            'communal']['revive'])

    def test_delete_removes_the_bucket_and_then_the_credential(self):
        storage = self._eon()
        self.properties['ec2_access'] = 'AK'
        storage.secret.return_value = 'SK'
        order = MagicMock()
        order.attach_mock(storage.purge_bucket, 'purge_bucket')
        order.attach_mock(storage.delete_credential, 'delete_credential')
        self._delete()
        storage.secret.assert_called_once_with('user', 'AK')
        storage.client.assert_called_once_with('AK', 'SK')
        self.assertEqual(
            [('purge_bucket', (storage.client.return_value,
                               'trove-vertica-cluster'), {}),
             ('delete_credential', ('user', 'AK'), {})],
            order.mock_calls)

    def test_delete_keeps_what_it_cannot_remove(self):
        storage = self._eon()
        self.properties['ec2_access'] = 'AK'
        # The credential is gone: the bucket cannot be reached any more.
        storage.secret.return_value = None
        self._delete()
        storage.purge_bucket.assert_not_called()
        storage.delete_credential.assert_not_called()
        # The bucket could not be emptied: the credential is kept for it.
        storage.secret.return_value = 'SK'
        storage.purge_bucket.side_effect = Exception('timed out')
        self._delete()
        storage.delete_credential.assert_not_called()

    def test_delete_leaves_storage_alone(self):
        storage = self._eon()
        self.properties['ec2_access'] = 'AK'
        # The members are not gone yet: the cluster is not deleted.
        self._delete(deleted=False)
        # A cluster that failed before it had a credential.
        del self.properties['ec2_access']
        self._delete()
        # Enterprise Mode.
        self.properties.clear()
        self._delete()
        storage.secret.assert_not_called()
        storage.purge_bucket.assert_not_called()

    def _reset_credential(self, user='user-2'):
        context = MagicMock(user_id=user)
        self.tasks.reset_communal_credential(context, 'cluster')
        return context

    def _reset_storage(self):
        storage = self._eon()
        self.properties['ec2_access'] = 'AK'
        storage.credential.return_value = ('AK2', 'SK2')
        storage.bucket_exists.return_value = True
        return storage

    def test_reset_gives_the_database_a_new_credential(self):
        storage = self._reset_storage()
        master = self._guest(MagicMock(id='a'))
        order = MagicMock()
        order.attach_mock(storage.credential, 'credential')
        order.attach_mock(storage.bucket_exists, 'bucket_exists')
        order.attach_mock(master.set_communal_credential, 'set')
        order.attach_mock(storage.delete_credential, 'delete_credential')
        self._reset_credential()
        self.assertEqual(
            ['credential', 'bucket_exists', 'set', 'delete_credential'],
            [c[0] for c in order.mock_calls])
        # A new credential of the user asking, in the project.
        storage.credential.assert_called_once_with('user-2', 'project')
        storage.bucket_exists.assert_called_once_with(
            storage.client.return_value, 'trove-vertica-cluster')
        storage.client.assert_called_once_with('AK2', 'SK2')
        master.set_communal_credential.assert_called_once_with('AK2', 'SK2')
        # The old one goes once the database has the new one.
        storage.delete_credential.assert_called_once_with('user', 'AK')
        self.assertEqual('user-2', self.properties['ec2_user_id'])
        self.assertEqual('AK2', self.properties['ec2_access'])
        self.tasks.reset_task.assert_called_once_with()

    def test_reset_that_fails_keeps_the_old_credential(self):
        for failure in ('bucket', 'guest'):
            storage = self._reset_storage()
            self.properties.update({'ec2_user_id': 'user',
                                    'ec2_access': 'AK'})
            self.guests.clear()
            master = self._guest(MagicMock(id='a'))
            self.tasks.reset_task.reset_mock()
            if failure == 'bucket':
                storage.bucket_exists.return_value = False
            else:
                master.set_communal_credential.side_effect = Exception('x')
            self._reset_credential()
            # Only the new credential goes.
            storage.delete_credential.assert_called_once_with('user-2',
                                                              'AK2')
            self.assertEqual('AK', self.properties['ec2_access'])
            self.assertEqual('user', self.properties['ec2_user_id'])
            self.tasks.reset_task.assert_called_once_with()
            if failure == 'bucket':
                master.set_communal_credential.assert_not_called()


class VerticaClusterApiTest(trove_testtools.TestCase):

    def test_grow_and_shrink_are_refused(self):
        cluster = api.VerticaCluster.__new__(api.VerticaCluster)
        self.assertRaises(exception.BadRequest, cluster.grow,
                          [{'flavor_id': 'f'}], 'image')
        self.assertRaises(exception.BadRequest, cluster.shrink, ['id1'])


class VerticaGuestAgentApiTest(trove_testtools.TestCase):

    def test_communal_storage_goes_only_to_eon_mode(self):
        # Guest agents older than Eon Mode do not take the argument.
        client = guestagent.VerticaGuestAgentAPI.__new__(
            guestagent.VerticaGuestAgentAPI)
        with patch.object(guestagent.VerticaGuestAgentAPI, '_call') as call:
            client.install_cluster(['10.0.0.1'])
            client.install_cluster(['10.0.0.1'], communal={'location': 'l'})
        self.assertEqual(
            [{'members': ['10.0.0.1']},
             {'members': ['10.0.0.1'], 'communal': {'location': 'l'}}],
            [{k: v for k, v in c[1].items() if k != 'version'}
             for c in call.call_args_list])

    def test_the_communal_credential_goes_to_the_guest(self):
        client = guestagent.VerticaGuestAgentAPI.__new__(
            guestagent.VerticaGuestAgentAPI)
        client.agent_high_timeout = 600
        with patch.object(guestagent.VerticaGuestAgentAPI, '_call') as call:
            client.set_communal_credential('AK', 'SK')
        self.assertEqual('set_communal_credential', call.call_args[0][0])
        self.assertEqual({'access': 'AK', 'secret': 'SK'},
                         {k: v for k, v in call.call_args[1].items()
                          if k != 'version'})
