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
            ['ip-b', 'ip-a', 'ip-c'])
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


class VerticaClusterApiTest(trove_testtools.TestCase):

    def test_grow_and_shrink_are_refused(self):
        cluster = api.VerticaCluster.__new__(api.VerticaCluster)
        self.assertRaises(exception.BadRequest, cluster.grow,
                          [{'flavor_id': 'f'}], 'image')
        self.assertRaises(exception.BadRequest, cluster.shrink, ['id1'])
