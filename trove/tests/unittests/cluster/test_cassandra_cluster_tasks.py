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

from unittest.mock import MagicMock
from unittest.mock import patch

from trove.common import exception
from trove.common.strategies.cluster.experimental.cassandra import taskmanager
from trove.instance import tasks as inst_tasks
from trove.tests.unittests import trove_testtools

Tasks = taskmanager.CassandraClusterTasks


class CassandraShrinkTest(trove_testtools.TestCase):
    """The shrink of a cluster of three members, a, b and c, whose seeds
    are a and b: of b, whose server is gone (the repair replaces it), and
    of b with its server.
    """

    def setUp(self):
        super(CassandraShrinkTest, self).setUp()
        self.tasks = Tasks.__new__(Tasks)
        self.guests = {}
        self.ring = []
        for name, mock in (
                ('get_guest', MagicMock(side_effect=self._guest)),
                ('get_ip', MagicMock(side_effect=lambda i: 'ip-' + i.id)),
                ('_all_instances_shutdown', MagicMock(return_value=True)),
                ('reset_task', MagicMock()),
                ('update_statuses_on_failure', MagicMock())):
            self._patch(Tasks, name, new=mock)
        self.load = self._patch(taskmanager.Instance, 'load',
                                side_effect=lambda ctx, i: MagicMock(id=i))
        self.delete = self._patch(taskmanager.Instance, 'delete')
        self.find_all = self._patch(taskmanager.DBInstance, 'find_all')
        self.find_all.return_value.all.return_value = [
            MagicMock(id=i) for i in ('a', 'b', 'c')]
        self.load_any = self._patch(taskmanager, 'load_any_instance')
        self.known = self._patch(taskmanager.health, 'known_members',
                                 return_value={'ip-b': 'b'})
        # The wait for the ring to forget a node: one look.
        self._patch(taskmanager.utils, 'poll_until', side_effect=self._poll)
        self._gone()
        for i in ('a', 'c'):
            guest = self._guest(MagicMock(id=i))
            guest.get_seeds.return_value = ['ip-a', 'ip-b']
            guest.get_data_center.return_value = 'dc1'
            guest.get_rack.return_value = 'rack1'
            guest.get_ring.side_effect = lambda: list(self.ring)
            guest.remove_node.side_effect = self._forget

    def _patch(self, target, name, **kwargs):
        patcher = patch.object(target, name, **kwargs)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    @staticmethod
    def _poll(fn, sleep_time=None, time_out=None):
        if not fn():
            raise exception.PollTimeOut()

    def _guest(self, instance):
        return self.guests.setdefault(instance.id, MagicMock(name=instance.id))

    def _gone(self, *ids):
        def load(ctx, i):
            inst = MagicMock(id=i, server=None if i in ids else object())
            inst.name = i
            return inst
        self.load_any.side_effect = load

    def _ring_with(self, *nodes):
        """The ring as the nodes that stay see it: (member, state)."""
        self.ring = [{'state': state, 'address': 'ip-' + i,
                      'host_id': 'hid-' + i} for i, state in nodes]

    def _forget(self, host_id):
        self.forgotten = host_id
        self.ring = [n for n in self.ring if n['host_id'] != host_id]

    def test_shrink_a_member_whose_server_is_gone(self):
        self._gone('b')
        self._ring_with(('a', 'UN'), ('b', 'DN'), ('c', 'UN'))
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        # A node that stays takes it out of the ring by its host id; it is
        # asked nothing itself, and is deleted once the ring forgot it.
        self.guests['a'].remove_node.assert_called_once_with('hid-b')
        self.guests['c'].remove_node.assert_not_called()
        self.assertNotIn('b', self.guests)
        deleted = [c[0][0] for c in self.delete.call_args_list]
        self.assertEqual(['b'], [i.id for i in deleted])
        deleted[0].update_db.assert_called_once_with(cluster_id=None)
        self.tasks._all_instances_shutdown.assert_not_called()
        # It was a seed: the nodes that stay get the seeds without it.
        for i in ('a', 'c'):
            self.guests[i].set_seeds.assert_called_once_with({'ip-a'})
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once_with()

    def test_a_gone_member_the_ring_forgot_already(self):
        self._gone('b')
        self._ring_with(('a', 'UN'), ('c', 'UN'))
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        self.guests['a'].remove_node.assert_not_called()
        self.assertEqual(['b'],
                         [c[0][0].id for c in self.delete.call_args_list])
        self.tasks.update_statuses_on_failure.assert_not_called()

    def test_a_ring_that_keeps_it_keeps_the_member(self):
        self._gone('b')
        self._ring_with(('a', 'UN'), ('b', 'DN'), ('c', 'UN'))
        self.guests['a'].remove_node.side_effect = None
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        self.guests['a'].remove_node.assert_called_once_with('hid-b')
        self.delete.assert_not_called()
        self.guests['a'].set_seeds.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster', status=inst_tasks.InstanceTasks.SHRINKING_ERROR)
        self.tasks.reset_task.assert_called_once_with()

    def test_a_gone_member_the_ring_holds_up_is_not_removed(self):
        self._gone('b')
        self._ring_with(('a', 'UN'), ('b', 'UN'), ('c', 'UN'))
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        self.guests['a'].remove_node.assert_not_called()
        self.delete.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster', status=inst_tasks.InstanceTasks.SHRINKING_ERROR)

    def test_a_gone_member_the_health_never_learnt(self):
        self._gone('b')
        self._ring_with(('a', 'UN'), ('b', 'DN'), ('c', 'UN'))
        self.known.return_value = {}
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        self.guests['a'].remove_node.assert_not_called()
        self.delete.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster', status=inst_tasks.InstanceTasks.SHRINKING_ERROR)

    def test_a_member_with_a_server_decommissions_itself(self):
        self._ring_with(('a', 'UN'), ('b', 'UN'), ('c', 'UN'))
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        self.guests['b'].node_decommission.assert_called_once_with()
        self.guests['a'].remove_node.assert_not_called()
        self.guests['a'].get_ring.assert_not_called()
        self.tasks._all_instances_shutdown.assert_called_once_with(
            ['b'], 'cluster')
        self.assertEqual(['b'],
                         [c[0][0].id for c in self.delete.call_args_list])
        for i in ('a', 'c'):
            self.guests[i].set_seeds.assert_called_once_with({'ip-a'})
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once_with()
