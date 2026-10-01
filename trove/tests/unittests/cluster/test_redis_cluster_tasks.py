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

from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

from trove.common.strategies.cluster.experimental.redis import taskmanager
from trove.instance import tasks as inst_tasks
from trove.tests.unittests import trove_testtools

Tasks = taskmanager.RedisClusterTasks


class SlotRangesTest(trove_testtools.TestCase):

    def test_ranges_cover_every_slot_once(self):
        for nodes in (1, 2, 3, 5, 7, 16):
            ranges = Tasks.slot_ranges(nodes)
            self.assertEqual(nodes, len(ranges))
            covered = [slot for first, last in ranges
                       for slot in range(first, last + 1)]
            self.assertEqual(list(range(16384)), covered)
            for first, last in ranges:
                self.assertIsInstance(first, int)
                self.assertIsInstance(last, int)

    def test_leftover_goes_to_the_first(self):
        # 16384 = 3 * 5461 + 1
        self.assertEqual([(0, 5461), (5462, 10922), (10923, 16383)],
                         Tasks.slot_ranges(3))


class RedisClusterTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(RedisClusterTasksTest, self).setUp()
        self.tasks = Tasks.__new__(Tasks)
        self.guests = {}
        # MagicMocks on the class are not bound: they get the arguments
        # the code passes, without self.
        for name, mock in (
                ('get_guest', MagicMock(side_effect=self._guest)),
                ('get_ip', MagicMock(side_effect=lambda i: 'ip-' + i.id)),
                ('_all_instances_ready', MagicMock(return_value=True)),
                ('reset_task', MagicMock()),
                ('update_statuses_on_failure', MagicMock())):
            patcher = patch.object(Tasks, name, mock)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.load = self._patch(taskmanager.Instance, 'load',
                                side_effect=lambda ctx, i: MagicMock(id=i))
        self.delete = self._patch(taskmanager.Instance, 'delete')
        self.find_all = self._patch(taskmanager.DBInstance, 'find_all')

    def _patch(self, target, name, **kwargs):
        patcher = patch.object(target, name, **kwargs)
        mock = patcher.start()
        self.addCleanup(patcher.stop)
        return mock

    def _guest(self, instance):
        return self.guests.setdefault(instance.id, MagicMock(name=instance.id))

    def _members(self, *ids, replicas=()):
        """Members by id; replicas are (replica id, its master's id)."""
        rows = [MagicMock(id=i, type='member', shard_id='s-' + i)
                for i in ids]
        rows += [MagicMock(id=r, type='replica', shard_id='s-' + m)
                 for r, m in replicas]
        self.find_all.return_value.all.return_value = rows

    def _nodes(self, *nodes):
        """CLUSTER NODES, as any node tells it: (id, role, master id, has
        slots).
        """
        listed = [
            {'id': 'node-' + i, 'address': 'ip-' + i, 'role': role,
             'master_id': master and 'node-' + master, 'has_slots': slots}
            for i, role, master, slots in nodes]
        for i, _role, _master, _slots in nodes:
            guest = self._guest(MagicMock(id=i))
            guest.get_node_id.return_value = 'node-' + i
            guest.get_cluster_nodes.return_value = listed

    def test_create(self):
        self._members('a', 'b', 'c')
        self.tasks.create_cluster(None, 'cluster')
        a, b, c = (self.guests[i] for i in 'abc')
        passwords = {g.cluster_init.call_args[0][0] for g in (a, b, c)}
        self.assertEqual(1, len(passwords))
        a.cluster_meet.assert_not_called()
        b.cluster_meet.assert_called_once_with('ip-a', '6379')
        c.cluster_meet.assert_called_once_with('ip-a', '6379')
        self.assertEqual([call(0, 5461)], a.cluster_addslots.call_args_list)
        self.assertEqual([call(10923, 16383)],
                         c.cluster_addslots.call_args_list)
        for guest in (a, b, c):
            guest.cluster_wait.assert_called_once_with(3)
            guest.cluster_complete.assert_called_once_with()
        self.tasks.reset_task.assert_called_once_with()

    def test_grow(self):
        # Every new member joins, waits and completes: the guests were a
        # map, used up by the first loop.
        self._members('a', 'b', 'c', 'd', 'e')
        self._guest(MagicMock(id='a')).get_node_ip.return_value = [
            '10.0.0.1', '6379']
        self.guests['a'].get_cluster_password.return_value = 'pw'
        self.guests['a'].get_root_password.return_value = 'rootpw'
        self.tasks.grow_cluster(None, 'cluster', ['d', 'e'])
        for new in ('d', 'e'):
            guest = self.guests[new]
            guest.cluster_init.assert_called_once_with('pw')
            # The root user the cluster has, for clients sent here.
            guest.enable_root_with_password.assert_called_once_with('rootpw')
            guest.cluster_meet.assert_called_once_with('10.0.0.1', '6379')
            guest.cluster_wait.assert_called_once_with(5)
            guest.cluster_complete.assert_called_once_with()
        self.guests['a'].cluster_rebalance.assert_called_once_with(
            use_empty_masters=True)
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once_with()

    def test_shrink(self):
        self._members('a', 'b', 'c')
        self._nodes(('a', 'master', None, True), ('b', 'master', None, True),
                    ('c', 'master', None, True))
        self.tasks.shrink_cluster(None, 'cluster', ['c'])
        head = self.guests['a']
        self.assertEqual(
            [call.cluster_rebalance(weights={'node-c': 0}),
             call.cluster_del_node('node-c')],
            [c for c in head.mock_calls
             if c[0] in ('cluster_rebalance', 'cluster_del_node')])
        self.assertEqual(1, self.delete.call_count)
        self.assertEqual('c', self.delete.call_args[0][0].id)
        self.tasks.reset_task.assert_called_once_with()

    def test_shrink_failure_keeps_the_members_and_clears_the_task(self):
        self._members('a', 'b', 'c')
        self._nodes(('a', 'master', None, True), ('b', 'master', None, True),
                    ('c', 'master', None, True))
        self._guest(MagicMock(id='a')).cluster_rebalance.side_effect = (
            Exception('slots stuck'))
        self.tasks.shrink_cluster(None, 'cluster', ['c'])
        self.delete.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster', status=inst_tasks.InstanceTasks.SHRINKING_ERROR)
        self.tasks.reset_task.assert_called_once_with()

    def test_grow_without_root(self):
        self._members('a', 'b', 'c', 'd')
        self._guest(MagicMock(id='a')).get_node_ip.return_value = [
            '10.0.0.1', '6379']
        self.guests['a'].get_root_password.return_value = None
        self.tasks.grow_cluster(None, 'cluster', ['d'])
        self.guests['d'].enable_root_with_password.assert_not_called()
        self.guests['d'].cluster_complete.assert_called_once_with()

    def test_create_with_replicas(self):
        # Masters a, b, c; d follows a, e follows b, f follows c.
        self._members('a', 'b', 'c',
                      replicas=(('d', 'a'), ('e', 'b'), ('f', 'c')))
        for i in 'abcdef':
            self._guest(MagicMock(id=i)).get_node_id.return_value = (
                'node-' + i)
        self.tasks.create_cluster(None, 'cluster')
        # The masters alone share the slots.
        self.assertEqual([call(0, 5461)],
                         self.guests['a'].cluster_addslots.call_args_list)
        for r in 'def':
            self.guests[r].cluster_addslots.assert_not_called()
        for r, m in (('d', 'a'), ('e', 'b'), ('f', 'c')):
            self.guests[r].cluster_replicate.assert_called_once_with(
                'node-' + m)
        for guest in self.guests.values():
            guest.cluster_wait.assert_called_once_with(6)
            guest.cluster_complete.assert_called_once_with()

    def test_grow_by_a_master_and_its_replica(self):
        self._members('a', 'b', 'c', 'g',
                      replicas=(('d', 'a'), ('e', 'b'), ('f', 'c'),
                                ('h', 'g')))
        head = self._guest(MagicMock(id='a'))
        head.get_node_ip.return_value = ['10.0.0.1', '6379']
        head.get_root_password.return_value = None
        self._guest(MagicMock(id='g')).get_node_id.return_value = 'node-g'
        order = MagicMock()
        order.attach_mock(self._guest(MagicMock(id='h')).cluster_replicate,
                          'replicate')
        order.attach_mock(head.cluster_rebalance, 'rebalance')
        self.tasks.grow_cluster(None, 'cluster', ['g', 'h'])
        # The replica follows its master before the rebalance, or it would
        # take slots as an empty master.
        self.assertEqual(['replicate', 'rebalance'],
                         [c[0] for c in order.mock_calls])
        self.guests['h'].cluster_replicate.assert_called_once_with('node-g')
        self.guests['g'].cluster_addslots.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_not_called()

    def test_shrink_a_master_whose_replica_stays(self):
        # b leaves; its replica e stays and follows the master with the
        # fewest replicas, c.
        self._members('a', 'b', 'c', replicas=(('d', 'a'), ('e', 'b')))
        self._nodes(('a', 'master', None, True), ('b', 'master', None, True),
                    ('c', 'master', None, True), ('d', 'replica', 'a', False),
                    ('e', 'replica', 'b', False))
        self.tasks.shrink_cluster(None, 'cluster', ['b'])
        head = self.guests['a']
        head.cluster_rebalance.assert_called_once_with(weights={'node-b': 0})
        self.guests['e'].cluster_replicate.assert_called_once_with('node-c')
        head.cluster_del_node.assert_called_once_with('node-b')
        self.tasks.update_statuses_on_failure.assert_not_called()

    def test_shrink_after_a_failover_goes_by_the_live_roles(self):
        # b failed over to its replica e: removing e drains slots, b is a
        # replica now and is removed first.
        self._members('a', 'b', 'c', replicas=(('e', 'b'),))
        self._nodes(('a', 'master', None, True), ('b', 'replica', 'e', False),
                    ('c', 'master', None, True), ('e', 'master', None, True))
        self.tasks.shrink_cluster(None, 'cluster', ['b', 'e'])
        head = self.guests['a']
        head.cluster_rebalance.assert_called_once_with(weights={'node-e': 0})
        self.assertEqual([call('node-b'), call('node-e')],
                         head.cluster_del_node.call_args_list)

    def test_shrink_must_keep_a_master(self):
        # The replica d would be all that is left.
        self._members('a', 'b', replicas=(('d', 'a'),))
        self._nodes(('a', 'master', None, True), ('b', 'master', None, True),
                    ('d', 'replica', 'a', False))
        self.tasks.shrink_cluster(None, 'cluster', ['b', 'a'])
        self.guests['d'].get_cluster_nodes.assert_called_once_with()
        self.delete.assert_not_called()
        self.guests['d'].cluster_rebalance.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with(
            'cluster', status=inst_tasks.InstanceTasks.SHRINKING_ERROR)
