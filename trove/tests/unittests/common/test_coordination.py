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

"""Which replicas cover which keys: the rendezvous hash and the group
membership.
"""

from unittest import mock

from trove.common import coordination
from trove.tests.unittests import trove_testtools


class RendezvousTest(trove_testtools.TestCase):

    def test_the_same_everywhere(self):
        members = [b'tm-a:1', b'tm-b:2', b'tm-c:3', b'tm-d:4']
        for key in ('c1', b'c2', 'a-cluster-id'):
            first = coordination.coverers(key, members, 2)
            self.assertEqual(2, len(first))
            self.assertEqual(first, coordination.coverers(
                key, list(reversed(members)), 2))
            self.assertEqual(first, coordination.coverers(
                key, sorted(members, reverse=True), 2))

    def test_fewer_members_than_k_and_no_k(self):
        self.assertEqual([b'a'], coordination.coverers('c1', [b'a'], 2))
        self.assertEqual([], coordination.coverers('c1', [], 2))
        self.assertEqual([], coordination.coverers('c1', [b'a'], 0))
        self.assertTrue(coordination.covers(b'a', 'c1', [b'a', b'b'], 2))
        self.assertFalse(coordination.covers(b'z', 'c1', [b'a', b'b'], 2))

    def test_ties_break_by_id(self):
        with mock.patch.object(coordination, 'hrw_score', return_value=7):
            self.assertEqual([b'a', b'b'], coordination.coverers(
                'c1', [b'c', b'b', b'a'], 2))

    def test_balanced_and_minimal_reshuffle(self):
        members = [('tm-%d' % n).encode() for n in range(4)]
        keys = ['cluster-%d' % n for n in range(4000)]
        before = {key: coordination.coverers(key, members, 2) for key in keys}
        load = {m: sum(1 for c in before.values() if m in c)
                for m in members}
        # Each of 4 members covers about half of the keys (2 of 4).
        for count in load.values():
            self.assertLess(abs(count - 2000), 300, load)
        # A fifth member: only the keys it now covers move, about 2/5
        # of them, and every key stays covered by 2.
        after = {key: coordination.coverers(key, members + [b'tm-4'], 2)
                 for key in keys}
        moved = sum(1 for key in keys if before[key] != after[key])
        self.assertLess(abs(moved - 1600), 300, moved)
        for key in keys:
            self.assertEqual(2, len(after[key]))
            if before[key] != after[key]:
                self.assertIn(b'tm-4', after[key])
        # The mixer: an FNV sum and its mix differ, and the mix is 64 bits.
        self.assertNotEqual(coordination.mix64(1), 1)
        self.assertLess(coordination.hrw_score('k', 'm'), 1 << 64)


class CoordinatorTest(trove_testtools.TestCase):

    def setUp(self):
        super(CoordinatorTest, self).setUp()
        self.patch_conf_property('cluster_recovery_coverage', 2)
        self.backend = mock.patch('tooz.coordination.get_coordinator').start()
        self.tooz = self.backend.return_value
        self.members = {b'tm-a', b'tm-b', b'tm-c'}
        self.tooz.get_members.return_value.get.side_effect = (
            lambda: set(self.members))
        self.addCleanup(mock.patch.stopall)

    def _coordinator(self, url):
        self.patch_conf_property('backend_url', url, section='coordination')
        coordinator = coordination.Coordinator(b'g', b'tm-a')
        coordinator.start()
        return coordinator

    def test_without_a_backend_this_replica_covers_everything(self):
        coordinator = self._coordinator(None)
        self.assertFalse(coordinator.active)
        self.assertEqual([b'tm-a'], coordinator.members())
        self.assertEqual({'c1', 'c2'}, coordinator.covered(['c1', 'c2']))
        self.assertTrue(coordinator.covers('c9'))
        self.backend.assert_not_called()

    def test_joins_and_covers_its_share(self):
        coordinator = self._coordinator('etcd3+http://e:2379')
        self.backend.assert_called_once_with('etcd3+http://e:2379', b'tm-a')
        self.tooz.start.assert_called_once_with(start_heart=True)
        self.tooz.join_group_create.assert_called_once_with(b'g')
        self.assertEqual([b'tm-a', b'tm-b', b'tm-c'], coordinator.members())
        keys = ['cluster-%d' % n for n in range(300)]
        covered = coordinator.covered(keys)
        self.assertLess(abs(len(covered) - 200), 60, len(covered))
        for key in keys:
            self.assertEqual(key in covered, coordination.covers(
                b'tm-a', key, [b'tm-a', b'tm-b', b'tm-c'], 2))
        # Two replicas: both cover everything.
        self.members = {b'tm-a', b'tm-b'}
        coordinator._fetched_at = None
        self.assertEqual(set(keys), coordinator.covered(keys))

    def test_the_membership_is_cached(self):
        coordinator = self._coordinator('etcd3+http://e:2379')
        coordinator.members()
        self.assertEqual(1, self.tooz.get_members.call_count, 'first')
        coordinator.members()
        self.assertEqual(1, self.tooz.get_members.call_count, 'second')
        coordinator.covers('c1')
        self.assertEqual(1, self.tooz.get_members.call_count, 'covers')
        # Past the membership timeout: asked again.
        later = coordinator._fetched_at + 31
        with mock.patch.object(coordination.time, 'monotonic',
                               return_value=later):
            coordinator.members()
        self.assertEqual(2, self.tooz.get_members.call_count)

    def test_joins_again_when_the_membership_lapsed(self):
        coordinator = self._coordinator('etcd3+http://e:2379')
        self.members = {b'tm-b'}
        coordinator._fetched_at = None
        members = coordinator.members()
        self.assertEqual(2, self.tooz.join_group_create.call_count)
        self.assertEqual([b'tm-b'], members)

    def test_a_backend_that_never_answered_or_stopped_answering(self):
        self.tooz.start.side_effect = Exception('refused')
        coordinator = self._coordinator('etcd3+http://e:2379')
        self.assertFalse(coordinator.connected)
        self.assertEqual([b'tm-a'], coordinator.members())
        self.assertEqual({'c1'}, coordinator.covered(['c1']))
        # It answers: joined, the membership read.
        self.tooz.start.side_effect = None
        coordinator._fetched_at = None
        self.assertEqual([b'tm-a', b'tm-b', b'tm-c'], coordinator.members())
        # It stops answering: the last membership stands.
        self.tooz.get_members.return_value.get.side_effect = Exception('gone')
        coordinator._fetched_at = None
        self.assertEqual([b'tm-a', b'tm-b', b'tm-c'], coordinator.members())
        self.assertTrue(coordinator._failing)

    def test_stop_leaves_the_group(self):
        coordinator = self._coordinator('etcd3+http://e:2379')
        coordinator.stop()
        self.tooz.leave_group.assert_called_once_with(b'g')
        self.tooz.stop.assert_called_once_with()
        self.assertFalse(coordinator.connected)
