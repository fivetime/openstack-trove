# Copyright 2026 Simon Zhou
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

from trove.common import cfg
from trove.common import coordination
from trove.conductor import cluster_events
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class Clock(object):
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class ClusterEventGateTest(trove_testtools.TestCase):

    def setUp(self):
        super(ClusterEventGateTest, self).setUp()
        self.clock = Clock()
        self.gate = cluster_events.ClusterEventGate(clock=self.clock)
        CONF.set_override('cluster_event_forward_interval', 30)
        self.addCleanup(CONF.clear_override,
                        'cluster_event_forward_interval')

    def test_same_kind_dropped_within_the_interval(self):
        self.assertTrue(self.gate.admit('c1', 'abnormal'))
        self.clock.now += 10
        self.assertFalse(self.gate.admit('c1', 'abnormal'))
        self.clock.now += 19
        self.assertFalse(self.gate.admit('c1', 'abnormal'))
        self.clock.now += 1
        self.assertTrue(self.gate.admit('c1', 'abnormal'))

    def test_clusters_apart(self):
        self.assertTrue(self.gate.admit('c1', 'abnormal'))
        self.assertTrue(self.gate.admit('c2', 'abnormal'))
        self.assertFalse(self.gate.admit('c2', 'abnormal'))

    def test_another_kind_passes(self):
        self.assertTrue(self.gate.admit('c1', 'abnormal'))
        self.assertTrue(self.gate.admit('c1', 'normal'))
        self.assertTrue(self.gate.admit('c1', 'abnormal'))
        self.assertFalse(self.gate.admit('c1', 'abnormal'))

    def test_lease_request_always_passes(self):
        for _ in range(3):
            self.assertTrue(self.gate.admit('c1', 'lease_request'))
        # And does not count as the cluster's last kind.
        self.assertTrue(self.gate.admit('c1', 'abnormal'))
        self.assertTrue(self.gate.admit('c1', 'lease_request'))
        self.assertFalse(self.gate.admit('c1', 'abnormal'))

    def test_zero_forwards_everything(self):
        CONF.set_override('cluster_event_forward_interval', 0)
        for _ in range(3):
            self.assertTrue(self.gate.admit('c1', 'abnormal'))

    def test_old_entries_pruned(self):
        for i in range(100):
            self.gate.admit('c%d' % i, 'abnormal')
        self.clock.now += 31
        self.gate.admit('fresh', 'abnormal')
        self.assertEqual(['fresh'], list(self.gate._last))


class ClusterEventRouteTest(trove_testtools.TestCase):

    def setUp(self):
        super(ClusterEventRouteTest, self).setUp()
        self.patch_conf_property('cluster_recovery_slots', 4)
        self.patch_conf_property('cluster_recovery_coverage', 2)
        self.patch_conf_property('backend_url', 'etcd3+http://e:2379',
                                 section='coordination')
        self.observer = mock.Mock()
        self.observer.members.return_value = [
            b'recovery-0', b'recovery-1', b'recovery-2', b'recovery-3']
        self.route = cluster_events.ClusterEventRoute(self.observer)

    def test_to_the_two_that_watch_the_cluster(self):
        servers = self.route.servers('c1', 'abnormal')
        self.assertEqual(
            [m.decode() for m in coordination.coverers(
                'c1', self.observer.members.return_value, 2)], servers)
        self.assertEqual(2, len(servers))
        self.observer.start.assert_called_once_with()

    def test_a_lease_request_goes_to_every_one(self):
        self.assertIsNone(self.route.servers('c1', 'lease_request'))

    def test_every_one_without_slots(self):
        self.patch_conf_property('cluster_recovery_slots', 0)
        self.assertIsNone(self.route.servers('c1', 'abnormal'))
        self.observer.members.assert_not_called()

    def test_every_one_without_a_backend(self):
        self.patch_conf_property('backend_url', None, section='coordination')
        self.assertIsNone(self.route.servers('c1', 'abnormal'))

    def test_every_one_without_a_membership(self):
        self.observer.members.return_value = []
        self.assertIsNone(self.route.servers('c1', 'abnormal'))
        self.observer.members.side_effect = Exception('backend')
        self.assertIsNone(self.route.servers('c1', 'abnormal'))

    def test_every_one_during_an_upgrade(self):
        # An older task manager is a member as host:pid; it has no slot
        # queue, so it must get every event.
        self.observer.members.return_value = [b'recovery-0', b'tm-a:7']
        self.assertIsNone(self.route.servers('c1', 'abnormal'))
