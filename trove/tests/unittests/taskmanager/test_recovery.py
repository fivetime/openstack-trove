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

"""The recovery hub: which clusters a task manager replica looks at,
and when.
"""

import datetime
from unittest import mock

from oslo_utils import timeutils

from trove.cluster.models import DBCluster
from trove.cluster.models import DBClusterHealth
from trove.cluster.tasks import ClusterTasks
from trove.common import cluster_health
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import health
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import recovery
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class HubTest(trove_testtools.TestCase):

    def setUp(self):
        super(HubTest, self).setUp()
        self.patch_conf_property('cluster_event_coalesce', 5)
        self.patch_conf_property('cluster_recovery_workers', 4)
        self.coordinator = mock.Mock()
        self.coordinator.covers.side_effect = lambda key: key != 'elsewhere'
        self.coordinator.covered.side_effect = lambda keys: {
            k for k in keys if k != 'elsewhere'}
        self.hub = recovery.RecoveryHub(self.coordinator)
        self.hub.context = 'ctx'
        self.now = 1000.0
        mock.patch.object(recovery.time, 'monotonic',
                          side_effect=lambda: self.now).start()
        self.addCleanup(mock.patch.stopall)

    @mock.patch.object(recovery.rpc, 'get_server')
    def test_a_held_slot_is_served_then_joined(self, get_server):
        self.patch_conf_property('taskmanager_queue', 'taskmanager')
        with mock.patch.object(recovery.threading, 'Thread') as thread:
            self.hub._serve_slot('recovery-1')
            target_fn = thread.call_args.kwargs['target']
            args = thread.call_args.kwargs['args']
        target_fn(*args)
        target = get_server.call_args.args[0]
        # Not the task manager's own topic: a server consumes its topic's
        # shared queue and fanout too, and would take the task manager's
        # casts (create_instance...) it has no method for. 2026-10-15 rev
        # 108 had them on the same topic for two minutes.
        self.assertEqual('taskmanager-slots', target.topic)
        self.assertNotEqual(recovery.CONF.taskmanager_queue, target.topic)
        self.assertEqual('recovery-1', target.server)
        self.assertEqual(['cluster_event'], [
            m for m in dir(get_server.call_args.args[1][0])
            if not m.startswith('_') and m not in ('hub', 'target')])
        get_server.return_value.start.assert_called_once_with()
        self.coordinator.slot_served.assert_called_once_with()
        endpoint = get_server.call_args.args[1][0]
        endpoint.cluster_event('ctx', 'c1', 'i1', 'abnormal',
                               payload={'reason': 'r'}, sent=1.0)
        self.assertIn('c1', self.hub.pending)
        # A lost slot, or the hub stopped: the server stops.
        server = get_server.return_value
        self.hub._stop_slot()
        server.stop.assert_called_once_with()
        self.assertIsNone(self.hub._slot_server)
        self.hub.stop()
        self.coordinator.stop.assert_called_once_with()

    @mock.patch.object(recovery.rpc, 'get_server')
    def test_a_slot_whose_server_does_not_start_is_given_up(self,
                                                            get_server):
        with mock.patch.object(recovery.threading, 'Thread') as thread:
            self.hub._serve_slot('recovery-2')
            target_fn = thread.call_args.kwargs['target']
            args = thread.call_args.kwargs['args']
        # Not up after the timeout: given up.
        self.hub._check_slot(self.now + 1)
        self.coordinator.give_up_slot.assert_not_called()
        self.hub._check_slot(self.now + recovery.SLOT_SERVE_TIMEOUT + 1)
        self.coordinator.give_up_slot.assert_called_once_with()
        # Its server comes up late: stopped at once, no member.
        target_fn(*args)
        get_server.return_value.stop.assert_called_once_with()
        self.coordinator.slot_served.assert_not_called()
        self.assertIsNone(self.hub._slot_server)

    def test_without_a_slot_the_membership_is_asked_now_and_then(self):
        self.hub._pool = mock.Mock()
        self.coordinator.watching = False
        self.hub._tick()
        self.hub._tick()
        self.assertEqual(1, self.coordinator.members.call_count)
        self.now += recovery.SLOT_POKE
        self.hub._tick()
        self.assertEqual(2, self.coordinator.members.call_count)
        self.coordinator.watching = True
        self.now += recovery.SLOT_POKE
        self.hub._tick()
        self.assertEqual(2, self.coordinator.members.call_count)

    def test_the_hub_wires_the_slot_callbacks(self):
        self.hub.start('ctx')
        self.addCleanup(self.hub.stop)
        self.assertEqual(self.hub._serve_slot,
                         self.coordinator.on_slot_held)
        self.assertEqual(self.hub._stop_slot,
                         self.coordinator.on_slot_lost)

    def test_an_event_asks_for_a_look_at_a_covered_cluster(self):
        self.hub.on_event('c1', 'i1', 'abnormal', {'reason': 'detached'})
        self.hub.on_event('elsewhere', 'i9', 'abnormal')
        self.assertEqual({'c1': (1000.0, 'event abnormal from member i1 '
                                         '(detached)')}, self.hub.pending)

    def test_the_looks_at_one_cluster_are_coalesced(self):
        self.hub.last_look['c1'] = 998.0
        self.hub.on_event('c1', 'i1', 'abnormal')
        self.hub.on_event('c1', 'i2', 'abnormal')
        self.hub.request_look('c1', 'later', delay=30)
        # One look, 5 s after the last one, with the earliest reason.
        self.assertEqual({'c1': (1003.0, 'event abnormal from member i1 '
                                         '(no reason)')}, self.hub.pending)
        self.hub.request_look('c1', 'sooner', delay=1)
        self.assertEqual((1003.0, 'event abnormal from member i1 '
                                  '(no reason)'), self.hub.pending['c1'])

    def test_the_tick_submits_the_due_looks_once(self):
        self.hub._pool = mock.Mock()
        self.hub.request_look('c1', 'now')
        self.hub.request_look('c2', 'later', delay=10)
        self.hub._tick()
        self.hub._pool.submit.assert_called_once_with(
            self.hub._look_safely, 'c1', 'now')
        self.assertEqual({'c1'}, self.hub.inflight)
        # An event during the look: one more look after it.
        self.hub.request_look('c1', 'again')
        self.hub._tick()
        self.assertEqual(1, self.hub._pool.submit.call_count)
        self.hub._look = mock.Mock()
        self.hub._look_safely('c1', 'now')
        self.assertEqual(set(), self.hub.inflight)
        self.assertEqual(1000.0, self.hub.last_look['c1'])
        self.now = 1006.0
        self.hub._tick()
        self.assertEqual([mock.call(self.hub._look_safely, 'c1', 'now'),
                          mock.call(self.hub._look_safely, 'c1', 'again')],
                         self.hub._pool.submit.call_args_list)

    @mock.patch.object(recovery.DBCluster, 'find_by')
    def test_a_look_runs_the_recovery_and_looks_again_while_watching(
            self, find_by):
        db_cluster = mock.Mock(id='c1', datastore_version_id='v1')
        find_by.return_value = db_cluster
        rec = mock.Mock()
        rec.look_again_in.return_value = 30
        rec.still_watching.return_value = True
        self.hub.cluster_manager['c1'] = 'postgresql'
        self.hub.recoveries['postgresql'] = rec
        self.hub._look('c1', 'event')
        find_by.assert_called_once_with(id='c1', deleted=False)
        rec.check_now.assert_called_once_with('ctx', db_cluster, 'event')
        self.assertEqual({'c1': (1030.0, 'still watching')},
                         self.hub.pending)
        rec.look_again_in.assert_called_once_with('c1')
        # Settled: nothing more.
        rec.still_watching.return_value = False
        self.hub.pending.clear()
        self.hub._look('c1', 'event')
        self.assertEqual({}, self.hub.pending)

    @mock.patch.object(recovery.datastore_models.DatastoreVersion,
                       'load_by_uuid')
    @mock.patch.object(recovery.strategy, 'load_taskmanager_strategy')
    def test_the_recovery_of_a_cluster_comes_from_its_datastore(
            self, load_strategy, load_by_uuid):
        load_by_uuid.side_effect = lambda i: mock.Mock(
            manager={'v-pxc': 'pxc', 'v-mysql': 'mysql'}[i])
        strategies = {'pxc': mock.Mock(), 'mysql': mock.Mock()}
        strategies['mysql'].cluster_recovery.return_value = None
        load_strategy.side_effect = lambda m: strategies[m]
        pxc = self.hub._recovery_of(mock.Mock(id='c1',
                                              datastore_version_id='v-pxc'))
        self.assertIs(strategies['pxc'].cluster_recovery.return_value, pxc)
        self.assertIs(pxc, self.hub._recovery_of(
            mock.Mock(id='c3', datastore_version_id='v-pxc')))
        self.assertIsNone(self.hub._recovery_of(
            mock.Mock(id='c2', datastore_version_id='v-mysql')))
        # Each version and each manager asked once.
        self.assertEqual(1, load_by_uuid.call_args_list.count(
            mock.call('v-pxc')))
        self.assertEqual(2, load_strategy.call_count)

    @mock.patch.object(recovery, 'suspect_clusters')
    def test_the_scan_looks_at_the_suspects_it_covers(self, suspects):
        stale = timeutils.utcnow() - datetime.timedelta(seconds=200)
        suspects.return_value = [
            ('c1', ClusterTasks.NONE.code, 'v1', stale),
            ('c2', ClusterTasks.RECOVERING_CLUSTER.code, 'v1', None),
            ('elsewhere', ClusterTasks.NONE.code, 'v1', stale)]
        self.hub.scan()
        self.assertEqual(
            {'c1': (1000.0, 'scan: the oldest heartbeat %s' % stale),
             'c2': (1000.0, 'scan: a recovery under way')},
            self.hub.pending)
        self.assertLess(suspects.call_args.args[0], timeutils.utcnow())
        # The sweep: every cluster.
        self.hub.pending.clear()
        self.hub.sweep()
        suspects.assert_called_with(None)
        self.assertEqual({'c1', 'c2'}, set(self.hub.pending))


class SuspectClustersTest(trove_testtools.TestCase):
    """The scan's query, on the real tables."""

    def setUp(self):
        super(SuspectClustersTest, self).setUp()
        try:
            util.init_db()
        except exception.BadRequest:
            # Set up earlier in this process and cleaned since: the
            # tables are there, the session too; only db_sync balks.
            pass
        self.addCleanup(self._clean)
        self.made = []

    def _clean(self):
        for row in reversed(self.made):
            try:
                row.delete()
            except Exception:
                pass

    def _cluster(self, task=ClusterTasks.NONE, deleted=False,
                 heartbeats=(0, 0, 0)):
        cluster = DBCluster(task, id=utils.generate_uuid(), name='c',
                            tenant_id='t', datastore_version_id='v1',
                            created=timeutils.utcnow())
        # The constructor sets deleted False.
        cluster.deleted = deleted
        cluster.save()
        self.made.append(cluster)
        for age in heartbeats:
            member = DBInstance(InstanceTasks.NONE, id=utils.generate_uuid(),
                                name='m', tenant_id='t', cluster_id=cluster.id,
                                datastore_version_id='v1', deleted=False,
                                created=timeutils.utcnow())
            member.save()
            self.made.append(member)
            status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                           id=utils.generate_uuid(),
                                           instance_id=member.id)
            status.save()
            self.made.append(status)
            if age:
                with InstanceServiceStatus.query() as query:
                    query.filter_by(instance_id=member.id).update(
                        {'updated_at': timeutils.utcnow() -
                         datetime.timedelta(seconds=age)})
        return cluster.id

    def test_the_suspects(self):
        # Without its health recorded a cluster is a suspect: looked at
        # once, by the scan.
        unseen = self._cluster()
        fresh = self._cluster()
        health.record(fresh, cluster_health.view(cluster_health.HEALTHY))
        self.made.append(DBClusterHealth.find_by(id=fresh))
        stale = self._cluster(heartbeats=(0, 200, 0))
        recovering = self._cluster(task=ClusterTasks.RECOVERING_CLUSTER)
        busy = self._cluster(task=ClusterTasks.GROWING_CLUSTER,
                             heartbeats=(200, 200, 200))
        gone = self._cluster(deleted=True, heartbeats=(200, 200, 200))
        stale_before = timeutils.utcnow() - datetime.timedelta(seconds=90)
        rows = recovery.suspect_clusters(stale_before)
        found = {row[0]: row for row in rows}
        self.assertIn(unseen, found)
        self.assertIn(stale, found)
        self.assertIn(recovering, found)
        self.assertNotIn(fresh, found)
        self.assertNotIn(busy, found)
        self.assertNotIn(gone, found)
        self.assertEqual(ClusterTasks.RECOVERING_CLUSTER.code,
                         found[recovering][1])
        self.assertEqual('v1', found[stale][2])
        self.assertLess(found[stale][3], stale_before)
        # Every cluster that is not deleted or busy.
        every = {row[0] for row in recovery.suspect_clusters(None)}
        self.assertTrue({fresh, stale, recovering} <= every)
        self.assertNotIn(busy, every)
        self.assertNotIn(gone, every)
