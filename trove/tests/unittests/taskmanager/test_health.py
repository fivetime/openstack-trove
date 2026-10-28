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

import datetime
import json
import time
from unittest import mock

from oslo_utils import timeutils

from trove.cluster.models import DBCluster
from trove.cluster.models import DBClusterHealth
from trove.cluster.tasks import ClusterTasks
from trove.cluster import views as cluster_views
from trove.common import cluster_health as ch
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import (
    recovery as common_recovery)
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.galera_common import (
    recovery as galera_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    recovery as pg_recovery)
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import recovery
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class HealthTestBase(trove_testtools.TestCase):
    """On the real tables."""

    def setUp(self):
        super(HealthTestBase, self).setUp()
        try:
            util.init_db()
        except exception.BadRequest:
            pass
        self.made = []
        self.addCleanup(self._clean)

    def _clean(self):
        for row in reversed(self.made):
            try:
                row.delete()
            except Exception:
                pass

    def _cluster(self, n=3, stale=(), task=ClusterTasks.NONE):
        cluster = DBCluster(task, id=utils.generate_uuid(), name='c',
                            tenant_id='t', datastore_version_id='v1',
                            created=timeutils.utcnow())
        cluster.save()
        self.made.append(cluster)
        members = []
        for i in range(n):
            member = DBInstance(InstanceTasks.NONE, id=utils.generate_uuid(),
                                name='c-member-%d' % (i + 1), tenant_id='t',
                                cluster_id=cluster.id,
                                datastore_version_id='v1', deleted=False,
                                created=timeutils.utcnow())
            member.save()
            self.made.append(member)
            status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                           id=utils.generate_uuid(),
                                           instance_id=member.id)
            status.save()
            self.made.append(status)
            if i in stale:
                with InstanceServiceStatus.query() as query:
                    query.filter_by(instance_id=member.id).update(
                        {'updated_at': timeutils.utcnow() -
                         datetime.timedelta(seconds=600)})
            members.append(member)
        return cluster, members

    def _recorded(self, cluster_id):
        row = DBClusterHealth.find_by(id=cluster_id)
        self.made.append(row)
        return row.status, json.loads(row.detail), row.since


class TestHealthNotification(HealthTestBase):
    """A change of the health is told, once."""

    def setUp(self):
        super(TestHealthNotification, self).setUp()
        self.notifier = mock.patch.object(
            health.rpc, 'get_notifier').start().return_value
        self.addCleanup(mock.patch.stopall)

    def test_every_change_is_told_once(self):
        cluster, members = self._cluster()
        healthy = ch.combine([('c-member-1', ch.view(ch.HEALTHY)),
                              ('c-member-2', ch.view(ch.HEALTHY))], [])
        self.assertTrue(health.record(cluster.id, healthy))
        self._recorded(cluster.id)
        # The same again (another task manager, the next look): nothing.
        self.assertFalse(health.record(cluster.id, healthy))
        degraded = ch.combine([('c-member-1', ch.view(ch.HEALTHY))],
                              ['c-member-2'])
        self.assertTrue(health.record(cluster.id, degraded))

        told = [c[0] for c in self.notifier.info.call_args_list]
        self.assertEqual(['trove.cluster.health'] * 2, [t[1] for t in told])
        first, second = told[0][2], told[1][2]
        self.assertEqual(
            {'cluster_id': cluster.id, 'cluster_name': 'c',
             'tenant_id': 't', 'datastore_version_id': 'v1',
             'status': ch.HEALTHY, 'previous_status': None,
             'members_out': [], 'reason': None, 'writers': [],
             'previous_writers': [], 'writer_changes': 0},
            {k: v for k, v in first.items() if k not in ('since', 'region')})
        self.assertEqual(
            (ch.DEGRADED, ch.HEALTHY, ['c-member-2'],
             'not answering: c-member-2'),
            (second['status'], second['previous_status'],
             second['members_out'], second['reason']))
        self.assertEqual(self._recorded(cluster.id)[2].isoformat()[:19],
                         second['since'][:19])

    def test_a_notification_not_sent_leaves_the_record(self):
        self.notifier.info.side_effect = RuntimeError('no broker')
        cluster, members = self._cluster()
        self.assertTrue(health.record(cluster.id, ch.combine(
            [('c-member-1', ch.view(ch.HEALTHY))], [])))
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])


class TestHealthWatch(HealthTestBase):

    def _watch(self, answers):
        """A watch whose members answer as given, by name: a view, None
        (watches nothing) or an exception (does not answer).
        """
        def guest(context, member):
            client = mock.Mock()
            answer = answers[member.name]
            if isinstance(answer, Exception):
                client.get_cluster_health.side_effect = answer
            else:
                client.get_cluster_health.return_value = answer
            return client
        return health.HealthWatch('redis', guest_factory=guest)

    def test_a_cluster_short_of_a_member(self):
        cluster, members = self._cluster(stale=(2,))
        seen = ch.view(ch.DEGRADED, ['10.0.0.3'], 'failing: 10.0.0.3')
        watch = self._watch({
            'c-member-1': dict(seen, me='10.0.0.1'),
            'c-member-2': dict(seen, me='10.0.0.2'),
            # Its heartbeat is stale: not asked.
            'c-member-3': AssertionError('asked')})

        watch.check_now(None, cluster, 'test')

        status, detail, since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['10.0.0.3', 'c-member-3'], detail['out'])
        self.assertEqual('not answering: c-member-3; failing: 10.0.0.3 '
                         '(c-member-1, c-member-2)', detail['reason'])
        self.assertEqual({'10.0.0.1': 'c-member-1',
                          '10.0.0.2': 'c-member-2'}, detail['members'])
        self.assertTrue(watch.still_watching(cluster.id))
        self.assertEqual(120, watch.look_again_in(cluster.id))

    def test_whole_again_and_the_names_learnt(self):
        cluster, members = self._cluster()
        watch = self._watch({
            'c-member-%d' % i: ch.view(ch.HEALTHY, me='10.0.0.%d' % i)
            for i in (1, 2, 3)})
        watch.check_now(None, cluster, 'test')
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])
        self.assertFalse(watch.still_watching(cluster.id))
        # The third is gone now: the others name it by its address, which
        # the record maps to its name.
        seen = ch.view(ch.DEGRADED, ['10.0.0.3'], 'failing: 10.0.0.3')
        watch = self._watch({'c-member-1': seen, 'c-member-2': seen,
                             'c-member-3': RuntimeError('timed out')})
        watch.check_now(None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(['c-member-3'], detail['out'])

    def _slow_look(self, slow, records=None):
        """A look at a cluster whose primary c-member-3 is gone: the others
        first answer DOWN, then (asked again) c-member-1 takes the writes;
        c-member-3 keeps the look waiting, then answers or fails as
        ``slow`` gives. Returns the record and who was asked; ``records``
        gets every record written, with whether c-member-3's call had
        ended then.
        """
        cluster, members = self._cluster()
        down = ch.view(ch.DOWN, ['10.0.0.3'],
                       'the sentinels hold the primary down')
        told = {'c-member-1': [dict(down, me='10.0.0.1'),
                               dict(down, me='10.0.0.1', writable=True)],
                'c-member-2': [dict(down, me='10.0.0.2'),
                               dict(down, me='10.0.0.2')],
                'c-member-3': [slow]}
        asked = []
        ended = []

        def answer(name):
            asked.append(name)
            # The last answer, once the others are given, again.
            given = told[name].pop(0) if len(told[name]) > 1 \
                else told[name][0]
            if name == 'c-member-3':
                time.sleep(0.3)
                ended.append(True)
            if isinstance(given, Exception):
                raise given
            return given

        def guest(context, member):
            client = mock.Mock()
            client.get_cluster_health.side_effect = (
                lambda timeout: answer(member.name))
            return client

        real_record = health.record

        def record(cluster_id, view):
            if records is not None:
                records.append((bool(ended), view['status'],
                                view.get('writers')))
            return real_record(cluster_id, view)

        with mock.patch.object(health, 'ANSWER_MAX_AGE', 0.1, create=True), \
                mock.patch.object(health, 'record', side_effect=record):
            health.HealthWatch('redis', guest_factory=guest).check_now(
                None, cluster, 'test')
        return self._recorded(cluster.id), sorted(asked)

    def test_asked_again_when_a_member_kept_the_look_waiting(self):
        # A Redis sentinel primary just powered off, its heartbeat not
        # stale yet (production, 2026-10-26): the look waits the whole
        # timeout for it, while the replica the sentinels promote takes
        # the writes. What the others said when the look began is not
        # recorded; what they say then is.
        (status, detail, _since), asked = self._slow_look(
            exception.GuestTimeout())
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['10.0.0.3', 'c-member-3'], detail['out'])
        # Asked again while it kept the look waiting, and at the end.
        self.assertEqual(1, asked.count('c-member-3'))
        self.assertGreaterEqual(asked.count('c-member-1'), 3)
        self.assertEqual(asked.count('c-member-1'),
                         asked.count('c-member-2'))

    def test_the_writer_known_while_a_member_keeps_the_look_waiting(self):
        # mc-valkey-s (production, Incus, 2026-10-28): the replica the
        # sentinels promoted took the writes 4 s after the event, Octavia
        # had it as the primary then; the record named it 26 s later, once
        # the call to the powered off member had timed out.
        records = []
        (status, detail, _since), _asked = self._slow_look(
            exception.GuestTimeout(), records)
        before = [r for r in records if not r[0]]
        self.assertTrue(before, records)
        self.assertEqual((False, ch.DEGRADED, ['c-member-1']), before[0])
        self.assertEqual(['c-member-1'], detail['writers'])

    def test_asked_again_when_a_member_was_slow_to_answer(self):
        # A primary hard rebooted (production, 2026-10-26): it answers,
        # late; the others are asked again, not it.
        (status, detail, _since), asked = self._slow_look(None)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(1, asked.count('c-member-3'))
        self.assertGreaterEqual(asked.count('c-member-1'), 3)

    def test_asked_once_when_every_member_answers(self):
        cluster, members = self._cluster()
        asked = []

        def guest(context, member):
            asked.append(member.name)
            client = mock.Mock()
            client.get_cluster_health.return_value = ch.view(ch.HEALTHY)
            return client

        health.HealthWatch('redis', guest_factory=guest).check_now(
            None, cluster, 'test')
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])
        self.assertEqual(3, len(asked))

    def test_written_only_when_it_changes(self):
        cluster, members = self._cluster()
        answers = {'c-member-%d' % i: ch.view(ch.HEALTHY)
                   for i in (1, 2, 3)}
        self.assertTrue(health.record(cluster.id, ch.combine(
            list(answers.items()), [])))
        since = self._recorded(cluster.id)[2]
        self.assertFalse(health.record(cluster.id, ch.combine(
            list(answers.items()), [])))
        self.assertEqual(since, self._recorded(cluster.id)[2])

    def test_no_member_answers(self):
        cluster, members = self._cluster(stale=(0, 1, 2))
        self._watch({}).check_now(None, cluster, 'test')
        self.assertEqual(ch.DOWN, self._recorded(cluster.id)[0])

    def test_a_query_router_watches_nothing(self):
        cluster, members = self._cluster(n=2)
        self._watch({'c-member-1': ch.view(ch.HEALTHY),
                     'c-member-2': None}).check_now(None, cluster, 'test')
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])

    def test_guest_agents_from_before_the_watch(self):
        # They answer the call with an error: up, as their heartbeats say;
        # a timeout is no answer.
        cluster, members = self._cluster()
        self._watch({
            'c-member-1': exception.GuestError(original_message='NoSuch'),
            'c-member-2': exception.GuestError(original_message='NoSuch'),
            'c-member-3': exception.GuestTimeout()}).check_now(
                None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['c-member-3'], detail['out'])

    def test_a_member_whose_database_does_not_answer(self):
        cluster, members = self._cluster(n=2)
        self._watch({'c-member-1': ch.view(ch.HEALTHY),
                     'c-member-2': {'down': True, 'reason': 'no'}}
                    ).check_now(None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(['c-member-2'], detail['out'])

    def test_a_cluster_with_a_task_is_left(self):
        cluster, members = self._cluster(task=ClusterTasks.GROWING_CLUSTER)
        self._watch({}).check_now(None, cluster, 'test')
        self.assertRaises(exception.NotFound, DBClusterHealth.find_by,
                          id=cluster.id)

    def _member_task(self, member, task):
        with DBInstance.query() as query:
            query.filter_by(id=member.id).update({'task_id': task.code})

    def test_a_member_waiting_for_a_restart_is_looked_at(self):
        # A configuration group that needs a restart leaves the members
        # RESTART_REQUIRED until the tenant restarts them, running on as
        # they were: their cluster's health is still looked at.
        cluster, members = self._cluster()
        for member in members:
            self._member_task(member, InstanceTasks.RESTART_REQUIRED)
        seen = ch.view(ch.DEGRADED, ['10.0.0.3'], 'failing: 10.0.0.3')
        self._watch({'c-member-1': seen, 'c-member-2': seen,
                     'c-member-3': RuntimeError('timed out')}
                    ).check_now(None, cluster, 'test')
        self.assertEqual(ch.DEGRADED, self._recorded(cluster.id)[0])
        # A member being restarted is another matter: its cluster is left.
        cluster, members = self._cluster()
        self._member_task(members[0], InstanceTasks.REBOOTING)
        self._watch({}).check_now(None, cluster, 'test')
        self.assertRaises(exception.NotFound, DBClusterHealth.find_by,
                          id=cluster.id)

    def test_a_member_waiting_for_a_restart_is_not_busy(self):
        busy = common_recovery.ClusterRecovery._member_busy
        self.assertFalse(busy(mock.Mock(task_status=InstanceTasks.NONE)))
        self.assertFalse(busy(
            mock.Mock(task_status=InstanceTasks.RESTART_REQUIRED)))
        self.assertTrue(busy(mock.Mock(task_status=InstanceTasks.REBOOTING)))

    def test_the_hub_gives_the_clusters_that_heal_themselves_a_watch(self):
        hub = recovery.RecoveryHub(coordinator=mock.Mock())
        hub.cluster_manager = {'c1': 'redis', 'c2': 'couchbase'}
        self.assertIsInstance(hub._recovery_of(mock.Mock(id='c1')),
                              health.HealthWatch)
        # No clusters that watch: nothing.
        with mock.patch.object(recovery.strategy,
                               'load_taskmanager_strategy') as load:
            del load.return_value.cluster_recovery
            self.assertIsNone(hub._recovery_of(mock.Mock(id='c2')))


class TestRecoveredHealth(HealthTestBase):
    """The health of a Galera cluster, from the views its recovery gets."""

    def test_from_the_views(self):
        cluster, members = self._cluster()
        views = [(members[0], {'in_group': True}),
                 (members[1], {'in_group': False, 'waiting': True}),
                 (members[2], None)]
        galera = galera_recovery.GaleraClusterRecovery('mariadb')
        galera._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['c-member-2', 'c-member-3'], detail['out'])

        # Looked at again until healthy: a member that comes back says
        # nothing (seen on 2026-10-19 before).
        self.assertTrue(galera.still_watching(cluster.id))
        # As often as the health of [mariadb] says, not every recovery
        # interval.
        self.assertEqual(120, galera.look_again_in(cluster.id))

        # No member in a primary component: down.
        views = [(m, {'in_group': False}) for m in members]
        galera._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DOWN, status)
        self.assertTrue(detail['reason'].startswith('no member serves'))

        views = [(m, {'in_group': True}) for m in members]
        galera._record_health(cluster.id, members, views)
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])
        self.assertFalse(galera.still_watching(cluster.id))

    def test_a_postgresql_cluster_down_names_who_serves_the_reads(self):
        # The primary powered off (e2-pg18g, 2026-10-28): no member takes
        # writes, the standbys within the cluster's read grace still
        # serve reads; the reason says so, the status stays DOWN.
        cluster, members = self._cluster()
        detached = {'state': 'standby', 'receiver': None, 'writable': False}
        views = [(members[0], None),
                 (members[1], dict(detached, readable=True)),
                 (members[2], dict(detached, readable=False))]
        pg = pg_recovery.PostgresqlClusterRecovery('postgresql')
        pg._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DOWN, status)
        self.assertTrue(detail['reason'].startswith(
            'no member takes writes; reads served by c-member-2;'),
            detail['reason'])
        # None serves reads (no read grace, or past it; a guest of
        # before that does not say): only the writes.
        views = [(members[0], None),
                 (members[1], dict(detached)),
                 (members[2], dict(detached, readable=False))]
        pg._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DOWN, status)
        self.assertTrue(detail['reason'].startswith(
            'no member takes writes;'), detail['reason'])
        self.assertNotIn('reads served', detail['reason'])

    def test_the_scan_looks_at_a_cluster_not_looked_at_yet(self):
        new, _m = self._cluster()
        seen, _m = self._cluster()
        health.record(seen.id, ch.combine([('c-member-1',
                                            ch.view(ch.HEALTHY))], []))
        self.made.append(DBClusterHealth.find_by(id=seen.id))
        # Not healthy: looked at again by the scan, whatever a task
        # manager started meanwhile remembers.
        short, _m = self._cluster()
        health.record(short.id, ch.combine([('c-member-1',
                                             ch.view(ch.HEALTHY))],
                                           ['c-member-2']))
        self.made.append(DBClusterHealth.find_by(id=short.id))
        found = {row[0] for row in recovery.suspect_clusters(
            timeutils.utcnow() - datetime.timedelta(seconds=90))}
        self.assertIn(new.id, found)
        self.assertNotIn(seen.id, found)
        self.assertIn(short.id, found)


class TestWriters(HealthTestBase):
    """Who takes the writes, and how many times a member started to."""

    def setUp(self):
        super(TestWriters, self).setUp()
        self.notifier = mock.patch.object(
            health.rpc, 'get_notifier').start().return_value
        self.addCleanup(mock.patch.stopall)

    @staticmethod
    def _seen(writers, silent=()):
        names = ['c-member-%d' % i for i in (1, 2, 3)
                 if 'c-member-%d' % i not in silent]
        return ch.combine(
            [(n, ch.view(ch.DEGRADED if silent else ch.HEALTHY,
                         list(silent), 'out' if silent else None,
                         writable=n in writers)) for n in names],
            list(silent))

    def test_a_primary_that_fails_over_within_a_look(self):
        # A Redis sentinel primary hard rebooted (kb8, kb9 on 2026-10-27):
        # the promoted replica took the writes and the old primary came
        # back as a replica within one look, which found the cluster as
        # healthy as before. The health stays as it was; the writer moved.
        cluster, _m = self._cluster()
        self.assertTrue(health.record(cluster.id, self._seen(['c-member-3'])))
        status, detail, since = self._recorded(cluster.id)
        self.assertEqual((['c-member-3'], ['c-member-3'], 0),
                         (detail['writers'], detail['last_writers'],
                          detail['writer_changes']))
        # The first writers found start the count, not counted.
        self.assertIsNotNone(detail['writer_since'])

        self.assertTrue(health.record(cluster.id, self._seen(['c-member-1'])))
        status, detail, again = self._recorded(cluster.id)
        self.assertEqual(ch.HEALTHY, status)
        self.assertEqual((['c-member-1'], 1),
                         (detail['writers'], detail['writer_changes']))
        # Healthy since as before.
        self.assertEqual(since, again)
        told = self.notifier.info.call_args_list[-1][0][2]
        self.assertEqual(
            (ch.HEALTHY, ch.HEALTHY, ['c-member-1'], ['c-member-3'], 1),
            (told['status'], told['previous_status'], told['writers'],
             told['previous_writers'], told['writer_changes']))
        self.assertEqual(since.isoformat()[:19], told['since'][:19])
        # The other task manager records the same: not counted twice.
        self.assertFalse(health.record(cluster.id,
                                       self._seen(['c-member-1'])))

    def test_no_writer_in_the_middle_of_a_failover(self):
        # The old primary is gone and the replica not promoted yet: the
        # last writer is kept, and its return is no change.
        cluster, _m = self._cluster()
        health.record(cluster.id, self._seen(['c-member-1']))
        self.assertTrue(health.record(
            cluster.id, self._seen([], silent=['c-member-1'])))
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(([], ['c-member-1'], 0),
                         (detail['writers'], detail['last_writers'],
                          detail['writer_changes']))
        health.record(cluster.id, self._seen(['c-member-1']))
        self.assertEqual(0, self._recorded(cluster.id)[1]['writer_changes'])
        # A replica promoted after the gap is a change.
        health.record(cluster.id, self._seen([], silent=['c-member-1']))
        health.record(cluster.id, self._seen(['c-member-2'],
                                             silent=['c-member-1']))
        self.assertEqual(1, self._recorded(cluster.id)[1]['writer_changes'])

    def test_a_member_of_a_multi_primary_group_that_comes_back(self):
        # Every member takes writes: one out is no change, its return is
        # (it starts taking the writes again).
        cluster, _m = self._cluster()
        every = ['c-member-1', 'c-member-2', 'c-member-3']
        health.record(cluster.id, self._seen(every))
        health.record(cluster.id, self._seen(every[:2],
                                             silent=['c-member-3']))
        self.assertEqual(0, self._recorded(cluster.id)[1]['writer_changes'])
        health.record(cluster.id, self._seen(every))
        self.assertEqual(1, self._recorded(cluster.id)[1]['writer_changes'])

    def _recorded_meanwhile(self, cluster_id, health_seen, times=1):
        """The record as read, with another task manager recording
        ``health_seen`` right after each of the first ``times`` reads.
        """
        read = health._previous
        left = [times]

        def previous(cid):
            got = read(cid)
            if left[0]:
                left[0] -= 1
                with mock.patch.object(health, '_previous', read):
                    health.record(cid, health_seen)
            return got
        return mock.patch.object(health, '_previous', side_effect=previous)

    def test_a_record_another_task_manager_wrote_meanwhile_stays(self):
        # Two task managers look at the same cluster (2026-10-27): one finds
        # the promoted replica and records the move; the other, which looked
        # in the middle of the failover and read the record before that,
        # does not write the count and the writer of before back.
        cluster, _m = self._cluster()
        health.record(cluster.id, self._seen(['c-member-1']))
        with self._recorded_meanwhile(cluster.id, self._seen(['c-member-2'])):
            self.assertTrue(health.record(
                cluster.id, self._seen([], silent=['c-member-1'])))
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual((ch.DEGRADED, ['c-member-2'], 1),
                         (status, detail['last_writers'],
                          detail['writer_changes']))
        # The move is told once, by the one that found it.
        moved = [c[0][2] for c in self.notifier.info.call_args_list
                 if c[0][2]['writer_changes'] == 1 and
                 c[0][2]['previous_writers'] == ['c-member-1']]
        self.assertEqual(1, len(moved))
        # Its next look, the cluster as the other one found it: no change.
        self.assertTrue(health.record(cluster.id,
                                      self._seen(['c-member-2'])))
        self.assertEqual(1, self._recorded(cluster.id)[1]['writer_changes'])

    def test_the_same_health_recorded_meanwhile_is_no_change(self):
        cluster, _m = self._cluster()
        health.record(cluster.id, self._seen(['c-member-1']))
        seen = self._seen(['c-member-2'])
        with self._recorded_meanwhile(cluster.id, seen):
            self.assertFalse(health.record(cluster.id, seen))
        self.assertEqual(1, self._recorded(cluster.id)[1]['writer_changes'])

    def test_a_first_record_another_task_manager_wrote_meanwhile(self):
        cluster, _m = self._cluster()
        with self._recorded_meanwhile(cluster.id, self._seen(['c-member-1'])):
            self.assertTrue(health.record(cluster.id,
                                          self._seen(['c-member-2'])))
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual((['c-member-2'], 1),
                         (detail['last_writers'], detail['writer_changes']))

    def test_a_record_that_keeps_changing_is_left_to_the_next_look(self):
        cluster, _m = self._cluster()
        health.record(cluster.id, self._seen(['c-member-1']))
        others = iter([self._seen(['c-member-%d' % (2 + i % 2)])
                       for i in range(health.RECORD_ATTEMPTS)])
        read = health._previous

        def previous(cid):
            got = read(cid)
            with mock.patch.object(health, '_previous', read):
                health.record(cid, next(others))
            return got
        with mock.patch.object(health, '_previous', side_effect=previous):
            self.assertFalse(health.record(
                cluster.id, self._seen([], silent=['c-member-1'])))
        # What the others recorded stays, the last of them.
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual((ch.HEALTHY, health.RECORD_ATTEMPTS),
                         (status, detail['writer_changes']))

    def test_no_writers_named_leaves_the_record_as_before(self):
        # Cassandra, Vertica, or a guest agent that does not name them yet.
        cluster, _m = self._cluster()
        health.record(cluster.id, self._seen([]))
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(['members', 'out', 'reason'], sorted(detail))

    def test_a_watch_records_the_writer_its_members_name(self):
        cluster, _m = self._cluster()
        answers = {'c-member-%d' % i: ch.view(ch.HEALTHY, writable=i == 2)
                   for i in (1, 2, 3)}

        def guest(context, member):
            client = mock.Mock()
            client.get_cluster_health.return_value = answers[member.name]
            return client
        health.HealthWatch('redis', guest_factory=guest).check_now(
            None, cluster, 'test')
        self.assertEqual(['c-member-2'],
                         self._recorded(cluster.id)[1]['writers'])

    def test_a_recovery_records_the_writer_of_its_views(self):
        # A Galera member's view says writable when it is the writer.
        cluster, members = self._cluster()
        views = [(members[0], {'in_group': True}),
                 (members[1], {'in_group': True, 'writable': True}),
                 (members[2], {'in_group': True})]
        galera_recovery.GaleraClusterRecovery('mariadb')._record_health(
            cluster.id, members, views)
        self.assertEqual(['c-member-2'],
                         self._recorded(cluster.id)[1]['writers'])


class TestHealthView(HealthTestBase):

    def test_unknown_until_looked_at_then_recorded(self):
        cluster, _m = self._cluster()
        view = mock.Mock(spec=cluster_views.ClusterView)
        view.cluster = mock.Mock(health=None)
        self.assertEqual({'status': ch.UNKNOWN},
                         cluster_views.ClusterView._build_health(view))
        health.record(cluster.id, ch.combine([('c-member-1',
                                               ch.view(ch.HEALTHY))],
                                             ['c-member-2']))
        row = DBClusterHealth.find_by(id=cluster.id)
        self.made.append(row)
        view.cluster = mock.Mock(health=row)
        shown = cluster_views.ClusterView._build_health(view)
        self.assertEqual(ch.DEGRADED, shown['status'])
        self.assertEqual(['c-member-2'], shown['members_out'])
        self.assertEqual('not answering: c-member-2', shown['reason'])
        self.assertEqual(row.since, shown['since'])
        self.assertEqual(([], 0, None), (shown['writers'],
                                         shown['writer_changes'],
                                         shown['writer_since']))
        health.record(cluster.id, ch.combine(
            [('c-member-1', ch.view(ch.HEALTHY, writable=True))], []))
        view.cluster = mock.Mock(health=DBClusterHealth.find_by(
            id=cluster.id))
        shown = cluster_views.ClusterView._build_health(view)
        self.assertEqual((['c-member-1'], 0), (shown['writers'],
                                               shown['writer_changes']))
        self.assertIsNotNone(shown['writer_since'])

    def test_a_list_loads_them_at_once(self):
        one, _m = self._cluster()
        two, _m = self._cluster()
        health.record(one.id, ch.combine([('c-member-1',
                                           ch.view(ch.HEALTHY))], []))
        self.made.append(DBClusterHealth.find_by(id=one.id))
        clusters = [mock.Mock(id=one.id), mock.Mock(id=two.id)]
        listed = cluster_views.ClustersView(clusters)
        with mock.patch.object(listed, 'data_for_cluster',
                               side_effect=lambda c: c.health):
            rows = listed.data()['clusters']
        self.assertEqual(ch.HEALTHY, rows[0].status)
        self.assertIsNone(rows[1])
