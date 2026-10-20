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

"""The task manager's failover of a PostgreSQL cluster: the rules on the
members' answers, and the reconciler that applies them.
"""

import time
from unittest import mock

from trove.cluster.tasks import ClusterTasks
from trove.common.strategies.cluster.experimental.common import (
    recovery as common_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    recovery as pg_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    topology)
from trove.instance.tasks import InstanceTasks
from trove.tests.unittests import trove_testtools

MiB = 1024 * 1024


def primary(ip, standbys=(), lsn=1000, timeline=1, no_standby_for=None,
            self_id=None, primary_id=None, state='primary'):
    return {'ip': ip, 'state': state, 'role': 'PRIMARY', 'mode': 'sync',
            'self_id': self_id, 'primary_id': primary_id or self_id,
            'standbys': {i: {'state': 'streaming', 'sync_state': 'quorum',
                             'lag_bytes': 0} for i in standbys},
            'lsn': lsn, 'timeline': timeline, 'no_standby_for': no_standby_for,
            'writable': True, 'fenced': False, 'lease_fresh': True}


def standby(ip, sender='10.0.0.1', status='streaming', lsn=1000, replay=None,
            timeline=1, disconnected_for=None, primary_id='i1',
            rebuilding=False, mode='sync', self_id=None, peers=None):
    answer = {'ip': ip, 'state': 'rebuilding' if rebuilding else 'standby',
              'role': 'STREAMING' if status == 'streaming' else 'DETACHED',
              'mode': mode, 'self_id': self_id, 'primary_id': primary_id,
              'receiver': {'status': status, 'sender_host': sender,
                           'last_msg_age': 1.0, 'replay_lag_bytes': 0}
              if status else None,
              'lsn': lsn, 'replay_lsn': lsn if replay is None else replay,
              'timeline': timeline, 'disconnected_for': disconnected_for,
              'writable': False, 'fenced': False, 'lease_fresh': False,
              'rebuilding': rebuilding}
    if peers is not None:
        answer['peers'] = peers
    return answer


def cut(*reached, **unreached):
    """What a standby reaches on the tenant network: the members given
    and not the primary (i1), unless said otherwise.
    """
    peers = {'i1': False}
    peers.update({i: True for i in reached})
    peers.update(unreached)
    return peers


def stopped(ip, primary_id='i1', fenced=False):
    return {'ip': ip, 'state': 'stopped', 'role': None, 'mode': 'sync',
            'primary_id': primary_id, 'writable': False, 'fenced': fenced,
            'lease_fresh': False}


def _top(answers):
    members = [mock.Mock(id=i) for i in answers]
    return topology.Topology(members, [(m, answers[m.id]) for m in members])


class TopologyTest(trove_testtools.TestCase):

    def test_the_leader(self):
        self.assertEqual((None, False), _top({'i1': None}).leader())
        self.assertEqual(('i1', False), _top(
            {'i1': primary('10.0.0.1'), 'i2': standby('10.0.0.2')}).leader())
        # A former primary back on a lower timeline is not the leader.
        self.assertEqual(('i2', False), _top(
            {'i1': primary('10.0.0.1', timeline=1),
             'i2': primary('10.0.0.2', timeline=2)}).leader())
        # Two primaries on one timeline: nobody is.
        self.assertEqual((None, True), _top(
            {'i1': primary('10.0.0.1', timeline=2),
             'i2': primary('10.0.0.2', timeline=2)}).leader())

    def test_whom_the_members_take_for_the_primary(self):
        top = _top({'i1': None,
                    'i2': standby('10.0.0.2', primary_id='i1',
                                  status='stopping'),
                    'i3': standby('10.0.0.3', primary_id='i1',
                                  sender='10.0.0.1')})
        self.assertEqual(('i1', '10.0.0.1'), top.expected_primary())
        self.assertEqual((None, None), _top({'i1': None}).expected_primary())

    def test_the_primary_is_dead(self):
        gone = standby('10.0.0.2', status=None, disconnected_for=40)
        fresh = standby('10.0.0.2', status=None, disconnected_for=10)
        cases = [
            # A standby still streams from it: alive, whatever else.
            ({'i1': None, 'i2': standby('10.0.0.2'), 'i3': gone},
             False, 'streams from it'),
            # No standby answers: nothing to go on.
            ({'i1': None, 'i2': None}, False, 'no standby that follows'),
            # A standby that follows another primary (just after a
            # failover) says nothing about this one.
            ({'i1': None, 'i2': standby('10.0.0.2', status=None,
                                        disconnected_for=40,
                                        primary_id='i9')},
             False, 'no standby that follows'),
            # A standby has not been without it for the grace period.
            ({'i1': None, 'i2': fresh, 'i3': gone}, False, 'has not been'),
            # Every standby without it, and it does not answer.
            ({'i1': None, 'i2': gone, 'i3': gone}, True, 'does not answer'),
            # It answers, and its server is down.
            ({'i1': stopped('10.0.0.1'), 'i2': gone}, True, 'server is down'),
            # It answers as a primary that has had no standby as long.
            ({'i1': primary('10.0.0.1', no_standby_for=35), 'i2': gone},
             True, 'has had no standby'),
            # It answers as a primary with a standby, or recently.
            ({'i1': primary('10.0.0.1', no_standby_for=5), 'i2': gone},
             False, 'reports a standby'),
            # It answers, being rebuilt.
            ({'i1': standby('10.0.0.1', rebuilding=True), 'i2': gone},
             False, 'answers as rebuilding'),
            # The standbys say what they reach on the tenant network:
            # not it, but each other (a majority without it): gone.
            ({'i1': None,
              'i2': standby('10.0.0.2', status=None, disconnected_for=40,
                            peers=cut('i3')),
              'i3': standby('10.0.0.3', status=None, disconnected_for=40,
                            peers=cut('i2'))},
             True, 'cut off on the tenant network'),
            # Not it, nor each other: they are cut off themselves.
            ({'i1': primary('10.0.0.1', no_standby_for=35),
              'i2': standby('10.0.0.2', status=None, disconnected_for=40,
                            peers=cut(i3=False)),
              'i3': standby('10.0.0.3', status=None, disconnected_for=40,
                            peers=cut(i2=False))},
             False, 'cut off themselves'),
            # One reaches it: it lives on the tenant network.
            ({'i1': None,
              'i2': standby('10.0.0.2', status=None, disconnected_for=40,
                            peers=cut('i3')),
              'i3': standby('10.0.0.3', status=None, disconnected_for=40,
                            peers=cut('i2', i1=True))},
             False, 'reaches it'),
            # Its server is down: the network does not matter.
            ({'i1': stopped('10.0.0.1'),
              'i2': standby('10.0.0.2', status=None, disconnected_for=40,
                            peers=cut(i3=False))},
             True, 'server is down'),
        ]
        for answers, dead, why in cases:
            got = topology.primary_dead(_top(answers), 'i1', '10.0.0.1', 30)
            self.assertEqual(dead, got[0], (answers, got))
            self.assertIn(why, got[1])

    def test_cut_off(self):
        def detached(ip, peers=None, **kwargs):
            return standby(ip, status=None, disconnected_for=40,
                           peers=peers, **kwargs)
        cases = [
            # No standby says what it reaches.
            ({'i1': None, 'i2': detached('10.0.0.2')}, None, None,
             'no standby says'),
            # Three members: the two standbys reach each other.
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3')),
              'i3': detached('10.0.0.3', cut('i2'))},
             True, ['i2', 'i3'], 'i2 reaches 1 other'),
            # One standby suffices to say so, as long as it reaches the
            # other (which may not answer the task manager).
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3')),
              'i3': None}, True, ['i2', 'i3'], 'cut off'),
            # Five members: a standby reaching one other is no majority.
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3')),
              'i3': detached('10.0.0.3', cut('i2')), 'i4': None,
              'i5': None}, False, None, '2 of 5'),
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3', 'i4')),
              'i3': detached('10.0.0.3', cut('i2')), 'i4': None,
              'i5': None}, True, ['i2', 'i3', 'i4'], 'reaches 2 other'),
            # A standby being rebuilt says nothing; one that does not
            # say whether it reaches the primary neither.
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3'),
                                         rebuilding=True),
              'i3': detached('10.0.0.3', {'i2': True})},
             False, None, 'cut off themselves'),
            # One reaches the primary: not cut off, whatever the others.
            ({'i1': None, 'i2': detached('10.0.0.2', cut('i3')),
              'i3': detached('10.0.0.3', cut('i2', i1=True))},
             False, None, 'i3 reaches it'),
        ]
        for answers, verdict, side, why in cases:
            got = topology.cut_off(_top(answers), 'i1')
            self.assertEqual((verdict, side), got[:2], (answers, got))
            self.assertIn(why, got[2])

    def test_the_alone_check(self):
        here = standby('10.0.0.2', status=None, disconnected_for=100,
                       peers=cut('i3'))
        there = standby('10.0.0.3', status=None, disconnected_for=90,
                        peers=cut('i2'))
        # The primary does not answer: nothing to check unless it is
        # known to have gone on alone, and then nothing to go on.
        self.assertIsNone(topology.alone_check(
            _top({'i1': None, 'i2': here, 'i3': there}), 'i1', 20))
        self.assertIsNone(topology.alone_check(
            _top({'i1': None, 'i2': here, 'i3': there}), 'i1', 20, False))
        got = topology.alone_check(
            _top({'i1': None, 'i2': here, 'i3': there}), 'i1', 20, True)
        self.assertEqual(False, got[0])
        self.assertIn('does not answer how long', got[1])
        # It answers, with a standby all along: nothing to check.
        self.assertIsNone(topology.alone_check(
            _top({'i1': primary('10.0.0.1', no_standby_for=None),
                  'i2': here, 'i3': there}), 'i1', 20))
        # Alone no longer than the standbys have been without it, give
        # or take the slack, and cut off: safe.
        got = topology.alone_check(
            _top({'i1': primary('10.0.0.1', no_standby_for=115),
                  'i2': here, 'i3': there}), 'i1', 20)
        self.assertEqual(True, got[0])
        self.assertIn('cut off on the tenant network since', got[1])
        # Alone for longer: it may have acknowledged commits alone.
        got = topology.alone_check(
            _top({'i1': primary('10.0.0.1', no_standby_for=125),
                  'i2': here, 'i3': there}), 'i1', 20)
        self.assertEqual(False, got[0])
        self.assertIn('longer than the standbys', got[1])
        # Not cut off: the standbys are.
        got = topology.alone_check(
            _top({'i1': primary('10.0.0.1', no_standby_for=100),
                  'i2': standby('10.0.0.2', status=None,
                                disconnected_for=100, peers=cut(i3=False)),
                  'i3': standby('10.0.0.3', status=None,
                                disconnected_for=100,
                                peers=cut(i2=False))}), 'i1', 20)
        self.assertEqual(False, got[0])
        self.assertIn('cut off themselves', got[1])

    def test_the_candidates_and_the_choice(self):
        top = _top({'i1': None,
                    'i2': standby('10.0.0.2', lsn=500, timeline=2),
                    'i3': standby('10.0.0.3', lsn=900, timeline=1),
                    'i4': standby('10.0.0.4', lsn=500, replay=400,
                                  timeline=2),
                    'i5': standby('10.0.0.5', rebuilding=True, timeline=2),
                    'i6': standby('10.0.0.6', lsn=None, timeline=2)})
        self.assertEqual(['i2', 'i4'], topology.candidates(top, 2))
        # The same received position: the most replayed, then the
        # lowest address.
        self.assertEqual('i2', topology.choose(top, ['i2', 'i4']))
        top.answers['i4']['replay_lsn'] = 500
        self.assertEqual('i2', topology.choose(top, ['i4', 'i2']))
        top.answers['i4']['lsn'] = 501
        self.assertEqual('i4', topology.choose(top, ['i2', 'i4']))

    def test_the_quorum(self):
        three = _top({'i1': None, 'i2': standby('10.0.0.2'),
                      'i3': standby('10.0.0.3')})
        one_gone = _top({'i1': None, 'i2': standby('10.0.0.2'), 'i3': None})
        five = _top({'i1': None, 'i2': standby('10.0.0.2'),
                     'i3': standby('10.0.0.3'), 'i4': standby('10.0.0.4'),
                     'i5': standby('10.0.0.5')})
        cases = [
            # A majority of the members has to be here.
            (one_gone, ['i2'], 'async', None, 0, False, '1 of 3 members'),
            (three, ['i2', 'i3'], 'async', None, 0, True, 'async mode'),
            # Sync: R + 1 > N over the standbys that streamed; a commit
            # waited for any one of them, so every one of them.
            (three, ['i2', 'i3'], 'sync', ['i2', 'i3'], 0, True,
             '2 of the 2'),
            (five, ['i2', 'i3', 'i4', 'i5'], 'sync',
             ['i2', 'i3', 'i4', 'i5'], 0, True, '4 of the 4'),
            (five, ['i2', 'i3', 'i4'], 'sync', ['i2', 'i3', 'i4', 'i5'], 0,
             False, '3 of the 4'),
            (five, ['i2', 'i3'], 'strict', ['i2', 'i3', 'i4', 'i5'], 0,
             False, '2 of the 4'),
            # Not known (or empty): every standby has to be a candidate.
            (three, ['i2', 'i3'], 'sync', None, 0, True, 'every standby'),
            (three, ['i2'], 'sync', None, 0, False, 'i3 cannot take over'),
            (three, ['i2', 'i3'], 'sync', [], 0, True, 'every standby'),
            # Forced after a while.
            (five, ['i2', 'i3'], 'sync', ['i2', 'i3', 'i4', 'i5'], 100,
             True, 'forced'),
        ]
        for top, cands, mode, sync_set, failing, ok, why in cases:
            got = topology.quorum(top, cands, mode, sync_set, 'i1', failing,
                                  force_after=60)
            self.assertEqual(ok, got[0], (cands, mode, sync_set, got))
            self.assertIn(why, got[1])
        # Sync: a primary that went on without any standby may have
        # acknowledged commits alone, unless the check says it could
        # not; strict never commits alone.
        cases = [
            ('sync', (False, 'it may have'), 0, False, 'went on without'),
            ('sync', (True, 'it could not have'), 0, True, 'could not'),
            ('strict', (False, 'it may have'), 0, True, '2 of the 2'),
            ('sync', (False, 'it may have'), 100, True, 'forced'),
        ]
        for mode, alone, failing, ok, why in cases:
            got = topology.quorum(three, ['i2', 'i3'], mode, ['i2', 'i3'],
                                  'i1', failing, force_after=60, alone=alone)
            self.assertEqual(ok, got[0], (mode, alone, got))
            self.assertIn(why, got[1])

    def test_the_lag(self):
        top = _top({'i2': standby('10.0.0.2', lsn=1000)})
        self.assertTrue(topology.lag_ok(top, 'i2', 1000 + MiB, MiB))
        self.assertFalse(topology.lag_ok(top, 'i2', 1001 + MiB, MiB))
        self.assertTrue(topology.lag_ok(top, 'i2', 5 * MiB, 0))
        self.assertTrue(topology.lag_ok(top, 'i2', None, MiB))


class RecoveryTestBase(trove_testtools.TestCase):

    IDS = ('i1', 'i2', 'i3')

    def setUp(self):
        super(RecoveryTestBase, self).setUp()
        self.members = [mock.Mock(id=i, task_status=InstanceTasks.NONE)
                        for i in self.IDS]
        self.answers = {}
        self.calls = []
        self.shared = []
        self.guests = {}

        def guest(context, member):
            if member.id not in self.guests:
                g = mock.Mock()

                def view(timeout=None, _id=member.id):
                    answer = self.answers.get(_id)
                    if answer is None:
                        raise Exception('timed out')
                    # A copy, as an answer over RPC is.
                    return dict(answer)
                g.get_recovery_view.side_effect = view
                for name in ('renew_lease', 'fence_member', 'follow_primary',
                             'rejoin_cluster', 'update_cluster_members',
                             'create_member_slot'):
                    getattr(g, name).side_effect = (
                        lambda *a, _n=name, _i=member.id, **k:
                        self.calls.append((_n, _i) + a + (
                            (k,) if k else ())))
                # Kept apart: told on every change of the set.
                g.remember_sync_set.side_effect = (
                    lambda *a, _i=member.id, lsn=None, alone=False:
                    self.shared.append(
                        (_i,) + a + ((lsn,) if lsn is not None else ()) +
                        (('alone',) if alone else ())))
                g.promote_member.side_effect = (
                    lambda members, mode, _i=member.id:
                    self.calls.append(('promote', _i, mode)) or
                    {'lsn': 2000, 'timeline': 2})
                g.get_cluster_context.return_value = {
                    'members': {i: '10.0.0.%s' % i[-1] for i in self.IDS}}
                self.guests[member.id] = g
            return self.guests[member.id]
        self.recovery = pg_recovery.PostgresqlClusterRecovery('postgresql',
                                                              guest)
        self.recovery._members = mock.Mock(return_value=self.members)
        self.recovery.conf = mock.Mock(
            cluster_peer_timeout=3, cluster_recovery_grace=30,
            cluster_recovery_interval=30, cluster_recovery_timeout=3600,
            cluster_bootstrap_jitter=0, cluster_failover_force_after=0,
            cluster_failover_max_lag_bytes=MiB,
            cluster_standby_rebuild_after=600, cluster_wal_timeout=10,
            cluster_lease_ttl=60, cluster_fence_grace=15)
        # As [postgresql]: no health options, the recovery's interval.
        del self.recovery.conf.cluster_health_interval
        # A member this replica does not reach is down for the whole
        # control plane (its heartbeat is stale), unless a test says.
        self.stale = mock.patch.object(
            pg_recovery.PostgresqlClusterRecovery, '_stale_members',
            side_effect=lambda ids: set(ids)).start()
        mock.patch.object(pg_recovery.CONF, 'cluster_recovery_check_interval',
                          30).start()
        self.db_cluster = mock.Mock(id='c1', task_status=ClusterTasks.NONE,
                                    updated=None)
        mock.patch.object(common_recovery.cluster_models.DBCluster, 'find_by',
                          return_value=self.db_cluster).start()
        mock.patch.object(common_recovery.time, 'sleep').start()

        def take(cluster_id):
            if self.db_cluster.task_status != ClusterTasks.NONE:
                return False
            self.db_cluster.task_status = ClusterTasks.RECOVERING_CLUSTER
            self.recovery.forming_since[cluster_id] = \
                common_recovery.time.monotonic()
            return True
        mock.patch.object(common_recovery.ClusterRecovery, '_take',
                          side_effect=take).start()

        def release(cluster_id, db_cluster=None):
            self.db_cluster.task_status = ClusterTasks.NONE
            self.recovery.forming_since.pop(cluster_id, None)
            self.recovery.waiting_since.pop(cluster_id, None)
        mock.patch.object(common_recovery.ClusterRecovery, '_release',
                          side_effect=release).start()
        self.addCleanup(mock.patch.stopall)

    def _check_at(self, seconds):
        with mock.patch.object(common_recovery.time, 'monotonic',
                               return_value=seconds):
            self.recovery.check('ctx', self.db_cluster)

    def _healthy(self):
        self.answers = {'i1': primary('10.0.0.1', ['i2', 'i3'],
                                      self_id='i1'),
                        'i2': standby('10.0.0.2', self_id='i2'),
                        'i3': standby('10.0.0.3', self_id='i3')}

    def _promotions(self):
        return [c[1] for c in self.calls if c[0] == 'promote']


class HealthyRecoveryTest(RecoveryTestBase):

    def test_the_primary_with_standbys_needs_no_lease_and_is_remembered(
            self):
        self._healthy()
        self._check_at(0)
        self.assertEqual([], self.calls)
        self.assertEqual(
            {'primary_id': 'i1', 'primary_ip': '10.0.0.1', 'lsn': 1000,
             'timeline': 1, 'sync_set': ['i2', 'i3'], 'alone': False},
            self.recovery.memory['c1'])
        self.assertEqual({}, self.recovery.waiting_since)
        self.assertFalse(self.recovery.still_watching('c1'))

    def test_every_look_records_the_health(self):
        # The recovery's own look, and its look while it recovers: both
        # record what the views say (none did before 2026-10-20, and the
        # clusters stayed UNKNOWN).
        recorded = mock.patch.object(
            common_recovery.health, 'from_views',
            return_value={'status': 'DEGRADED'}).start()
        self._healthy()
        self.answers['i3'] = None
        self._check_at(0)
        cluster_id, members, views, _out, _serving = recorded.call_args[0]
        self.assertEqual('c1', cluster_id)
        self.assertEqual(['i1', 'i2', 'i3'], [m.id for m in members])
        self.assertIsNone(dict((m.id, a) for m, a in views)['i3'])
        self.assertTrue(self.recovery.still_watching('c1'))
        self.db_cluster.task_status = ClusterTasks.RECOVERING_CLUSTER
        self._check_at(10)
        self.assertEqual(2, recorded.call_count)

    def test_the_lease_when_the_primary_asks(self):
        # A primary with standbys asks (its request crossed a standby
        # coming back): granted all the same.
        self._healthy()
        with mock.patch.object(common_recovery.time, 'monotonic',
                               return_value=0):
            self.recovery.check_now('ctx', self.db_cluster,
                                    'event lease_request from member i1')
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)
        self.assertEqual(0, self.recovery.memory['c1']['lease_at'])

    def test_no_lease_while_the_unreached_members_heartbeat(self):
        # The standbys do not answer this replica, but their heartbeat is
        # fresh: this replica is cut off from them, not they from the
        # primary; another replica decides. No lease from here.
        self.answers = {'i1': primary('10.0.0.1', no_standby_for=100),
                        'i2': None, 'i3': None}
        self.stale.side_effect = lambda ids: set()
        self._check_at(0)
        self.assertEqual([], self.calls)
        self.stale.assert_called_once_with(['i2', 'i3'])
        self.stale.side_effect = lambda ids: {'i2', 'i3'}
        self._check_at(30)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)

    def test_the_synchronous_set_is_kept_by_the_members(self):
        # Told once, to every other member; not again while they keep it.
        self._healthy()
        self._check_at(0)
        self.assertEqual([('i2', 'i1', ['i2', 'i3']),
                          ('i3', 'i1', ['i2', 'i3'])], self.shared)
        record = {'primary_id': 'i1', 'members': ['i2', 'i3']}
        self.answers['i2']['sync_set'] = record
        self.answers['i3']['sync_set'] = record
        self.shared = []
        self._check_at(30)
        self.assertEqual([], self.shared)
        # i3 stops streaming: the set changes, and both are told.
        self.answers['i1']['standbys'].pop('i3')
        self._check_at(60)
        self.assertEqual([('i2', 'i1', ['i2']), ('i3', 'i1', ['i2'])],
                         self.shared)

    def test_a_new_primary_is_not_judged_by_standbys_still_on_the_old(self):
        # Just promoted: the other standby still follows the old primary
        # and has been without WAL for long; that is no evidence against
        # the new primary, which gets its lease and the standby is told
        # to follow.
        self.answers = {'i1': None,
                        'i2': primary('10.0.0.2', timeline=2, self_id='i2',
                                      no_standby_for=100),
                        'i3': standby('10.0.0.3', status=None,
                                      disconnected_for=100, primary_id='i1')}
        self._check_at(0)
        self.assertEqual([('renew_lease', 'i2', 60),
                          ('create_member_slot', 'i2', 'i3'),
                          ('follow_primary', 'i3',
                           {'id': 'i2', 'host': '10.0.0.2'})], self.calls)

    def test_a_primary_alone_keeps_the_lease(self):
        # Both standbys down: the primary goes on (sync falls back to
        # none), nothing is failed over.
        self.answers = {'i1': primary('10.0.0.1', no_standby_for=100),
                        'i2': None, 'i3': None}
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([('renew_lease', 'i1', 60)] * 2, self.calls)
        mem = self.recovery.memory['c1']
        self.assertEqual((None, True), (mem['sync_set'], mem['alone']))
        self.assertEqual(100, mem['lease_at'])

    def test_the_set_stands_while_the_primary_goes_on_alone(self):
        # The standbys stop streaming but answer (cut off themselves):
        # the set the primary last streamed to stands, and the members
        # are told it goes on alone; once they stream again, that it
        # does not.
        self._healthy()
        self._check_at(0)
        record = {'primary_id': 'i1', 'members': ['i2', 'i3']}
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set'] = record
        self._standbys_cut_off(since=20)
        self.shared = []
        self._check_at(30)
        self.assertEqual([('i2', 'i1', ['i2', 'i3'], 'alone'),
                          ('i3', 'i1', ['i2', 'i3'], 'alone')], self.shared)
        mem = self.recovery.memory['c1']
        self.assertEqual((['i2', 'i3'], True), (mem['sync_set'],
                                                mem['alone']))
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set'] = dict(record, alone=True)
        self.shared = []
        self._check_at(60)
        self.assertEqual([], self.shared)
        self._healthy()
        self._check_at(90)
        self.assertEqual([('i2', 'i1', ['i2', 'i3']),
                          ('i3', 'i1', ['i2', 'i3'])], self.shared)
        self.assertFalse(self.recovery.memory['c1']['alone'])

    def _standbys_cut_off(self, since=100):
        """The standbys lost the primary and reach nobody on the tenant
        network; the primary answers, without a standby as long.
        """
        self.answers = {
            'i1': primary('10.0.0.1', no_standby_for=since, self_id='i1'),
            'i2': standby('10.0.0.2', status=None, disconnected_for=since,
                          lsn=1500, self_id='i2', peers=cut(i3=False)),
            'i3': standby('10.0.0.3', status=None, disconnected_for=since,
                          lsn=1200, self_id='i3', peers=cut(i2=False))}

    def test_standbys_cut_off_leave_the_primary_its_lease(self):
        # Both standbys cut off from the tenant network: the primary
        # has no standby, but the standbys reach nobody either, so it
        # is not given up: it keeps its lease (and goes on, in sync
        # and async mode), and no standby is told to rejoin it.
        for mode in ('sync', 'async'):
            self._healthy()
            self._check_at(0)
            self._standbys_cut_off()
            for answer in self.answers.values():
                answer['mode'] = mode
            self.answers['i2']['disconnected_for'] = 700
            self.calls = []
            self._check_at(100)
            self._check_at(140)
            self.assertEqual([('renew_lease', 'i1', 60)] * 2, self.calls,
                             mode)
            self.assertEqual({}, self.recovery.waiting_since)

    def test_a_standby_that_reaches_the_primary_says_it_lives(self):
        # The standbys do not stream, but one reaches the primary on
        # the tenant network: no partition, nothing to fail over.
        self._healthy()
        self._check_at(0)
        self._standbys_cut_off()
        self.answers['i3']['peers'] = cut('i2', i1=True)
        self.calls = []
        self._check_at(100)
        self._check_at(140)
        self.assertEqual([('renew_lease', 'i1', 60)] * 2, self.calls)

    def test_a_standby_pointed_elsewhere_follows_the_primary(self):
        self._healthy()
        self.answers['i3'] = standby('10.0.0.3', sender='10.0.0.9',
                                     primary_id='i9')
        self._check_at(0)
        self._check_at(10)
        self.assertEqual([('create_member_slot', 'i1', 'i3'),
                          ('follow_primary', 'i3',
                           {'id': 'i1', 'host': '10.0.0.1'})],
                         [c for c in self.calls if c[0] != 'renew_lease'])

    def test_a_former_primary_rejoins(self):
        self._healthy()
        self.answers['i1'] = primary('10.0.0.1', ['i3'], timeline=2,
                                     self_id='i1')
        self.answers['i2'] = primary('10.0.0.2', timeline=1, self_id='i2')
        self._check_at(0)
        self.assertEqual([('create_member_slot', 'i1', 'i2'),
                          ('rejoin_cluster', 'i2',
                           {'id': 'i1', 'host': '10.0.0.1'})], self.calls)

    def test_a_stopped_member_rejoins(self):
        # Down: the fenced former primary, or a member whose rejoin
        # failed; told again every interval.
        self._healthy()
        self.answers['i2'] = stopped('10.0.0.2', primary_id='i1')
        self.answers['i3'] = stopped('10.0.0.3', primary_id='i9', fenced=True)
        self._check_at(0)
        self.assertEqual([('create_member_slot', 'i1', 'i2'),
                          ('rejoin_cluster', 'i2',
                           {'id': 'i1', 'host': '10.0.0.1'}),
                          ('create_member_slot', 'i1', 'i3'),
                          ('rejoin_cluster', 'i3',
                           {'id': 'i1', 'host': '10.0.0.1'})], self.calls)
        self.calls = []
        self._check_at(10)
        self.assertEqual([], self.calls)
        self._check_at(40)
        self.assertEqual(['i2', 'i3'], [c[1] for c in self.calls
                                        if c[0] == 'rejoin_cluster'])

    def test_a_standby_left_behind_rejoins(self):
        self._healthy()
        self.answers['i3'] = standby('10.0.0.3', status=None,
                                     disconnected_for=700)
        self._check_at(0)
        self.assertIn(('rejoin_cluster', 'i3', {'id': 'i1',
                                                'host': '10.0.0.1'}),
                      self.calls)
        # Not while it does not reach the primary on the tenant network:
        # the rebuild could not either.
        self.calls = []
        self.answers['i3']['peers'] = cut('i2')
        self._check_at(50)
        self.assertEqual([], self.calls)
        self.answers['i3']['disconnected_for'] = 100
        self.calls = []
        self._check_at(100)
        self.assertEqual([], self.calls)

    def test_a_member_being_rebuilt_is_left_alone(self):
        self._healthy()
        self.answers['i3'] = standby('10.0.0.3', rebuilding=True,
                                     primary_id='i9')
        self._check_at(0)
        self.assertEqual([], self.calls)

    def test_nothing_when_nobody_answers(self):
        self.answers = {}
        self._check_at(0)
        self.assertEqual([], self.calls)
        self.assertEqual({}, self.recovery.waiting_since)

    def test_nothing_with_two_primaries_on_one_timeline(self):
        self.answers = {'i1': primary('10.0.0.1', timeline=2),
                        'i2': primary('10.0.0.2', timeline=2),
                        'i3': standby('10.0.0.3')}
        self._check_at(0)
        self.assertEqual([], self.calls)


class FailoverTest(RecoveryTestBase):

    def _primary_gone(self, since=100):
        """The primary does not answer; the standbys lost it, and reach
        every member but it on the tenant network.
        """
        ids = [m.id for m in self.members]

        def peers(self_id):
            return {i: i != 'i1' for i in ids if i != self_id}
        self.answers = {
            'i1': None,
            'i2': standby('10.0.0.2', status=None, disconnected_for=since,
                          lsn=1500, self_id='i2', peers=peers('i2')),
            'i3': standby('10.0.0.3', status=None, disconnected_for=since,
                          lsn=1200, self_id='i3', peers=peers('i3'))}

    def test_the_failover(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.calls = []
        # The grace period first.
        self._check_at(10)
        self.assertEqual([], self._promotions())
        # Then the lease has to run out: renewed at 0, it runs 60 s.
        self._check_at(45)
        self.assertEqual([], self._promotions())
        self._check_at(90)
        self.assertEqual(
            [('promote', 'i2', 'sync'),
             ('create_member_slot', 'i2', 'i3'),
             ('follow_primary', 'i3', {'id': 'i2', 'host': '10.0.0.2'}),
             ('update_cluster_members', 'i2',
              {'i1': '10.0.0.1', 'i2': '10.0.0.2', 'i3': '10.0.0.3'}, 'sync',
              {'primary_id': 'i2'}),
             ('update_cluster_members', 'i3',
              {'i1': '10.0.0.1', 'i2': '10.0.0.2', 'i3': '10.0.0.3'}, 'sync',
              {'primary_id': 'i2'}),
             ('renew_lease', 'i2', 60)], self.calls)
        self.assertEqual(ClusterTasks.RECOVERING_CLUSTER,
                         self.db_cluster.task_status)
        mem = self.recovery.memory['c1']
        self.assertEqual(('i2', '10.0.0.2', 2000, 2, None),
                         (mem['primary_id'], mem['primary_ip'], mem['lsn'],
                          mem['timeline'], mem['sync_set']))
        # Under recovery: the new primary keeps its lease; once the
        # other standby streams from it, the cluster is free again.
        self.calls = []
        self.answers = {'i1': None,
                        'i2': primary('10.0.0.2', ['i3'], timeline=2),
                        'i3': standby('10.0.0.3', status=None)}
        self._check_at(100)
        self.assertEqual([('renew_lease', 'i2', 60)], self.calls)
        self.assertEqual(ClusterTasks.RECOVERING_CLUSTER,
                         self.db_cluster.task_status)
        self.answers['i3'] = standby('10.0.0.3', sender='10.0.0.2',
                                     primary_id='i2', timeline=2)
        self._check_at(130)
        self.assertEqual(ClusterTasks.NONE, self.db_cluster.task_status)
        # The old primary back later, as a primary on the old timeline:
        # it rejoins.
        self.calls = []
        self.answers['i1'] = primary('10.0.0.1', timeline=1, self_id='i1')
        self._check_at(200)
        self.assertEqual([('create_member_slot', 'i2', 'i1'),
                          ('rejoin_cluster', 'i1',
                           {'id': 'i2', 'host': '10.0.0.2'})], self.calls)

    def test_a_reachable_primary_without_standbys_is_fenced(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = primary('10.0.0.1', no_standby_for=100,
                                     self_id='i1')
        self.answers['i1']['writable'] = False
        self.calls = []
        # It answers as a primary, but is given up all the same: no
        # lease for it, and after the grace period a failover.
        self._check_at(100)
        self.assertEqual([], self.calls)
        self._check_at(140)
        self.assertEqual(
            ['fence_member', 'promote', 'create_member_slot',
             'follow_primary', 'update_cluster_members',
             'update_cluster_members', 'create_member_slot',
             'rejoin_cluster', 'renew_lease'],
            [c[0] for c in self.calls])
        # It takes no writes already: the fence is told, not waited for.
        self.assertEqual(('fence_member', 'i1', {'wait': False}),
                         self.calls[0])
        self.assertEqual(('create_member_slot', 'i2', 'i1'), self.calls[6])
        self.assertEqual(('rejoin_cluster', 'i1',
                          {'id': 'i2', 'host': '10.0.0.2'}), self.calls[7])

    def test_a_primary_cut_off_in_sync_mode_is_replaced(self):
        # The primary cut off from the tenant network: it went on
        # without a standby (committing alone, sync mode), but for no
        # longer than the standbys have been without it, so nothing was
        # acknowledged meanwhile: fenced and replaced. Alone for longer
        # (the standbys were away first): it may have acknowledged
        # commits no standby holds, and the failover waits, unless
        # forced.
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = primary('10.0.0.1', no_standby_for=130,
                                     self_id='i1')
        self.answers['i1']['writable'] = False
        self._check_at(100)
        self._check_at(140)
        self.assertEqual([], self._promotions())
        self.answers['i1']['no_standby_for'] = 115
        self.calls = []
        self._check_at(170)
        self.assertEqual(['i2'], self._promotions())
        self.assertEqual(('fence_member', 'i1', {'wait': False}),
                         self.calls[0])

    def test_the_fence_is_waited_for_while_the_old_primary_writes(self):
        # Cut off from the standbys but still writable as far as it
        # knows: the failover waits out its lease, then fences it and
        # waits (briefly) for the fence before promoting.
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = primary('10.0.0.1', no_standby_for=100,
                                     self_id='i1')
        self.calls = []
        self._check_at(100)
        self._check_at(140)
        self.assertEqual([], self._promotions())
        self._check_at(180)
        self.assertEqual(['i2'], self._promotions())
        self.assertEqual(('fence_member', 'i1', {'wait': True}),
                         self.calls[0])

    def test_the_watch_ends_when_another_replica_freed_the_cluster(self):
        # This replica followed the recovery; the other freed the
        # cluster: the next look at the healthy cluster ends the watch.
        self._healthy()
        self.recovery.forming_since['c1'] = 10
        self.assertTrue(self.recovery.still_watching('c1'))
        self._check_at(100)
        self.assertFalse(self.recovery.still_watching('c1'))

    def test_a_primary_alone_for_longer_is_replaced_only_when_forced(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = primary('10.0.0.1', no_standby_for=200,
                                     self_id='i1')
        self._check_at(100)
        self._check_at(140)
        self._check_at(170)
        self.assertEqual([], self._promotions())
        self.recovery.conf.cluster_failover_force_after = 60
        self._check_at(200)
        self.assertEqual(['i2'], self._promotions())

    def test_a_restart_learns_from_the_members_that_it_went_on_alone(self):
        # The primary does not answer, and the members keep that it
        # went on without any standby after streaming to them: whether
        # it acknowledged commits alone cannot be known; waiting.
        self._primary_gone()
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set'] = {
                'primary_id': 'i1', 'members': ['i2', 'i3'], 'alone': True}
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set']['alone'] = False
        self._check_at(130)
        self.assertEqual(['i2'], self._promotions())

    def test_the_candidate_is_on_the_side_of_the_majority(self):
        # Five members, async: i5 holds the most but reaches nobody;
        # i2, i3 and i4 reach each other: the best of them takes over.
        for i in ('i4', 'i5'):
            self.members.append(mock.Mock(id=i,
                                          task_status=InstanceTasks.NONE))
        self._primary_gone()
        self._async()
        self.answers['i4'] = standby('10.0.0.4', status=None,
                                     disconnected_for=100, lsn=1600,
                                     self_id='i4', mode='async',
                                     peers=cut('i2', 'i3', i5=False))
        self.answers['i5'] = standby('10.0.0.5', status=None,
                                     disconnected_for=100, lsn=1900,
                                     self_id='i5', mode='async',
                                     peers=cut(i2=False, i3=False, i4=False))
        for i in ('i2', 'i3'):
            self.answers[i]['peers']['i5'] = False
        self._check_at(0)
        self._check_at(100)
        self.assertEqual(['i4'], self._promotions())

    def test_a_primary_whose_server_is_down_is_replaced(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = stopped('10.0.0.1')
        self._check_at(100)
        self._check_at(140)
        self.assertEqual(['i2'], self._promotions())

    def test_not_while_a_standby_streams_from_the_primary(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i3'] = standby('10.0.0.3', lsn=1200)
        self._check_at(100)
        self._check_at(200)
        self.assertEqual([], self._promotions())
        self.assertEqual({}, self.recovery.waiting_since)

    def test_not_before_every_standby_has_been_without_it(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i3']['disconnected_for'] = 5
        self._check_at(100)
        self.assertEqual([], self._promotions())

    def test_not_with_two_members(self):
        self.members.pop()
        self.answers = {'i1': None,
                        'i2': standby('10.0.0.2', status=None,
                                      disconnected_for=100)}
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())

    def test_not_without_a_majority(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i3'] = None
        self._check_at(100)
        self.assertEqual([], self._promotions())

    def test_the_synchronous_quorum(self):
        # The primary last streamed to i2, i3 and i4 (any one of them
        # has every acknowledged commit); i4 answering without a
        # position, i2 and i3 cannot be known to hold them.
        self.members.append(mock.Mock(id='i4', task_status=InstanceTasks.NONE))
        self._healthy()
        self.answers['i1']['standbys']['i4'] = {'state': 'streaming',
                                                'sync_state': 'quorum'}
        self.answers['i4'] = standby('10.0.0.4')
        self._check_at(0)
        self._primary_gone()
        self.answers['i4'] = standby('10.0.0.4', status=None,
                                     disconnected_for=100, lsn=None)
        self._check_at(100)
        self._check_at(140)
        self.assertEqual([], self._promotions())
        # Forced after a while.
        self.recovery.conf.cluster_failover_force_after = 60
        self._check_at(180)
        self.assertEqual(['i2'], self._promotions())

    def test_the_synchronous_set_not_known(self):
        # The task manager restarted: every standby has to be able to
        # take over.
        self._primary_gone()
        self.answers['i3'] = None
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())
        self.answers['i3'] = standby('10.0.0.3', status=None,
                                     disconnected_for=100, lsn=1200)
        self._check_at(130)
        self.assertEqual(['i2'], self._promotions())

    def _four_members_primary_gone(self, kept):
        # i4 answers without a position (it was not streaming when the
        # primary died, and cannot take over); the task manager
        # restarted, so the set is only what the members keep.
        self.members.append(mock.Mock(id='i4', task_status=InstanceTasks.NONE))
        self._primary_gone()
        self.answers['i4'] = standby('10.0.0.4', status=None,
                                     disconnected_for=100, lsn=None,
                                     self_id='i4', peers=cut('i2', 'i3'))
        for member_id, record in kept.items():
            self.answers[member_id]['sync_set'] = record

    def test_the_synchronous_set_kept_by_the_members(self):
        # Without the kept set, i4 would have to answer; with it (i2
        # and i3 were the ones streaming), i2 takes over.
        record = {'primary_id': 'i1', 'members': ['i2', 'i3']}
        self._four_members_primary_gone({'i2': record, 'i3': record})
        self._check_at(0)
        self._check_at(100)
        self.assertEqual(['i2'], self._promotions())

    def test_a_kept_set_of_another_primary_says_nothing(self):
        record = {'primary_id': 'i9', 'members': ['i2', 'i3']}
        self._four_members_primary_gone({'i2': record, 'i3': record})
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())

    def test_the_kept_sets_together(self):
        # i3 missed the update that took i4 out of the set: the sets
        # together still name i4, and the failover waits for it.
        self._four_members_primary_gone({
            'i2': {'primary_id': 'i1', 'members': ['i2', 'i3']},
            'i3': {'primary_id': 'i1', 'members': ['i2', 'i3', 'i4']}})
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())

    def _async(self):
        for answer in self.answers.values():
            if answer:
                answer['mode'] = 'async'

    def test_where_the_async_primary_is_kept_by_the_members(self):
        # Sent with the set; again only once the primary has moved on by
        # more than half the lag a failover may lose.
        self._healthy()
        self._async()
        self._check_at(0)
        self.assertEqual([('i2', 'i1', ['i2', 'i3'], 1000),
                          ('i3', 'i1', ['i2', 'i3'], 1000)], self.shared)
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set'] = {
                'primary_id': 'i1', 'members': ['i2', 'i3'], 'lsn': 1000}
        self.shared = []
        self.answers['i1']['lsn'] = 1000 + MiB // 2
        self._check_at(30)
        self.assertEqual([], self.shared)
        self.answers['i1']['lsn'] = 1001 + MiB // 2
        self._check_at(60)
        self.assertEqual(['i2', 'i3'], [c[0] for c in self.shared])

    def test_the_lag_after_a_restart(self):
        # The task manager restarted: where the primary was is what the
        # members keep, and the best standby is too far behind it.
        self._primary_gone()
        self._async()
        for i in ('i2', 'i3'):
            self.answers[i]['sync_set'] = {
                'primary_id': 'i1', 'members': ['i2', 'i3'],
                'lsn': 1500 + 2 * MiB}
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._promotions())
        # Nothing kept: nothing to compare with, the failover goes on.
        for i in ('i2', 'i3'):
            self.answers[i].pop('sync_set')
        self._check_at(130)
        self.assertEqual(['i2'], self._promotions())

    def test_the_next_look_is_when_the_wait_ends(self):
        # Seen gone at 100: the grace period ends at 130; then the
        # lease (and the fence grace) at 175, never later than the
        # interval; a look that waits for the lease is no failover try,
        # so the look at 175 is not rate-limited.
        self._primary_gone()
        self._check_at(100)
        self.assertEqual(30, self.recovery.look_again_in('c1'))
        self.assertEqual(30, self.recovery.look_again_in('c2'))
        self._check_at(130)
        self.assertEqual(30, self.recovery.look_again_in('c1'))
        self._check_at(160)
        self.assertEqual(15, self.recovery.look_again_in('c1'))
        self._check_at(175)
        self.assertEqual(['i2'], self._promotions())
        self.assertEqual(30, self.recovery.look_again_in('c1'))

    def test_a_lease_granted_before_a_restart(self):
        # The task manager first looks at 100: the primary may hold a
        # lease its predecessor (or another replica) renewed just
        # before, until 160, and keeps its port open 15 s after.
        self._primary_gone()
        self._check_at(100)
        self._check_at(130)
        self._check_at(170)
        self.assertEqual([], self._promotions())
        self._check_at(200)
        self.assertEqual(['i2'], self._promotions())

    def test_the_lease_runs_from_the_last_grant_or_the_first_failing_look(
            self):
        # Granted at 50 (the primary alone, asking); gone at 100: as far
        # as this replica knows another may have granted one until it
        # first saw the cluster failing, so the lease and the fence
        # grace run until 175.
        self.answers = {'i1': primary('10.0.0.1', no_standby_for=10),
                        'i2': None, 'i3': None}
        self._check_at(50)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)
        self._primary_gone()
        self._check_at(100)
        self._check_at(150)
        self.assertEqual([], self._promotions())
        self._check_at(180)
        self.assertEqual(['i2'], self._promotions())

    def test_the_address_of_the_primary_after_a_restart(self):
        # No standby streams and the task manager has no memory: the
        # address comes from the members' list of the cluster.
        self._primary_gone()
        top = pg_recovery.topology.Topology(
            self.members, [(m, self.answers[m.id]) for m in self.members])
        self.recovery._current_members = self.members
        self.assertEqual(('i1', '10.0.0.1'),
                         self.recovery._expected_primary('ctx', 'c1', top))

    def test_async_with_the_best_standby_too_far_behind(self):
        self._healthy()
        self.answers['i1']['lsn'] = 1500 + MiB
        for answer in self.answers.values():
            answer['mode'] = 'async'
        self._check_at(0)
        self._primary_gone()
        for answer in self.answers.values():
            if answer:
                answer['mode'] = 'async'
        self._check_at(100)
        self.assertEqual([], self._promotions())
        self.answers['i2']['lsn'] = 1500 + MiB
        self._check_at(180)
        self.assertEqual(['i2'], self._promotions())

    def test_the_timeline_rules_out_a_candidate(self):
        self._primary_gone()
        self.answers['i2']['timeline'] = 1
        self.answers['i3']['timeline'] = 2
        self._check_at(0)
        self._check_at(100)
        # i3 alone is on the latest timeline; the synchronous set is not
        # known, so i2 would have to be a candidate too: nothing.
        self.assertEqual([], self._promotions())
        self.recovery.memory['c1'] = {'primary_id': 'i1',
                                      'sync_set': ['i3']}
        self._check_at(130)
        self.assertEqual(['i3'], self._promotions())

    def test_nothing_when_the_cluster_changes_meanwhile(self):
        self._primary_gone()
        self._check_at(0)
        original = self.recovery._views

        def views(context, members, _calls=[]):
            if _calls:
                self.answers['i3']['lsn'] += 1
            _calls.append(1)
            return original(context, members)
        self.recovery._views = views
        self._check_at(100)
        self.assertEqual([], self._promotions())
        self.assertEqual(ClusterTasks.NONE, self.db_cluster.task_status)

    def test_a_failed_promotion_frees_the_cluster(self):
        self._primary_gone()
        self._check_at(0)
        self.guests.clear()
        self.recovery.guest_factory('ctx', self.members[1]).promote_member \
            .side_effect = Exception('no')
        self._check_at(100)
        self.assertEqual(ClusterTasks.NONE, self.db_cluster.task_status)
        self.assertEqual([], [c for c in self.calls
                              if c[0] == 'follow_primary'])


class ParallelLookTest(trove_testtools.TestCase):
    """The clusters are looked at several at a time, and the members of
    each at once: a look at many clusters ends well within the interval
    between looks.
    """

    def test_many_clusters_within_the_interval(self):
        clusters = [mock.Mock(id='c%s' % n, task_status=ClusterTasks.NONE)
                    for n in range(40)]
        members = {c.id: [mock.Mock(id='%s-i%s' % (c.id, i),
                                    task_status=InstanceTasks.NONE)
                          for i in (1, 2, 3)] for c in clusters}
        seen = []

        def guest(context, member):
            g = mock.Mock()

            def view(timeout=None, _id=member.id):
                # The time a member takes to answer over RPC.
                time.sleep(0.02)
                seen.append(_id)
                if _id.endswith('-i1'):
                    return primary('10.0.0.1', [_id[:-3] + '-i2',
                                                _id[:-3] + '-i3'])
                return standby('10.0.0.%s' % _id[-1])
            g.get_recovery_view.side_effect = view
            return g
        recovery = pg_recovery.PostgresqlClusterRecovery('postgresql', guest)
        recovery._members = lambda context, cluster_id: members[cluster_id]
        recovery.conf = mock.Mock(
            cluster_peer_timeout=3, cluster_recovery_grace=30,
            cluster_recovery_interval=30, cluster_failover_max_lag_bytes=0,
            cluster_standby_rebuild_after=600)
        started = time.monotonic()
        recovery.run('ctx', clusters)
        took = time.monotonic() - started
        # One at a time, it would take 40 clusters x 3 members x 0.02 s
        # = 2.4 s; several at a time, a fraction of that.
        self.assertLess(took, 1.0, took)
        self.assertEqual(120, len(seen))
        self.assertEqual(
            {c.id: c.id + '-i1' for c in clusters},
            {i: m['primary_id'] for i, m in recovery.memory.items()})


class TwoReplicasTest(RecoveryTestBase):
    """Two task managers watch the cluster: both look, one promotes."""

    def test_one_promotion(self):
        other = pg_recovery.PostgresqlClusterRecovery(
            'postgresql', self.recovery.guest_factory)
        other._members = self.recovery._members
        other.conf = self.recovery.conf
        FailoverTest._primary_gone(self)
        for replica in (self.recovery, other):
            with mock.patch.object(common_recovery.time, 'monotonic',
                                   return_value=0):
                replica.check('ctx', self.db_cluster)
        for replica in (self.recovery, other):
            with mock.patch.object(common_recovery.time, 'monotonic',
                                   return_value=100):
                replica.check('ctx', self.db_cluster)
        self.assertEqual(['i2'], self._promotions())
        self.assertEqual(ClusterTasks.RECOVERING_CLUSTER,
                         self.db_cluster.task_status)
        # The loser follows the recovery from the same start.
        self.assertIn('c1', other.forming_since)
        self.assertTrue(other.still_watching('c1'))
