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
            rebuilding=False, mode='sync', self_id=None):
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
    return answer


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
        ]
        for answers, dead, why in cases:
            got = topology.primary_dead(_top(answers), 'i1', '10.0.0.1', 30)
            self.assertEqual(dead, got[0], (answers, got))
            self.assertIn(why, got[1])

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
            # Not known: every standby has to be a candidate.
            (three, ['i2', 'i3'], 'sync', None, 0, True, 'every standby'),
            (three, ['i2'], 'sync', None, 0, False, 'i3 cannot take over'),
            # Empty: the primary committed without any standby.
            (three, ['i2', 'i3'], 'sync', [], 0, False, 'without any'),
            # Forced after a while.
            (three, ['i2', 'i3'], 'sync', [], 100, True, 'forced'),
            (five, ['i2', 'i3'], 'sync', ['i2', 'i3', 'i4', 'i5'], 100,
             True, 'forced'),
        ]
        for top, cands, mode, sync_set, failing, ok, why in cases:
            got = topology.quorum(top, cands, mode, sync_set, 'i1', failing,
                                  force_after=60)
            self.assertEqual(ok, got[0], (cands, mode, sync_set, got))
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
                    lambda *a, _i=member.id: self.shared.append((_i,) + a))
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
            cluster_standby_rebuild_after=600)
        mock.patch.object(pg_recovery.CONF, 'cluster_recovery_check_interval',
                          30).start()
        self.db_cluster = mock.Mock(id='c1', task_status=ClusterTasks.NONE)
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

    def test_the_primary_gets_the_lease_and_is_remembered(self):
        self._healthy()
        self._check_at(0)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)
        self.assertEqual(
            {'primary_id': 'i1', 'primary_ip': '10.0.0.1', 'lsn': 1000,
             'timeline': 1, 'sync_set': ['i2', 'i3'], 'lease_at': 0},
            self.recovery.memory['c1'])
        self.assertEqual({}, self.recovery.waiting_since)

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
        self.assertEqual([], self.recovery.memory['c1']['sync_set'])

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
        self.assertEqual([('renew_lease', 'i1', 60),
                          ('create_member_slot', 'i1', 'i2'),
                          ('rejoin_cluster', 'i2',
                           {'id': 'i1', 'host': '10.0.0.1'})], self.calls)

    def test_a_stopped_member_rejoins(self):
        # Down: the fenced former primary, or a member whose rejoin
        # failed; told again every interval.
        self._healthy()
        self.answers['i2'] = stopped('10.0.0.2', primary_id='i1')
        self.answers['i3'] = stopped('10.0.0.3', primary_id='i9', fenced=True)
        self._check_at(0)
        self.assertEqual([('renew_lease', 'i1', 60),
                          ('create_member_slot', 'i1', 'i2'),
                          ('rejoin_cluster', 'i2',
                           {'id': 'i1', 'host': '10.0.0.1'}),
                          ('create_member_slot', 'i1', 'i3'),
                          ('rejoin_cluster', 'i3',
                           {'id': 'i1', 'host': '10.0.0.1'})], self.calls)
        self.calls = []
        self._check_at(10)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)
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
        self.answers['i3']['disconnected_for'] = 100
        self.calls = []
        self._check_at(100)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)

    def test_a_member_being_rebuilt_is_left_alone(self):
        self._healthy()
        self.answers['i3'] = standby('10.0.0.3', rebuilding=True,
                                     primary_id='i9')
        self._check_at(0)
        self.assertEqual([('renew_lease', 'i1', 60)], self.calls)

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
        self.answers = {
            'i1': None,
            'i2': standby('10.0.0.2', status=None, disconnected_for=since,
                          lsn=1500, self_id='i2'),
            'i3': standby('10.0.0.3', status=None, disconnected_for=since,
                          lsn=1200, self_id='i3')}

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
        self.assertEqual([('renew_lease', 'i2', 60),
                          ('create_member_slot', 'i2', 'i1'),
                          ('rejoin_cluster', 'i1',
                           {'id': 'i2', 'host': '10.0.0.2'})], self.calls)

    def test_a_reachable_primary_without_standbys_is_fenced(self):
        self._healthy()
        self._check_at(0)
        self._primary_gone()
        self.answers['i1'] = primary('10.0.0.1', no_standby_for=100,
                                     self_id='i1')
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
        self.assertEqual(('fence_member', 'i1'), self.calls[0])
        self.assertEqual(('create_member_slot', 'i2', 'i1'), self.calls[6])
        self.assertEqual(('rejoin_cluster', 'i1',
                          {'id': 'i2', 'host': '10.0.0.2'}), self.calls[7])

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
        self._check_at(170)
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
        self.answers = {
            'i1': None,
            'i2': standby('10.0.0.2', status=None, disconnected_for=100,
                          lsn=1500, self_id='i2'),
            'i3': standby('10.0.0.3', status=None, disconnected_for=100,
                          lsn=1200, self_id='i3'),
            'i4': standby('10.0.0.4', status=None, disconnected_for=100,
                          lsn=None, self_id='i4')}
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
        self._check_at(130)
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
