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

from unittest import mock

from trove.common import cluster_health as ch
from trove.guestagent.common import cluster_probe as probe
from trove.guestagent.common import readyport
from trove.tests.unittests import trove_testtools


def _view(state, role, writable=None, down=None):
    if writable is None:
        writable = state == 'ONLINE' and role == 'PRIMARY'
    if down is None:
        down = state in ('OFFLINE', 'ERROR')
    return probe.MemberView(state, role, writable, down)


class ProbeTestBase(trove_testtools.TestCase):
    """A probe on a mocked app, container and configuration."""

    def setUp(self):
        super(ProbeTestBase, self).setUp()
        self.app = mock.MagicMock()
        self.app.DATABASE_PORT = 3306
        self.app.is_cluster_member.return_value = True
        self.app.is_cluster_complete.return_value = False
        self.app.member_view.return_value = _view('ONLINE', 'PRIMARY')
        self.app.was_last_standing.return_value = False
        # What the apps of the datastores that recover their clusters do
        # not have.
        del self.app.cluster_health
        del self.app.is_watched_member
        self.conf = mock.Mock(cluster_probe_interval=3,
                              cluster_read_port=0,
                              cluster_probe_timeout=5,
                              cluster_ready_port_reconcile=20,
                              cluster_ready_port=3307,
                              cluster_recovery_grace=60,
                              cluster_recovery_interval=30,
                              cluster_peer_timeout=3,
                              cluster_auto_bootstrap=True,
                              cluster_bootstrap_needs_all_members=False,
                              cluster_bootstrap_jitter=0,
                              cluster_events=False,
                              cluster_event_interval=60,
                              cluster_event_min_interval=5)
        # No eventlet timeout around the query in tests.
        timeout = mock.patch.object(probe.eventlet, 'Timeout').start()
        timeout.return_value.__enter__.return_value = None
        self.addCleanup(mock.patch.stopall)
        self.probe = probe.ClusterProbe(self.app, mock.MagicMock(),
                                        conf=self.conf)
        self.probe.port = mock.MagicMock(spec=readyport.ReadyPort)
        self.probe.port.pid = 4242
        self.probe.port.applied = None
        self.probe.port.container_pid.return_value = 4242


class TestClusterProbe(ProbeTestBase):

    def test_the_ready_port_redirects_to_the_database_port(self):
        self.app.DATABASE_PORT = 5432
        port = probe.ClusterProbe(self.app, mock.MagicMock(),
                                  conf=self.conf).port
        self.assertEqual((3307, 5432), (port.port, port.target_port))

    def test_flags_are_read_once(self):
        self.probe._tick()
        self.probe._tick()
        self.app.is_cluster_member.assert_called_once()
        self.app.is_cluster_complete.assert_called_once()
        self.assertTrue(self.probe.member)
        self.assertFalse(self.probe.complete)

    def test_not_a_member(self):
        self.app.is_cluster_member.return_value = False
        self.probe._tick()
        self.app.member_view.assert_not_called()
        self.probe.port.ensure.assert_not_called()

    def test_the_port_follows_the_member(self):
        for view, wanted in ((_view('ONLINE', 'PRIMARY'), True),
                             (_view('ONLINE', 'SECONDARY'), False),
                             (_view('RECOVERING', 'PRIMARY'), False),
                             (_view('Synced', None, writable=True), True),
                             (probe.UNKNOWN_MEMBER, False)):
            self.app.member_view.return_value = view
            self.probe.port.ensure.reset_mock()
            self.probe.port.applied = None
            self.probe._tick()
            self.probe.port.ensure.assert_called_once_with(wanted, pid=4242)
            self.assertEqual(wanted, self.probe.writable())

    def test_the_port_is_checked_only_when_something_changed(self):
        self.probe.port.applied = True
        self.probe.ticks = 1
        self.probe._tick()
        self.probe.port.ensure.assert_not_called()
        # The container was recreated: the rule went with its namespace.
        self.probe.port.container_pid.return_value = 4343
        self.probe._tick()
        self.probe.port.ensure.assert_called_once_with(True, pid=4343)
        # And now and then regardless.
        self.probe.port.pid = 4343
        self.probe.port.ensure.reset_mock()
        self.probe.ticks = 20
        self.probe._tick()
        self.probe.port.ensure.assert_called_once()

    def test_a_failure_does_not_stop_the_probe(self):
        self.app.member_view.side_effect = Exception('gone')
        self.probe._tick()
        self.app.member_view.side_effect = None
        self.probe.port.ensure.side_effect = Exception('no sudo')
        self.probe._tick()
        self.assertEqual(('ONLINE', 'PRIMARY'),
                         (self.probe.state, self.probe.role))

    def test_a_leaving_member_closes_the_port(self):
        self.probe.disable()
        self.probe._tick()
        self.probe.port.ensure.assert_called_once_with(False, pid=4242)

    def test_the_cluster_calls_set_the_flags(self):
        self.probe.enable_member()
        self.assertTrue(self.probe.member)
        self.assertFalse(self.probe.complete)
        self.probe.enable_complete()
        self.assertTrue(self.probe.complete)
        self.probe._tick()
        # Told, not read.
        self.app.is_cluster_member.assert_not_called()

    def test_snapshot(self):
        self.probe.port.applied = True
        self.probe._tick()
        self.assertEqual({'state': 'ONLINE', 'role': 'PRIMARY',
                          'writable': True, 'ready_port': True},
                         self.probe.snapshot())

    def test_a_view_is_not_readable_unless_it_says_so(self):
        # Galera and Group Replication views have no readable member: the
        # field defaults to False and the probe asks for it with a default.
        view = probe.MemberView('ONLINE', 'SECONDARY', False, False)
        self.assertFalse(view.readable)
        self.assertIsNone(self.probe.read_port)
        self.probe.view = view
        self.assertFalse(self.probe.readable())

    def test_the_read_port_follows_the_readable_member(self):
        # A datastore with a read port (a standby serving reads): the
        # probe keeps it as it keeps the ready port, from the view's
        # readable, and tells it in its snapshot.
        self.conf.cluster_read_port = 5434
        self.app.DATABASE_PORT = 5432
        p = probe.ClusterProbe(self.app, mock.MagicMock(), conf=self.conf)
        self.assertEqual((5434, 5432),
                         (p.read_port.port, p.read_port.target_port))
        p.port = mock.MagicMock(spec=readyport.ReadyPort)
        p.port.pid = 4242
        p.port.applied = None
        p.port.container_pid.return_value = 4242
        p.read_port = mock.MagicMock(spec=readyport.ReadyPort)
        p.read_port.pid = 4242
        p.read_port.applied = None
        p.member = True
        p._flags_loaded = True
        self.app.member_view.return_value = probe.MemberView(
            'streaming', 'standby', False, False, True)
        p._tick()
        p.port.ensure.assert_called_once_with(False, pid=4242)
        p.read_port.ensure.assert_called_once_with(True, pid=4242)
        p.port.applied = False
        p.read_port.applied = True
        self.assertEqual({'state': 'streaming', 'role': 'standby',
                          'writable': False, 'ready_port': False,
                          'readable': True, 'read_port': True},
                         p.snapshot())
        # Leaving closes both.
        p.disable()
        p.port.ensure.reset_mock()
        p.read_port.ensure.reset_mock()
        p._tick()
        p.read_port.ensure.assert_called_once_with(False, pid=4242)

    def test_the_reconcile_hook_runs_every_tick(self):
        # An app with a reconcile is given every view; its failure does
        # not keep the ports from being set.
        self.app.reconcile.side_effect = Exception('later')
        self.probe._tick()
        self.app.reconcile.assert_called_once_with(self.probe.view)
        self.probe.port.ensure.assert_called_once_with(True, pid=4242)
        # An app without one.
        del self.app.reconcile
        self.probe._tick()

    @mock.patch.object(probe.loopingcall, 'FixedIntervalLoopingCall')
    def test_start(self, looping_call):
        self.probe.start()
        looping_call.assert_called_once_with(self.probe._tick)
        looping_call.return_value.start.assert_called_once_with(
            interval=3, initial_delay=3, stop_on_exception=False)
        # Once.
        self.probe.start()
        looping_call.assert_called_once()

    @mock.patch.object(probe.loopingcall, 'FixedIntervalLoopingCall')
    def test_start_turned_off(self, looping_call):
        self.conf.cluster_probe_interval = 0
        probe.ClusterProbe(self.app, mock.MagicMock(),
                           conf=self.conf).start()
        looping_call.assert_not_called()


class TestClusterEventReporter(trove_testtools.TestCase):
    """The state machine alone: when a signal is worth telling."""

    def setUp(self):
        super(TestClusterEventReporter, self).setUp()
        self.now = 1000.0
        self.sent = []
        self.reporter = probe.ClusterEventReporter(
            60, 5, send=lambda kind, signal, view, since: self.sent.append(
                (kind, signal.reason, since)),
            now=lambda: self.now)
        self.abnormal = probe.ClusterSignal(True, probe.ABNORMAL, 'out')
        self.lease = probe.ClusterSignal(True, probe.LEASE_REQUEST, 'alone',
                                         resend_after=20)

    def _at(self, now, signal):
        self.now = now
        return self.reporter.observe(signal, 'view')

    def test_a_member_that_starts_normal_says_nothing(self):
        self.assertIsNone(self._at(1000, probe.NORMAL))
        self.assertIsNone(self._at(1003, probe.NORMAL))
        self.assertEqual([], self.sent)

    def test_a_member_that_starts_abnormal_says_so_once(self):
        self.assertEqual('abnormal', self._at(1000, self.abnormal))
        self.assertIsNone(self._at(1003, self.abnormal))
        self.assertEqual([('abnormal', 'out', 0)], self.sent)

    def test_every_transition_is_told(self):
        self._at(1000, probe.NORMAL)
        self.assertEqual('abnormal', self._at(1003, self.abnormal))
        # A change of kind, once the gap has passed.
        self.assertEqual('lease_request', self._at(1009, self.lease))
        self.assertEqual('normal', self._at(1015, probe.NORMAL))
        self.assertIsNone(self._at(1018, probe.NORMAL))
        self.assertEqual(['abnormal', 'lease_request', 'normal'],
                         [s[0] for s in self.sent])
        # Since when the state lasts.
        self.assertEqual(0, self.sent[2][2])

    def test_an_abnormal_member_tells_again_every_interval(self):
        self._at(1000, self.abnormal)
        self.assertIsNone(self._at(1059, self.abnormal))
        self.assertEqual('abnormal', self._at(1060, self.abnormal))
        self.assertEqual(60, self.sent[-1][2])
        # Sooner when the signal asks for it.
        self._at(1066, self.lease)
        self.assertIsNone(self._at(1085, self.lease))
        self.assertEqual('lease_request', self._at(1086, self.lease))

    def test_no_more_than_one_report_per_gap(self):
        self._at(1000, self.abnormal)
        # Flapping within the gap: deferred, then the latest state once.
        self.assertIsNone(self._at(1001, probe.NORMAL))
        self.assertIsNone(self._at(1002, self.abnormal))
        self.assertIsNone(self._at(1004, probe.NORMAL))
        self.assertEqual('normal', self._at(1005, probe.NORMAL))
        self.assertEqual(['abnormal', 'normal'], [s[0] for s in self.sent])

    def test_a_change_of_reason_alone_is_not_told(self):
        self._at(1000, self.abnormal)
        self.assertIsNone(self._at(1010, probe.ClusterSignal(
            True, probe.ABNORMAL, 'another reason')))

    def test_a_failing_send_is_tried_again_next_tick(self):
        def fail(*args):
            raise RuntimeError('no broker')
        self.reporter.send = fail
        self.assertRaises(RuntimeError, self._at, 1000, self.abnormal)
        self.reporter.send = lambda *a: self.sent.append(a[0])
        self.assertEqual('abnormal', self._at(1003, self.abnormal))


class TestClusterEvents(ProbeTestBase):
    """The probe tells the control plane through the conductor."""

    def setUp(self):
        super(TestClusterEvents, self).setUp()
        self.conf.cluster_events = True
        self.probe = probe.ClusterProbe(self.app, mock.MagicMock(),
                                        conf=self.conf)
        self.probe.port = mock.MagicMock(spec=readyport.ReadyPort)
        self.probe.port.pid = 4242
        self.probe.port.applied = None
        self.probe.port.container_pid.return_value = 4242
        self.probe.member = self.probe.complete = True
        self.probe._flags_loaded = True
        # The cast runs on the tick in tests, and the conductor is a mock.
        self.probe._spawn = lambda fn, *args: fn(*args)
        self.conductor = mock.patch.object(probe.conductor_api, 'API').start()
        self.cast = self.conductor.return_value.cluster_event
        self.now = 1000.0
        mock.patch.object(probe.time, 'monotonic',
                          side_effect=lambda: self.now).start()
        self.probe.reporter._now = lambda: self.now
        self.patch_conf_property('guest_id', 'i2')
        self.patch_conf_property('datastore_manager', 'postgresql')
        # No datastore hook: the generic signal.
        del self.app.cluster_signal
        del self.app.cluster_event_view
        self.app.recovery_view.return_value = {'state': 'x'}

    def _tick(self, view, now=None):
        if now is not None:
            self.now = now
        self.app.member_view.return_value = view
        self.probe._tick()

    def test_a_member_that_comes_up_normal_says_nothing(self):
        self._tick(_view('ONLINE', 'PRIMARY'))
        self.cast.assert_not_called()

    def test_a_member_that_comes_up_abnormal_says_so_once(self):
        self._tick(_view('OFFLINE', None))
        self._tick(_view('OFFLINE', None), 1003)
        self.cast.assert_called_once()
        kwargs = self.cast.call_args.kwargs
        self.assertEqual(('i2', 'abnormal'), self.cast.call_args.args)
        self.assertIsInstance(kwargs['sent'], float)
        payload = kwargs['payload']
        self.assertEqual('out of the cluster: OFFLINE', payload['reason'])
        self.assertEqual({'state': 'x'}, payload['view'])
        self.assertEqual('postgresql', payload['datastore_manager'])
        self.assertEqual(self.probe.snapshot(), payload['member'])
        self.assertEqual({}, payload['extra'])
        self.assertEqual(0, payload['since'])

    def test_transitions_and_the_datastore_hook(self):
        self.app.cluster_signal = mock.Mock(return_value=probe.NORMAL)
        self._tick(_view('ONLINE', 'PRIMARY'))
        self.app.cluster_signal.assert_called_once_with(
            _view('ONLINE', 'PRIMARY'))
        self.app.cluster_signal.return_value = probe.ClusterSignal(
            True, probe.LEASE_REQUEST, 'alone', extra={'left': 0})
        self._tick(_view('ONLINE', 'PRIMARY'), 1003)
        self.assertEqual(('i2', 'lease_request'), self.cast.call_args.args)
        self.assertEqual({'left': 0},
                         self.cast.call_args.kwargs['payload']['extra'])
        self.app.cluster_signal.return_value = probe.NORMAL
        self._tick(_view('ONLINE', 'PRIMARY'), 1010)
        self.assertEqual(('i2', 'normal'), self.cast.call_args.args)
        self.assertEqual(2, self.cast.call_count)

    def test_the_event_view_hook(self):
        self.app.cluster_event_view = mock.Mock(return_value={'gr': 1})
        self._tick(_view('OFFLINE', None))
        self.app.cluster_event_view.assert_called_once_with(
            _view('OFFLINE', None))
        self.assertEqual({'gr': 1}, self.cast.call_args.kwargs['payload']
                         ['view'])
        # A failing view does not keep the event.
        self.app.cluster_event_view.side_effect = Exception('no')
        self._tick(_view('OFFLINE', None), 1060)
        self.assertIsNone(self.cast.call_args.kwargs['payload']['view'])

    def test_the_ports_are_set_before_and_regardless_of_the_event(self):
        order = []
        self.probe.port.ensure.side_effect = lambda *a, **k: order.append(
            'port')
        self.app.reconcile.side_effect = lambda v: order.append('reconcile')
        self.app.cluster_signal = mock.Mock(
            side_effect=lambda v: order.append('signal') or probe.NORMAL)
        self._tick(_view('ONLINE', 'PRIMARY'))
        self.assertEqual(['reconcile', 'port', 'signal'], order)
        # A failing hook or cast does not break the tick.
        self.app.cluster_signal.side_effect = Exception('hook')
        self._tick(_view('ONLINE', 'PRIMARY'), 1003)
        self.conductor.side_effect = Exception('no transport')
        self.app.cluster_signal.side_effect = None
        self.app.cluster_signal.return_value = probe.ClusterSignal(
            True, probe.ABNORMAL, 'out')
        self._tick(_view('ONLINE', 'PRIMARY'), 1006)
        self.assertFalse(self.probe._sending)
        self.assertEqual(3, self.probe.port.ensure.call_count)

    def test_nothing_before_the_cluster_is_complete_or_while_leaving(self):
        self.probe.complete = False
        self._tick(_view('OFFLINE', None))
        self.cast.assert_not_called()
        self.probe.complete = True
        self.probe.disable()
        self._tick(_view('OFFLINE', None), 1003)
        self.cast.assert_not_called()

    def test_a_cast_in_flight_is_not_doubled(self):
        self.probe._sending = True
        self._tick(_view('OFFLINE', None))
        self.cast.assert_not_called()

    def test_events_off(self):
        self.conf.cluster_events = False
        p = probe.ClusterProbe(self.app, mock.MagicMock(), conf=self.conf)
        p.port = self.probe.port
        p.member = p.complete = p._flags_loaded = True
        self.app.member_view.return_value = _view('OFFLINE', None)
        p._tick()
        self.cast.assert_not_called()


def _peer(ip, in_group=False, position='u:1-10', reachable=True,
          sees_group=False):
    return probe.PeerView(ip, reachable, in_group, sees_group, position)


def _not_ahead(a, b):
    # Positions as "u:1-N": a holds nothing b lacks when a's N is not
    # above b's.
    def last(s):
        return int(s.rsplit('-', 1)[-1]) if s else 0
    return last(a) <= last(b)


class TestDecide(trove_testtools.TestCase):

    def decide(self, self_ip, position, peers, n=3, needs_all=False):
        return probe.decide(self_ip, position, peers, n, _not_ahead,
                            needs_all)

    def test_a_group_that_is_up_is_joined(self):
        for peer in (_peer('10.0.0.2', in_group=True),
                     _peer('10.0.0.2', sees_group=True)):
            action, _ = self.decide('10.0.0.1', 'u:1-10',
                                    [peer, _peer('10.0.0.3')])
            self.assertEqual(probe.REJOIN, action)

    def test_no_majority_waits(self):
        action, reason = self.decide(
            '10.0.0.1', 'u:1-10',
            [_peer('10.0.0.2', reachable=False),
             _peer('10.0.0.3', reachable=False)])
        self.assertEqual(probe.WAIT, action)
        self.assertIn('1 of 3', reason)

    def test_the_lowest_address_with_everything_forms_the_group(self):
        action, _ = self.decide(
            '10.0.0.1', 'u:1-10',
            [_peer('10.0.0.2'), _peer('10.0.0.3', reachable=False)])
        self.assertEqual(probe.BOOTSTRAP, action)

    def test_missing_transactions_wait(self):
        action, reason = self.decide(
            '10.0.0.1', 'u:1-10',
            [_peer('10.0.0.2', position='u:1-12'), _peer('10.0.0.3')])
        self.assertEqual(probe.WAIT, action)
        self.assertIn('10.0.0.2', reason)

    def test_a_lower_address_with_everything_goes_first(self):
        action, reason = self.decide(
            '10.0.0.3', 'u:1-10', [_peer('10.0.0.1'), _peer('10.0.0.2')])
        self.assertEqual(probe.WAIT, action)
        self.assertIn('deferring to 10.0.0.1', reason)
        # Unless the lower one lacks transactions: then this member goes,
        # whatever its address.
        action, _ = self.decide(
            '10.0.0.3', 'u:1-12', [_peer('10.0.0.1'), _peer('10.0.0.2')])
        self.assertEqual(probe.BOOTSTRAP, action)

    def test_addresses_compare_as_numbers(self):
        action, _ = self.decide(
            '10.0.0.9', 'u:1-10', [_peer('10.0.0.10'), _peer('10.0.0.11')])
        self.assertEqual(probe.BOOTSTRAP, action)

    def test_every_member_when_asked(self):
        peers = [_peer('10.0.0.2'), _peer('10.0.0.3', reachable=False)]
        self.assertEqual(probe.BOOTSTRAP,
                         self.decide('10.0.0.1', 'u:1-10', peers)[0])
        self.assertEqual(probe.WAIT, self.decide(
            '10.0.0.1', 'u:1-10', peers, needs_all=True)[0])


class TestRecovery(ProbeTestBase):

    def setUp(self):
        super(TestRecovery, self).setUp()
        self.probe.enable_complete()
        self.app._recovery_credentials.return_value = ('r', 'p')
        self.app._self_ip.return_value = '10.0.0.1'
        self.app._seed_ips.return_value = ['10.0.0.1', '10.0.0.2',
                                           '10.0.0.3']
        self.app._position.return_value = 'u:1-10'
        self.app._not_ahead.side_effect = _not_ahead
        self.peers = {'10.0.0.2': _peer('10.0.0.2'),
                      '10.0.0.3': _peer('10.0.0.3')}
        self.app._query_peer.side_effect = (
            lambda ip, user, pw, timeout: self.peers[ip])
        mock.patch.object(probe.time, 'sleep').start()
        self.app.member_view.return_value = _view('OFFLINE', None)

    def _tick_at(self, seconds):
        with mock.patch.object(probe.time, 'monotonic',
                               return_value=seconds):
            self.probe._tick()

    def test_recovery_waits_for_the_grace_period(self):
        # The first tick sees the member out of the group; the grace
        # period counts from then.
        self._tick_at(0)
        self._tick_at(59)
        self.app._query_peer.assert_not_called()
        self._tick_at(61)
        # Two peers, asked twice: once to decide, once more before forming
        # the group.
        self.assertEqual(4, self.app._query_peer.call_count)
        self.app._query_peer.assert_any_call('10.0.0.2', 'r', 'p', 3)
        # And between two tries.
        self._tick_at(80)
        self.assertEqual(4, self.app._query_peer.call_count)
        self._tick_at(92)
        self.assertEqual(8, self.app._query_peer.call_count)

    def test_only_a_member_that_is_down(self):
        # Unknown is not down: the database may be starting.
        self.app.member_view.return_value = probe.UNKNOWN_MEMBER
        self._tick_at(0)
        self._tick_at(100)
        self.app._query_peer.assert_not_called()

    def test_rejoins_a_group_that_is_up(self):
        self.peers['10.0.0.2'] = _peer('10.0.0.2', in_group=True)
        self._tick_at(0)
        self._tick_at(100)
        self.app.rejoin_group.assert_called_once_with('OFFLINE')
        self.app.bootstrap_group.assert_not_called()

    def test_forms_the_group_again(self):
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_called_once()
        self.app.rejoin_group.assert_not_called()

    def test_the_last_one_standing_forms_the_group_alone(self):
        # Marked as the last member in the group: it holds everything
        # the group committed, and does not wait for the others.
        self.conf.cluster_bootstrap_needs_all_members = True
        self.probe.needs_all = True
        self.peers = {'10.0.0.2': _peer('10.0.0.2', reachable=False),
                      '10.0.0.3': _peer('10.0.0.3', reachable=False)}
        self.app.was_last_standing.return_value = True
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_called_once()
        self.app.rejoin_group.assert_not_called()

    def test_the_last_one_standing_is_not_trusted_against_a_peer_ahead(self):
        self.probe.needs_all = True
        self.peers['10.0.0.2'] = _peer('10.0.0.2', position='u:1-12')
        self.peers['10.0.0.3'] = _peer('10.0.0.3', reachable=False)
        self.app.was_last_standing.return_value = True
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_not_called()
        self.app.rejoin_group.assert_not_called()

    def test_the_last_one_standing_joins_a_group_that_is_up(self):
        self.peers['10.0.0.2'] = _peer('10.0.0.2', in_group=True)
        self.app.was_last_standing.return_value = True
        self._tick_at(0)
        self._tick_at(100)
        self.app.rejoin_group.assert_called_once_with('OFFLINE')
        self.app.bootstrap_group.assert_not_called()

    def test_does_not_form_the_group_when_the_peers_changed(self):
        answers = [self.peers['10.0.0.2'], self.peers['10.0.0.3'],
                   _peer('10.0.0.2', in_group=True), self.peers['10.0.0.3']]
        self.app._query_peer.side_effect = lambda *a: answers.pop(0)
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_not_called()

    def test_auto_bootstrap_off(self):
        self.probe.auto_bootstrap = False
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_not_called()
        self.app.rejoin_group.assert_not_called()

    def test_not_before_the_cluster_is_complete(self):
        self.probe.complete = False
        self._tick_at(0)
        self._tick_at(100)
        self.app._query_peer.assert_not_called()

    def test_not_while_leaving(self):
        self.probe.disable()
        self._tick_at(0)
        self._tick_at(100)
        self.app._query_peer.assert_not_called()


class _WatchedApp(object):
    """An app of a datastore whose clusters heal themselves, as the
    watch sees it: no ready port, no recovery.
    """

    def __init__(self):
        self.health = ch.view(ch.HEALTHY, me='10.0.0.1')

    def is_watched_member(self):
        return True

    def member_view(self):
        return probe.MemberView('ok', 'master', True, False)

    def cluster_health(self):
        return self.health


class TestClusterWatch(trove_testtools.TestCase):
    """The watch tells what the member sees of its cluster."""

    def setUp(self):
        super(TestClusterWatch, self).setUp()
        self.app = _WatchedApp()
        self.conf = mock.Mock(cluster_probe_interval=30,
                              cluster_probe_timeout=10, cluster_events=True,
                              cluster_event_interval=300,
                              cluster_event_min_interval=5)
        timeout = mock.patch.object(probe.eventlet, 'Timeout').start()
        timeout.return_value.__enter__.return_value = None
        self.os = mock.patch.object(probe, 'operating_system').start()
        self.os.exists.return_value = False
        mock.patch.object(probe.guestagent_utils, 'get_conf_dir',
                          return_value='/var/lib/redis/conf.d').start()
        self.cast = mock.patch.object(
            probe.conductor_api, 'API').start().return_value.cluster_event
        self.addCleanup(mock.patch.stopall)
        self.now = 1000.0
        mock.patch.object(probe.time, 'monotonic',
                          side_effect=lambda: self.now).start()
        self.patch_conf_property('guest_id', 'i1')
        self.patch_conf_property('datastore_manager', 'redis')
        self.watch = self._watch()

    def _watch(self):
        watch = probe.ClusterWatch(self.app, conf=self.conf)
        watch._spawn = lambda fn, *args: fn(*args)
        watch.reporter._now = lambda: self.now
        return watch

    def _tick(self, now):
        self.now = now
        self.watch._tick()

    def test_quiet_until_the_cluster_is_complete(self):
        self.app.health = ch.view(ch.DEGRADED, ['10.0.0.2'], 'failing')
        self._tick(1000)
        self.assertTrue(self.watch.member)
        self.assertFalse(self.watch.complete)
        self.assertIsNone(self.watch.health)
        self.cast.assert_not_called()

    def test_complete_is_kept_for_the_next_start(self):
        self.watch.enable_complete()
        self.os.write_file.assert_called_once_with(
            '/var/lib/redis/conf.d/cluster_complete', 'complete',
            as_root=True)
        # The guest agent started again: read back.
        self.os.exists.return_value = True
        watch = self._watch()
        watch._load_flags()
        self.assertTrue(watch.complete)
        self.os.exists.assert_called_with(
            '/var/lib/redis/conf.d/cluster_complete', as_root=True)

    def test_a_cluster_short_of_a_member_and_whole_again(self):
        self.watch.enable_complete()
        self._tick(1000)
        self.cast.assert_not_called()
        self.app.health = ch.view(ch.DEGRADED, ['10.0.0.2'],
                                  'failing: 10.0.0.2', me='10.0.0.1')
        self._tick(1030)
        kind, payload = (self.cast.call_args[0][1],
                         self.cast.call_args[1]['payload'])
        self.assertEqual(probe.ABNORMAL, kind)
        self.assertEqual('the cluster is DEGRADED: failing: 10.0.0.2',
                         payload['reason'])
        self.assertEqual(self.app.health, payload['health'])
        # No ready port on a watched member.
        self.assertEqual({'state': 'ok', 'role': 'master', 'writable': True},
                         payload['member'])
        # Again only after the event interval while it lasts.
        self._tick(1060)
        self.assertEqual(1, self.cast.call_count)
        self._tick(1330)
        self.assertEqual(2, self.cast.call_count)
        self.app.health = ch.view(ch.HEALTHY, me='10.0.0.1')
        self._tick(1360)
        self.assertEqual(probe.NORMAL_KIND, self.cast.call_args[0][1])

    def test_a_member_that_does_not_answer(self):
        self.watch.enable_complete()
        self.app.member_view = lambda: probe.UNKNOWN_MEMBER
        self.app.cluster_health = mock.Mock(side_effect=RuntimeError('no'))
        self._tick(1000)
        self.assertIsNone(self.watch.health)
        self.assertEqual('the member does not answer',
                         self.cast.call_args[1]['payload']['reason'])

    def test_leaving_says_nothing(self):
        self.watch.enable_complete()
        self.watch.disable()
        self.app.health = ch.view(ch.DOWN, [], 'cluster_state fail')
        self._tick(1000)
        self.cast.assert_not_called()


class TestWatchedManagers(trove_testtools.TestCase):
    """The managers of the datastores whose clusters heal themselves run
    the watch.
    """

    MANAGERS = (
        'trove.guestagent.datastore.redis.manager.Manager',
        'trove.guestagent.datastore.valkey.manager.Manager',
        'trove.guestagent.datastore.keydb.manager.Manager',
        'trove.guestagent.datastore.mongodb.manager.Manager',
        'trove.guestagent.datastore.cassandra.manager.Manager',
        'trove.guestagent.datastore.vertica.manager.Manager',
    )

    def test_they_watch(self):
        from oslo_utils import importutils
        from trove.guestagent.common import cluster_member
        for path in self.MANAGERS:
            cls = importutils.import_class(path)
            self.assertTrue(
                issubclass(cls, cluster_member.ClusterWatchMixin), path)

    def test_complete_and_asked(self):
        from trove.guestagent.common import cluster_member

        class Base(object):
            def cluster_complete(self, context):
                self.completed = True

        class Manager(cluster_member.ClusterWatchMixin, Base):
            pass

        manager = Manager()
        manager.app = mock.Mock()
        manager.app.cluster_health.return_value = ch.view(ch.HEALTHY)
        with mock.patch.object(probe, 'ClusterWatch') as watch:
            manager.init_cluster_watch()
        watch.return_value.start.assert_called_once_with()
        manager.cluster_complete(None)
        self.assertTrue(manager.completed)
        watch.return_value.enable_complete.assert_called_once_with()
        self.assertEqual(ch.view(ch.HEALTHY),
                         manager.get_cluster_health(None))
