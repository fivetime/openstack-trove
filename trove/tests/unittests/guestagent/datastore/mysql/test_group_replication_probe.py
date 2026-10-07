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

from trove.common import exception
from trove.guestagent.datastore.group_replication import probe
from trove.tests.unittests import trove_testtools

RULE = ['PREROUTING', '-p', 'tcp', '--dport', '3307',
        '-j', 'REDIRECT', '--to-ports', '3306']


def _iptables_call(pid, action):
    return mock.call('nsenter', '-t', str(pid), '-n', 'iptables', '-w', '5',
                     '-t', 'nat', action, *RULE,
                     run_as_root=True, root_helper='sudo', timeout=15)


class TestReadyPort(trove_testtools.TestCase):

    def setUp(self):
        super(TestReadyPort, self).setUp()
        self.docker = mock.MagicMock()
        self.docker.containers.get.return_value.attrs = {
            'State': {'Pid': 4242}}
        self.execute = mock.patch.object(probe.utils,
                                         'execute_with_timeout').start()
        self.addCleanup(mock.patch.stopall)
        self.port = probe.ReadyPort(self.docker, 3307)

    def _rule_absent(self):
        # -C exits with 1 when there is no such rule.
        def execute(*args, **kwargs):
            if '-C' in args:
                raise exception.ProcessExecutionError(exit_code=1)
        self.execute.side_effect = execute

    def test_no_container(self):
        self.docker.containers.get.side_effect = Exception('no such')
        self.assertIsNone(self.port.ensure(True))
        self.execute.assert_not_called()
        self.assertIsNone(self.port.pid)

    def test_opens_a_missing_rule(self):
        self._rule_absent()
        self.assertTrue(self.port.ensure(True))
        self.assertEqual([_iptables_call(4242, '-C'),
                          _iptables_call(4242, '-A')],
                         self.execute.call_args_list)
        self.assertEqual(4242, self.port.pid)
        self.assertTrue(self.port.applied)

    def test_leaves_a_present_rule(self):
        self.assertTrue(self.port.ensure(True))
        self.assertEqual([_iptables_call(4242, '-C')],
                         self.execute.call_args_list)

    def test_closes_a_present_rule(self):
        self.assertFalse(self.port.ensure(False))
        self.assertEqual([_iptables_call(4242, '-C'),
                          _iptables_call(4242, '-D')],
                         self.execute.call_args_list)
        self.assertFalse(self.port.applied)

    def test_leaves_a_missing_rule_closed(self):
        self._rule_absent()
        self.assertFalse(self.port.ensure(False))
        self.assertEqual(1, self.execute.call_count)

    def test_a_failing_check_is_not_an_answer(self):
        self.execute.side_effect = exception.ProcessExecutionError(
            exit_code=4)
        self.assertRaises(exception.ProcessExecutionError,
                          self.port.ensure, True)
        self.assertIsNone(self.port.applied)


class ProbeTestBase(trove_testtools.TestCase):
    """A probe on a mocked app, container and configuration."""

    def setUp(self):
        super(ProbeTestBase, self).setUp()
        self.app = mock.MagicMock()
        self.app.is_cluster_member.return_value = True
        self.app.configuration_manager.has_system_override.return_value = (
            False)
        self.app._member_state.return_value = ('ONLINE', 'PRIMARY')
        self.conf = mock.Mock(group_replication_probe_interval=3,
                              group_replication_probe_timeout=5,
                              group_replication_ready_port_reconcile=20,
                              group_replication_ready_port=3307,
                              group_replication_recovery_grace=60,
                              group_replication_recovery_interval=30,
                              group_replication_peer_timeout=3,
                              group_replication_auto_bootstrap=True,
                              group_replication_bootstrap_needs_all_members=(
                                  False),
                              group_replication_bootstrap_jitter=0)
        # No eventlet timeout around the query in tests.
        timeout = mock.patch.object(probe.eventlet, 'Timeout').start()
        timeout.return_value.__enter__.return_value = None
        self.addCleanup(mock.patch.stopall)
        self.probe = probe.RoleProbe(self.app, mock.MagicMock(),
                                     conf=self.conf)
        self.probe.port = mock.MagicMock(spec=probe.ReadyPort)
        self.probe.port.pid = 4242
        self.probe.port.applied = None
        self.probe.port.container_pid.return_value = 4242


class TestRoleProbe(ProbeTestBase):

    def test_flags_are_read_once(self):
        self.probe._tick()
        self.probe._tick()
        self.app.is_cluster_member.assert_called_once()
        self.app.configuration_manager.has_system_override.\
            assert_called_once_with('cluster-started')
        self.assertTrue(self.probe.member)
        self.assertFalse(self.probe.complete)

    def test_not_a_member(self):
        self.app.is_cluster_member.return_value = False
        self.probe._tick()
        self.app._member_state.assert_not_called()
        self.probe.port.ensure.assert_not_called()

    def test_the_port_follows_the_role(self):
        for state, wanted in ((('ONLINE', 'PRIMARY'), True),
                              (('ONLINE', 'SECONDARY'), False),
                              (('RECOVERING', 'PRIMARY'), False),
                              ((None, None), False)):
            self.app._member_state.return_value = state
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
        self.app._member_state.side_effect = Exception('gone')
        self.probe._tick()
        self.app._member_state.side_effect = None
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
        self.conf.group_replication_probe_interval = 0
        probe.RoleProbe(self.app, mock.MagicMock(), conf=self.conf).start()
        looping_call.assert_not_called()


def _peer(ip, state='OFFLINE', gtid='u:1-10', reachable=True,
          sees_group=False):
    return probe.gr_service.PeerView(ip, reachable, state, sees_group, gtid)


def _subset(a, b):
    # GTID sets as "u:1-N": a within b when a's N is not above b's.
    def last(s):
        return int(s.rsplit('-', 1)[-1]) if s else 0
    return last(a) <= last(b)


class TestDecide(trove_testtools.TestCase):

    def decide(self, self_ip, self_gtid, peers, n=3, needs_all=False):
        return probe.decide(self_ip, self_gtid, peers, n, _subset,
                            needs_all)

    def test_a_group_that_is_up_is_joined(self):
        for peer in (_peer('10.0.0.2', 'ONLINE'),
                     _peer('10.0.0.2', 'RECOVERING'),
                     _peer('10.0.0.2', 'OFFLINE', sees_group=True)):
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
            [_peer('10.0.0.2', gtid='u:1-12'), _peer('10.0.0.3')])
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
        self.app._gtid_executed.return_value = 'u:1-10'
        self.app._gtid_subset.side_effect = _subset
        self.peers = {'10.0.0.2': _peer('10.0.0.2'),
                      '10.0.0.3': _peer('10.0.0.3')}
        self.app._query_peer.side_effect = (
            lambda ip, user, pw, timeout: self.peers[ip])
        mock.patch.object(probe.time, 'sleep').start()
        self.app._member_state.return_value = ('OFFLINE', None)

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

    def test_rejoins_a_group_that_is_up(self):
        self.peers['10.0.0.2'] = _peer('10.0.0.2', 'ONLINE')
        self._tick_at(0)
        self._tick_at(100)
        self.app.rejoin_group.assert_called_once_with('OFFLINE')
        self.app.bootstrap_group.assert_not_called()

    def test_forms_the_group_again(self):
        self._tick_at(0)
        self._tick_at(100)
        self.app.bootstrap_group.assert_called_once()
        self.app.rejoin_group.assert_not_called()

    def test_does_not_form_the_group_when_the_peers_changed(self):
        answers = [self.peers['10.0.0.2'], self.peers['10.0.0.3'],
                   _peer('10.0.0.2', 'ONLINE'), self.peers['10.0.0.3']]
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
