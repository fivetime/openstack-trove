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
from trove.guestagent.common import readyport
from trove.tests.unittests import trove_testtools

RULE = ['PREROUTING', '-p', 'tcp', '--dport', '3307',
        '-j', 'REDIRECT', '--to-ports', '3306']


RESET = ['INPUT', '-p', 'tcp', '--dport', '3306',
         '-m', 'conntrack', '--ctorigdstport', '3307',
         '-j', 'REJECT', '--reject-with', 'tcp-reset']


def _iptables_call(pid, action):
    return mock.call('nsenter', '-t', str(pid), '-n', 'iptables', '-w', '5',
                     '-t', 'nat', action, *RULE,
                     run_as_root=True, root_helper='sudo', timeout=15)


def _reset_call(pid, action):
    return mock.call('nsenter', '-t', str(pid), '-n', 'iptables', '-w', '5',
                     '-t', 'filter', action, *RESET,
                     run_as_root=True, root_helper='sudo', timeout=15)


class TestReadyPort(trove_testtools.TestCase):

    def setUp(self):
        super(TestReadyPort, self).setUp()
        self.docker = mock.MagicMock()
        self.docker.containers.get.return_value.attrs = {
            'State': {'Pid': 4242}}
        self.execute = mock.patch.object(readyport.utils,
                                         'execute_with_timeout').start()
        self.addCleanup(mock.patch.stopall)
        self.port = readyport.ReadyPort(self.docker, 3307, 3306)

    def _rules(self, redirect, reset):
        # -C exits with 1 when there is no such rule.
        def execute(*args, **kwargs):
            if '-C' in args:
                there = reset if 'filter' in args else redirect
                if not there:
                    raise exception.ProcessExecutionError(exit_code=1)
        self.execute.side_effect = execute

    def _rule_absent(self):
        # An open port: the redirect there, the reset not.
        self._rules(redirect=False, reset=False)

    def test_no_container(self):
        self.docker.containers.get.side_effect = Exception('no such')
        self.assertIsNone(self.port.ensure(True))
        self.execute.assert_not_called()
        self.assertIsNone(self.port.pid)

    def test_the_database_container(self):
        self.port.ensure(True)
        self.docker.containers.get.assert_called_with('database')
        readyport.ReadyPort(self.docker, 3307, 3306,
                            container_name='db').ensure(True)
        self.docker.containers.get.assert_called_with('db')

    def test_opens_a_missing_rule(self):
        self._rule_absent()
        self.assertTrue(self.port.ensure(True))
        self.assertEqual([_iptables_call(4242, '-C'), _reset_call(4242, '-C'),
                          _iptables_call(4242, '-A')],
                         self.execute.call_args_list)
        self.assertEqual(4242, self.port.pid)
        self.assertTrue(self.port.applied)

    def test_opens_a_closed_port(self):
        # Closed: the reset rule is there; it goes before the redirect
        # comes, so that no connection is reset after it was redirected.
        self._rules(redirect=False, reset=True)
        self.assertTrue(self.port.ensure(True))
        self.assertEqual([_iptables_call(4242, '-C'), _reset_call(4242, '-C'),
                          _reset_call(4242, '-D'), _iptables_call(4242, '-A')],
                         self.execute.call_args_list)

    def test_leaves_a_present_rule(self):
        self._rules(redirect=True, reset=False)
        self.assertTrue(self.port.ensure(True))
        self.assertEqual([_iptables_call(4242, '-C'),
                          _reset_call(4242, '-C')],
                         self.execute.call_args_list)

    def test_closes_a_present_rule(self):
        # The redirect goes, then the connections made through the port
        # are reset while it stays closed.
        self._rules(redirect=True, reset=False)
        self.assertFalse(self.port.ensure(False))
        self.assertEqual([_iptables_call(4242, '-C'), _reset_call(4242, '-C'),
                          _iptables_call(4242, '-D'), _reset_call(4242, '-A')],
                         self.execute.call_args_list)
        self.assertFalse(self.port.applied)

    def test_leaves_a_missing_rule_closed(self):
        self._rules(redirect=False, reset=True)
        self.assertFalse(self.port.ensure(False))
        self.assertEqual(2, self.execute.call_count)

    def test_a_closed_port_without_the_reset_gets_it(self):
        # As after the container was created anew: no rule at all.
        self._rules(redirect=False, reset=False)
        self.assertFalse(self.port.ensure(False))
        self.assertEqual([_iptables_call(4242, '-C'), _reset_call(4242, '-C'),
                          _reset_call(4242, '-A')],
                         self.execute.call_args_list)

    def test_a_failing_check_is_not_an_answer(self):
        self.execute.side_effect = exception.ProcessExecutionError(
            exit_code=4)
        self.assertRaises(exception.ProcessExecutionError,
                          self.port.ensure, True)
        self.assertIsNone(self.port.applied)
