# Copyright 2026 PS Cloud
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
import random
from unittest import mock

from oslo_service import periodic_task

from trove.common import exception
from trove.common import ssl
from trove.guestagent.common import operating_system
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.instance import service_status
from trove.tests.unittests import trove_testtools


class TestManager(trove_testtools.TestCase):
    def setUp(self):
        super(TestManager, self).setUp()
        self.manager = base_manager.Manager('test')

    def test_ssl_mode_at_least(self):
        self.assertTrue(self.manager.ssl_mode_at_least(
            ssl.MODE_BASIC, ssl.MODE_BASIC))
        self.assertTrue(self.manager.ssl_mode_at_least(
            ssl.MODE_ENFORCED, ssl.MODE_BASIC))
        self.assertTrue(self.manager.ssl_mode_at_least(
            ssl.MODE_ENFORCED, ssl.MODE_ENFORCED))
        self.assertTrue(self.manager.ssl_mode_at_least(
            ssl.MODE_MTLS, ssl.MODE_ENFORCED))
        self.assertFalse(self.manager.ssl_mode_at_least(
            ssl.MODE_BASIC, ssl.MODE_MTLS))
        self.assertFalse(self.manager.ssl_mode_at_least(
            'invalid', ssl.MODE_BASIC))
        self.assertFalse(self.manager.ssl_mode_at_least(
            ssl.MODE_BASIC, 'invalid'))
        self.assertFalse(self.manager.ssl_mode_at_least(
            None, ssl.MODE_BASIC))

    def test_replication_positions_not_told_are_not_supported(self):
        # The task manager's eject asks every replica; a datastore that
        # does not tell says so, rather than "Endpoint does not support
        # RPC method" (Redis and PostgreSQL said that until 2026-10-20).
        for call in (self.manager.get_last_txn,
                     self.manager.get_latest_txn_id):
            self.assertRaises(exception.DatastoreOperationNotSupported,
                              call, None)

    @mock.patch.object(operating_system, 'chmod')
    @mock.patch.object(operating_system, 'chown')
    @mock.patch.object(operating_system, 'exists', return_value=True)
    def test_validate_log_file_owner_and_group(
            self, mock_exists, mock_chown, mock_chmod):
        self.manager.validate_log_file('/fake/test.log', '1001', group='1002')

        mock_chown.assert_called_once_with(
            '/fake/test.log', user='1001', group='1002', as_root=True)

    @mock.patch.object(operating_system, 'chmod')
    @mock.patch.object(operating_system, 'chown')
    @mock.patch.object(operating_system, 'exists', return_value=True)
    def test_validate_log_file_group_defaults_to_owner(
            self, mock_exists, mock_chown, mock_chmod):
        self.manager.validate_log_file('/fake/test.log', '1001')

        mock_chown.assert_called_once_with(
            '/fake/test.log', user='1001', group='1001', as_root=True)


class TestHeartbeat(trove_testtools.TestCase):
    """The heartbeat (update_status) as oslo's periodic loop runs it: the
    real run_periodic_tasks, a pass every report_interval starting a
    little late at random, and a clock of our own.
    """

    def _gaps(self, passes=2000):
        manager = base_manager.Manager('test')
        # Taken at import from the real clock; this clock starts at 0.
        manager._periodic_last_run['update_status'] = None
        manager.status = mock.Mock(is_installed=True)
        clock = [0.0]
        beats = []
        manager.status.set_status.side_effect = lambda *a, **kw: \
            beats.append(clock[0])
        rand = random.Random(1)
        interval = base_manager.CONF.report_interval
        with mock.patch.object(periodic_task, 'now',
                               side_effect=lambda: clock[0]), \
                mock.patch.object(periodic_task.random, 'random',
                                  side_effect=rand.random), \
                mock.patch.object(periodic_task.time, 'sleep'):
            for k in range(passes):
                clock[0] = interval * k + rand.random() * 2
                manager.run_periodic_tasks(None)
        return [b - a for a, b in zip(beats, beats[1:])]

    def test_the_heartbeat_is_at_most_two_passes_apart(self):
        # agent_heartbeat_expiry is 90 s: a third pass would be too late.
        self.assertEqual(30, base_manager.CONF.report_interval)
        gaps = self._gaps()
        self.assertLess(max(gaps), 2 * 30 + 3)
        # Most beats are a minute apart, as before: not twice as many.
        self.assertGreater(sum(g > 45 for g in gaps), 0.8 * len(gaps))

    def test_the_spacing_follows_the_report_interval(self):
        self.assertEqual(55, base_manager.heartbeat_spacing(30))
        self.assertEqual(15, base_manager.heartbeat_spacing(10))
        self.assertEqual(114, base_manager.heartbeat_spacing(60))
        self.assertEqual(55, base_manager.Manager('test')._periodic_spacing[
            'update_status'])


class TestFirstHeartbeat(trove_testtools.TestCase):
    """The agent's first heartbeat goes out when it starts serving, with
    the real status object and the conductor's cast mocked.
    """

    def setUp(self):
        super(TestFirstHeartbeat, self).setUp()
        self.manager = base_manager.Manager('test')
        self.manager.status = base_service.BaseDbStatus(mock.Mock())
        self.installed = mock.patch.object(
            base_service.BaseDbStatus, 'is_installed',
            new_callable=mock.PropertyMock, return_value=True).start()
        mock.patch.object(
            base_service.BaseDbStatus, 'get_actual_db_status',
            return_value=service_status.ServiceStatuses.HEALTHY).start()
        self.conductor = mock.patch.object(
            base_service.conductor_api, 'API').start()
        self.addCleanup(mock.patch.stopall)

    def test_the_first_heartbeat_goes_out_when_the_agent_serves(self):
        self.assertFalse(self.manager.status.heartbeat_sent)
        self.manager.service_started()
        beat = self.conductor.return_value.heartbeat
        beat.assert_called_once()
        self.assertEqual({'service_status': 'healthy'}, beat.call_args.args[1])
        self.assertTrue(self.manager.status.heartbeat_sent)

    def test_none_before_the_database_is_installed(self):
        self.installed.return_value = False
        self.manager.service_started()
        self.conductor.return_value.heartbeat.assert_not_called()
        self.assertFalse(self.manager.status.heartbeat_sent)

    def test_a_failing_first_heartbeat_does_not_stop_the_agent(self):
        self.conductor.return_value.heartbeat.side_effect = RuntimeError(
            'no broker')
        self.manager.service_started()
        self.assertFalse(self.manager.status.heartbeat_sent)

    def test_a_manager_without_a_status_sends_nothing(self):
        manager = base_manager.Manager('test')
        manager.service_started()
        self.conductor.return_value.heartbeat.assert_not_called()
