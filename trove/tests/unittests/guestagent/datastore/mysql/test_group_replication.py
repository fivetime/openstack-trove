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

from oslo_utils import importutils

from trove.common import constants
from trove.common import exception
from trove.guestagent.common import cluster_probe
from trove.guestagent.datastore.group_replication import manager as gr_manager
from trove.guestagent.datastore.group_replication import service as gr_service
from trove.guestagent.datastore.mysql import manager as mysql_manager
from trove.guestagent.datastore.mysql import service as mysql_service
from trove.guestagent.datastore.percona import service as percona_service
from trove.tests.unittests import trove_testtools


class TestGroupReplicationWiring(trove_testtools.TestCase):

    def test_mysql_and_percona_managers(self):
        for datastore in ('mysql', 'percona'):
            manager_cls = importutils.import_class(
                constants.REGISTRY_EXT_DEFAULTS[datastore])
            mro = manager_cls.__mro__
            self.assertLess(
                mro.index(gr_manager.GroupReplicationManagerMixin),
                mro.index(mysql_manager.BaseManager))

    def test_apps(self):
        for cls in (mysql_service.GroupReplicationMySqlApp,
                    percona_service.PerconaApp):
            mro = cls.__mro__
            self.assertLess(mro.index(gr_service.GroupReplicationAppMixin),
                            mro.index(mysql_service.MySqlApp))

    @mock.patch.object(cluster_probe.ClusterProbe, 'start')
    @mock.patch.object(mysql_manager.BaseManager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_mysql_manager_runs_the_group_replication_app(self, _docker,
                                                          probe_start):
        manager = mysql_manager.Manager()
        self.assertIsInstance(manager.app,
                              mysql_service.GroupReplicationMySqlApp)
        self.assertIs(manager.app, manager.adm.mysql_app)
        # With the cluster probe running, on the ready port of the mysql
        # options.
        self.assertIsInstance(manager.cluster_probe,
                              cluster_probe.ClusterProbe)
        port = manager.cluster_probe.port
        self.assertEqual((3307, 3306), (port.port, port.target_port))
        probe_start.assert_called_once()

    @mock.patch.object(cluster_probe.ClusterProbe, 'start')
    @mock.patch.object(mysql_manager.BaseManager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_cluster_calls_tell_the_probe(self, _docker, _start):
        manager = mysql_manager.Manager()
        manager.app = mock.MagicMock()
        manager.status = mock.MagicMock()
        manager.cluster_probe = mock.MagicMock()
        calls = mock.Mock()
        calls.attach_mock(manager.cluster_probe, 'probe')
        calls.attach_mock(manager.app, 'app')

        manager.cluster_complete(None)
        # Group Replication's completion, not Galera's.
        manager.app.complete_cluster.assert_called_once()
        manager.app.leave_bootstrap.assert_not_called()
        manager.cluster_probe.enable_complete.assert_called_once()

        manager.leave_cluster(None)
        # The probe is disabled before the member leaves.
        self.assertEqual([mock.call.probe.disable(),
                          mock.call.app.leave_group()],
                         [c for c in calls.mock_calls
                          if 'disable' in str(c) or 'leave_group' in str(c)])

        manager.app.get_member_role.return_value = {'state': 'ONLINE'}
        self.assertEqual({'state': 'ONLINE'}, manager.get_member_role(None))


class TestGroupReplicationApp(trove_testtools.TestCase):

    def setUp(self):
        super(TestGroupReplicationApp, self).setUp()
        patcher = mock.patch.object(
            mysql_service.GroupReplicationMySqlApp, '__init__',
            return_value=None)
        patcher.start()
        self.addCleanup(mock.patch.stopall)
        self.app = mysql_service.GroupReplicationMySqlApp()
        self.app.docker_client = mock.MagicMock()
        self.configuration_manager = mock.MagicMock()
        mock.patch.object(
            mysql_service.GroupReplicationMySqlApp, 'configuration_manager',
            new_callable=mock.PropertyMock,
            return_value=self.configuration_manager).start()
        self.app.status = mock.MagicMock()
        self.sql = []
        self.client = mock.MagicMock()
        # SqlClient.execute takes the parameters as keywords only.
        self.client.execute.side_effect = (
            lambda stmt, **k: self.sql.append(str(stmt)))
        client_cls = mock.patch.object(gr_service.mysql_util,
                                       'SqlClient').start()
        client_cls.return_value.__enter__.return_value = self.client
        self.app.get_engine = mock.Mock()
        self.app.execute_sql = mock.Mock(
            side_effect=lambda stmt: self.sql.append(str(stmt)) or [])
        mock.patch.object(gr_service.docker_util,
                          'remove_container').start()
        mock.patch.object(gr_service.time, 'sleep').start()
        self.app.stop_db = mock.Mock()
        self.app.start_db = mock.Mock()
        self.app._write_cluster_healthcheck_file = mock.Mock()
        self.app._is_mysql84 = mock.Mock(return_value=True)

    def _states(self, *states):
        self.app._member_state = mock.Mock(side_effect=list(states))

    def test_bootstrap_forms_the_group(self):
        # The first state is the one the configuration is written in.
        self._states(('OFFLINE', None), ('ONLINE', 'PRIMARY'))
        self.app.install_cluster({'name': 'r', 'password': 'p'}, 'cfg',
                                 '--datadir=x', bootstrap=True)
        self.assertNotIn('RESET BINARY LOGS AND GTIDS', self.sql)
        start = self.sql.index('START GROUP_REPLICATION')
        self.assertEqual('SET GLOBAL group_replication_bootstrap_group = ON',
                         self.sql[start - 1])
        self.assertEqual(
            'SET GLOBAL group_replication_bootstrap_group = OFF',
            self.sql[start + 1])

    def test_join_resets_its_own_transactions_first(self):
        self._states(('OFFLINE', None), ('RECOVERING', 'SECONDARY'),
                     ('ONLINE', 'SECONDARY'))
        self.app.install_cluster({'name': 'r', 'password': 'p'}, 'cfg',
                                 '--datadir=x', bootstrap=False)
        self.assertLess(self.sql.index('RESET BINARY LOGS AND GTIDS'),
                        self.sql.index('START GROUP_REPLICATION'))
        self.assertTrue(any('FOR CHANNEL' in s for s in self.sql))
        self.assertNotIn('SET GLOBAL group_replication_bootstrap_group = ON',
                         self.sql)

    def test_join_starts_again_after_a_clone(self):
        # The clone stops the server; the container starts it again with
        # Group Replication off.
        self.app._restart_count = mock.Mock(side_effect=[0, 1])
        self._states(('OFFLINE', None), ('RECOVERING', 'SECONDARY'),
                     (None, None),
                     ('OFFLINE', None), ('RECOVERING', 'SECONDARY'),
                     ('ONLINE', 'SECONDARY'))
        self.app.install_cluster({'name': 'r', 'password': 'p'}, 'cfg',
                                 '--datadir=x', bootstrap=False)
        self.assertEqual(2, self.sql.count('START GROUP_REPLICATION'))

    def test_join_does_not_retry_without_a_restart(self):
        self.app._restart_count = mock.Mock(return_value=0)
        self.app._member_state = mock.Mock(return_value=('ERROR', None))
        with mock.patch.object(gr_service.time, 'time',
                               side_effect=[0, 1, 2, 10 ** 9]):
            self.assertRaises(exception.TroveError, self.app.install_cluster,
                              {'name': 'r', 'password': 'p'}, 'cfg',
                              '--datadir=x', bootstrap=False)
        self.assertEqual(1, self.sql.count('START GROUP_REPLICATION'))

    def test_the_only_member_of_the_group_is_marked_the_last_standing(self):
        members = [('a', 'ONLINE')]
        self.app.execute_sql = mock.Mock(side_effect=lambda stmt: (
            list(members) if 'replication_group_members' in str(stmt)
            else []))
        self.configuration_manager.has_system_override.return_value = False
        # Alone in the group: marked.
        self._states(('ONLINE', 'PRIMARY'))
        self.assertTrue(self.app.member_view().writable)
        self.configuration_manager.apply_system_override.\
            assert_called_once_with(
                {'trove': {'cluster_last_standing': '1'}},
                gr_service.CNF_LAST)
        self.configuration_manager.remove_system_override.assert_not_called()
        # Another member in: the mark goes.
        self.configuration_manager.has_system_override.return_value = True
        members.append(('b', 'RECOVERING'))
        self._states(('ONLINE', 'PRIMARY'))
        self.app.member_view()
        self.configuration_manager.remove_system_override.\
            assert_called_once_with(gr_service.CNF_LAST)
        # Out of the group: the mark, whatever it is, stays for the
        # recovery to read.
        self.configuration_manager.remove_system_override.reset_mock()
        self.configuration_manager.apply_system_override.reset_mock()
        self._states(('OFFLINE', None))
        self.assertTrue(self.app.member_view().down)
        self.configuration_manager.apply_system_override.assert_not_called()
        self.configuration_manager.remove_system_override.assert_not_called()
        self.assertTrue(self.app.was_last_standing())

    def test_complete_cluster_starts_on_boot(self):
        # Whatever command the first member was started with: nothing to
        # undo on it.
        self.app.complete_cluster('--datadir=x')
        self.configuration_manager.apply_system_override.\
            assert_called_once_with(
                {'mysqld': {'group_replication_start_on_boot': 'ON'}},
                gr_service.CNF_STARTED)
        self.app.stop_db.assert_not_called()
        # And that is what makes the cluster complete.
        self.configuration_manager.has_system_override.return_value = True
        self.assertTrue(self.app.is_cluster_complete())
        self.configuration_manager.has_system_override.\
            assert_called_once_with(gr_service.CNF_STARTED)

    def test_running_member_gets_the_new_members(self):
        self._states(('ONLINE', 'PRIMARY'))
        mock.patch.object(
            mysql_service.GroupReplicationMySqlApp, 'cluster_configuration',
            new_callable=mock.PropertyMock,
            return_value={
                'group_replication_ip_allowlist': '"10.0.0.1,10.0.0.4"',
                'group_replication_group_seeds': '"10.0.0.1:33061"'}).start()
        self.app.write_cluster_configuration_overrides('cfg')
        self.configuration_manager.apply_system_override.\
            assert_called_once_with('cfg', 'cluster')
        values = [c[1]['value'] for c in self.client.execute.call_args_list
                  if 'value' in c[1]]
        self.assertEqual(['10.0.0.1,10.0.0.4', '10.0.0.1:33061'], values)

    def test_stopped_member_only_keeps_the_configuration(self):
        self._states(('OFFLINE', None))
        self.app.write_cluster_configuration_overrides('cfg')
        self.client.execute.assert_not_called()

    def test_keep_group_mode(self):
        self.app.keep_group_mode('multi-primary')
        self.configuration_manager.apply_system_override.\
            assert_called_once_with(
                {'mysqld': {
                    'loose-group_replication_single_primary_mode': 'OFF',
                    'loose-group_replication_enforce_update_everywhere_'
                    'checks': 'ON'}},
                gr_service.CNF_MODE)
        self.assertRaises(exception.BadRequest, self.app.keep_group_mode,
                          'both')

    def test_group_mode(self):
        for configuration, mode in (
                ({}, 'single-primary'),
                ({'loose-group_replication_single_primary_mode': 'OFF'},
                 'multi-primary'),
                ({'group_replication_single_primary_mode': 'ON',
                  'loose-group_replication_single_primary_mode': 'OFF'},
                 'single-primary')):
            with mock.patch.object(
                    type(self.app), 'cluster_configuration',
                    new_callable=mock.PropertyMock,
                    return_value=configuration):
                self.assertEqual(mode, self.app._group_mode())

    def _configuration(self, **values):
        return mock.patch.object(
            mysql_service.GroupReplicationMySqlApp, 'cluster_configuration',
            new_callable=mock.PropertyMock, return_value=values)

    def test_self_and_seed_addresses(self):
        with self._configuration(
                report_host='10.0.0.2',
                group_replication_group_seeds=(
                    '"10.0.0.1:33061, 10.0.0.2:33061,[fd00::3]:33061"')):
            self.assertEqual('10.0.0.2', self.app._self_ip())
            self.assertEqual(['10.0.0.1', '10.0.0.2', 'fd00::3'],
                             self.app._seed_ips())
        with self._configuration(
                group_replication_local_address='"10.0.0.2:33061"'):
            self.assertEqual('10.0.0.2', self.app._self_ip())

    def test_query_peer(self):
        # The question goes out from the database container: the tenant
        # NIC is in there.
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (
            0, b'u-2\tOFFLINE\nu-3\tONLINE\nu-2\ng:1-5,\\nh:1-2\n')
        peer = self.app._query_peer('10.0.0.2', 'r', 'p', 3)
        # Out of the group itself, but it sees a member in it.
        self.assertEqual(cluster_probe.PeerView(
            '10.0.0.2', True, False, True, 'g:1-5,h:1-2'), peer)
        self.app.docker_client.containers.get.assert_called_with('database')
        command = container.exec_run.call_args.args[0]
        self.assertEqual('mysql', command[0])
        self.assertIn('--connect-timeout=3', command)
        self.assertIn('--host=10.0.0.2', command)
        self.assertIn('--port=3306', command)
        self.assertIn('--user=r', command)
        self.assertIn('--skip-column-names', command)
        self.assertEqual({'MYSQL_PWD': 'p'},
                         container.exec_run.call_args.kwargs['environment'])
        self.assertNotIn('p', command)
        # In the group.
        container.exec_run.return_value = (
            0, b'u-2\tRECOVERING\nu-3\tONLINE\nu-2\ng:1-5\n')
        self.assertEqual((True, True, 'g:1-5'),
                         self.app._query_peer('10.0.0.2', 'r', 'p', 3)[2:5])
        # Nobody in the group, nothing executed yet: the gtid set is
        # empty.
        container.exec_run.return_value = (0, b'u-2\n\n')
        self.assertEqual((False, False, ''),
                         self.app._query_peer('10.0.0.2', 'r', 'p', 3)[2:5])
        # Not answering: the client fails, or the container is not there.
        container.exec_run.return_value = (
            1, b"ERROR 2003 (HY000): Can't connect to MySQL server")
        self.assertFalse(self.app._query_peer('10.0.0.2', 'r', 'p', 3)
                         .reachable)
        self.app.docker_client.containers.get.side_effect = Exception('gone')
        self.assertFalse(self.app._query_peer('10.0.0.2', 'r', 'p', 3)
                         .reachable)

    def test_position_and_not_ahead(self):
        self.app.execute_sql = mock.Mock(return_value=iter([('a:1-2',)]))
        self.assertEqual('a:1-2', self.app._position())
        self.client.execute.side_effect = lambda stmt, **k: iter([(1,)])
        self.assertTrue(self.app._not_ahead('a:1', 'a:1-2'))
        stmt, kwargs = self.client.execute.call_args[0][0], \
            self.client.execute.call_args[1]
        self.assertIn('GTID_SUBSET', str(stmt))
        self.assertEqual({'subset': 'a:1', 'superset': 'a:1-2'}, kwargs)

    def test_rejoin_and_bootstrap(self):
        self.app.rejoin_group('ERROR')
        self.assertEqual(['STOP GROUP_REPLICATION', 'START GROUP_REPLICATION'],
                         [s for s in self.sql if 'GROUP_REPLICATION' in s])
        self.sql.clear()
        # Stopped first all the same: a member that failed to join by
        # itself when it started may be left half way.
        self.app.rejoin_group('OFFLINE')
        self.assertEqual(['STOP GROUP_REPLICATION', 'START GROUP_REPLICATION'],
                         [s for s in self.sql if 'GROUP_REPLICATION' in s])
        self.sql.clear()
        self.app.bootstrap_group()
        self.assertEqual(
            ['SET GLOBAL group_replication_bootstrap_group = ON',
             'START GROUP_REPLICATION',
             'SET GLOBAL group_replication_bootstrap_group = OFF'],
            self.sql)

    def test_member_view(self):
        # Writable as the primary that is ONLINE; down when out of the
        # group, not when the state is not known.
        for state, writable, down in ((('ONLINE', 'PRIMARY'), True, False),
                                      (('ONLINE', 'SECONDARY'), False, False),
                                      (('RECOVERING', 'PRIMARY'), False,
                                       False),
                                      (('OFFLINE', None), False, True),
                                      (('ERROR', None), False, True),
                                      ((None, None), False, False)):
            self._states(state)
            view = self.app.member_view()
            self.assertEqual(state + (writable, down), tuple(view))
            self._states(state)
            self.assertEqual(writable, self.app.is_writable_member())

    def test_member_role(self):
        self._states(('ONLINE', 'SECONDARY'))
        self.assertEqual({'state': 'ONLINE', 'role': 'SECONDARY',
                          'writable': False}, self.app.get_member_role())
        self._states((None, None))
        self.assertEqual({'state': None, 'role': None, 'writable': False},
                         self.app.get_member_role())
