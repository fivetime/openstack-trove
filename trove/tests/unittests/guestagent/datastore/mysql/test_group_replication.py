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

    @mock.patch.object(mysql_manager.BaseManager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_mysql_manager_runs_the_group_replication_app(self, _docker):
        manager = mysql_manager.Manager()
        self.assertIsInstance(manager.app,
                              mysql_service.GroupReplicationMySqlApp)
        self.assertIs(manager.app, manager.adm.mysql_app)


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

    def test_complete_cluster_starts_on_boot(self):
        self.app.complete_cluster()
        self.configuration_manager.apply_system_override.\
            assert_called_once_with(
                {'mysqld': {'group_replication_start_on_boot': 'ON'}},
                gr_service.CNF_STARTED)

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

    def test_writable_member(self):
        for state, writable in ((('ONLINE', 'PRIMARY'), True),
                                (('ONLINE', 'SECONDARY'), False),
                                (('RECOVERING', 'PRIMARY'), False)):
            self._states(state)
            self.assertEqual(writable, self.app.is_writable_member())
