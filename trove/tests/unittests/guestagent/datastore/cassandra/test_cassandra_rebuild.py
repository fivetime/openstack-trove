#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from unittest import mock

import bcrypt

from trove.common import exception
from trove.guestagent.datastore.cassandra import manager as cassandra_manager
from trove.guestagent.datastore.cassandra import service as cassandra_service
from trove.guestagent.datastore import manager as base_manager
from trove.tests.unittests.guestagent.datastore.cassandra import (
    test_cassandra)

CONFIG = 'num_tokens: 16\n'
OVERRIDES = {'concurrent_reads': 64}


class TestCassandraRebuild(test_cassandra.CassandraGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = cassandra_manager.Manager()
        manager.app = mock.Mock()
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _rebuild(self, manager, **kwargs):
        manager.rebuild(mock.Mock(), '5.0', config_contents=CONFIG,
                        config_overrides=OVERRIDES, **kwargs)

    def test_rebuild(self):
        manager = self._manager()
        app = manager.app
        order = mock.Mock()
        order.attach_mock(manager.status.begin_install, 'begin')
        order.attach_mock(app.configuration_manager.reset_configuration,
                          'reset_configuration')
        order.attach_mock(app.apply_initial_guestagent_configuration,
                          'initial_configuration')
        order.attach_mock(app.update_overrides, 'update_overrides')
        order.attach_mock(app.write_cluster_topology, 'topology')
        order.attach_mock(app.reset_admin_password, 'reset_password')
        order.attach_mock(app.start_db, 'start_db')
        order.attach_mock(manager.status.end_install, 'end')

        self._rebuild(manager, properties={'unused': True})

        self.assertEqual(['begin', 'reset_configuration',
                          'initial_configuration', 'update_overrides',
                          'topology', 'reset_password', 'start_db', 'end'],
                         [call[0] for call in order.mock_calls])
        app.configuration_manager.reset_configuration.assert_called_once_with(
            CONFIG)
        # A single instance, named after itself.
        app.apply_initial_guestagent_configuration.assert_called_once_with()
        app.update_overrides.assert_called_once_with(OVERRIDES)
        app.write_cluster_topology.assert_called_once_with(
            cassandra_service.DEFAULT_DATA_CENTER,
            cassandra_service.DEFAULT_RACK)
        app.reset_admin_password.assert_called_once_with(ds_version='5.0')
        app.start_db.assert_called_once_with(ds_version='5.0')
        # The superuser and the users are in the data: no new superuser
        # with the built-in one, no restore.
        app.secure.assert_not_called()
        app.apply_post_restore_updates.assert_not_called()
        manager.adm.create_user.assert_not_called()
        manager.adm.create_database.assert_not_called()
        manager.status.end_install.assert_called_once_with(
            error_occurred=False)
        self.assertFalse(manager.prepare_error)

    def test_failure(self):
        manager = self._manager()
        manager.app.reset_admin_password.side_effect = \
            exception.TroveError('boom')

        self.assertRaisesRegex(exception.TroveError, 'boom', self._rebuild,
                               manager)

        manager.app.start_db.assert_not_called()
        self.assertTrue(manager.prepare_error)
        manager.status.begin_install.assert_called_once_with()
        manager.status.end_install.assert_called_once_with(
            error_occurred=True)

    @mock.patch.object(cassandra_service.CassandraApp, '_write_credentials')
    @mock.patch.object(cassandra_service.CassandraApp, 'stop_db')
    @mock.patch.object(cassandra_service.CassandraApp, 'start_db')
    def test_reset_admin_password(self, mock_start, mock_stop, mock_write):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        app.adm = mock.Mock()
        app.adm.is_available.return_value = True
        seen = {}
        mock_start.side_effect = lambda **kw: seen.update(
            app.configuration_manager.parse_configuration())

        with mock.patch.object(cassandra_service.CassandraApp,
                               'nodetool') as nodetool:
            app.reset_admin_password(ds_version='5.0')

        # Started once under the name the data already has, this
        # instance's, without authentication, from the instance only.
        mock_start.assert_called_once_with(ds_version='5.0')
        self.assertEqual('guest-0001', seen['cluster_name'])
        self.assertEqual(cassandra_service.ALLOW_ALL_AUTHENTICATOR,
                         seen['authenticator'])
        self.assertEqual('127.0.0.1', seen['listen_address'])
        app.adm.is_available.assert_called_with(authenticate=False)

        name, salted_hash = app.adm.reset_superuser.call_args[0]
        username, password = mock_write.call_args[0]
        self.assertEqual('os_admin', name)
        self.assertEqual('os_admin', username)
        self.assertTrue(bcrypt.checkpw(password.encode(),
                                       salted_hash.encode()))
        nodetool.assert_any_call('flush', 'system_auth')

        # Then itself again, with authentication.
        mock_stop.assert_called_once()
        config = app.configuration_manager.parse_configuration()
        self.assertEqual('guest-0001', config['cluster_name'])
        self.assertEqual(cassandra_service.PASSWORD_AUTHENTICATOR,
                         config['authenticator'])
        self.assertEqual('10.0.0.5', config['listen_address'])

    @mock.patch.object(cassandra_service.CassandraApp, '_write_credentials')
    @mock.patch.object(cassandra_service.CassandraApp, 'stop_db')
    @mock.patch.object(cassandra_service.CassandraApp, 'start_db',
                       side_effect=exception.TroveError('boom'))
    def test_reset_admin_password_failure(self, mock_start, mock_stop,
                                          mock_write):
        app = self._app()
        app.apply_initial_guestagent_configuration()
        self.assertRaises(exception.TroveError, app.reset_admin_password)
        config = app.configuration_manager.parse_configuration()
        self.assertEqual(cassandra_service.PASSWORD_AUTHENTICATOR,
                         config['authenticator'])
        mock_write.assert_not_called()
