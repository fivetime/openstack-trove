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

from trove.common import exception
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore.mongodb import manager as mongodb_manager
from trove.guestagent.datastore.mongodb import service as mongodb_service
from trove.tests.unittests.guestagent.datastore.mongodb import test_mongodb

CONFIG = 'systemLog:\n  verbosity: 0\n'
OVERRIDES = {'net.maxIncomingConnections': 500}


class TestMongoDBRebuild(test_mongodb.MongoDBGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = mongodb_manager.Manager()
        manager.app = mock.Mock()
        manager.adm = manager.app.adm
        manager.adm.has_replica_set_config.return_value = False
        manager.status = mock.Mock()
        return manager

    def _rebuild(self, manager, **kwargs):
        manager.rebuild(mock.Mock(), '8.2', config_contents=CONFIG,
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
        order.attach_mock(app.start_temporary_db, 'start_temp')
        order.attach_mock(manager.adm.has_replica_set_config, 'is_member')
        order.attach_mock(app.reset_admin_password, 'reset_password')
        order.attach_mock(app.stop_temporary_db, 'stop_temp')
        order.attach_mock(app.start_db, 'start_db')
        order.attach_mock(manager.status.end_install, 'end')

        self._rebuild(manager, properties={'unused': True})

        self.assertEqual(['begin', 'reset_configuration',
                          'initial_configuration', 'update_overrides',
                          'start_temp', 'is_member', 'reset_password',
                          'stop_temp', 'start_db', 'end'],
                         [call[0] for call in order.mock_calls])
        app.configuration_manager.reset_configuration.assert_called_once_with(
            CONFIG)
        # As a single instance: no cluster configuration.
        app.apply_initial_guestagent_configuration.assert_called_once_with()
        app.update_overrides.assert_called_once_with(OVERRIDES)
        app.start_temporary_db.assert_called_once_with(ds_version='8.2')
        app.start_db.assert_called_once_with(ds_version='8.2')
        # The admin user is in the data: it gets a new password, it is not
        # created again with the localhost exception, and nothing else
        # touches the data.
        app.reset_admin_password.assert_called_once_with()
        app.secure.assert_not_called()
        app.resync.assert_not_called()
        manager.adm.create_users.assert_not_called()
        manager.adm.create_database.assert_not_called()
        manager.status.end_install.assert_called_once_with(
            error_occurred=False)
        self.assertFalse(manager.prepare_error)

    def test_replica_set_member_is_refused(self):
        manager = self._manager()
        manager.adm.has_replica_set_config.return_value = True

        self.assertRaisesRegex(exception.TroveError, 'replica set member',
                               self._rebuild, manager)

        manager.app.reset_admin_password.assert_not_called()
        manager.app.stop_temporary_db.assert_called_once_with()
        manager.app.start_db.assert_not_called()
        self.assertTrue(manager.prepare_error)
        manager.status.end_install.assert_called_once_with(
            error_occurred=True)

    def test_failure(self):
        manager = self._manager()
        manager.app.reset_admin_password.side_effect = Exception('boom')

        self.assertRaisesRegex(Exception, 'boom', self._rebuild, manager)

        # The server without access control does not stay up.
        manager.app.stop_temporary_db.assert_called_once_with()
        manager.app.start_db.assert_not_called()
        self.assertTrue(manager.prepare_error)
        manager.status.begin_install.assert_called_once_with()
        manager.status.end_install.assert_called_once_with(
            error_occurred=True)

    def test_has_replica_set_config(self):
        app = test_mongodb._app()
        client = mock.MagicMock()
        client.__enter__.return_value = client
        with mock.patch.object(app.adm, '_client',
                               return_value=client) as get_client:
            client.local.system.replset.find_one.return_value = None
            self.assertFalse(app.adm.has_replica_set_config())
            client.local.system.replset.find_one.return_value = {
                '_id': 'rs1'}
            self.assertTrue(app.adm.has_replica_set_config())
        # Asked of the server without access control.
        get_client.assert_called_with(authenticate=False)

    def test_reset_admin_password_saves_the_new_password(self):
        app = test_mongodb._app()
        app.adm = mock.Mock()
        with mock.patch.object(mongodb_service.MongoDBApp,
                               'save_password') as save, \
                mock.patch.object(mongodb_service.utils,
                                  'generate_random_password',
                                  return_value='new-pw'):
            app.reset_admin_password()
        app.adm.set_admin_password.assert_called_once_with('new-pw')
        save.assert_called_once_with('os_admin', 'new-pw')
