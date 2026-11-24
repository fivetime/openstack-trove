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
from trove.guestagent.datastore.db2 import manager as db2_manager
from trove.guestagent.datastore import manager as base_manager
from trove.tests.unittests.guestagent.datastore.db2 import test_db2

CONFIG = 'LOGFILSIZ=10240\n'
OVERRIDES = {'MAXAPPLS': 200}


class TestDB2Rebuild(test_db2.DB2GuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = db2_manager.Manager()
        manager.app = mock.Mock()
        manager.app.mount_point = '/var/lib/db2'
        # The password of the instance owner was on the old root disk.
        manager.app.has_admin.return_value = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _rebuild(self, manager, **kwargs):
        manager.rebuild(mock.Mock(), '12.1.5.0', config_contents=CONFIG,
                        config_overrides=OVERRIDES, **kwargs)

    def test_rebuild(self):
        manager = self._manager()
        app = manager.app
        order = mock.Mock()
        order.attach_mock(manager.status.begin_install, 'begin')
        order.attach_mock(app.configuration_manager.reset_configuration,
                          'reset_configuration')
        order.attach_mock(app.update_overrides, 'update_overrides')
        order.attach_mock(app.secure, 'secure')
        order.attach_mock(app.start_db, 'start_db')
        order.attach_mock(app.apply_settings, 'apply_settings')
        order.attach_mock(manager.status.end_install, 'end')

        self._rebuild(manager, properties={'unused': True})

        self.assertEqual(['begin', 'reset_configuration', 'update_overrides',
                          'secure', 'start_db', 'apply_settings', 'end'],
                         [call[0] for call in order.mock_calls])
        app.configuration_manager.reset_configuration.assert_called_once_with(
            CONFIG)
        app.update_overrides.assert_called_once_with(OVERRIDES)
        app.secure.assert_called_once_with()
        app.start_db.assert_called_once_with(ds_version='12.1.5.0')
        # The databases and the users are on the volume: nothing restored
        # or created.
        app.restore_databases.assert_not_called()
        manager.adm.create_user.assert_not_called()
        manager.adm.create_database.assert_not_called()
        manager.status.end_install.assert_called_once_with(
            error_occurred=False)
        self.assertFalse(manager.prepare_error)

    def test_failure(self):
        manager = self._manager()
        manager.app.start_db.side_effect = exception.TroveError('boom')

        self.assertRaisesRegex(exception.TroveError, 'boom', self._rebuild,
                               manager)

        manager.app.apply_settings.assert_not_called()
        self.assertTrue(manager.prepare_error)
        manager.status.begin_install.assert_called_once_with()
        manager.status.end_install.assert_called_once_with(
            error_occurred=True)
