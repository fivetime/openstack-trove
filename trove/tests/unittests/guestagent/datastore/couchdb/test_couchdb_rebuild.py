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
from trove.guestagent.datastore.couchdb import manager as couchdb_manager
from trove.guestagent.datastore import manager as base_manager
from trove.tests.unittests.guestagent.datastore.couchdb import test_couchdb

CONFIG = '[couchdb]\n'
OVERRIDES = {'chttpd': {'max_http_request_size': 4294967296}}


class TestCouchDBRebuild(test_couchdb.CouchDBGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = couchdb_manager.Manager()
        manager.app = mock.Mock()
        manager.app.datadir = '/var/lib/couchdb/data'
        # The settings with the admin were on the old root disk.
        manager.app.has_admin.return_value = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _rebuild(self, manager, **kwargs):
        manager.rebuild(mock.Mock(), '3.5', config_contents=CONFIG,
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
        order.attach_mock(app.secure, 'secure')
        order.attach_mock(app.start_db, 'start_db')
        order.attach_mock(manager.status.end_install, 'end')

        self._rebuild(manager, properties={'unused': True})

        # The admin is written before the server starts: it does not
        # start without one.
        self.assertEqual(['begin', 'reset_configuration',
                          'initial_configuration', 'update_overrides',
                          'secure', 'start_db', 'end'],
                         [call[0] for call in order.mock_calls])
        app.configuration_manager.reset_configuration.assert_called_once_with(
            CONFIG)
        app.update_overrides.assert_called_once_with(OVERRIDES)
        app.secure.assert_called_once_with()
        app.start_db.assert_called_once_with(ds_version='3.5')
        # The users and the databases are in the data.
        manager.adm.create_user.assert_not_called()
        manager.adm.create_database.assert_not_called()
        manager.status.end_install.assert_called_once_with(
            error_occurred=False)
        self.assertFalse(manager.prepare_error)

    def test_an_admin_already_written_is_kept(self):
        manager = self._manager()
        manager.app.has_admin.return_value = True
        self._rebuild(manager)
        manager.app.secure.assert_not_called()
        manager.app.start_db.assert_called_once_with(ds_version='3.5')

    def test_failure(self):
        manager = self._manager()
        manager.app.start_db.side_effect = exception.TroveError('boom')

        self.assertRaisesRegex(exception.TroveError, 'boom', self._rebuild,
                               manager)

        self.assertTrue(manager.prepare_error)
        manager.status.begin_install.assert_called_once_with()
        manager.status.end_install.assert_called_once_with(
            error_occurred=True)
