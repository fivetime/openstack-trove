# Copyright 2021 Catalyst Cloud
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
import os
from unittest import mock

from trove.common import cfg
from trove.guestagent.common import cluster_probe
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.postgres import manager
from trove.guestagent.datastore.postgres import service
from trove.tests.unittests import trove_testtools


class TestPostgresManager(trove_testtools.TestCase):
    def setUp(self):
        super(TestPostgresManager, self).setUp()
        manager.PostgresManager._docker_client = mock.MagicMock()
        self.patch_datastore_manager('postgresql')
        # The manager starts the cluster probe; not its loop here.
        mock.patch.object(cluster_probe.ClusterProbe, 'start').start()
        self.addCleanup(mock.patch.stopall)
        self.pg_manager = manager.PostgresManager()
        m = mock.Mock()
        m.side_effect = lambda *a, **kw: m.call_count == 1
        self.first_call_true = m

    @mock.patch('trove.guestagent.common.operating_system.remove')
    @mock.patch('os.listdir')
    @mock.patch('trove.guestagent.common.operating_system.exists')
    def test_clean_wal_archives(self, mock_exists, mock_listdir, mock_remove):
        mock_exists.return_value = True
        mock_listdir.return_value = [
            '0000000100000002000000D4',
            '00000001000000000000008D',
            '0000000100000000000000A7.00000028.backup',
            '0000000100000000000000A7',
            '0000000100000002000000E7'
        ]

        # prevent infinite recursive call
        self.pg_manager._check_wal_archive_size = self.first_call_true
        self.pg_manager.app.get_config_param = mock.Mock()
        self.pg_manager.app.get_config_param.return_value = ''
        self.pg_manager.clean_wal_archives(mock.ANY)
        self.assertEqual(1, mock_remove.call_count)

        archive_path = service.WAL_ARCHIVE_DIR
        expected_calls = [
            mock.call(
                path=os.path.join(archive_path, '00000001000000000000008D'),
                force=True, recursive=False,
                as_root=True),
        ]
        self.assertEqual(expected_calls, mock_remove.call_args_list)

    @mock.patch('trove.guestagent.common.operating_system.remove')
    @mock.patch('os.listdir')
    @mock.patch('trove.guestagent.common.operating_system.exists')
    def test_clean_wal_archives_no_backups(self, mock_exists,
                                           mock_listdir,
                                           mock_remove):
        mock_exists.return_value = True
        mock_listdir.return_value = [
            '0000000100000002000000D4',
            '00000001000000000000008D',
            '0000000100000000000000A7',
            '0000000100000002000000E7'
        ]

        # prevent infinite recursive call
        self.pg_manager._check_wal_archive_size = self.first_call_true
        self.pg_manager.app.get_config_param = mock.Mock()
        self.pg_manager.app.get_config_param.return_value = ''
        self.pg_manager.clean_wal_archives(mock.ANY)

        self.assertEqual(3, mock_remove.call_count)

        archive_path = service.WAL_ARCHIVE_DIR
        expected_calls = [
            mock.call(
                path=os.path.join(archive_path, '0000000100000002000000D4'),
                force=True, recursive=False,
                as_root=True),
            mock.call(
                path=os.path.join(archive_path, '0000000100000000000000A7'),
                force=True, recursive=False,
                as_root=True),
            mock.call(
                path=os.path.join(archive_path, '00000001000000000000008D'),
                force=True, recursive=False,
                as_root=True),
        ]
        self.assertEqual(expected_calls, mock_remove.call_args_list)

    @mock.patch.object(operating_system, 'get_dir_size')
    @mock.patch.object(operating_system, 'get_filesystem_size')
    def test_check_wal_archive_size_units(
        self, mock_get_filesystem_size, mock_get_dir_size
    ):
        """Test _check_wal_archive_size with various max_wal_size formats."""
        cases = [
            ('200', True),    # interpreted as MB
            ('200MB', True),
            ('200M', True),
            ('1G', False),
            ('2GB', False),
            ('2G', False),
            ('10G', False),
            ('2T', False),
        ]

        # fake archive dir size
        mock_get_dir_size.return_value = 5 * 1024**3  # 5GB

        # fake data dir size
        mock_get_filesystem_size.return_value = 100 * 1024**3  # 100GB

        self.pg_manager.app.get_config_param = mock.Mock()

        for val, expected_result in cases:
            with self.subTest(value=val):
                # Set fake max_wal_size in config
                self.pg_manager.app.get_config_param.return_value = val

                result = self.pg_manager._check_wal_archive_size(
                    '/fake/archive', '/fake/data'
                )

                # since archive size is 5GB, only <1GB max_wal_size config
                # should trigger True
                self.assertEqual(
                    expected_result, result,
                    ('For %s should be %s', val, expected_result))

    @mock.patch.object(operating_system, 'get_dir_size')
    @mock.patch.object(operating_system, 'get_filesystem_size')
    def test_check_wal_archive_bigger_than_half(
        self, mock_get_filesystem_size, mock_get_dir_size
    ):
        # fake archive dir size
        mock_get_dir_size.return_value = 600 * 1024**2  # 600MB

        # fake data dir size
        mock_get_filesystem_size.return_value = 1024**3  # 1GB

        self.pg_manager.app.get_config_param = mock.Mock()
        # Set fake max_wal_size in config
        self.pg_manager.app.get_config_param.return_value = '1G'

        # archive size is 600MB and data volume is 1GB, check should pass
        self.assertTrue(self.pg_manager._check_wal_archive_size(
            '/fake/archive', '/fake/data'
        ))

    def test_skip_archive_cleanup_on_disable_mark_present(self):
        self.pg_manager.app.get_config_param = mock.Mock()
        self.pg_manager.app.get_config_param.return_value = (
            "echo DISABLE_TROVE_WAL_CLEANUP > /dev/null")

        with mock.patch.object(cfg, 'get_configuration_property') as m:
            self.pg_manager.clean_wal_archives(mock.ANY)
            m.assert_not_called()

    def test_get_datastore_log_defs_owner_fallback(self):
        """The log file owner should fall back to the DEFAULT group value
        when 'database_service_uid' is not set in the datastore group.
        """
        self.pg_manager.app.get_data_dir = mock.Mock(
            return_value='/var/lib/postgresql/data')
        self.pg_manager.build_log_file_name = mock.Mock(
            return_value='/var/lib/postgresql/data/postgres-general.log')

        log_defs = self.pg_manager.get_datastore_log_defs()

        expected_owner = cfg.CONF.database_service_uid
        self.assertIsNotNone(expected_owner)
        self.pg_manager.build_log_file_name.assert_called_once_with(
            self.pg_manager.GUEST_LOG_DEFS_GENERAL_LABEL, expected_owner,
            group=expected_owner, datastore_dir='/var/lib/postgresql/data')
        general_def = log_defs[self.pg_manager.GUEST_LOG_DEFS_GENERAL_LABEL]
        self.assertEqual(
            expected_owner,
            general_def[self.pg_manager.GUEST_LOG_USER_LABEL])

    def test_upgrade_updates_guest_info(self):
        upgrade_info = {'datastore_version': '8.4.4'}
        self.pg_manager.app = mock.Mock()
        self.pg_manager.override_guest_info = mock.Mock()

        self.pg_manager.upgrade(None, upgrade_info)

        self.pg_manager.app.upgrade.assert_called_once_with(upgrade_info)
        self.pg_manager.override_guest_info.assert_called_once_with(
            datastore_version='8.4.4')

    def test_upgrade_does_not_update_guest_info_on_failure(self):
        upgrade_info = {'datastore_version': '8.4.4'}
        self.pg_manager.app = mock.Mock()
        self.pg_manager.app.upgrade.side_effect = RuntimeError
        self.pg_manager.override_guest_info = mock.Mock()

        self.assertRaises(
            RuntimeError,
            self.pg_manager.upgrade,
            None,
            upgrade_info)

        self.pg_manager.override_guest_info.assert_not_called()

    def test_a_wal_location_as_a_number(self):
        self.assertEqual(0x16 << 32 | 0xB374D848,
                         service.lsn_number('16/B374D848'))
        # PostgreSQL prints no leading zeros: as text '0/A000000' would
        # come after '0/9FFFFFF0'.
        self.assertLess(service.lsn_number('0/A000000'),
                        service.lsn_number('0/9FFFFFF0'))

    def test_the_last_txn_of_a_standby(self):
        app = self.pg_manager.app
        with mock.patch.object(app, 'is_replica', return_value=True), \
                mock.patch.object(app, 'get_last_wal_replay_lsn',
                                  return_value='0/9FFFFFF0'), \
                mock.patch.object(app, 'get_primary_host',
                                  return_value='10.0.0.1'):
            self.assertEqual(['10.0.0.1', 0x9FFFFFF0],
                             self.pg_manager.get_last_txn(None))

    def test_a_standby_waits_for_the_location_as_a_number(self):
        app = self.pg_manager.app
        replayed = iter(['0/A000000', '0/9FFFFFF0'])

        def poll(caught_up, **kwargs):
            while not caught_up():
                pass
        with mock.patch.object(app, 'is_replica', return_value=True), \
                mock.patch.object(app, 'get_last_wal_replay_lsn',
                                  side_effect=lambda: next(replayed)), \
                mock.patch.object(manager.utils, 'poll_until',
                                  side_effect=poll):
            self.pg_manager.wait_for_txn(None, '0/9FFFFFF0')
        # Not caught up at 0/A000000, which is before 0/9FFFFFF0.
        self.assertRaises(StopIteration, next, replayed)
