# Copyright 2026 OpenStack Foundation
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
from unittest import mock

from trove.common import cfg
from trove.guestagent.common import cluster_probe
from trove.guestagent.datastore.mysql import manager
from trove.tests.unittests import trove_testtools


class TestMySqlManager(trove_testtools.TestCase):
    def setUp(self):
        super(TestMySqlManager, self).setUp()
        manager.Manager._docker_client = mock.MagicMock()
        self.patch_datastore_manager('mysql')
        # The cluster probe would loop in the test process.
        mock.patch.object(cluster_probe.ClusterProbe, 'start').start()
        self.addCleanup(mock.patch.stopall)
        self.mysql_manager = manager.Manager()

    def test_get_datastore_log_defs_owner_fallback(self):
        """The log file owner should fall back to the DEFAULT group value
        when 'database_service_uid' is not set in the datastore group.
        """
        self.mysql_manager.app.get_data_dir = mock.Mock(
            return_value='/var/lib/mysql/data')
        self.mysql_manager.build_log_file_name = mock.Mock(
            side_effect=lambda log_name, owner, **kwargs:
                '/var/lib/mysql/data/mysql-%s.log' % log_name)
        self.mysql_manager.validate_log_file = mock.Mock(
            return_value='/var/lib/mysql/mysqld.log')

        log_defs = self.mysql_manager.get_datastore_log_defs()

        expected_owner = cfg.CONF.database_service_uid
        self.assertIsNotNone(expected_owner)
        self.mysql_manager.build_log_file_name.assert_any_call(
            self.mysql_manager.GUEST_LOG_DEFS_GENERAL_LABEL, expected_owner,
            group=expected_owner, datastore_dir='/var/lib/mysql/data')
        self.mysql_manager.validate_log_file.assert_called_once_with(
            '/var/lib/mysql/mysqld.log', expected_owner, group=expected_owner)
        for log_def in log_defs.values():
            self.assertEqual(
                expected_owner,
                log_def[self.mysql_manager.GUEST_LOG_USER_LABEL])

    def test_get_datastore_log_defs_separate_group(self):
        """A datastore-specific gid different from the uid should be passed
        through as the group when creating log files.
        """
        cfg.CONF.set_override('database_service_uid', '1100', 'mysql')
        self.addCleanup(
            cfg.CONF.clear_override, 'database_service_uid', 'mysql')
        cfg.CONF.set_override('database_service_gid', '1101', 'mysql')
        self.addCleanup(
            cfg.CONF.clear_override, 'database_service_gid', 'mysql')
        self.mysql_manager.app.get_data_dir = mock.Mock(
            return_value='/var/lib/mysql/data')
        self.mysql_manager.build_log_file_name = mock.Mock(
            side_effect=lambda log_name, owner, **kwargs:
                '/var/lib/mysql/data/mysql-%s.log' % log_name)
        self.mysql_manager.validate_log_file = mock.Mock(
            return_value='/var/lib/mysql/mysqld.log')

        self.mysql_manager.get_datastore_log_defs()

        self.mysql_manager.build_log_file_name.assert_any_call(
            self.mysql_manager.GUEST_LOG_DEFS_GENERAL_LABEL, '1100',
            group='1101', datastore_dir='/var/lib/mysql/data')
        self.mysql_manager.validate_log_file.assert_called_once_with(
            '/var/lib/mysql/mysqld.log', '1100', group='1101')

    def _enable_overrides(self, mode):
        self.mysql_manager._get_ssl_files = mock.Mock(return_value={
            'certificate': '/c', 'private_key': '/k', 'ca': '/a'})
        self.mysql_manager._get_default_tls_versions = mock.Mock(
            return_value='TLSv1.2,TLSv1.3')
        return self.mysql_manager._get_enable_ssl_overrides(mode)

    def test_basic_ssl_leaves_plain_connections_allowed(self):
        # Basic offers TLS without requiring it.
        overrides = self._enable_overrides('basic')
        self.assertEqual('OFF', overrides['require_secure_transport'])
        self.assertEqual('/c', overrides['ssl_cert'])

    def test_enforced_and_mtls_ssl_require_secure_transport(self):
        for mode in ('enforced', 'mtls'):
            self.assertEqual(
                'ON', self._enable_overrides(mode)['require_secure_transport'],
                mode)

    def test_enabling_ssl_writes_the_overrides_of_its_mode(self):
        self.patch_conf_property('datastore_version', '8.4')
        self.mysql_manager._get_ssl_files = mock.Mock(return_value={
            'certificate': '/c', 'private_key': '/k', 'ca': '/a'})
        self.mysql_manager._get_default_tls_versions = mock.Mock(
            return_value='TLSv1.2,TLSv1.3')
        self.mysql_manager.app.update_overrides = mock.Mock()
        self.mysql_manager.app.update_client_overrides = mock.Mock()
        self.mysql_manager.adm = mock.Mock()
        self.mysql_manager.status = mock.Mock()
        self.assertTrue(self.mysql_manager._enable_ssl_certificate_impl(
            'basic'))
        written = self.mysql_manager.app.update_overrides.call_args[0][0]
        self.assertEqual('OFF', written['require_secure_transport'])
        self.mysql_manager.adm.set_users_access_mode.assert_called_once_with(
            'basic')
