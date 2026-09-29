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

from oslo_utils import importutils

from trove.common import cfg
from trove.common import configurations
from trove.common import constants
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.datastore.mysql import manager as mysql_manager
from trove.guestagent.datastore.mysql import service as mysql_service
from trove.guestagent.datastore.percona import service as percona_service
from trove.guestagent.strategies.replication import mysql_base
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


def _render(manager, version='8.4'):
    ds_version = mock.Mock()
    ds_version.datastore_name = manager
    ds_version.manager = manager
    ds_version.name = version
    ds_version.version = version
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': 2048, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b').render()


class TestPerconaDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['percona'])

        self.assertTrue(issubclass(manager_cls, mysql_manager.Manager))

    @mock.patch.object(mysql_manager.Manager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_manager_runs_the_percona_app(self, mock_docker_client):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['percona'])

        manager = manager_cls()

        self.assertIsInstance(manager.app, percona_service.PerconaApp)
        self.assertIs(manager.app, manager.adm.mysql_app)
        self.assertIs(manager.app.status, manager.status)

    def test_has_every_mysql_option(self):
        # The MySQL guest agent code reads the group named after the
        # datastore manager. An option missing from [percona] fails only
        # when the guest reaches the code path that reads it.
        for name in CONF.mysql:
            self.assertIn(name, CONF.percona,
                          '[percona] lacks the MySQL option %s' % name)

    def test_shares_mysql_defaults_except_images(self):
        for name in CONF.mysql:
            if name in ('docker_image', 'backup_docker_image'):
                continue
            self.assertEqual(CONF.mysql.get(name), CONF.percona.get(name),
                             name)
        self.assertEqual('percona/percona-server',
                         CONF.percona.docker_image)
        self.assertEqual('mysql', CONF.mysql.docker_image)

    def test_replication_strategy_resolves(self):
        strategy_cls = importutils.import_class('%s.%s' % (
            CONF.percona.replication_namespace,
            CONF.percona.replication_strategy))

        self.assertTrue(
            issubclass(strategy_cls, mysql_base.MysqlReplicationBase))

    def test_backup_strategy_is_the_mysql_one(self):
        self.assertEqual(mysql_service.MySqlApp.get_backup_strategy,
                         percona_service.PerconaApp.get_backup_strategy)

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('percona',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'percona',
            extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_configuration_parser(self):
        self.assertIs(configurations.MySQLConfParser,
                      template.SERVICE_PARSERS['percona'])


class TestPerconaConfigTemplate(trove_testtools.TestCase):

    def test_same_configuration_as_mysql(self):
        for version in ('8.0', '8.4'):
            self.assertEqual(_render('mysql', version),
                             _render('percona', version), version)

    def test_no_options_removed_from_the_server(self):
        rendered = _render('percona')

        # The query cache went away in 8.0; the server refuses to start
        # when it is configured ("unknown variable 'query_cache_type=1'").
        self.assertNotIn('query_cache', rendered)

    def test_does_not_switch_user(self):
        # The container is started as the database service user and cannot
        # change to another one.
        lines = [line.replace(' ', '') for line in
                 _render('percona').splitlines()]

        self.assertFalse([line for line in lines
                          if line.startswith('user=')])
