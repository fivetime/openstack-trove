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

import inspect
from unittest import mock

from oslo_utils import importutils

from trove.common import cfg
from trove.common import constants
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guest_api)
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_tasks)
from trove.common import template
from trove.common import utils
from trove.extensions.common import models as extension_models
from trove.guestagent import api as guest_api
from trove.guestagent.datastore.galera_common import manager as galera_manager
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mysql import manager as mysql_manager
from trove.guestagent.datastore.mysql import service as mysql_service
from trove.guestagent.datastore.mysql_common import service as common_service
from trove.guestagent.datastore.pxc import manager as pxc_manager
from trove.guestagent.datastore.pxc import service as pxc_service
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


def _section(rendered, name='mysqld'):
    return common_service.BaseMySqlApp.CFG_CODEC.deserialize(rendered)[name]


class TestPXCDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['pxc'])

        self.assertIs(pxc_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, mysql_manager.Manager))
        self.assertTrue(
            issubclass(manager_cls, galera_manager.GaleraManagerMixin))

    @mock.patch.object(mysql_manager.Manager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_manager_runs_the_pxc_app(self, mock_docker_client):
        manager = pxc_manager.Manager()

        self.assertIsInstance(manager.app, pxc_service.PXCApp)
        self.assertIs(manager.app, manager.adm.mysql_app)

    def test_cluster_calls_run_before_the_mysql_ones(self):
        # The mixins only work when they come first.
        for cls, mixin, base in (
                (pxc_manager.Manager, galera_manager.GaleraManagerMixin,
                 mysql_manager.Manager),
                (pxc_service.PXCApp, galera_service.GaleraAppMixin,
                 mysql_service.MySqlApp)):
            mro = cls.__mro__
            self.assertLess(mro.index(mixin), mro.index(base), cls)

    def test_manager_has_every_call_the_task_manager_makes(self):
        generic = {name for name, _ in inspect.getmembers(
            guest_api.API, inspect.isfunction)}
        cluster_calls = {
            name for name, _ in inspect.getmembers(
                galera_guest_api.GaleraCommonGuestAgentAPI,
                inspect.isfunction)
            if name not in generic and not name.startswith('_')}

        self.assertTrue(cluster_calls)
        for name in cluster_calls:
            self.assertTrue(
                callable(getattr(pxc_manager.Manager, name, None)),
                'the PXC manager does not implement %s' % name)

    def test_server_reads_only_the_configuration_trove_wrote(self):
        manager = pxc_manager.Manager.__new__(pxc_manager.Manager)

        command = manager.get_start_db_params('/var/lib/mysql/data')

        # --defaults-file is only honoured as the first option.
        self.assertEqual(
            ['--defaults-file=/etc/mysql/my.cnf',
             '--datadir=/var/lib/mysql/data'],
            command.split())

    def test_has_every_mysql_option(self):
        for name in CONF.mysql:
            self.assertIn(name, CONF.pxc,
                          '[pxc] lacks the MySQL option %s' % name)

    def test_cluster_options(self):
        self.assertTrue(CONF.pxc.cluster_support)
        self.assertEqual(3, CONF.pxc.min_cluster_member_count)
        for option in ('api_strategy', 'taskmanager_strategy',
                       'guestagent_strategy'):
            self.assertIsNotNone(
                importutils.import_class(CONF.pxc.get(option)))

    def test_galera_ports_are_opened(self):
        ports = [port for ports in CONF.pxc.tcp_ports for port in ports]

        for port in (3306, 4444, 4567, 4568):
            self.assertIn(port, ports)

    def test_internal_accounts_are_hidden_from_users(self):
        hidden = CONF.pxc.ignore_users

        self.assertIn(
            galera_tasks.GaleraCommonClusterTasks.CLUSTER_REPLICATION_USER,
            hidden)
        # Created by the image.
        for name in ('clustercheck', 'monitor', 'xtrabackup'):
            self.assertIn(name, hidden)
        for name in CONF.mysql.ignore_users:
            self.assertIn(name, hidden)

    def test_images(self):
        self.assertEqual('percona/percona-xtradb-cluster',
                         CONF.pxc.docker_image)

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('pxc',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'pxc', extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)


class TestPXCConfigTemplate(trove_testtools.TestCase):

    def test_every_instance_is_a_galera_node(self):
        options = _section(_render('pxc'))

        self.assertEqual('/usr/lib64/galera4/libgalera_smm.so',
                         options['wsrep_provider'])
        # Forms a cluster of its own.
        self.assertEqual('gcomm://', options['wsrep_cluster_address'])
        self.assertEqual('xtrabackup-v2', options['wsrep_sst_method'])
        self.assertEqual('ROW', options['binlog_format'])

    def test_trove_can_parse_it(self):
        # The server tolerates an option given twice. The guest agent's
        # parser does not, and the instance fails to prepare.
        for version in ('8.0', '8.4'):
            self.assertIn('wsrep_provider',
                          _section(_render('pxc', version)))

    def test_is_the_mysql_configuration_otherwise(self):
        mysql = _section(_render('mysql'))
        pxc = _section(_render('pxc'))

        for name, value in mysql.items():
            self.assertEqual(value, pxc.get(name), name)

    def test_options_the_server_no_longer_has(self):
        for rendered in (_render('pxc'), self._render_cluster()):
            # Either makes Percona XtraDB Cluster 8 refuse to start.
            self.assertNotIn('wsrep_sst_auth', rendered)
            self.assertNotIn('query_cache', rendered)
            self.assertNotIn('wsrep_slave_threads', rendered)

    def _render_cluster(self):
        return utils.ENV.get_template(
            'pxc/cluster.config.template').render(
                flavor={'ram': 2048, 'vcpus': 2},
                replication_user_pass='clusterrepuser:rep-pw',
                cluster_ips='10.0.0.1,10.0.0.2,10.0.0.3',
                cluster_name='c1', instance_ip='10.0.0.1',
                instance_name='n1')

    def test_cluster_template(self):
        codec = common_service.BaseMySqlApp.CFG_CODEC
        options = codec.deserialize(codec.serialize(
            codec.deserialize(self._render_cluster())))['mysqld']

        self.assertEqual('"gcomm://10.0.0.1,10.0.0.2,10.0.0.3"',
                         options['wsrep_cluster_address'])
        self.assertEqual('c1', options['wsrep_cluster_name'])
        self.assertEqual('10.0.0.1', options['wsrep_node_address'])
        self.assertEqual('"gcache.size=512M; gcache.page_size=1G"',
                         options['wsrep_provider_options'])

    def test_cluster_template_names_nothing_the_base_does_not_load(self):
        # The provider comes from the base configuration.
        self.assertNotIn('wsrep_provider=', self._render_cluster())
        self.assertNotIn('wsrep_provider =', self._render_cluster())


class TestPXCApp(trove_testtools.TestCase):

    def _app(self):
        return pxc_service.PXCApp(mock.Mock(), mock.Mock())

    def test_image_accounts_get_random_passwords(self):
        app = self._app()

        first = app._extra_envs
        second = app._extra_envs

        self.assertEqual(
            {'XTRABACKUP_PASSWORD', 'MONITOR_PASSWORD',
             'CLUSTERCHECK_PASSWORD'}, set(first))
        self.assertEqual(3, len(set(first.values())))
        self.assertNotIn('xtrabackup', first.values())
        self.assertNotEqual(first, second)

    def test_cluster_healthcheck(self):
        single = dict(mysql_service.MySqlApp.HEALTHCHECK)

        with mock.patch.object(
                pxc_service.PXCApp, 'cluster_healthcheck_file',
                new_callable=mock.PropertyMock,
                return_value='/var/lib/mysql/conf.d/x.cnf'):
            healthcheck = self._app().cluster_healthcheck

        self.assertEqual('CMD-SHELL', healthcheck['test'][0])
        command = healthcheck['test'][1]
        # --defaults-file is only honoured as the first option.
        self.assertTrue(command.startswith(
            'mysql --defaults-file=/var/lib/mysql/conf.d/x.cnf '))
        self.assertIn("wsrep_local_state'", command)
        self.assertTrue(command.endswith('| grep -qw 4'))
        # The socket answers while the image initializes the data
        # directory; the file makes the client use TCP.
        self.assertNotIn('sock', command)
        self.assertEqual(single, mysql_service.MySqlApp.HEALTHCHECK)

    @mock.patch.object(mysql_service.MySqlApp, 'start_db')
    @mock.patch.object(pxc_service, 'operating_system')
    def test_node_config_is_there_before_the_container_starts(
            self, mock_os, mock_start_db):
        app = self._app()
        configuration_manager = mock.Mock()
        configuration_manager.has_system_override.return_value = False
        order = mock.Mock()
        order.attach_mock(mock_os.write_file, 'write_file')
        order.attach_mock(mock_start_db, 'start_db')

        with mock.patch.object(
                pxc_service.PXCApp, 'configuration_manager',
                new_callable=mock.PropertyMock,
                return_value=configuration_manager), \
            mock.patch.object(
                pxc_service.PXCApp, 'database_service_uid',
                new_callable=mock.PropertyMock, return_value='1001'), \
            mock.patch.object(
                pxc_service.PXCApp, 'database_service_gid',
                new_callable=mock.PropertyMock, return_value='1001'):
            app.start_db(command='--datadir=/var/lib/mysql/data')

        self.assertEqual(['write_file', 'start_db'],
                         [call[0] for call in order.mock_calls])
        path, content = mock_os.write_file.call_args[0][:2]
        self.assertEqual('/etc/mysql/node.cnf', path)
        # The entrypoint prints the file with grep -v and stops when that
        # prints nothing.
        self.assertTrue([line for line in content.splitlines()
                         if line and 'wsrep_sst_auth' not in line])
        mock_os.chown.assert_called_once_with(
            '/etc/mysql/node.cnf', '1001', '1001', as_root=True)

    @mock.patch.object(pxc_service, 'operating_system')
    def test_cluster_context_comes_from_the_healthcheck_file(self, mock_os):
        app = self._app()
        configuration_manager = mock.Mock()
        configuration_manager.get_value.return_value = {
            'wsrep_cluster_name': 'c1'}
        mock_os.read_file.return_value = {
            'client': {'user': 'clusterrepuser', 'password': 'a:b',
                       'host': '127.0.0.1', 'protocol': 'tcp'}}

        with mock.patch.object(
                pxc_service.PXCApp, 'configuration_manager',
                new_callable=mock.PropertyMock,
                return_value=configuration_manager), \
            mock.patch.object(
                pxc_service.PXCApp, 'cluster_healthcheck_file',
                new_callable=mock.PropertyMock,
                return_value='/var/lib/mysql/conf.d/x.cnf'), \
            mock.patch.object(pxc_service.PXCApp, 'get_auth_password',
                              return_value='admin-pw'):
            context = app.get_cluster_context()

        self.assertEqual(
            {'replication_user': {'name': 'clusterrepuser',
                                  'password': 'a:b'},
             'cluster_name': 'c1',
             'admin_password': 'admin-pw'},
            context)
