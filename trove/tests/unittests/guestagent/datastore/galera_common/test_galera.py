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

import docker

from trove.common import cfg
from trove.common import exception
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guest_api)
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent import api as guest_api
from trove.guestagent.datastore.galera_common import manager as galera_manager
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mariadb import manager as mariadb_manager
from trove.guestagent.datastore.mariadb import service as mariadb_service
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.instance import service_status
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF

REPLICATION_USER = {'name': 'clusterrepuser', 'password': 'rep-pw'}
CLUSTER_CONFIGURATION = '[galera]\nwsrep_on = ON\n'
COMMAND = '--datadir=/var/lib/mysql/data'


class FakeBaseApp(object):
    """Stands in for BaseMySqlApp and records what is done, in order."""

    HEALTHCHECK = {'test': ['single']}
    CFG_CODEC = stream_codecs.IniCodec()

    def __init__(self):
        self.calls = []
        self.status = mock.Mock()
        self.status.wait_for_status.return_value = True
        self.docker_client = mock.Mock()
        self.configuration_manager = mock.Mock()
        self.configuration_manager.has_system_override.return_value = False
        self.start_db_error = None

    def start_db(self, *args, **kwargs):
        self.calls.append(
            ('start_db', kwargs.get('command'), self.HEALTHCHECK))
        if self.start_db_error:
            raise self.start_db_error

    def stop_db(self, *args, **kwargs):
        self.calls.append(('stop_db',))

    def wipe_ib_logfiles(self):
        self.calls.append(('wipe_ib_logfiles',))


class FakeApp(galera_service.GaleraAppMixin, FakeBaseApp):
    cluster_healthcheck = {'test': ['cluster']}


def _container(command):
    container = mock.Mock()
    container.attrs = {'Config': {'Cmd': command}}
    return container


class TestStartDb(trove_testtools.TestCase):

    def test_single_instance_keeps_its_healthcheck(self):
        app = FakeApp()

        app.start_db(command=COMMAND)

        self.assertEqual([('start_db', COMMAND, {'test': ['single']})],
                         app.calls)

    def test_cluster_member_gets_the_cluster_healthcheck(self):
        app = FakeApp()
        app.configuration_manager.has_system_override.return_value = True

        app.start_db(command=COMMAND)

        self.assertEqual([('start_db', COMMAND, {'test': ['cluster']})],
                         app.calls)
        app.configuration_manager.has_system_override.assert_called_with(
            galera_service.CNF_CLUSTER)
        # Other instances and the class are not affected.
        self.assertEqual({'test': ['single']}, FakeApp.HEALTHCHECK)
        self.assertEqual({'test': ['single']}, FakeApp().HEALTHCHECK)


@mock.patch.object(galera_service.docker_util, 'remove_container')
class TestStartClusterNode(trove_testtools.TestCase):

    def test_bootstrap_forms_a_new_cluster(self, mock_remove):
        app = FakeApp()

        app.start_cluster_node(COMMAND, bootstrap=True)

        self.assertEqual(
            COMMAND + ' --wsrep-new-cluster', app.calls[0][1])

    def test_member_joins(self, mock_remove):
        app = FakeApp()

        app.start_cluster_node(COMMAND)

        self.assertEqual(COMMAND, app.calls[0][1])

    def test_bootstrap_without_other_options(self, mock_remove):
        app = FakeApp()

        app.start_cluster_node('', bootstrap=True)

        self.assertEqual('--wsrep-new-cluster', app.calls[0][1])

    def test_the_old_container_is_removed_first(self, mock_remove):
        app = FakeApp()
        mock_remove.side_effect = lambda client: app.calls.append(
            ('remove_container',))

        app.start_cluster_node(COMMAND)

        self.assertEqual(['remove_container', 'start_db'],
                         [call[0] for call in app.calls])
        mock_remove.assert_called_once_with(app.docker_client)

    @mock.patch.object(galera_service.docker_util, 'get_container_status',
                       return_value='running')
    def test_waits_for_a_member_that_receives_state(self, mock_status,
                                                    mock_remove):
        app = FakeApp()
        app.start_db_error = exception.TroveError('Failed to start')

        app.start_cluster_node(COMMAND)

        app.status.wait_for_status.assert_called_once_with(
            service_status.ServiceStatuses.HEALTHY,
            CONF.restore_usage_timeout)

    @mock.patch.object(galera_service.docker_util, 'get_container_status',
                       return_value='running')
    def test_gives_up_when_the_state_never_arrives(self, mock_status,
                                                   mock_remove):
        app = FakeApp()
        app.start_db_error = exception.TroveError('Failed to start')
        app.status.wait_for_status.return_value = False

        self.assertRaises(exception.TroveError,
                          app.start_cluster_node, COMMAND)

    @mock.patch.object(galera_service.docker_util, 'get_container_status',
                       return_value='exited')
    def test_does_not_wait_for_a_stopped_container(self, mock_status,
                                                   mock_remove):
        app = FakeApp()
        app.start_db_error = exception.TroveError('Failed to start')

        self.assertRaises(exception.TroveError,
                          app.start_cluster_node, COMMAND)
        app.status.wait_for_status.assert_not_called()

    @mock.patch.object(galera_service.docker_util, 'get_container_status',
                       return_value='running')
    def test_does_not_wait_for_the_bootstrap(self, mock_status, mock_remove):
        # The member that forms the cluster has nothing to receive.
        app = FakeApp()
        app.start_db_error = exception.TroveError('Failed to start')

        self.assertRaises(exception.TroveError,
                          app.start_cluster_node, COMMAND, bootstrap=True)
        app.status.wait_for_status.assert_not_called()


class TestInstallCluster(trove_testtools.TestCase):

    def _install(self, bootstrap):
        app = FakeApp()
        record = app.calls.append
        patches = [
            mock.patch.object(
                app, '_create_cluster_replication_user',
                side_effect=lambda user: record(('create_user', user))),
            mock.patch.object(
                app, '_write_cluster_healthcheck_file',
                side_effect=lambda user: record(('healthcheck', user))),
            mock.patch.object(
                app, 'write_cluster_configuration_overrides',
                side_effect=lambda conf: record(('configuration', conf))),
            mock.patch.object(
                app, 'start_cluster_node',
                side_effect=lambda command, bootstrap=False: record(
                    ('start_cluster_node', command, bootstrap))),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

        app.install_cluster(REPLICATION_USER, CLUSTER_CONFIGURATION,
                            COMMAND, bootstrap=bootstrap)
        return app.calls

    def test_order(self):
        calls = self._install(bootstrap=False)

        # The user is created while the database still runs, and the
        # database starts only after everything it reads is in place.
        self.assertEqual(
            [('create_user', REPLICATION_USER),
             ('stop_db',),
             ('configuration', CLUSTER_CONFIGURATION),
             ('healthcheck', REPLICATION_USER),
             ('wipe_ib_logfiles',),
             ('start_cluster_node', COMMAND, False)],
            calls)

    def test_bootstrap(self):
        calls = self._install(bootstrap=True)

        self.assertEqual(('start_cluster_node', COMMAND, True), calls[-1])


class TestLeaveBootstrap(trove_testtools.TestCase):

    def test_started_with_bootstrap(self):
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data', '--wsrep-new-cluster'])

        self.assertTrue(app.started_with_bootstrap())
        app.docker_client.containers.get.assert_called_once_with('database')

    def test_started_as_a_member(self):
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data'])

        self.assertFalse(app.started_with_bootstrap())

    def test_container_without_a_command(self):
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(None)

        self.assertFalse(app.started_with_bootstrap())

    def test_no_container(self):
        app = FakeApp()
        app.docker_client.containers.get.side_effect = (
            docker.errors.NotFound('no such container'))

        self.assertFalse(app.started_with_bootstrap())

    @mock.patch.object(galera_service.docker_util, 'remove_container')
    def test_the_bootstrap_member_is_started_again_as_a_member(
            self, mock_remove):
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data', '--wsrep-new-cluster'])

        app.leave_bootstrap(COMMAND)

        self.assertEqual([('stop_db',),
                          ('start_db', COMMAND, {'test': ['single']})],
                         app.calls)
        mock_remove.assert_called_once_with(app.docker_client)

    @mock.patch.object(galera_service.docker_util, 'remove_container')
    def test_other_members_are_left_alone(self, mock_remove):
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data'])

        app.leave_bootstrap(COMMAND)

        self.assertEqual([], app.calls)
        mock_remove.assert_not_called()


class TestClusterContext(trove_testtools.TestCase):

    def _app(self, section):
        app = FakeApp()
        app.configuration_manager.get_value.return_value = section
        app.get_auth_password = mock.Mock(return_value='admin-pw')
        return app

    def test_context(self):
        app = self._app({'wsrep_sst_auth': '"clusterrepuser:rep-pw"',
                         'wsrep_cluster_name': 'c1'})

        self.assertEqual(
            {'replication_user': REPLICATION_USER,
             'cluster_name': 'c1',
             'admin_password': 'admin-pw'},
            app.get_cluster_context())
        app.configuration_manager.get_value.assert_called_once_with(
            'mysqld')

    def test_password_with_a_colon(self):
        app = self._app({'wsrep_sst_auth': '"clusterrepuser:a:b:c"',
                         'wsrep_cluster_name': 'c1'})

        self.assertEqual(
            {'name': 'clusterrepuser', 'password': 'a:b:c'},
            app.get_cluster_context()['replication_user'])

    def test_section_is_the_apps(self):
        class OwnSection(FakeApp):
            CLUSTER_CONF_SECTION = 'galera'

        app = OwnSection()
        app.configuration_manager.get_value.return_value = {}
        app.get_auth_password = mock.Mock(return_value='admin-pw')

        app.get_cluster_context()

        app.configuration_manager.get_value.assert_called_once_with(
            'galera')


class TestResetAdminPassword(trove_testtools.TestCase):

    @mock.patch.object(galera_service.mysql_util, 'SqlClient')
    def test_password_is_set_stored_and_the_engine_dropped(self,
                                                           mock_client):
        app = FakeApp()
        app.get_engine = mock.Mock()
        app._create_admin_user = mock.Mock()
        app.save_password = mock.Mock()
        self.addCleanup(setattr, mysql_service, 'ENGINE',
                        mysql_service.ENGINE)
        mysql_service.ENGINE = mock.sentinel.engine

        app.reset_admin_password('cluster-admin-pw')

        app._create_admin_user.assert_called_once_with(
            mock_client.return_value.__enter__.return_value,
            'cluster-admin-pw')
        app.save_password.assert_called_once_with(
            'os_admin', 'cluster-admin-pw')
        self.assertIsNone(mysql_service.ENGINE)


class FakeBaseManager(object):
    def __init__(self):
        self.app = mock.Mock()
        self.app.get_data_dir.return_value = '/var/lib/mysql/data'
        self.status = mock.Mock()
        self.completed = []

    def get_start_db_params(self, data_dir):
        return '--datadir=%s' % data_dir

    def cluster_complete(self, context):
        self.completed.append(list(self.app.method_calls))


class FakeManager(galera_manager.GaleraManagerMixin, FakeBaseManager):
    pass


class TestManager(trove_testtools.TestCase):

    def test_install_cluster(self):
        manager = FakeManager()

        manager.install_cluster(mock.sentinel.context, REPLICATION_USER,
                                CLUSTER_CONFIGURATION, True)

        manager.app.install_cluster.assert_called_once_with(
            REPLICATION_USER, CLUSTER_CONFIGURATION, COMMAND,
            bootstrap=True)
        manager.status.set_status.assert_not_called()

    def test_install_cluster_failure_is_reported(self):
        manager = FakeManager()
        manager.app.install_cluster.side_effect = RuntimeError('no')

        self.assertRaises(
            RuntimeError, manager.install_cluster, mock.sentinel.context,
            REPLICATION_USER, CLUSTER_CONFIGURATION, False)
        manager.status.set_status.assert_called_once_with(
            service_status.ServiceStatuses.FAILED)

    def test_cluster_complete_leaves_bootstrap_first(self):
        manager = FakeManager()

        manager.cluster_complete(mock.sentinel.context)

        manager.app.leave_bootstrap.assert_called_once_with(COMMAND)
        # The base class reports the instance as installed; by then the
        # member has to be running the way it will from now on.
        self.assertEqual(1, len(manager.completed))
        self.assertIn(mock.call.leave_bootstrap(COMMAND),
                      manager.completed[0])


class TestMariaDB(trove_testtools.TestCase):

    def test_manager_has_every_call_the_task_manager_makes(self):
        # The task manager strategy and the guest agent are two halves.
        # The guest agent half was once removed while the other stayed,
        # and creating a cluster failed at the first of these calls.
        generic = {name for name, _ in inspect.getmembers(
            guest_api.API, inspect.isfunction)}
        cluster_calls = {
            name for name, _ in inspect.getmembers(
                galera_guest_api.GaleraCommonGuestAgentAPI,
                inspect.isfunction)
            if name not in generic and not name.startswith('_')}

        self.assertEqual(
            {'install_cluster', 'reset_admin_password', 'cluster_complete',
             'get_cluster_context',
             'write_cluster_configuration_overrides'},
            cluster_calls)
        for name in cluster_calls:
            self.assertTrue(
                callable(getattr(mariadb_manager.Manager, name, None)),
                'the MariaDB manager does not implement %s' % name)

    def test_arguments_match_the_task_manager(self):
        # The arguments travel by name.
        for name, sent in (
                ('install_cluster',
                 {'replication_user', 'cluster_configuration',
                  'bootstrap'}),
                ('reset_admin_password', {'admin_password'}),
                ('cluster_complete', set()),
                ('get_cluster_context', set()),
                ('write_cluster_configuration_overrides',
                 {'cluster_configuration'})):
            parameters = set(inspect.signature(
                getattr(mariadb_manager.Manager, name)).parameters)
            self.assertEqual(sent | {'self', 'context'}, parameters, name)

    def test_cluster_healthcheck(self):
        app = mariadb_service.MariaDBApp(mock.Mock(), mock.Mock())
        single = dict(mariadb_service.MariaDBApp.HEALTHCHECK)

        with mock.patch.object(
                mariadb_service.MariaDBApp, 'cluster_healthcheck_file',
                new_callable=mock.PropertyMock,
                return_value='/var/lib/mysql/conf.d/x.cnf'):
            healthcheck = app.cluster_healthcheck

        self.assertEqual(
            ['CMD', 'healthcheck.sh', '--defaults-file',
             '/var/lib/mysql/conf.d/x.cnf', '--connect',
             '--innodb_initialized', '--galera_online'],
            healthcheck['test'])
        self.assertEqual(single['interval'], healthcheck['interval'])
        self.assertEqual(single, mariadb_service.MariaDBApp.HEALTHCHECK)

    def test_wsrep_options_are_read_from_their_section(self):
        self.assertEqual('galera',
                         mariadb_service.MariaDBApp.CLUSTER_CONF_SECTION)

    def test_cluster_template_survives_the_configuration_codec(self):
        # The configuration is parsed and written again before the server
        # reads it, and ";" starts a comment in this format.
        rendered = utils.ENV.get_template(
            'mariadb/cluster.config.template').render(
                flavor={'ram': 2048, 'vcpus': 2},
                replication_user_pass='clusterrepuser:a;b#c:d',
                cluster_ips='10.0.0.1,10.0.0.2,10.0.0.3',
                cluster_name='c1', instance_ip='10.0.0.1',
                instance_name='n1')

        codec = mysql_service.BaseMySqlApp.CFG_CODEC
        section = codec.deserialize(
            codec.serialize(codec.deserialize(rendered)))['galera']

        self.assertEqual('"gcache.size=512M; gcache.page_size=1G"',
                         section['wsrep_provider_options'])
        self.assertEqual('"clusterrepuser:a;b#c:d"',
                         section['wsrep_sst_auth'])
        self.assertEqual('"gcomm://10.0.0.1,10.0.0.2,10.0.0.3"',
                         section['wsrep_cluster_address'])
        self.assertEqual('ON', section['wsrep_on'])
