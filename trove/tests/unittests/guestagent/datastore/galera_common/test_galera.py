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
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_tasks)
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent import api as guest_api
from trove.guestagent.datastore.galera_common import manager as galera_manager
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mariadb import manager as mariadb_manager
from trove.guestagent.datastore.mariadb import service as mariadb_service
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.guestagent.datastore.pxc import service as pxc_service
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
    DATABASE_PORT = 3306
    PEER_CLIENT = 'mysql'

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


class TestHealthcheckFile(trove_testtools.TestCase):

    @mock.patch.object(galera_service, 'operating_system')
    def test_file(self, mock_os):
        app = FakeApp()
        app.database_service_uid = '1001'
        app.database_service_gid = '1001'

        with mock.patch.object(
                FakeApp, 'cluster_healthcheck_file',
                new_callable=mock.PropertyMock,
                return_value='/var/lib/mysql/conf.d/x.cnf'):
            app._write_cluster_healthcheck_file(REPLICATION_USER)

        path, content = mock_os.write_file.call_args[0][:2]
        self.assertEqual('/var/lib/mysql/conf.d/x.cnf', path)
        self.assertEqual(
            {'client': {'user': 'clusterrepuser', 'password': 'rep-pw',
                        'host': '127.0.0.1', 'protocol': 'tcp'}},
            content)
        # The check runs in the container, as the database user, and the
        # file holds a password.
        mock_os.chown.assert_called_once_with(
            '/var/lib/mysql/conf.d/x.cnf', '1001', '1001', as_root=True)
        mock_os.chmod.assert_called_once_with(
            '/var/lib/mysql/conf.d/x.cnf', mock_os.FileMode.SET_USR_RW,
            as_root=True)


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
    def test_complete_cluster_leaves_bootstrap(self, _remove):
        # Galera's completion: the member that formed the cluster is
        # started again as an ordinary member.
        app = FakeApp()
        app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data', '--wsrep-new-cluster'])

        app.complete_cluster(COMMAND)

        self.assertEqual(
            [('stop_db',), ('start_db', COMMAND, app.HEALTHCHECK)],
            app.calls)
        # Nothing marks the cluster complete yet.
        self.assertFalse(app.is_cluster_complete())

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
             'admin_password': 'admin-pw',
             'writer_mode': 'single'},
            app.get_cluster_context())
        app.configuration_manager.get_value.assert_any_call('mysqld')

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

        app.configuration_manager.get_value.assert_any_call('galera')


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
        self.prepared = []

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info, config_contents,
                   root_password, overrides, cluster_config, snapshot,
                   ds_version=None):
        self.prepared.append(cluster_config)

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

    def test_cluster_complete_completes_the_app_first(self):
        manager = FakeManager()

        manager.cluster_complete(mock.sentinel.context)

        manager.app.complete_cluster.assert_called_once_with(COMMAND)
        # The base class reports the instance as installed; by then the
        # member has to be running the way it will from now on.
        self.assertEqual(1, len(manager.completed))
        self.assertIn(mock.call.complete_cluster(COMMAND),
                      manager.completed[0])

    def test_the_cluster_calls_tell_the_probe(self):
        manager = FakeManager()
        manager.cluster_probe = mock.Mock()
        calls = mock.Mock()
        calls.attach_mock(manager.cluster_probe, 'probe')
        calls.attach_mock(manager.app, 'app')

        manager.install_cluster(mock.sentinel.context, REPLICATION_USER,
                                CLUSTER_CONFIGURATION, False)
        manager.cluster_probe.enable_member.assert_called_once()
        manager.cluster_complete(mock.sentinel.context)
        manager.cluster_probe.enable_complete.assert_called_once()
        manager.leave_cluster(mock.sentinel.context)
        # The probe is disabled before the member leaves.
        self.assertEqual([mock.call.probe.disable(),
                          mock.call.app.leave_group()],
                         [c for c in calls.mock_calls
                          if 'disable' in str(c) or 'leave_group' in str(c)])
        manager.app.get_member_role.return_value = {'state': 'Synced'}
        self.assertEqual({'state': 'Synced'},
                         manager.get_member_role(mock.sentinel.context))

    def test_a_failed_install_does_not_enable_the_probe(self):
        manager = FakeManager()
        manager.cluster_probe = mock.Mock()
        manager.app.install_cluster.side_effect = RuntimeError('no')

        self.assertRaises(
            RuntimeError, manager.install_cluster, mock.sentinel.context,
            REPLICATION_USER, CLUSTER_CONFIGURATION, False)
        manager.cluster_probe.enable_member.assert_not_called()

    def test_init_cluster_probe(self):
        manager = FakeManager()
        manager.docker_client = mock.Mock()
        manager.app.DATABASE_PORT = 3306
        with mock.patch.object(galera_manager.cluster_probe.ClusterProbe,
                               'start') as start:
            manager.init_cluster_probe()
        self.assertIsInstance(manager.cluster_probe,
                              galera_manager.cluster_probe.ClusterProbe)
        self.assertIs(manager.app, manager.cluster_probe.app)
        start.assert_called_once()
        # A manager whose probe cannot be set up is still a manager.
        with mock.patch.object(galera_manager.cluster_probe, 'ClusterProbe',
                               side_effect=RuntimeError('no')):
            manager.init_cluster_probe()

    def test_without_a_probe(self):
        manager = FakeManager()
        manager.install_cluster(mock.sentinel.context, REPLICATION_USER,
                                CLUSTER_CONFIGURATION, False)
        manager.cluster_complete(mock.sentinel.context)
        manager.leave_cluster(mock.sentinel.context)
        manager.app.leave_group.assert_called_once()


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
             'get_cluster_context', 'write_cluster_configuration_overrides',
             'leave_cluster', 'is_writable_member', 'get_member_role',
             'get_recovery_view', 'bootstrap_cluster'},
            cluster_calls)
        for name in cluster_calls:
            self.assertTrue(
                callable(getattr(mariadb_manager.Manager, name, None)),
                'the MariaDB manager does not implement %s' % name)

    def test_bootstrap_cluster_is_told_not_waited_for(self):
        api = galera_guest_api.GaleraCommonGuestAgentAPI.__new__(
            galera_guest_api.GaleraCommonGuestAgentAPI)
        api._cast, api._call = mock.Mock(), mock.Mock()
        api.bootstrap_cluster()
        api._cast.assert_called_once_with(
            'bootstrap_cluster', version=guest_api.API.API_BASE_VERSION)
        api._call.assert_not_called()

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
                 {'cluster_configuration'}),
                ('leave_cluster', set()),
                ('is_writable_member', set()),
                ('get_member_role', set()),
                ('get_recovery_view', set()),
                ('bootstrap_cluster', set())):
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

    def test_the_cluster_account_is_hidden_from_users(self):
        # It would otherwise be listed by the user API, where its password
        # can be changed and the account deleted. Members log in with it
        # for their health check.
        self.assertIn(
            galera_tasks.GaleraCommonClusterTasks.CLUSTER_REPLICATION_USER,
            CONF.mariadb.ignore_users)

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
        parsed = codec.deserialize(
            codec.serialize(codec.deserialize(rendered)))
        section = parsed['galera']

        # Trove's section, which the server leaves alone: single writer
        # unless the task manager says.
        self.assertEqual({'cluster_writer_mode': 'single'}, parsed['trove'])
        self.assertEqual('"gcache.size=512M; gcache.page_size=1G"',
                         section['wsrep_provider_options'])
        self.assertEqual('"clusterrepuser:a;b#c:d"',
                         section['wsrep_sst_auth'])
        self.assertEqual('"gcomm://10.0.0.1,10.0.0.2,10.0.0.3"',
                         section['wsrep_cluster_address'])
        self.assertEqual('ON', section['wsrep_on'])


def _status(**values):
    """The wsrep status of a synced member of a primary component of
    three, with what the test changes.
    """
    status = {
        'wsrep_ready': 'ON', 'wsrep_cluster_status': 'Primary',
        'wsrep_local_state_comment': 'Synced', 'wsrep_cluster_conf_id': '5',
        'wsrep_incoming_addresses':
            '10.0.0.2:3306,10.0.0.3:3306,10.0.0.1:3306',
        'wsrep_last_committed': '26', 'wsrep_cluster_state_uuid': 'u1',
        'wsrep_local_state_uuid': 'u1', 'wsrep_cluster_size': '3'}
    status.update(values)
    return status


def _batch(status):
    return '\n'.join('%s\t%s' % item for item in status.items()) + '\n'


class TestStatusParsing(trove_testtools.TestCase):

    def test_parse_status(self):
        # The warning is the mariadb client's; the rows are the status.
        output = ('WARNING: option --ssl-verify-server-cert is disabled\n'
                  'wsrep_ready\tON\n'
                  'wsrep_incoming_addresses\t10.0.0.2:3306,10.0.0.1:3306\n')
        self.assertEqual(
            {'wsrep_ready': 'ON',
             'wsrep_incoming_addresses': '10.0.0.2:3306,10.0.0.1:3306'},
            galera_service._parse_status(output))
        self.assertEqual({}, galera_service._parse_status(''))

    def test_address(self):
        for address, parsed in (('10.0.0.1:3306', ('10.0.0.1', '3306')),
                                ('10.0.0.1:0', ('10.0.0.1', '0')),
                                ('10.0.0.1', ('10.0.0.1', '')),
                                (' 10.0.0.1 ', ('10.0.0.1', '')),
                                ('[fd00::1]:4567', ('fd00::1', '4567')),
                                ('[fd00::1]', ('fd00::1', ''))):
            self.assertEqual(parsed, galera_service._address(address))


class TestWriterMode(trove_testtools.TestCase):

    def setUp(self):
        super(TestWriterMode, self).setUp()
        self.app = FakeApp()
        self.sections = {}
        self.app.configuration_manager.get_value.side_effect = (
            lambda section: self.sections.get(section))

    def test_single_unless_told(self):
        self.assertEqual('single', self.app.writer_mode)
        self.sections['trove'] = {'cluster_writer_mode': 'multi'}
        self.assertEqual('multi', self.app.writer_mode)
        self.sections['trove'] = {'cluster_writer_mode': '"single"'}
        self.assertEqual('single', self.app.writer_mode)
        self.sections['trove'] = {'cluster_writer_mode': 'both'}
        self.assertEqual('single', self.app.writer_mode)

    def test_keep_writer_mode(self):
        self.app.keep_writer_mode('multi')
        self.app.configuration_manager.apply_system_override.\
            assert_called_once_with({'trove': {'cluster_writer_mode':
                                               'multi'}}, 'cluster-mode')
        self.assertRaises(exception.BadRequest, self.app.keep_writer_mode,
                          'both')

    def test_the_context_carries_the_mode(self):
        self.sections['trove'] = {'cluster_writer_mode': 'multi'}
        self.sections['mysqld'] = {
            'wsrep_sst_auth': '"clusterrepuser:rep-pw"',
            'wsrep_cluster_name': 'c1'}
        self.app.get_auth_password = mock.Mock(return_value='admin-pw')
        self.assertEqual(
            {'replication_user': {'name': 'clusterrepuser',
                                  'password': 'rep-pw'},
             'cluster_name': 'c1', 'admin_password': 'admin-pw',
             'writer_mode': 'multi'},
            self.app.get_cluster_context())

    @mock.patch.object(galera_service.docker_util, 'remove_container')
    def test_complete_cluster_marks_the_cluster_complete(self, _remove):
        self.app.docker_client.containers.get.return_value = _container(
            ['--datadir=/var/lib/mysql/data'])
        self.app.complete_cluster(COMMAND)
        self.app.configuration_manager.apply_system_override.\
            assert_called_once_with(
                {'trove': {'cluster_complete': 'yes'}}, 'cluster-complete')
        self.app.configuration_manager.has_system_override.return_value = (
            True)
        self.assertTrue(self.app.is_cluster_complete())
        self.app.configuration_manager.has_system_override.\
            assert_called_with('cluster-complete')


class TestMemberView(trove_testtools.TestCase):
    """The member as the cluster probe sees it, on a member of three,
    10.0.0.2, asking its peers from the database container.
    """

    def setUp(self):
        super(TestMemberView, self).setUp()
        self.app = FakeApp()
        self.sections = {
            'mysqld': {'wsrep_node_address': '10.0.0.2',
                       'wsrep_cluster_address':
                           '"gcomm://10.0.0.1:4567,10.0.0.2,[fd00::3]:4567"'},
            'trove': {}}
        self.app.configuration_manager.get_value.side_effect = (
            lambda section: self.sections.get(section))
        self.status = _status()
        self.app.execute_sql = mock.Mock(
            side_effect=lambda sql: list(self.status.items()))
        self.app._recovery_credentials = mock.Mock(return_value=('r', 'p'))
        self.peers = {'10.0.0.1': _status(), '10.0.0.3': _status()}
        self.container = self.app.docker_client.containers.get.return_value
        self.container.exec_run.side_effect = self._exec
        mock.patch.object(galera_service.time, 'monotonic',
                          return_value=1000.0).start()
        self.addCleanup(mock.patch.stopall)
        self.patch_datastore_manager('pxc')

    def _exec(self, command, environment=None):
        host = [a for a in command if a.startswith('--host=')][0][7:]
        peer = self.peers.get(host)
        if peer is None:
            return 1, b"ERROR 2003 (HY000): Can't connect to MySQL server"
        return 0, _batch(peer).encode()

    def _asked(self):
        return [[a for a in c.args[0] if a.startswith('--host=')][0][7:]
                for c in self.container.exec_run.call_args_list]

    def test_no_cluster_account_yet(self):
        # While the cluster is built: no account to ask the members below
        # with, so no writer is known and the member has no role, quietly.
        self.app._recovery_credentials = mock.Mock(
            side_effect=KeyError('/var/lib/mysql/conf.d/cluster-healthcheck'))
        self.app.is_cluster_complete = mock.Mock(return_value=False)
        self.app.execute_sql = mock.Mock(side_effect=lambda sql: list(
            _status().items()))
        with mock.patch.object(galera_service.LOG, 'warning') as warning:
            view = self.app.member_view()
        self.assertEqual((None, False), (view.role, view.writable))
        warning.assert_not_called()
        # Once complete, a missing account is worth a warning, once a tick.
        self.app.is_cluster_complete = mock.Mock(return_value=True)
        with mock.patch.object(galera_service.LOG, 'warning') as warning:
            self.assertIsNone(self.app.member_view().role)
        warning.assert_called_once()

    def test_addresses_from_the_configuration(self):
        self.assertEqual('10.0.0.2', self.app._self_ip())
        self.assertEqual(['10.0.0.1', '10.0.0.2', 'fd00::3'],
                         self.app._seed_ips())

    def test_no_answer_is_unknown(self):
        self.app.execute_sql.side_effect = Exception('gone')
        self.assertEqual(galera_service.cluster_probe.UNKNOWN_MEMBER,
                         self.app.member_view())

    def test_out_of_the_primary_component(self):
        # Cut off: Initialized, non-Primary, not ready. No role, no
        # writes; not brought back yet (the next step).
        self.status = _status(wsrep_cluster_status='non-Primary',
                              wsrep_local_state_comment='Initialized',
                              wsrep_ready='OFF', wsrep_cluster_size='1')
        self.assertEqual(('Initialized', None, False, False),
                         tuple(self.app.member_view()))
        self.assertEqual([], self._asked())

    def test_a_joiner_has_no_role(self):
        self.status = _status(wsrep_local_state_comment='Joiner',
                              wsrep_ready='OFF')
        self.assertEqual(('Joiner', None, False, False),
                         tuple(self.app.member_view()))

    def test_multi_writer(self):
        self.sections['trove'] = {'cluster_writer_mode': 'multi'}
        self.assertEqual(('Synced', 'PRIMARY', True, False),
                         tuple(self.app.member_view()))
        # Without asking anybody.
        self.assertEqual([], self._asked())

    def test_single_writer_is_the_lowest_synced_member(self):
        # 10.0.0.1 is synced: it is the writer; 10.0.0.3 is above and is
        # not asked.
        self.assertEqual(('Synced', 'SECONDARY', False, False),
                         tuple(self.app.member_view()))
        self.assertEqual(['10.0.0.1'], self._asked())
        command = self.container.exec_run.call_args.args[0]
        self.assertEqual('mysql', command[0])
        self.assertIn('--user=r', command)
        self.assertEqual({'MYSQL_PWD': 'p'},
                         self.container.exec_run.call_args.kwargs[
                             'environment'])
        self.assertNotIn('p', command)

    def test_the_lowest_member_asks_nobody(self):
        self.sections['mysqld']['wsrep_node_address'] = '10.0.0.1'
        self.assertEqual(('Synced', 'PRIMARY', True, False),
                         tuple(self.app.member_view()))
        self.assertEqual([], self._asked())

    def test_the_writer_is_asked_once_per_view(self):
        self.app.member_view()
        self.app.member_view()
        self.assertEqual(['10.0.0.1'], self._asked())
        # Until the view changes.
        self.status = _status(wsrep_cluster_conf_id='6')
        self.app.member_view()
        self.assertEqual(['10.0.0.1', '10.0.0.1'], self._asked())
        # Or now and then: 20 checks of 3 seconds.
        galera_service.time.monotonic.return_value = 1061.0
        self.app.member_view()
        self.assertEqual(3, len(self._asked()))

    def test_a_lower_member_that_is_not_synced_yet(self):
        # 10.0.0.1 is joining: this member takes the writes, and asks
        # again at the next check, as 10.0.0.1 may be synced by then.
        self.peers['10.0.0.1'] = _status(wsrep_local_state_comment='Joined')
        self.assertEqual(('Synced', 'PRIMARY', True, False),
                         tuple(self.app.member_view()))
        self.app.member_view()
        self.assertEqual(['10.0.0.1', '10.0.0.1'], self._asked())
        self.peers['10.0.0.1'] = _status()
        self.assertEqual('SECONDARY', self.app.member_view().role)

    def test_a_member_receiving_a_state_transfer_is_not_asked(self):
        # Port 0: it does not take connections.
        self.status = _status(
            wsrep_incoming_addresses='10.0.0.2:3306,10.0.0.3:3306,10.0.0.1:0')
        self.assertEqual('PRIMARY', self.app.member_view().role)
        self.assertEqual([], self._asked())

    def test_a_lower_member_that_does_not_answer(self):
        # Still in the view but not answering: not the writer.
        del self.peers['10.0.0.1']
        self.assertEqual('PRIMARY', self.app.member_view().role)
        self.assertEqual(['10.0.0.1'], self._asked())

    def test_a_lower_member_out_of_the_primary_component(self):
        self.peers['10.0.0.1'] = _status(wsrep_cluster_status='non-Primary')
        self.assertEqual('PRIMARY', self.app.member_view().role)

    def test_member_role_and_writable(self):
        self.assertEqual({'state': 'Synced', 'role': 'SECONDARY',
                          'writable': False}, self.app.get_member_role())
        self.assertFalse(self.app.is_writable_member())

    def test_query_peer(self):
        peer = self.app._query_peer('10.0.0.1', 'r', 'p', 3)
        self.assertEqual(galera_service.cluster_probe.PeerView(
            '10.0.0.1', True, True, True, ('u1', '26')), peer)
        self.peers['10.0.0.1'] = _status(wsrep_cluster_status='non-Primary')
        self.assertEqual((True, False, False),
                         self.app._query_peer('10.0.0.1', 'r', 'p', 3)[1:4])
        self.assertFalse(self.app._query_peer('10.0.0.9', 'r', 'p', 3)
                         .reachable)
        self.app.docker_client.containers.get.side_effect = Exception('x')
        self.assertFalse(self.app._query_peer('10.0.0.1', 'r', 'p', 3)
                         .reachable)


class TestClientCommands(trove_testtools.TestCase):

    def test_the_mariadb_image_has_the_mariadb_client(self):
        self.assertEqual('mariadb', mariadb_service.MariaDBApp.PEER_CLIENT)
        self.assertEqual('mysql', pxc_service.PXCApp.PEER_CLIENT)

    @mock.patch.object(galera_manager.cluster_probe.ClusterProbe, 'start')
    @mock.patch.object(mariadb_manager.Manager, 'docker_client',
                       new_callable=mock.PropertyMock)
    def test_the_mariadb_manager_runs_the_probe(self, _docker, start):
        self.patch_datastore_manager('mariadb')
        manager = mariadb_manager.Manager()
        self.assertIsInstance(manager.cluster_probe,
                              galera_manager.cluster_probe.ClusterProbe)
        self.assertEqual(3307, manager.cluster_probe.port.port)
        start.assert_called_once()

    def test_prepare_keeps_the_writer_mode(self):
        manager = FakeManager()
        manager.do_prepare('ctx', [], [], 512, [], None, None, None, None,
                           None, None, {'id': 'i1', 'writer_mode': 'multi'},
                           None)
        manager.app.keep_writer_mode.assert_called_once_with('multi')
        manager.do_prepare('ctx', [], [], 512, [], None, None, None, None,
                           None, None, {'id': 'i1'}, None)
        manager.app.keep_writer_mode.assert_called_once()


GRASTATE = """# GALERA saved state
version: 2.1
uuid:    eb210882-c280-11f1-9bce-6fce320fdaa0
seqno:   36
safe_to_bootstrap: 1
"""
PXC_LOG = (b"2026-10-07T19:02:48.49Z 0 [Note] [Galera] Assign initial "
           b"position for certification: eb210882-c280-11f1-9bce-"
           b"6fce320fdaa0:31, protocol version: -1\n"
           b"2026-10-07T19:02:48.49Z 0 [Note] [Galera] Setting GCS initial "
           b"position to eb210882-c280-11f1-9bce-6fce320fdaa0:31\n"
           b"2026-10-07T19:08:40.95Z 0 [Note] [Galera] No nodes coming from "
           b"primary view, primary view is not possible\n"
           b"2026-10-07T19:08:41.45Z 0 [Note] [Galera] Received "
           b"NON-PRIMARY.\n")


class TestRecoveryView(trove_testtools.TestCase):
    """What a member answers the task manager with after every member
    went down, and how it forms the cluster again.
    """

    def setUp(self):
        super(TestRecoveryView, self).setUp()
        self.app = FakeApp()
        self.app.get_data_dir = mock.Mock(return_value='/var/lib/mysql/data')
        self.app.database_service_uid = 1001
        self.app.database_service_gid = 1001
        self.sections = {'mysqld': {'wsrep_node_address': '10.0.0.2'}}
        self.app.configuration_manager.get_value.side_effect = (
            lambda section: self.sections.get(section))
        self.container = self.app.docker_client.containers.get.return_value
        self.container.status = 'running'
        self.container.logs.return_value = PXC_LOG
        self.container.attrs = {'Config': {'Cmd': [
            '--defaults-file=/etc/mysql/my.cnf',
            '--datadir=/var/lib/mysql/data']}}
        self.files = {'/var/lib/mysql/data/grastate.dat': GRASTATE}
        self.os = mock.patch.object(galera_service, 'operating_system').start()
        self.os.read_file.side_effect = (
            lambda path, **k: self.files[path])
        self.os.write_file.side_effect = (
            lambda path, data, **k: self.files.__setitem__(path, data))
        # The database port is closed: the server waits for a primary view.
        self.app.execute_sql = mock.Mock(side_effect=Exception('refused'))
        mock.patch.object(galera_service.time, 'sleep').start()
        self.patch_datastore_manager('pxc')
        self.addCleanup(mock.patch.stopall)

    def test_grastate(self):
        self.assertEqual(
            {'version': '2.1', 'uuid': 'eb210882-c280-11f1-9bce-6fce320fdaa0',
             'seqno': '36', 'safe_to_bootstrap': '1'},
            self.app._grastate())
        del self.files['/var/lib/mysql/data/grastate.dat']
        self.assertEqual({}, self.app._grastate())

    def test_the_position_the_server_logged_comes_first(self):
        # After a crash grastate.dat says -1 and the log has it.
        self.assertEqual(('eb210882-c280-11f1-9bce-6fce320fdaa0', 31),
                         self.app._position())
        # After a clean stop the log has none and grastate.dat has it.
        self.container.logs.return_value = b'nothing yet\n'
        self.assertEqual(('eb210882-c280-11f1-9bce-6fce320fdaa0', 36),
                         self.app._position())
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'seqno:   36', 'seqno:   -1')
        self.assertIsNone(self.app._position())
        # MariaDB's line, and a recovery run's.
        for line in (b'[Galera] Setting initial position to ab-cd:7\n',
                     b'[WSREP] Recovered position: ab-cd:7\n'):
            self.container.logs.return_value = (
                b'x\n' + line.replace(b'ab-cd', b'0' * 8 + b'-' + b'0' * 27))
            self.assertEqual(7, self.app._position()[1])

    def test_only_the_log_of_the_current_run_counts(self):
        # Without a start time the whole tail is read; with one, only
        # what the container logged since: an earlier run's position is
        # where the member stood then.
        self.app._position()
        self.container.logs.assert_called_with(tail=galera_service.LOG_TAIL)
        self.container.attrs['State'] = {
            'StartedAt': '2026-10-08T17:12:30.123456789Z'}
        self.app._position()
        self.container.logs.assert_called_with(
            tail=galera_service.LOG_TAIL, since=1791479550)
        self.assertIsNone(galera_service._epoch(None))
        self.assertIsNone(galera_service._epoch('junk'))

    def test_a_logged_minus_one_is_no_position(self):
        # What a server logs that does not know either, as MariaDB does
        # after a crash: the recovery run is asked.
        self.container.logs.return_value = PXC_LOG.replace(b':31', b':-1')
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'seqno:   36', 'seqno:   -1')
        self.app._recover_position = mock.Mock(return_value=None)
        self.assertIsNone(self.app._position())
        self.app._recover_position.assert_called_once_with()

    def test_the_recovery_run_is_kept_until_the_member_is_in_a_group(self):
        uuid = 'eb210882-c280-11f1-9bce-6fce320fdaa0'
        self.container.logs.return_value = b'nothing\n'
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'seqno:   36', 'seqno:   -1')
        run = self.app.docker_client.containers.run
        run.return_value = (b'x\n[Note] WSREP: Recovered position: ' +
                            uuid.encode() + b':8\n')

        self.assertEqual((uuid, 8), self.app._position())
        self.assertEqual((uuid, 8), self.app._position())

        # Run once, with the database stopped and the container started
        # again after; the answer is kept while the member waits.
        self.assertEqual(1, run.call_count)
        self.assertIn('--wsrep-recover', run.call_args[0][1])
        self.assertEqual('mysqld', run.call_args[1]['entrypoint'])
        self.assertEqual([('stop_db',)], self.app.calls)
        self.container.start.assert_called_once_with()
        # In a primary component again: forgotten, so a later wait asks
        # again.
        self.app.execute_sql = mock.Mock(side_effect=lambda sql: list(
            _status().items()))
        self.assertTrue(self.app.recovery_view()['in_group'])
        self.app.execute_sql = mock.Mock(side_effect=Exception('refused'))
        self.app._position()
        self.assertEqual(2, run.call_count)

    def test_the_recovery_run_without_a_position(self):
        self.container.logs.return_value = b'nothing\n'
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'seqno:   36', 'seqno:   -1')
        self.app.docker_client.containers.run.return_value = b'no luck\n'
        self.assertIsNone(self.app._position())
        self.assertIsNone(self.app._position())
        self.assertEqual(1, self.app.docker_client.containers.run.call_count)

    def test_not_ahead(self):
        u, v = ('u1', 10), ('u1', 12)
        self.assertTrue(self.app._not_ahead(u, v))
        self.assertTrue(self.app._not_ahead(u, u))
        self.assertFalse(self.app._not_ahead(v, u))
        self.assertTrue(self.app._not_ahead(None, u))
        self.assertFalse(self.app._not_ahead(u, None))
        self.assertFalse(self.app._not_ahead(('u2', 1), v))

    def test_waiting_for_a_primary_view(self):
        # The database port does not answer while the container runs.
        # grastate.dat marks it the last to leave the cluster.
        self.assertEqual(
            {'ip': '10.0.0.2', 'in_group': False, 'waiting': True,
             'position': ('eb210882-c280-11f1-9bce-6fce320fdaa0', 31),
             'safe_to_bootstrap': True, 'bootstrapped': False},
            self.app.recovery_view())
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'safe_to_bootstrap: 1', 'safe_to_bootstrap: 0')
        self.assertFalse(self.app.recovery_view()['safe_to_bootstrap'])
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE
        # Whatever the log says: it holds every run of the container.
        self.container.logs.return_value = PXC_LOG + (
            b'[Galera] New COMPONENT: primary = yes, bootstrap = no\n'
            b'[Galera] Received SELF-LEAVE.\n')
        self.assertTrue(self.app.recovery_view()['waiting'])
        # Started again and again: still waiting.
        self.container.status = 'restarting'
        self.assertTrue(self.app.recovery_view()['waiting'])
        # A container that is not running, or none, waits for nothing.
        self.container.status = 'exited'
        self.assertFalse(self.app.recovery_view()['waiting'])
        self.app.docker_client.containers.get.side_effect = (
            docker.errors.NotFound('gone'))
        self.assertFalse(self.app.recovery_view()['waiting'])

    def test_in_a_primary_component(self):
        self.app.execute_sql = mock.Mock(side_effect=lambda sql: list(
            _status(wsrep_last_committed='40').items()))
        self.assertEqual(
            {'ip': '10.0.0.2', 'in_group': True, 'waiting': False,
             'position': ('u1', 40), 'safe_to_bootstrap': False,
             'bootstrapped': False},
            self.app.recovery_view())
        # Cut off: non-Primary, waiting.
        self.app.execute_sql = mock.Mock(side_effect=lambda sql: list(
            _status(wsrep_cluster_status='non-Primary',
                    wsrep_local_state_comment='Initialized').items()))
        view = self.app.recovery_view()
        self.assertEqual((False, True), (view['in_group'], view['waiting']))

    def test_rejoin_does_nothing(self):
        self.app.rejoin_group('Initialized')
        self.assertEqual([], self.app.calls)

    @mock.patch.object(galera_service.docker_util, 'remove_container')
    def test_bootstrap_group(self, _remove):
        # Marked safe to bootstrap, started with the flag, and once the
        # others have joined and are synced, without it: alone, then a
        # joiner receiving the state (port 0, this member its donor), then
        # a member in the view but not synced yet, then everyone synced.
        self.files['/var/lib/mysql/data/grastate.dat'] = GRASTATE.replace(
            'safe_to_bootstrap: 1', 'safe_to_bootstrap: 0')
        # The port is closed when the member looks before forming the
        # cluster; then its own statuses as the cluster forms.
        own = iter([
            None,
            _status(wsrep_cluster_size='1',
                    wsrep_incoming_addresses='10.0.0.2:3306'),
            _status(wsrep_cluster_size='2',
                    wsrep_local_state_comment='Donor/Desynced',
                    wsrep_incoming_addresses='10.0.0.2:3306,10.0.0.3:0'),
            _status(wsrep_cluster_size='2',
                    wsrep_incoming_addresses='10.0.0.2:3306,10.0.0.3:3306'),
            _status(wsrep_cluster_size='3')])

        def own_status(sql):
            status = next(own)
            if status is None:
                raise Exception('refused')
            return list(status.items())
        self.app.execute_sql = mock.Mock(side_effect=own_status)
        self.app._recovery_credentials = mock.Mock(return_value=('r', 'p'))
        peers = iter([_status(wsrep_local_state_comment='Joined'),
                      _status(), _status()])
        self.app._peer_status = mock.Mock(side_effect=lambda *a: next(peers))
        # Started with the flag, as the container will show after.
        self.container.attrs['Config']['Cmd'].append('--wsrep-new-cluster')
        # The waiting server is killed, then grastate.dat is marked.
        self.container.kill.side_effect = (
            lambda: self.app.calls.append(('kill',)))
        write_file = self.os.write_file.side_effect
        self.os.write_file.side_effect = (
            lambda path, data, **k: (self.app.calls.append(('grastate',)),
                                     write_file(path, data, **k)))

        self.app.bootstrap_group()

        self.assertIn('safe_to_bootstrap: 1',
                      self.files['/var/lib/mysql/data/grastate.dat'])
        self.os.chown.assert_called_once()
        command = ('--defaults-file=/etc/mysql/my.cnf '
                   '--datadir=/var/lib/mysql/data')
        self.assertEqual(
            [('kill',), ('grastate',),
             ('start_db', command + ' --wsrep-new-cluster', app_hc(self.app)),
             ('stop_db',),
             ('start_db', command, app_hc(self.app))],
            self.app.calls)
        # The joiner at port 0 was not asked; the other two were, with the
        # cluster account.
        self.assertEqual([mock.call('10.0.0.3', 'r', 'p', 3)] +
                         [mock.call(ip, 'r', 'p', 3)
                          for ip in ('10.0.0.3', '10.0.0.1')],
                         self.app._peer_status.call_args_list)

    def test_a_member_forming_the_cluster_does_not_start_over(self):
        self.app._form_group = mock.Mock()
        with galera_service.GaleraAppMixin._forming:
            self.app.bootstrap_group()
        self.app._form_group.assert_not_called()
        self.app.bootstrap_group()
        self.app._form_group.assert_called_once_with()

    @mock.patch.object(galera_service.docker_util, 'remove_container')
    def test_bootstrap_group_alone(self, _remove):
        # Nobody joined in time: the flag goes all the same.
        alone = iter([None])

        def own_status(sql):
            if next(alone, 1) is None:
                raise Exception('refused')
            return list(_status(
                wsrep_cluster_size='1',
                wsrep_incoming_addresses='10.0.0.2:3306').items())
        self.app.execute_sql = mock.Mock(side_effect=own_status)
        self.container.attrs['Config']['Cmd'].append('--wsrep-new-cluster')
        with mock.patch.object(galera_service.time, 'time',
                               side_effect=[0, 1, 10 ** 9, 10 ** 9]):
            self.app.bootstrap_group()
        self.container.kill.assert_called_once_with()
        self.assertEqual(['start_db', 'stop_db', 'start_db'],
                         [c[0] for c in self.app.calls])
        self.assertNotIn('--wsrep-new-cluster', self.app.calls[-1][1])

    def test_not_forming_when_a_primary_component_appeared_meanwhile(self):
        # On this member: the database port answers from a primary
        # component.
        self.app.execute_sql = mock.Mock(side_effect=lambda sql: list(
            _status().items()))
        self.app.bootstrap_group()
        self.container.kill.assert_not_called()
        self.assertEqual([], self.app.calls)
        # On another member of the cluster: it is joined, not rivalled.
        self.app.execute_sql = mock.Mock(side_effect=Exception('refused'))
        self.sections['mysqld']['wsrep_cluster_address'] = (
            'gcomm://10.0.0.1,10.0.0.2,10.0.0.3')
        self.app._recovery_credentials = mock.Mock(return_value=('r', 'p'))
        self.app._peer_status = mock.Mock(side_effect=[
            _status(wsrep_cluster_status='non-Primary'), _status()])
        self.app.bootstrap_group()
        self.container.kill.assert_not_called()
        self.assertEqual([], self.app.calls)
        self.assertEqual(['10.0.0.1', '10.0.0.3'], [
            c[0][0] for c in self.app._peer_status.call_args_list])


def app_hc(app):
    return app.HEALTHCHECK


class TestRecoveryRun(trove_testtools.TestCase):
    """MariaDB logs no position and leaves -1 in grastate.dat: a recovery
    run of the server, with the database stopped, finds it.
    """

    def setUp(self):
        super(TestRecoveryRun, self).setUp()
        self.app = FakeApp()
        self.app.SERVER_BINARY = 'mariadbd'
        self.app.get_data_dir = mock.Mock(return_value='/var/lib/mysql/data')
        self.app.database_service_uid = 1001
        self.app.database_service_gid = 1001
        self.container = self.app.docker_client.containers.get.return_value
        self.container.status = 'running'
        self.container.logs.return_value = b'WSREP: Non-primary view\n'
        self.container.attrs = {
            'Config': {'Cmd': ['--defaults-file=/etc/mysql/my.cnf',
                               '--datadir=/var/lib/mysql/data']},
            'State': {'StartedAt': 't1'}}
        self.app.docker_client.containers.run.return_value = (
            b'2026-10-07 19:19:43 0 [Note] WSREP: Recovered position: '
            b'cd7ef434-c283-11f1-b9c7-ff3171d9ab0e:8\n')
        self.os = mock.patch.object(galera_service, 'operating_system').start()
        self.os.read_file.return_value = GRASTATE.replace('seqno:   36',
                                                          'seqno:   -1')
        self.addCleanup(mock.patch.stopall)
        self.patch_datastore_manager('mariadb')

    def test_recovery_run(self):
        self.assertEqual(('cd7ef434-c283-11f1-b9c7-ff3171d9ab0e', 8),
                         self.app._position())
        # The database was stopped for it and started again after.
        self.assertEqual([('stop_db',)], self.app.calls)
        self.container.start.assert_called_once()
        run = self.app.docker_client.containers.run
        args, kwargs = run.call_args
        self.assertEqual(['--defaults-file=/etc/mysql/my.cnf',
                          '--datadir=/var/lib/mysql/data', '--wsrep-recover'],
                         args[1])
        self.assertEqual('mariadbd', kwargs['entrypoint'])
        self.assertEqual('1001:1001', kwargs['user'])
        self.assertTrue(kwargs['remove'])
        self.assertIn('/var/lib/mysql', kwargs['volumes'])
        self.assertTrue(args[0].endswith(':' + str(
            galera_service.CONF.datastore_version)))
        # Kept: the member waits, nothing changes on it.
        self.assertEqual(('cd7ef434-c283-11f1-b9c7-ff3171d9ab0e', 8),
                         self.app._position())
        run.assert_called_once()
        # Even after the container was started anew: MariaDB gives up
        # waiting and starts again and again, and the run itself starts
        # the container again.
        self.container.attrs['State']['StartedAt'] = 't2'
        self.app._position()
        run.assert_called_once()

    def test_a_failed_run_still_starts_the_database_again(self):
        self.app.docker_client.containers.run.side_effect = Exception('no')
        self.assertIsNone(self.app._position())
        self.container.start.assert_called_once()
        # Not tried again and again.
        self.assertIsNone(self.app._position())
        self.app.docker_client.containers.run.assert_called_once()

    def test_no_run_without_grastate(self):
        self.os.read_file.side_effect = Exception('no such file')
        self.assertIsNone(self.app._position())
        self.app.docker_client.containers.run.assert_not_called()
