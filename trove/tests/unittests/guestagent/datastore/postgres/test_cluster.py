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
from trove.common import exception
from trove.guestagent.common import cluster_probe
from trove.guestagent.datastore.postgres import cluster
from trove.guestagent.datastore.postgres import manager as pg_manager
from trove.instance import service_status
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
USER = {'name': 'replicator', 'password': 'pw'}


class FakeApp(cluster.PostgresqlClusterAppMixin):
    """The mixin on a mocked app: the files it keeps are a dict."""

    def __init__(self):
        self.configuration_manager = mock.Mock()
        self.adm = mock.Mock()
        self.docker_client = mock.MagicMock()
        self.datadir = '/var/lib/postgresql/data/pgdata'
        self.database_service_uid = '999'
        self.database_service_gid = '999'
        self.calls = []
        for name in ('restart', 'stop_db', 'start_db', 'reload',
                     'apply_access_rules'):
            setattr(self, name, mock.Mock(
                side_effect=lambda *a, _n=name, **k: self.calls.append(
                    (_n,) + a + ((k,) if k else ()))))
        self.get_config_param = mock.Mock(return_value='')
        self.container_spec = mock.Mock(return_value={
            'image': 'postgres:17', 'volumes': {'v': {}}, 'ports': {},
            'network_mode': 'docker-hostnic', 'user': '999:999',
            'environment': {'PGDATA': self.datadir}})


class ClusterAppTestBase(trove_testtools.TestCase):

    def setUp(self):
        super(ClusterAppTestBase, self).setUp()
        self.patch_datastore_manager('postgresql')
        self.patch_conf_property('guest_id', 'i2')
        self.files = {}
        mock.patch.object(cluster.operating_system, 'read_file',
                          side_effect=self._read).start()
        mock.patch.object(cluster.operating_system, 'write_file',
                          side_effect=self._write).start()
        for name in ('chmod', 'chown', 'ensure_directory',
                     'execute_shell_cmd'):
            mock.patch.object(cluster.operating_system, name).start()
        mock.patch.object(cluster.utils, 'execute_with_timeout').start()
        mock.patch.object(cluster.time, 'sleep').start()
        self.addCleanup(mock.patch.stopall)
        self.app = FakeApp()

    def _read(self, path, codec=None, as_root=False, **kwargs):
        if path not in self.files:
            raise IOError(path)
        return self.files[path]

    def _write(self, path, data, codec=None, as_root=False, **kwargs):
        self.files[path] = data

    def _override(self, change_id):
        """The last override of the name written, None for none."""
        written = [call.args[0] for call in
                   self.app.configuration_manager.apply_system_override
                   .call_args_list if call.args[1] == change_id]
        return written[-1] if written else None


class ClusterFileTest(ClusterAppTestBase):

    def test_what_the_member_knows(self):
        self.assertFalse(self.app.is_cluster_member())
        self.assertFalse(self.app.is_cluster_complete())
        self.assertEqual('sync', self.app.cluster_mode)
        self.assertEqual('i2', self.app.self_id)
        self.app.keep_cluster_mode('strict')
        self.assertEqual('strict', self.app.cluster_mode)
        self.assertRaises(exception.BadRequest, self.app.keep_cluster_mode,
                          'eventual')
        self.app.update_cluster_members({'i1': '10.0.0.1', 'i2': '10.0.0.2'},
                                        'async')
        self.assertTrue(self.app.is_cluster_member())
        self.assertEqual('async', self.app.cluster_mode)
        self.app.complete_cluster()
        self.assertTrue(self.app.is_cluster_complete())
        self.assertEqual({'replication_user': None, 'mode': 'async',
                          'members': {'i1': '10.0.0.1', 'i2': '10.0.0.2'},
                          'primary_id': None, 'self_id': None},
                         self.app.get_cluster_context())
        # Root alone reads the file: the replication password is in it.
        cluster.operating_system.chmod.assert_called_with(
            cluster.CLUSTER_FILE, cluster.operating_system.FileMode.SET_USR_RW,
            as_root=True)

    def test_slot_names(self):
        self.assertEqual('trove_3f5a_b', cluster.slot_name('3F5A-b'))
        self.assertRaises(exception.BadRequest, cluster.slot_name, 'a;b')


class InstallTest(ClusterAppTestBase):

    def _config(self, primary):
        return {'members': {'i1': '10.0.0.1', 'i2': '10.0.0.2'},
                'mode': 'strict', 'replication_user': USER,
                'self_id': 'i2', 'primary': primary}

    def test_the_primary(self):
        self.app.adm.user_exists.return_value = False
        self.app.install_cluster(self._config(None))
        settings = self._override(cluster.CNF_SETTINGS)
        self.assertEqual(('on', 20, 20, '10s', '10s', '1GB',
                          'scram-sha-256'),
                         (settings['hot_standby'],
                          settings['max_wal_senders'],
                          settings['max_replication_slots'],
                          settings['wal_sender_timeout'],
                          settings['wal_receiver_timeout'],
                          settings['max_slot_wal_keep_size'],
                          settings['password_encryption']))
        self.app.adm.create_user.assert_called_once()
        user = self.app.adm.create_user.call_args.args[0]
        self.assertEqual(('replicator', 'pw'), (user.name, user.password))
        self.assertEqual(('REPLICATION', 'SUPERUSER', 'LOGIN'),
                         self.app.adm.create_user.call_args.args[2:])
        # Strict: a commit waits for a standby from the start.
        self.assertEqual({'synchronous_standby_names': 'ANY 1 (*)'},
                         self._override(cluster.CNF_SYNC))
        self.assertEqual([('apply_access_rules', {
            'ssl': False, 'mtls': False, 'replication_user': 'replicator',
            'scram': True}), ('restart',)], self.app.calls)
        context = self.app.get_cluster_context()
        self.assertEqual(('i2', 'i2', USER),
                         (context['primary_id'], context['self_id'],
                          context['replication_user']))
        self.assertFalse(self.app.is_cluster_complete())

    def test_the_primary_of_a_sync_cluster_waits_for_nobody_at_first(self):
        self.app.adm.user_exists.return_value = True
        self.app.install_cluster(dict(self._config(None), mode='sync'))
        self.assertEqual({'synchronous_standby_names': ''},
                         self._override(cluster.CNF_SYNC))
        self.app.adm.alter_user.assert_called_once()

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_a_standby(self, remove, start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, b'done')
        self.app.adm.query.return_value = [('streaming',)]
        self.app.install_cluster(self._config({'id': 'i1',
                                               'host': '10.0.0.1'}))
        self.assertEqual(
            {'primary_conninfo': 'host=10.0.0.1 port=5432 user=replicator '
                                 'password=pw application_name=i2 '
                                 'dbname=postgres',
             'primary_slot_name': 'trove_i2'},
            self._override(cluster.CNF_REPLICATION))
        # Stopped, emptied, a base backup through its slot in the parked
        # container, standby.signal, started, streaming.
        self.assertEqual('stop_db', self.app.calls[1][0])
        # Emptied by find as root: a glob before sudo would see nothing
        # in the database user's directory.
        for path in (self.app.datadir, cluster.pg_service.WAL_ARCHIVE_DIR):
            cluster.utils.execute_with_timeout.assert_any_call(
                'find', path, '-mindepth', '1', '-delete',
                run_as_root=True, root_helper='sudo', timeout=600)
        remove.assert_called_once_with(self.app.docker_client)
        self.assertEqual('sleep infinity', start.call_args.kwargs['command'])
        self.assertEqual('no', start.call_args.kwargs['restart_policy'])
        self.assertEqual('docker-hostnic',
                         start.call_args.kwargs['network_mode'])
        command = container.exec_run.call_args.args[0]
        self.assertEqual(['pg_basebackup', '-h', '10.0.0.1', '-p', '5432',
                          '-U', 'replicator', '-D', self.app.datadir,
                          '-X', 'stream', '-S', 'trove_i2',
                          '--checkpoint=fast', '--no-password'], command)
        self.assertEqual({'PGPASSWORD': 'pw'},
                         container.exec_run.call_args.kwargs['environment'])
        container.remove.assert_called_once_with(force=True)
        cluster.operating_system.execute_shell_cmd.assert_called_once_with(
            'touch %s/standby.signal' % self.app.datadir, [], shell=True,
            as_root=True)
        self.assertEqual(('start_db', {
            'ds_version': CONF.datastore_version,
            'command': 'postgres -c config_file=/etc/postgresql/'
                       'postgresql.conf'}), self.app.calls[-1])
        self.assertFalse(self.app.rebuilding)
        self.assertEqual('i1', self.app.get_cluster_context()['primary_id'])

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_a_failed_base_backup(self, remove, start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (1, b'could not connect')
        self.assertRaises(exception.TroveError, self.app.install_cluster,
                          self._config({'id': 'i1', 'host': '10.0.0.1'}))
        # The parked container goes all the same; the member is not
        # rebuilding any more, nor started.
        container.remove.assert_called_once_with(force=True)
        self.assertFalse(self.app.rebuilding)
        self.assertNotIn('start_db', [c[0] for c in self.app.calls])

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    @mock.patch.object(cluster.time, 'time', side_effect=range(0, 1000, 10))
    def test_a_standby_that_does_not_stream(self, _time, remove, start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, b'')
        self.app.adm.query.return_value = [('startup',)]
        self.assertRaises(exception.TroveError, self.app.install_cluster,
                          self._config({'id': 'i1', 'host': '10.0.0.1'}))


class SlotTest(ClusterAppTestBase):

    def test_create_once(self):
        self.app.adm.query.side_effect = [[], None]
        self.app.create_member_slot('i3')
        self.app.adm.query.assert_called_with(
            "SELECT pg_create_physical_replication_slot('trove_i3')")
        self.app.adm.query.reset_mock()
        self.app.adm.query.side_effect = [[(None,)]]
        self.app.create_member_slot('i3')
        self.app.adm.query.assert_called_once()

    def test_drop_terminates_the_sender_first(self):
        self.app.adm.query.side_effect = [[(4242,)], None, None]
        self.app.drop_member_slot('i3')
        self.assertEqual(["SELECT pg_terminate_backend(4242)",
                          "SELECT pg_drop_replication_slot('trove_i3')"],
                         [c.args[0] for c in
                          self.app.adm.query.call_args_list[1:]])
        self.app.adm.query.reset_mock()
        self.app.adm.query.side_effect = [[]]
        self.app.drop_member_slot('i3')
        self.app.adm.query.assert_called_once()


class MemberViewTest(ClusterAppTestBase):

    def _primary(self, standbys):
        self.app.adm.query.side_effect = [[(False,)], standbys]

    def _standby(self, receiver):
        self.app.adm.query.side_effect = [[(True,)], receiver]

    def test_the_primary_takes_writes(self):
        self._primary([('i2', 'streaming', 'async', 10)])
        view = self.app.member_view()
        self.assertEqual(('primary', 'PRIMARY', True, False, False),
                         tuple(view))
        self.assertEqual({'i2': {'state': 'streaming', 'sync_state': 'async',
                                 'lag_bytes': 10}}, self.app._standbys)

    def test_a_strict_primary_takes_writes_with_a_synchronous_standby(self):
        self.app.keep_cluster_mode('strict')
        self._primary([('i2', 'streaming', 'async', 0)])
        self.assertFalse(self.app.member_view().writable)
        self._primary([('i2', 'streaming', 'quorum', 0)])
        self.assertTrue(self.app.member_view().writable)
        self._primary([('i2', 'catchup', 'quorum', 0)])
        self.assertFalse(self.app.member_view().writable)

    def test_a_standby_serves_reads_while_it_keeps_up(self):
        self._standby([('streaming', '10.0.0.1', 1.5, 1000)])
        self.assertEqual(('standby', 'STREAMING', False, False, True),
                         tuple(self.app.member_view()))
        # Behind on replay, or not heard from the primary for a while.
        self._standby([('streaming', '10.0.0.1', 1.5, 64 * 1024 * 1024)])
        self.assertFalse(self.app.member_view().readable)
        self._standby([('streaming', '10.0.0.1', 25.0, 0)])
        self.assertFalse(self.app.member_view().readable)
        # Not streaming.
        self._standby([('stopping', '10.0.0.1', 1.0, 0)])
        self.assertEqual(('standby', 'DETACHED', False, False, False),
                         tuple(self.app.member_view()))
        self._standby([])
        self.assertEqual('DETACHED', self.app.member_view().role)

    def test_stopped_and_rebuilding(self):
        self.app.adm.query.side_effect = Exception('refused')
        self.assertEqual(('stopped', None, False, False, False),
                         tuple(self.app.member_view()))
        self.app.rebuilding = True
        self.assertEqual(('rebuilding', None, False, False, False),
                         tuple(self.app.member_view()))
        self.app.adm.query.assert_called_once()

    def test_the_role_answer(self):
        self._primary([('i2', 'streaming', 'quorum', 0)])
        self.assertEqual(
            {'state': 'primary', 'role': 'PRIMARY', 'writable': True,
             'readable': False, 'mode': 'sync',
             'standbys': {'i2': {'state': 'streaming',
                                 'sync_state': 'quorum', 'lag_bytes': 0}}},
            self.app.get_member_role())
        self._standby([('streaming', '10.0.0.1', 1.0, 0)])
        self.assertEqual(
            {'state': 'standby', 'role': 'STREAMING', 'writable': False,
             'readable': True, 'mode': 'sync'},
            self.app.get_member_role())


class ReconcileTest(ClusterAppTestBase):

    def setUp(self):
        super(ReconcileTest, self).setUp()
        self.now = 0.0
        mock.patch.object(cluster.time, 'monotonic',
                          side_effect=lambda: self.now).start()
        self.app.get_config_param.return_value = ''

    def _tick(self, mode, standbys, at=None):
        if at is not None:
            self.now = at
        self.app.keep_cluster_mode(mode)
        self.app._standbys = standbys
        self.app.reconcile(cluster_probe.MemberView('primary', 'PRIMARY',
                                                    True, False))

    def _written(self):
        names = self._override(cluster.CNF_SYNC)
        return names['synchronous_standby_names'] if names else None

    def test_sync_waits_for_a_standby_that_streams(self):
        self._tick('sync', {'i2': {'state': 'streaming'}})
        self.assertEqual('ANY 1 (*)', self._written())
        self.app.reload.assert_called_once()
        # Nothing changed: nothing written.
        self._tick('sync', {'i2': {'state': 'streaming'}})
        self.app.reload.assert_called_once()

    def test_sync_falls_back_after_the_grace_period(self):
        self._tick('sync', {'i2': {'state': 'streaming'}}, at=0)
        # The grace period runs from the first tick without a standby.
        self._tick('sync', {}, at=5)
        self.assertEqual('ANY 1 (*)', self._written())
        self._tick('sync', {}, at=19)
        self.assertEqual('ANY 1 (*)', self._written())
        self._tick('sync', {}, at=20)
        self.assertEqual('', self._written())
        # A standby back: at once.
        self._tick('sync', {'i2': {'state': 'streaming'}}, at=21)
        self.assertEqual('ANY 1 (*)', self._written())
        # And the next wait starts afresh.
        self._tick('sync', {}, at=30)
        self.assertEqual('ANY 1 (*)', self._written())
        self._tick('sync', {}, at=45)
        self.assertEqual('', self._written())

    def test_strict_always_waits(self):
        self._tick('strict', {}, at=0)
        self.assertEqual('ANY 1 (*)', self._written())
        self._tick('strict', {}, at=100)
        self.assertEqual('ANY 1 (*)', self._written())
        self.app.reload.assert_called_once()

    def test_async_never_waits(self):
        self.app.get_config_param.return_value = 'ANY 1 (*)'
        self._tick('async', {'i2': {'state': 'streaming'}})
        self.assertEqual('', self._written())

    def test_a_standby_does_nothing(self):
        self.app.keep_cluster_mode('strict')
        self.app.reconcile(cluster_probe.MemberView('standby', 'STREAMING',
                                                    False, False, True))
        self.assertIsNone(self._written())
        self.app.get_config_param.assert_not_called()


class ManagerMixinTest(trove_testtools.TestCase):

    def setUp(self):
        super(ManagerMixinTest, self).setUp()
        self.patch_datastore_manager('postgresql')
        pg_manager.PostgresManager._docker_client = mock.MagicMock()
        mock.patch.object(cluster_probe.ClusterProbe, 'start').start()
        self.addCleanup(mock.patch.stopall)
        self.manager = pg_manager.PostgresManager()
        self.manager.app = mock.MagicMock()
        self.manager.status = mock.MagicMock()
        self.manager.cluster_probe = mock.MagicMock()

    def test_the_manager_is_a_cluster_member(self):
        self.assertIsInstance(pg_manager.PostgresManager(),
                              cluster.PostgresqlMemberMixin)
        self.assertEqual('replication_mode',
                         pg_manager.PostgresManager.CLUSTER_MODE_KEY)
        self.assertIsInstance(pg_manager.PostgresManager().app,
                              cluster.PgSqlClusterApp)
        self.assertEqual(5432, cluster.PgSqlClusterApp.DATABASE_PORT)

    def test_cluster_complete(self):
        calls = mock.Mock()
        calls.attach_mock(self.manager.app, 'app')
        calls.attach_mock(self.manager.status, 'status')
        calls.attach_mock(self.manager.cluster_probe, 'probe')
        self.manager.cluster_complete(None)
        self.assertEqual([mock.call.app.complete_cluster(),
                          mock.call.status.end_install(),
                          mock.call.probe.enable_complete()],
                         [c for c in calls.mock_calls
                          if '__bool__' not in str(c)])

    def test_install_cluster(self):
        self.manager.install_cluster(None, {'members': {}})
        self.manager.app.install_cluster.assert_called_once_with(
            {'members': {}})
        self.manager.cluster_probe.enable_member.assert_called_once()
        self.manager.app.install_cluster.side_effect = (
            exception.TroveError('no'))
        self.assertRaises(exception.TroveError, self.manager.install_cluster,
                          None, {})
        self.manager.status.set_status.assert_called_once_with(
            service_status.ServiceStatuses.FAILED)

    def test_the_slot_and_member_calls(self):
        self.manager.create_member_slot(None, 'i3')
        self.manager.drop_member_slot(None, 'i3')
        self.manager.update_cluster_members(None, {'i1': '10.0.0.1'},
                                            'async')
        self.manager.app.create_member_slot.assert_called_once_with('i3')
        self.manager.app.drop_member_slot.assert_called_once_with('i3')
        self.manager.app.update_cluster_members.assert_called_once_with(
            {'i1': '10.0.0.1'}, 'async')

    def test_prepare_keeps_the_mode(self):
        with mock.patch.object(pg_manager.PostgresManager,
                               '_apply_access_rules'), \
                mock.patch.object(pg_manager.operating_system,
                                  'ensure_directory'):
            self.manager.do_prepare(
                'ctx', [], [], 512, [], None, None, None, None, None, None,
                {'id': 'i1', 'replication_mode': 'async'}, None)
        self.manager.app.keep_cluster_mode.assert_called_once_with('async')
