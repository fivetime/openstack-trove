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
        self.assertTrue(self.app.remember_sync_set('i1', ['i3', 'i2']))
        self.assertEqual({'primary_id': 'i1', 'members': ['i2', 'i3'],
                          'lsn': None, 'alone': False},
                         self.app._read_cluster()['sync_set'])
        # Only where the primary is changed: not worth a line in the log.
        self.assertFalse(self.app.remember_sync_set('i1', ['i2', 'i3'],
                                                    lsn=5))
        self.assertEqual(5, self.app._read_cluster()['sync_set']['lsn'])
        # The primary goes on alone: the set it streamed to stands.
        self.assertTrue(self.app.remember_sync_set('i1', None, alone=True))
        self.assertEqual({'primary_id': 'i1', 'members': ['i2', 'i3'],
                          'lsn': None, 'alone': True},
                         self.app._read_cluster()['sync_set'])
        self.assertFalse(self.app.remember_sync_set('i1', None, alone=True))
        # Another primary, its set not known.
        self.assertTrue(self.app.remember_sync_set('i9', None))
        self.assertEqual({'primary_id': 'i9', 'members': None,
                          'lsn': None, 'alone': False},
                         self.app._read_cluster()['sync_set'])
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

    def test_the_time_without_a_standby_counts_in_every_mode(self):
        # An async primary cut off from its standbys has to report it as
        # well: the task manager gives it up on that.
        self._tick('async', {}, at=10)
        self.assertEqual(10, self.app._no_standby_since)
        self._tick('async', {'i2': {'state': 'streaming'}}, at=20)
        self.assertIsNone(self.app._no_standby_since)
        self._tick('strict', {'i2': {'state': 'catchup'}}, at=30)
        self.assertEqual(30, self.app._no_standby_since)

    def test_a_standby_does_nothing(self):
        self.app.keep_cluster_mode('strict')
        self.app.reconcile(cluster_probe.MemberView('standby', 'STREAMING',
                                                    False, False, True))
        self.assertIsNone(self._written())
        self.app.get_config_param.assert_not_called()


MEMBERS = {'i1': '10.0.0.1', 'i2': '10.0.0.2', 'i3': '10.0.0.3'}


class ClusterSignalTest(ClusterAppTestBase):
    """What the probe tells the control plane of this member."""

    def setUp(self):
        super(ClusterSignalTest, self).setUp()
        self.app._update_cluster(members=MEMBERS, mode='sync', self_id='i2',
                                 primary_id='i2')
        self.now = 1000.0
        mock.patch.object(cluster.time, 'monotonic',
                          side_effect=lambda: self.now).start()
        self.primary = cluster_probe.MemberView('primary', 'PRIMARY', True,
                                                False)
        self.detached = cluster_probe.MemberView('standby', 'DETACHED',
                                                 False, False)

    def test_a_primary_with_a_standby_or_alone_in_a_cluster_of_one(self):
        self.assertEqual(cluster_probe.NORMAL,
                         self.app.cluster_signal(self.primary))
        self.app._no_standby_since = 990.0
        self.app._update_cluster(members={'i2': '10.0.0.2'})
        self.assertEqual(cluster_probe.NORMAL,
                         self.app.cluster_signal(self.primary))

    def test_a_primary_alone_asks_for_its_lease(self):
        self.app._no_standby_since = 990.0
        signal = self.app.cluster_signal(self.primary)
        self.assertEqual((True, 'lease_request'),
                         (signal.abnormal, signal.kind))
        self.assertEqual('no standby streams for 10 seconds; the lease '
                         'runs 0 more seconds', signal.reason)
        self.assertEqual({'no_standby_for': 10.0, 'lease_left': 0,
                          'lease_ttl': 0, 'writable': True}, signal.extra)
        # Before a grant: every third of the fence grace.
        self.assertEqual(5.0, signal.resend_after)
        # Granted 60 s: again when a third of it is left.
        self.app.renew_lease(60)
        self.now = 1010.0
        signal = self.app.cluster_signal(self.primary)
        self.assertEqual(50.0, signal.extra['lease_left'])
        self.assertEqual(30.0, signal.resend_after)
        self.assertEqual(60, signal.extra['lease_ttl'])

    def test_a_strict_primary_alone_cannot_be_leased(self):
        self.app._update_cluster(mode='strict')
        self.app._no_standby_since = 990.0
        signal = self.app.cluster_signal(self.primary)
        self.assertEqual((True, 'abnormal'), (signal.abnormal, signal.kind))
        self.assertIn('strict mode', signal.reason)

    def test_a_primary_the_cluster_left_fenced_rebuilding_stopped(self):
        self.app._update_cluster(primary_id='i1')
        self.assertIn("cluster's primary is i1",
                      self.app.cluster_signal(self.primary).reason)
        self.app._update_cluster(primary_id='i2')
        self.app.fenced = True
        self.assertEqual('fenced', self.app.cluster_signal(self.primary)
                         .reason)
        self.app.fenced = False
        self.app.rebuilding = True
        self.assertEqual('rebuilding', self.app.cluster_signal(self.primary)
                         .reason)
        self.app.rebuilding = False
        stopped = cluster_probe.MemberView('stopped', None, False, False)
        self.assertEqual('the server does not answer',
                         self.app.cluster_signal(stopped).reason)

    def test_a_standby(self):
        streaming = cluster_probe.MemberView('standby', 'STREAMING', False,
                                             False, True)
        self.assertEqual(cluster_probe.NORMAL,
                         self.app.cluster_signal(streaming))
        self.app._detached_since = 970.0
        self.app._update_cluster(primary_id='i1')
        signal = self.app.cluster_signal(self.detached)
        self.assertEqual((True, 'abnormal'), (signal.abnormal, signal.kind))
        self.assertEqual('not streaming for 30 seconds', signal.reason)
        self.assertEqual({'disconnected_for': 30.0, 'primary_id': 'i1'},
                         signal.extra)


class SelfFencingTest(ClusterAppTestBase):
    """A primary takes writes only while a standby streams from it or
    the task manager's lease says it is the primary.
    """

    def setUp(self):
        super(SelfFencingTest, self).setUp()
        self.app._update_cluster(members=MEMBERS, mode='sync', self_id='i2',
                                 primary_id='i2')

    def _primary_at(self, seconds, standbys=()):
        self.app.adm.query.side_effect = [[(False,)], list(standbys)]
        with mock.patch.object(cluster.time, 'monotonic',
                               return_value=seconds):
            return self.app.member_view()

    def test_a_primary_alone_waits_for_the_lease(self):
        # Up without a standby or a lease: closed, until the task manager
        # says it is the primary; then open for the lease, and for the
        # fence grace period after it.
        self.assertFalse(self._primary_at(0).writable)
        with mock.patch.object(cluster.time, 'monotonic', return_value=0):
            self.app.renew_lease(60)
        self.assertTrue(self._primary_at(1).writable)
        self.assertTrue(self._primary_at(50).writable)
        self.assertTrue(self._primary_at(60).writable)
        self.assertFalse(self._primary_at(70).writable)
        # A standby streaming says it as well.
        streaming = [('i3', 'streaming', 'quorum', 0)]
        self.assertTrue(self._primary_at(70, streaming).writable)
        self.assertTrue(self._primary_at(80).writable)
        self.assertFalse(self._primary_at(90).writable)

    def test_a_primary_the_cluster_left(self):
        # Its data is a primary's, but the cluster's primary is another
        # member now: no writes, whatever streams from it.
        self.app._update_cluster(primary_id='i1')
        with mock.patch.object(cluster.time, 'monotonic', return_value=0):
            self.app.renew_lease(60)
        self.assertFalse(
            self._primary_at(1, [('i3', 'streaming', 'quorum', 0)]).writable)
        self.app._update_cluster(primary_id='i2')
        self.assertTrue(
            self._primary_at(2, [('i3', 'streaming', 'quorum', 0)]).writable)

    def test_a_fenced_primary(self):
        self.app.fenced = True
        self.assertFalse(
            self._primary_at(0, [('i3', 'streaming', 'quorum', 0)]).writable)

    def test_a_cluster_of_one(self):
        self.app._update_cluster(members={'i2': '10.0.0.2'})
        self.assertTrue(self._primary_at(0).writable)

    def test_a_standby_counts_its_time_without_the_primary(self):
        with mock.patch.object(cluster.time, 'monotonic', return_value=100):
            self.app.adm.query.side_effect = [[(True,)], []]
            self.app.member_view()
        self.assertEqual(100, self.app._detached_since)
        with mock.patch.object(cluster.time, 'monotonic', return_value=130):
            self.app.adm.query.side_effect = [
                [(True,)], [('streaming', '10.0.0.1', 1.0, 0)]]
            self.app.member_view()
        self.assertIsNone(self.app._detached_since)


class FailoverTest(ClusterAppTestBase):

    def setUp(self):
        super(FailoverTest, self).setUp()
        self.app._update_cluster(members=MEMBERS, mode='sync', self_id='i2',
                                 primary_id='i1', replication_user=USER)
        mock.patch.object(cluster.operating_system, 'remove').start()
        self.app._self_ip = mock.Mock(return_value='10.0.0.2')
        self.queries = []

    def _answers(self, *by_prefix):
        """What the server answers, by the start of the query."""
        def query(sql):
            self.queries.append(sql)
            for prefix, rows in by_prefix:
                if sql.startswith(prefix):
                    return rows
            return []
        self.app.adm.query.side_effect = query
        # Statements without rows (CHECKPOINT) go through psql; query
        # would fail on them with "no results to fetch".
        self.app.adm.psql.side_effect = lambda sql: self.queries.append(sql)

    def test_fence(self):
        self._answers()
        self.app.renew_lease(60)
        with mock.patch.object(cluster.docker_util, 'run_command') as run:
            self.app.fence_member()
        self.assertIn('CHECKPOINT', self.queries)
        run.assert_called_once_with(
            self.app.docker_client,
            'pg_ctl stop -m fast -D %s' % self.app.datadir)
        self.assertEqual([('stop_db',)], self.app.calls)
        self.assertTrue(self.app.fenced)
        self.assertFalse(self.app.lease_fresh())
        # A server that does not answer stops all the same.
        self.app.adm.psql.side_effect = Exception('refused')
        with mock.patch.object(cluster.docker_util, 'run_command',
                               side_effect=Exception('no container')):
            self.app.fence_member()
        self.assertEqual([('stop_db',), ('stop_db',)], self.app.calls)

    def test_promote(self):
        self._answers(('SELECT pg_is_in_recovery', [(True,)]),
                      ('SELECT pg_promote', [(True,)]),
                      ('SELECT timeline_id', [(2,)]),
                      ('SELECT pg_wal_lsn_diff', [(2000,)]))
        self.app.fenced = True
        result = self.app.promote_member(MEMBERS, 'sync')
        self.assertEqual({'lsn': 2000, 'timeline': 2}, result)
        self.app.configuration_manager.remove_system_override \
            .assert_called_once_with(cluster.CNF_REPLICATION)
        self.assertEqual({'synchronous_standby_names': ''},
                         self._override(cluster.CNF_SYNC))
        self.assertEqual('i2', self.app.primary_id)
        self.assertIn('SELECT pg_promote(true, 60)', self.queries)
        # The slots before the checkpoint: a checkpoint that fails does
        # not leave the members that rejoin without their slots.
        self.assertLess(self.queries.index(
            "SELECT pg_create_physical_replication_slot('trove_i3')"),
            self.queries.index('CHECKPOINT'))
        self.assertIn("SELECT pg_create_physical_replication_slot('trove_i1')",
                      self.queries)
        self.assertIn("SELECT pg_create_physical_replication_slot('trove_i3')",
                      self.queries)
        self.assertNotIn("SELECT pg_create_physical_replication_slot("
                         "'trove_i2')", self.queries)
        self.assertEqual([('reload',)], self.app.calls)
        self.assertFalse(self.app.fenced)
        # A checkpoint that fails is logged, not fatal.
        self.app.adm.psql.side_effect = Exception('no')
        self.app.promote_member(MEMBERS, 'sync')
        # Strict: commits wait from the start. Already a primary: no
        # promotion.
        self.queries = []
        self._answers(('SELECT pg_is_in_recovery', [(False,)]),
                      ('SELECT timeline_id', [(2,)]),
                      ('SELECT pg_wal_lsn_diff', [(2000,)]))
        self.app.promote_member(MEMBERS, 'strict')
        self.assertEqual({'synchronous_standby_names': 'ANY 1 (*)'},
                         self._override(cluster.CNF_SYNC))
        self.assertNotIn('SELECT pg_promote(true, 60)', self.queries)

    def test_a_promotion_that_does_not_happen(self):
        self._answers(('SELECT pg_is_in_recovery', [(True,)]),
                      ('SELECT pg_promote', [(False,)]))
        self.assertRaises(exception.TroveError, self.app.promote_member,
                          MEMBERS, 'sync')

    def test_follow_twice_is_once(self):
        # Already streaming from that primary (two task managers watch
        # the cluster): nothing to change.
        self.app._update_cluster(primary_id='i1')
        self._answers(('SELECT pg_is_in_recovery', [(True,)]),
                      ('SELECT status', [('streaming', '10.0.0.1', 1.0, 0)]))
        self.app.follow_primary({'id': 'i1', 'host': '10.0.0.1'})
        self.app.configuration_manager.apply_system_override \
            .assert_not_called()
        self.app.reload.assert_not_called()

    def test_follow(self):
        self._answers(('SELECT pg_is_in_recovery', [(True,)]))
        self.app._detached_since = 5
        self.app.follow_primary({'id': 'i3', 'host': '10.0.0.3'})
        self.assertEqual(
            {'primary_conninfo': 'host=10.0.0.3 port=5432 user=replicator '
                                 'password=pw application_name=i2 '
                                 'dbname=postgres',
             'primary_slot_name': 'trove_i2'},
            self._override(cluster.CNF_REPLICATION))
        self.assertEqual('i3', self.app.primary_id)
        self.assertEqual([('reload',)], self.app.calls)
        self.assertIsNone(self.app._detached_since)
        # A primary cannot follow: it rejoins.
        self._answers(('SELECT pg_is_in_recovery', [(False,)]))
        self.assertRaises(exception.TroveError, self.app.follow_primary,
                          {'id': 'i3', 'host': '10.0.0.3'})

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_rejoin_by_a_rewind(self, remove, start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, b'Done!')
        self.app.adm.query.return_value = [('streaming',)]
        self.app.fenced = True
        self.app.rejoin_cluster({'id': 'i3', 'host': '10.0.0.3'})
        # The primary is waited for from the parked container (the
        # tenant NIC is the database container's); the stale pid file
        # goes; then pg_rewind runs in the parked container against the
        # new primary, with the server's own configuration file.
        commands = [c.args[0] for c in container.exec_run.call_args_list]
        self.assertEqual(['sh', 'pg_rewind'], [c[0] for c in commands])
        self.assertIn('pg_isready -h 10.0.0.3 -p 5432 -U replicator '
                      '-d postgres', commands[0][2])
        cluster.operating_system.remove.assert_called_once_with(
            self.app.datadir + '/postmaster.pid', force=True, as_root=True)
        self.assertEqual(
            ['pg_rewind', '--target-pgdata=%s' % self.app.datadir,
             '--source-server=host=10.0.0.3 port=5432 user=replicator '
             'dbname=postgres',
             '--config-file=/etc/postgresql/postgresql.conf', '--progress'],
            commands[1])
        self.assertEqual({'PGPASSWORD': 'pw'},
                         container.exec_run.call_args.kwargs['environment'])
        cluster.utils.execute_with_timeout.assert_not_called()
        self.assertEqual('10.0.0.3', self._override(
            cluster.CNF_REPLICATION)['primary_conninfo'].split()[0][5:])
        self.app.configuration_manager.remove_system_override \
            .assert_called_once_with(cluster.CNF_SYNC)
        self.assertEqual('i3', self.app.primary_id)
        cluster.operating_system.execute_shell_cmd.assert_called_once_with(
            'touch %s/standby.signal' % self.app.datadir, [], shell=True,
            as_root=True)
        self.assertEqual(['stop_db', 'start_db'],
                         [c[0] for c in self.app.calls])
        self.assertFalse(self.app.rebuilding)
        self.assertFalse(self.app.fenced)

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_a_rejoin_without_the_primary_leaves_the_member_stopped(
            self, remove, start):
        # The member's own link may still be coming back: when the
        # primary is not reachable in time, the data is left alone, the
        # member stays stopped and the cluster file names the primary,
        # so that the task manager tells it again.
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.return_value = (1, b'not reachable')
        self.assertRaises(exception.TroveError, self.app.rejoin_cluster,
                          {'id': 'i3', 'host': '10.0.0.3'})
        self.assertEqual([('stop_db',)], self.app.calls)
        self.assertEqual(1, container.exec_run.call_count)
        self.assertEqual('i3', self.app.primary_id)
        self.assertFalse(self.app.rebuilding)

    def test_a_rejoin_under_way_is_not_started_twice(self):
        self.app.rebuilding = True
        self.app.rejoin_cluster({'id': 'i3', 'host': '10.0.0.3'})
        self.assertEqual([], self.app.calls)

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_rejoin_by_a_base_backup_when_the_rewind_fails(self, remove,
                                                           start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.side_effect = [
            (0, b''), (1, b'could not find common ancestor'), (0, b'')]
        self.app.adm.query.return_value = [('streaming',)]
        self.app.rejoin_cluster({'id': 'i3', 'host': '10.0.0.3'})
        commands = [c.args[0][0] for c in container.exec_run.call_args_list]
        self.assertEqual(['sh', 'pg_rewind', 'pg_basebackup'], commands)
        self.assertEqual('10.0.0.3',
                         container.exec_run.call_args.args[0][2])
        cluster.utils.execute_with_timeout.assert_any_call(
            'find', self.app.datadir, '-mindepth', '1', '-delete',
            run_as_root=True, root_helper='sudo', timeout=600)
        self.assertEqual(['stop_db', 'start_db'],
                         [c[0] for c in self.app.calls])

    @mock.patch.object(cluster.docker_util, 'start_container')
    @mock.patch.object(cluster.docker_util, 'remove_container')
    def test_a_rejoin_that_fails_leaves_the_member_down(self, remove,
                                                        start):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.side_effect = [(0, b''), (1, b'no'), (1, b'no')]
        self.assertRaises(exception.TroveError, self.app.rejoin_cluster,
                          {'id': 'i3', 'host': '10.0.0.3'})
        self.assertFalse(self.app.rebuilding)
        self.assertNotIn('start_db', [c[0] for c in self.app.calls])

    def test_the_recovery_view_of_a_primary(self):
        self.app._update_cluster(primary_id='i2')
        self._answers(('SELECT pg_is_in_recovery', [(False,)]),
                      ('SELECT application_name',
                       [('i3', 'streaming', 'quorum', 0)]),
                      ('SELECT timeline_id', [(1,)]),
                      ('SELECT pg_wal_lsn_diff', [(1000,)]))
        with mock.patch.object(cluster.time, 'monotonic', return_value=50):
            self.app.renew_lease(60)
            view = self.app.recovery_view()
        self.assertEqual(
            {'ip': '10.0.0.2', 'state': 'primary', 'role': 'PRIMARY',
             'mode': 'sync', 'self_id': 'i2', 'primary_id': 'i2',
             'sync_set': None, 'writable': True, 'fenced': False,
             'lease_fresh': True,
             'standbys': {'i3': {'state': 'streaming',
                                 'sync_state': 'quorum', 'lag_bytes': 0}},
             'lsn': 1000, 'timeline': 1, 'no_standby_for': None}, view)

    def test_the_recovery_view_of_a_primary_alone_probes_the_peers(self):
        self.app._update_cluster(primary_id='i2')
        self._answers(('SELECT pg_is_in_recovery', [(False,)]),
                      ('SELECT application_name', []),
                      ('SELECT timeline_id', [(1,)]),
                      ('SELECT pg_wal_lsn_diff', [(1000,)]))
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.side_effect = lambda command: (2, b'')
        self.app._no_standby_since = 1.0
        with mock.patch.object(cluster.time, 'monotonic', return_value=50):
            view = self.app.recovery_view()
        self.assertEqual({'i1': False, 'i3': False}, view['peers'])
        self.assertEqual(49, view['no_standby_for'])

    def test_the_recovery_view_of_a_standby(self):
        # Not streaming: with which members it reaches on the tenant
        # network, probed from the database container.
        self._answers(('SELECT pg_is_in_recovery', [(True,)]),
                      ('SELECT status', []),
                      ('SELECT timeline_id', [(1,)]),
                      ('SELECT pg_wal_lsn_diff', [(900,)]))
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.side_effect = lambda command: (
            (2, b'no response') if '10.0.0.1' in command else (1, b''))
        with mock.patch.object(cluster.time, 'monotonic',
                               side_effect=[100, 130, 130]):
            view = self.app.recovery_view()
        self.assertEqual(
            {'ip': '10.0.0.2', 'state': 'standby', 'role': 'DETACHED',
             'mode': 'sync', 'self_id': 'i2', 'primary_id': 'i1',
             'sync_set': None,
             'writable': False, 'fenced': False, 'lease_fresh': False,
             'receiver': None, 'lsn': 900, 'replay_lsn': 900,
             'timeline': 1, 'disconnected_for': 30,
             'peers': {'i1': False, 'i3': True}}, view)
        self.app.docker_client.containers.get.assert_called_with('database')
        self.assertEqual(
            sorted([['pg_isready', '-h', '10.0.0.1', '-p', '5432', '-t', '1',
                     '-U', 'replicator'],
                    ['pg_isready', '-h', '10.0.0.3', '-p', '5432', '-t', '1',
                     '-U', 'replicator']]),
            sorted(c.args[0] for c in container.exec_run.call_args_list))
        # Streaming: no probe.
        self._answers(('SELECT pg_is_in_recovery', [(True,)]),
                      ('SELECT status', [('streaming', '10.0.0.1', 1.0, 0)]),
                      ('SELECT timeline_id', [(1,)]),
                      ('SELECT pg_wal_lsn_diff', [(900,)]))
        container.exec_run.reset_mock()
        self.assertNotIn('peers', self.app.recovery_view())
        container.exec_run.assert_not_called()

    def test_a_probe_that_fails_says_nothing(self):
        container = self.app.docker_client.containers.get.return_value
        container.exec_run.side_effect = lambda command: (
            (0, b'') if '10.0.0.3' in command else Exception('no exec'))
        self.assertEqual({'i1': None, 'i3': True},
                         self.app._reach_peers(MEMBERS))

    def test_the_recovery_view_of_a_stopped_member(self):
        self.app.adm.query.side_effect = Exception('refused')
        view = self.app.recovery_view()
        self.assertEqual(('stopped', 'i1', False), (
            view['state'], view['primary_id'], view['writable']))
        self.assertNotIn('lsn', view)


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
            {'i1': '10.0.0.1'}, 'async', primary_id=None)
        self.manager.update_cluster_members(None, {'i1': '10.0.0.1'},
                                            'async', primary_id='i1')
        self.manager.app.update_cluster_members.assert_called_with(
            {'i1': '10.0.0.1'}, 'async', primary_id='i1')

    def test_the_failover_calls(self):
        self.manager.app.promote_member.return_value = {'lsn': 1}
        self.manager.renew_lease(None, 60)
        self.manager.remember_sync_set(None, 'i1', ['i2'], lsn=7)
        self.manager.app.remember_sync_set.assert_called_once_with(
            'i1', ['i2'], lsn=7, alone=False)
        self.manager.remember_sync_set(None, 'i1', None, alone=True)
        self.manager.app.remember_sync_set.assert_called_with(
            'i1', None, lsn=None, alone=True)
        self.manager.fence_member(None)
        self.assertEqual({'lsn': 1}, self.manager.promote_member(
            None, {'i1': '10.0.0.1'}, 'sync'))
        self.manager.follow_primary(None, {'id': 'i1', 'host': '10.0.0.1'})
        self.manager.rejoin_cluster(None, {'id': 'i1', 'host': '10.0.0.1'})
        self.manager.app.renew_lease.assert_called_once_with(60)
        self.manager.app.fence_member.assert_called_once_with()
        self.manager.app.promote_member.assert_called_once_with(
            {'i1': '10.0.0.1'}, 'sync')
        self.manager.app.follow_primary.assert_called_once_with(
            {'id': 'i1', 'host': '10.0.0.1'})
        self.manager.app.rejoin_cluster.assert_called_once_with(
            {'id': 'i1', 'host': '10.0.0.1'})
        self.manager.app.rejoin_cluster.side_effect = (
            exception.TroveError('no'))
        self.assertRaises(exception.TroveError, self.manager.rejoin_cluster,
                          None, {'id': 'i1', 'host': '10.0.0.1'})

    def test_prepare_keeps_the_mode(self):
        with mock.patch.object(pg_manager.PostgresManager,
                               '_apply_access_rules'), \
                mock.patch.object(pg_manager.operating_system,
                                  'ensure_directory'):
            self.manager.do_prepare(
                'ctx', [], [], 512, [], None, None, None, None, None, None,
                {'id': 'i1', 'replication_mode': 'async'}, None)
        self.manager.app.keep_cluster_mode.assert_called_once_with('async')
