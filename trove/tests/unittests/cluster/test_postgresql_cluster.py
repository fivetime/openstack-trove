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

from trove.cluster import views as cluster_views
from trove.common import cfg
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.common.strategies.cluster.experimental.common import (
    taskmanager as common_taskmanager)
from trove.common.strategies.cluster.experimental.postgresql import (
    api as pg_api)
from trove.common.strategies.cluster.experimental.postgresql import (
    guestagent as pg_guestagent)
from trove.common.strategies.cluster.experimental.postgresql import (
    recovery as pg_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    taskmanager as pg_taskmanager)
from trove.common.strategies.cluster import strategy
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class PostgresqlConfTest(trove_testtools.TestCase):

    def test_the_cluster_options(self):
        conf = CONF.postgresql
        self.assertTrue(conf.cluster_support)
        self.assertEqual(3, conf.min_cluster_member_count)
        self.assertEqual([5433, 5434], sorted(
            port for ports in conf.cluster_tcp_ports for port in ports))
        self.assertEqual((5433, 5434, 5432, 5433, 30),
                         (conf.cluster_ready_port, conf.cluster_read_port,
                          conf.cluster_load_balancer_port,
                          conf.cluster_read_load_balancer_port,
                          conf.cluster_recovery_grace))
        self.assertEqual('trove.extensions.common.service.'
                         'ClusterWriterRootController',
                         conf.root_controller)
        self.assertEqual((15, 10, '1GB', 600, 15, 0, 1024 * 1024),
                         (conf.cluster_sync_fallback_grace,
                          conf.cluster_wal_timeout,
                          conf.cluster_max_slot_wal_keep_size,
                          conf.cluster_standby_rebuild_after,
                          conf.cluster_fence_grace,
                          conf.cluster_failover_force_after,
                          conf.cluster_failover_max_lag_bytes))
        # Galera and Group Replication have no read port.
        self.assertEqual(0, CONF.pxc.cluster_read_port)
        self.assertEqual(0, CONF.mysql.cluster_read_load_balancer_port)

    def test_the_strategies_load(self):
        self.assertIsInstance(strategy.load_api_strategy('postgresql'),
                              pg_api.PostgresqlAPIStrategy)
        self.assertIs(pg_taskmanager.PostgresqlClusterTasks,
                      strategy.load_taskmanager_strategy(
                          'postgresql').task_manager_cluster_tasks_class)
        self.assertIs(pg_guestagent.PostgresqlGuestAgentAPI,
                      strategy.load_guestagent_strategy(
                          'postgresql').guest_client_class)


class PostgresqlAPITest(trove_testtools.TestCase):

    def test_the_mode(self):
        cluster = pg_api.PostgresqlCluster
        self.assertEqual({'replication_mode': 'sync'},
                         cluster._validate_mode(None))
        self.assertEqual({'replication_mode': 'strict'},
                         cluster._validate_mode({'replication_mode':
                                                 'strict'}))
        self.assertRaises(exception.BadRequest, cluster._validate_mode,
                          {'replication_mode': 'eventual'})

    def test_the_read_grace(self):
        cluster = pg_api.PostgresqlCluster
        # As the client sends it, a string; kept as a number.
        self.assertEqual({'replication_mode': 'sync',
                          'replica_read_grace': 60},
                         cluster._validate_mode({'replica_read_grace': '60'}))
        for wrong in ('-1', '3601', 'soon', ''):
            self.assertRaises(exception.BadRequest, cluster._validate_mode,
                              {'replica_read_grace': wrong})
        self.assertEqual({'replica_read_grace': 0},
                         cluster._kept_properties(
                             {'replica_read_grace': 0,
                              'replication_mode': 'sync'}))
        # None asked: none kept, the standbys serve as they did.
        self.assertEqual({}, cluster._kept_properties(
            cluster._validate_mode(None)))
        self.assertEqual(
            {'id': 'c1', 'instance_type': 'member',
             'replication_mode': 'async'},
            cluster._member_config(mock.Mock(id='c1'),
                                   {'replication_mode': 'async'}))

    @mock.patch.object(pg_api.DBInstance, 'find_all')
    @mock.patch.object(common_api.HomogeneousCluster, 'shrink')
    @mock.patch.object(pg_api.clients, 'create_guest_client')
    def test_shrink_the_primary_needs_a_standby_that_stays(
            self, create, shrink, find_all):
        cluster = pg_api.PostgresqlCluster.__new__(pg_api.PostgresqlCluster)
        cluster.context = 'ctx'
        cluster.ds_version = mock.Mock(manager='postgresql')
        cluster.db_info = mock.Mock(id='c1')
        find_all.return_value.all.return_value = [
            mock.Mock(id=i) for i in ('i1', 'i2', 'i3')]
        roles = {'i1': {'state': 'primary', 'role': 'PRIMARY'},
                 'i2': {'state': 'standby', 'role': 'STREAMING'},
                 'i3': exception.GuestTimeout()}

        def guest(context, instance_id, manager):
            g = mock.Mock()
            if isinstance(roles[instance_id], Exception):
                g.get_member_role.side_effect = roles[instance_id]
            else:
                g.get_member_role.return_value = roles[instance_id]
            return g
        create.side_effect = guest
        # The primary with the streaming standby: nobody left to take
        # over.
        self.assertRaises(exception.BadRequest, cluster.shrink, ['i2', 'i1'])
        shrink.assert_not_called()
        # The primary alone: i2 takes over (the task manager switches
        # over first).
        cluster.shrink(['i1'])
        shrink.assert_called_once_with(['i1'])
        # A standby, and a member that does not answer, go.
        cluster.shrink(['i2', 'i3'])
        shrink.assert_called_with(['i2', 'i3'])


class PostgresqlViewTest(trove_testtools.TestCase):

    def _view(self, roles, load_servers=True):
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'postgresql'
        cluster.instances = [mock.Mock(id=i, status='HEALTHY')
                             for i in roles]

        def guest(instance):
            g = mock.Mock()
            answer = roles[instance.id]
            if isinstance(answer, Exception):
                g.get_member_role.side_effect = answer
            else:
                g.get_member_role.return_value = answer
            return g
        cluster.get_guest = guest
        return pg_api.PostgresqlClusterView(cluster,
                                            load_servers=load_servers)

    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_roles_as_the_primary_sees_the_standbys(self, build):
        roles = {
            'i1': {'state': 'primary', 'role': 'PRIMARY', 'mode': 'sync',
                   'standbys': {'i2': {'state': 'streaming',
                                       'sync_state': 'quorum'},
                                'i3': {'state': 'streaming',
                                       'sync_state': 'async'}}},
            'i2': {'state': 'standby', 'role': 'STREAMING', 'mode': 'sync'},
            'i3': {'state': 'standby', 'role': 'STREAMING', 'mode': 'sync'},
            'i4': {'state': 'standby', 'role': 'DETACHED', 'mode': 'sync'},
            'i5': {'state': 'rebuilding', 'role': None, 'mode': 'sync'},
            'i6': {'state': 'stopped', 'role': None, 'mode': 'sync'},
            'i7': exception.GuestTimeout()}
        build.return_value = ([{'id': i} for i in roles], [])
        instances, _ips = self._view(roles).build_instances()
        self.assertEqual(['primary', 'sync-standby', 'standby',
                          'disconnected', 'recovering', 'offline',
                          'unknown'],
                         [i['role'] for i in instances])

    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_a_streaming_standby_without_a_primary_answering(self, build):
        roles = {'i1': exception.GuestTimeout(),
                 'i2': {'state': 'standby', 'role': 'STREAMING',
                        'mode': 'async'}}
        build.return_value = ([{'id': i} for i in roles], [])
        instances, _ips = self._view(roles).build_instances()
        self.assertEqual(['unknown', 'standby'],
                         [i['role'] for i in instances])

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(common_api.loadbalancer, 'OctaviaClient')
    @mock.patch.object(common_api.loadbalancer, 'find_endpoint')
    def test_the_endpoints_and_the_mode(self, find_endpoint, client, data):
        find_endpoint.return_value = {'address': '10.0.0.50', 'port': 5432,
                                      'status': 'ONLINE'}
        view = self._view({'i1': {'state': 'primary', 'role': 'PRIMARY',
                                  'mode': 'strict', 'standbys': {}}})
        # The roles, and the mode with them, come with the instances.
        view._member_roles(view.cluster.instances)
        result = view.data()['cluster']
        self.assertEqual({'address': '10.0.0.50', 'port': 5432,
                          'status': 'ONLINE'}, result['endpoint'])
        self.assertEqual({'address': '10.0.0.50', 'port': 5433,
                          'status': 'ONLINE'}, result['read_endpoint'])
        self.assertEqual('strict', result['replication_mode'])
        # One lookup: the two endpoints are one load balancer.
        find_endpoint.assert_called_once_with(
            client.return_value, 'trove-cluster-c1', 5432)

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(common_api.loadbalancer, 'find_endpoint')
    def test_a_list_asks_nothing(self, find_endpoint, data):
        view = self._view({'i1': {'state': 'primary'}}, load_servers=False)
        result = view.data()['cluster']
        self.assertNotIn('endpoint', result)
        self.assertNotIn('read_endpoint', result)
        self.assertNotIn('replication_mode', result)
        find_endpoint.assert_not_called()


class PostgresqlGuestAPITest(trove_testtools.TestCase):

    def test_the_calls(self):
        api = pg_guestagent.PostgresqlGuestAgentAPI.__new__(
            pg_guestagent.PostgresqlGuestAgentAPI)
        api.agent_low_timeout = 15
        api.agent_high_timeout = 600
        with mock.patch.object(api, '_call') as call, \
                mock.patch.object(pg_guestagent.CONF, 'cluster_usage_timeout',
                                  3600):
            api.install_cluster({'members': {}})
            api.create_member_slot('i2')
            api.drop_member_slot('i2')
            api.update_cluster_members({'i1': '10.0.0.1'}, 'sync')
            api.get_member_role()
            api.renew_lease(60)
            api.fence_member()
            api.promote_member({'i1': '10.0.0.1'}, 'sync')
            api.follow_primary({'id': 'i1', 'host': '10.0.0.1'})
        with mock.patch.object(api, '_cast') as cast:
            api.rejoin_cluster({'id': 'i1', 'host': '10.0.0.1'})
            api.fence_member(wait=False)
        self.assertEqual(
            [mock.call('install_cluster', 3600, version='1.0',
                       cluster_config={'members': {}}),
             mock.call('create_member_slot', 600, version='1.0',
                       member_id='i2'),
             mock.call('drop_member_slot', 600, version='1.0',
                       member_id='i2'),
             mock.call('update_cluster_members', 600, version='1.0',
                       members={'i1': '10.0.0.1'}, mode='sync',
                       primary_id=None),
             mock.call('get_member_role', 15, version='1.0'),
             mock.call('renew_lease', 15, version='1.0', ttl=60),
             mock.call('fence_member', 15, version='1.0'),
             mock.call('promote_member', 600, version='1.0',
                       members={'i1': '10.0.0.1'}, mode='sync'),
             mock.call('follow_primary', 600, version='1.0',
                       primary={'id': 'i1', 'host': '10.0.0.1'})],
            call.call_args_list)
        self.assertEqual(
            [mock.call('rejoin_cluster', version='1.0',
                       primary={'id': 'i1', 'host': '10.0.0.1'}),
             mock.call('fence_member', version='1.0')],
            cast.call_args_list)


class PostgresqlTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(PostgresqlTasksTest, self).setUp()
        self.tasks = pg_taskmanager.PostgresqlClusterTasks.__new__(
            pg_taskmanager.PostgresqlClusterTasks)
        self.tasks._datastore_version = mock.Mock()
        self.tasks.ds_version = mock.Mock(manager='postgresql')
        # The cluster's kept properties.
        self.props = {}
        self.tasks.db_info = mock.Mock(
            get_properties=lambda: dict(self.props))
        self.calls = []
        self.guests = {}
        self.roles = {'i1': 'primary', 'i2': 'standby', 'i3': 'standby'}

        def guest(instance):
            if instance.id not in self.guests:
                g = mock.Mock()
                for name in ('install_cluster', 'cluster_complete',
                             'leave_cluster', 'create_member_slot',
                             'drop_member_slot', 'update_cluster_members',
                             'fence_member', 'follow_primary'):
                    getattr(g, name).side_effect = (
                        lambda *a, _n=name, _i=instance.id, **k:
                        self.calls.append((_n, _i) + a + (
                            (k,) if k.get('primary_id') else ())))
                g.promote_member.side_effect = (
                    lambda members, mode, _i=instance.id:
                    self.calls.append(('promote_member', _i, members, mode))
                    or {'lsn': 2000, 'timeline': 2})
                g.get_recovery_view.side_effect = lambda _i=instance.id: {
                    'ip': '10.0.0.%s' % _i[-1], 'state': 'standby',
                    'timeline': 1, 'lsn': {'i2': 1500, 'i3': 1200}.get(_i),
                    'receiver': {'status': 'streaming'}}
                g.get_cluster_context.return_value = {
                    'mode': 'strict',
                    'replication_user': {'name': 'replicator',
                                         'password': 'pw'}}
                g.get_member_role.side_effect = lambda _i=instance.id: {
                    'state': self.roles.get(_i, 'standby')}
                self.guests[instance.id] = g
            return self.guests[instance.id]
        self.tasks.get_guest = guest
        # i1 has the lowest address.
        self.tasks.get_ip = lambda instance: '10.0.0.%s' % instance.id[-1]
        self.tasks._all_instances_ready = mock.Mock(return_value=True)
        self.tasks._check_cluster_for_root = mock.Mock()
        self.tasks.reset_task = mock.Mock()
        self.tasks.update_statuses_on_failure = mock.Mock()
        self.tasks._fail_new_members = mock.Mock(
            side_effect=lambda ids: self.calls.append(('fail', tuple(ids))))
        self.tasks._load_balancer_enabled = mock.Mock(return_value=True)
        self.tasks._sync_load_balancer = mock.Mock(
            side_effect=lambda ctx, cid, instances: self.calls.append(
                ('sync', tuple(i.id for i in instances))))
        mock.patch.object(pg_taskmanager.utils, 'generate_random_password',
                          return_value='pw').start()
        mock.patch.object(pg_taskmanager.utils, 'poll_until').start()
        # The members that leave a shrink, loaded with their servers, but
        # one whose server is gone (self.gone).
        self.gone = set()
        mock.patch.object(
            pg_taskmanager, 'load_any_instance', create=True,
            side_effect=lambda ctx, i: mock.Mock(
                id=i, server=None if i in self.gone else mock.Mock())
        ).start()
        self.addCleanup(mock.patch.stopall)

    def _instances(self, *ids):
        return [mock.Mock(id=i) for i in ids]

    def _config(self, self_id, primary, mode='strict', members=None,
                read_grace=0):
        return {'members': members or {'i1': '10.0.0.1', 'i2': '10.0.0.2',
                                       'i3': '10.0.0.3'},
                'mode': mode,
                'replication_user': {'name': 'replicator', 'password': 'pw'},
                'self_id': self_id, 'primary': primary,
                'read_grace': read_grace}

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_every_member_gets_the_read_grace(self, db_instance, instance):
        self.props = {'replica_read_grace': 60}
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i3', 'i1', 'i2'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.create_cluster('ctx', 'c1')
        installed = [c[2] for c in self.calls if c[0] == 'install_cluster']
        self.assertEqual(3, len(installed))
        self.assertEqual({60}, {c['read_grace'] for c in installed})

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_create(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i3', 'i1', 'i2'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.create_cluster('ctx', 'c1')
        primary = {'id': 'i1', 'host': '10.0.0.1'}
        # The lowest address is the primary and goes first; each standby
        # gets its slot before its base backup; then every member is
        # complete and the endpoints come last.
        self.assertEqual(
            [('install_cluster', 'i1', self._config('i1', None)),
             ('create_member_slot', 'i1', 'i3'),
             ('install_cluster', 'i3', self._config('i3', primary)),
             ('create_member_slot', 'i1', 'i2'),
             ('install_cluster', 'i2', self._config('i2', primary)),
             ('cluster_complete', 'i3'), ('cluster_complete', 'i1'),
             ('cluster_complete', 'i2'),
             ('sync', ('i3', 'i1', 'i2'))],
            self.calls)
        self.tasks.reset_task.assert_called_once()
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_create_fails_without_its_endpoint(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks._sync_load_balancer.side_effect = Exception('octavia')
        self.tasks.create_cluster('ctx', 'c1')
        self.tasks.update_statuses_on_failure.assert_called_once_with('c1')

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_grow(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3', 'i4'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.grow_cluster('ctx', 'c1', ['i4'])
        members = {'i1': '10.0.0.1', 'i2': '10.0.0.2', 'i3': '10.0.0.3',
                   'i4': '10.0.0.4'}
        self.assertEqual(
            [('create_member_slot', 'i1', 'i4'),
             ('install_cluster', 'i4', self._config(
                 'i4', {'id': 'i1', 'host': '10.0.0.1'}, members=members)),
             ('update_cluster_members', 'i1', members, 'strict'),
             ('update_cluster_members', 'i2', members, 'strict'),
             ('update_cluster_members', 'i3', members, 'strict'),
             ('update_cluster_members', 'i4', members, 'strict'),
             ('cluster_complete', 'i4'),
             ('sync', ('i1', 'i2', 'i3', 'i4'))],
            self.calls)
        self.tasks._check_cluster_for_root.assert_called_once()
        self.tasks.reset_task.assert_called_once()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_a_failed_grow_leaves_the_cluster_as_it_was(self, db_instance,
                                                        instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3', 'i4'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.guests['i4'] = mock.Mock()
        self.guests['i4'].install_cluster.side_effect = Exception('backup')
        self.tasks.grow_cluster('ctx', 'c1', ['i4'])
        self.assertEqual(('fail', ('i4',)), self.calls[-1])
        self.assertNotIn(('update_cluster_members', 'i1'),
                         [c[:2] for c in self.calls])
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_shrink_a_standby(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.shrink_cluster('ctx', 'c1', ['i3'])
        members = {'i1': '10.0.0.1', 'i2': '10.0.0.2'}
        self.assertEqual(
            [('leave_cluster', 'i3'), ('drop_member_slot', 'i1', 'i3'),
             ('update_cluster_members', 'i1', members, 'strict'),
             ('update_cluster_members', 'i2', members, 'strict'),
             ('sync', ('i1', 'i2'))],
            self.calls)
        instance.delete.assert_called_once()
        self.tasks.reset_task.assert_called_once()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_shrink_a_standby_whose_server_is_gone(self, db_instance,
                                                   instance):
        # The repair's replacement of a member lost for good: loaded with
        # its server it fails ("Instance ... is not ready."); it is not
        # asked, and its slot on the primary goes all the same.
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))

        def load(ctx, instance_id):
            if instance_id == 'i3':
                raise exception.UnprocessableEntity(
                    "Instance %s is not ready." % instance_id)
            return mock.Mock(id=instance_id)
        instance.load.side_effect = load
        self.gone.add('i3')
        self.tasks.shrink_cluster('ctx', 'c1', ['i3'])
        members = {'i1': '10.0.0.1', 'i2': '10.0.0.2'}
        self.assertEqual(
            [('drop_member_slot', 'i1', 'i3'),
             ('update_cluster_members', 'i1', members, 'strict'),
             ('update_cluster_members', 'i2', members, 'strict'),
             ('sync', ('i1', 'i2'))],
            self.calls)
        # Never asked anything: no guest client made for it.
        self.assertNotIn('i3', self.guests)
        instance.delete.assert_called_once()
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_shrink_the_primary_switches_over_first(self, db_instance,
                                                    instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.shrink_cluster('ctx', 'c1', ['i1'])
        members = {'i2': '10.0.0.2', 'i3': '10.0.0.3'}
        # The primary is fenced (its fast stop sends the standbys every
        # record), the standby that received the most is promoted, the
        # other follows it; then the old primary goes as a standby
        # would, its slot dropped on the new primary.
        self.assertEqual(
            [('fence_member', 'i1'),
             ('promote_member', 'i2', members, 'strict'),
             ('follow_primary', 'i3', {'id': 'i2', 'host': '10.0.0.2'}),
             ('update_cluster_members', 'i2', members, 'strict',
              {'primary_id': 'i2'}),
             ('update_cluster_members', 'i3', members, 'strict',
              {'primary_id': 'i2'}),
             ('leave_cluster', 'i1'), ('drop_member_slot', 'i2', 'i1'),
             ('update_cluster_members', 'i2', members, 'strict'),
             ('update_cluster_members', 'i3', members, 'strict'),
             ('sync', ('i2', 'i3'))],
            self.calls)
        instance.delete.assert_called_once()
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_shrink_the_primary_fails_without_a_successor(self, db_instance,
                                                          instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        for guest in (self.tasks.get_guest(i)
                      for i in self._instances('i2', 'i3')):
            guest.get_recovery_view.side_effect = Exception('timed out')
        self.tasks.shrink_cluster('ctx', 'c1', ['i1'])
        self.assertEqual([('fence_member', 'i1')], self.calls)
        instance.delete.assert_not_called()
        self.tasks.update_statuses_on_failure.assert_called_once_with('c1')

    @mock.patch.object(pg_taskmanager, 'Instance')
    @mock.patch.object(pg_taskmanager, 'DBInstance')
    def test_restart_and_upgrade_go_standbys_first(self, db_instance,
                                                   instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.rolling_restart_cluster = mock.Mock()
        self.tasks.rolling_upgrade_cluster = mock.Mock()
        self.tasks.restart_cluster('ctx', 'c1')
        self.tasks.upgrade_cluster('ctx', 'c1', 'v2')
        for call in (self.tasks.rolling_restart_cluster.call_args,
                     self.tasks.rolling_upgrade_cluster.call_args):
            key = call.kwargs['ordering_function']
            self.assertEqual(['i2', 'i3', 'i1'], [
                i.id for i in sorted(self._instances('i1', 'i2', 'i3'),
                                     key=key)])

    def test_the_recovery(self):
        recovery = pg_taskmanager.PostgresqlTaskManagerStrategy(
        ).cluster_recovery('postgresql')
        self.assertIsInstance(recovery,
                              pg_recovery.PostgresqlClusterRecovery)
        self.assertEqual('postgresql', recovery.manager)

    def test_root_follows_the_members_that_join(self):
        # The common layer's: a cluster with root gives it to the members
        # that join and records them as such.
        tasks = common_taskmanager.ClusterEntryTasksMixin()
        with mock.patch.object(common_taskmanager.ext_models.Root,
                               'load', side_effect=[None, True]), \
                mock.patch.object(common_taskmanager.ext_models.RootHistory,
                                  'create') as create:
            tasks._check_cluster_for_root(
                'ctx', self._instances('i1', 'i2'), self._instances('i3'))
        create.assert_called_once_with('ctx', 'i3')
