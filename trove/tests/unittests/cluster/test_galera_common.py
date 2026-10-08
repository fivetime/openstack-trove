# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A Galera cluster (pxc, mariadb) with its writer mode, the roles of its
members, its load balancer and the shared hooks of the strategies.
"""

import configparser
from unittest import mock

from trove.cluster import views as cluster_views
from trove.common import cfg
from trove.common import exception
from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guestagent)
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_taskmanager)
from trove.common.strategies.cluster import strategy
from trove.common import template
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class GaleraCommonConfigTest(trove_testtools.TestCase):

    def test_galera_clusters_have_the_probe_and_the_load_balancer(self):
        for manager in ('pxc', 'mariadb'):
            conf = CONF.get(manager)
            self.assertTrue(conf.cluster_support)
            self.assertTrue(conf.cluster_load_balancer)
            self.assertEqual(3307, conf.cluster_ready_port)
            self.assertEqual(3, conf.cluster_probe_interval)
            self.assertIn('3307', [str(p) for r in conf.cluster_tcp_ports
                                   for p in r])
            # Root of a cluster is enabled on a member that takes writes.
            self.assertEqual('trove.extensions.mysql.service.'
                             'ClusterWriterRootController',
                             conf.root_controller)
            self.assertIsInstance(strategy.load_api_strategy(manager),
                                  galera_api.GaleraCommonAPIStrategy)


class GaleraCommonAPITest(trove_testtools.TestCase):

    def test_the_writer_mode(self):
        cluster = galera_api.GaleraCommonCluster
        self.assertEqual({'writer_mode': 'single'},
                         cluster._validate_mode(None))
        self.assertEqual({'writer_mode': 'multi', 'x': 1},
                         cluster._validate_mode({'writer_mode': 'multi',
                                                 'x': 1}))
        self.assertRaises(exception.BadRequest, cluster._validate_mode,
                          {'writer_mode': 'both'})
        self.assertEqual(
            {'id': 'c1', 'instance_type': 'member', 'writer_mode': 'multi'},
            cluster._member_config(mock.Mock(id='c1'),
                                   {'writer_mode': 'multi'}))
        # A grow gives none: the task manager gives the cluster's.
        self.assertEqual({'id': 'c1', 'instance_type': 'member'},
                         cluster._member_config(mock.Mock(id='c1'), None))

    @mock.patch.object(galera_api.GaleraCommonCluster,
                       '_validate_cluster_instances')
    def test_create_refuses_an_unknown_mode(self, validate):
        datastore_version = mock.Mock(manager='pxc')
        self.assertRaises(
            exception.BadRequest, galera_api.GaleraCommonCluster.create,
            'ctx', 'c', 'ds', datastore_version, [{}, {}, {}],
            {'writer_mode': 'both'}, None, None)
        validate.assert_not_called()

    def _view(self, load_servers, roles, manager='pxc'):
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = manager
        cluster.instances = [mock.Mock(id=i, status='HEALTHY')
                             for i in roles]
        cluster.instances[0].status = 'BUILD'

        def guest(instance):
            g = mock.Mock()
            answer = roles[instance.id]
            if isinstance(answer, Exception):
                g.get_member_role.side_effect = answer
            else:
                g.get_member_role.return_value = answer
            return g
        cluster.get_guest = guest
        return galera_api.GaleraCommonClusterView(cluster,
                                                  load_servers=load_servers)

    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_roles_of_the_members(self, build):
        roles = {'i1': {'state': 'Synced', 'role': 'PRIMARY'},
                 'i2': {'state': 'Synced', 'role': 'PRIMARY'},
                 'i3': {'state': 'Synced', 'role': 'SECONDARY'},
                 'i4': {'state': 'Joiner', 'role': None},
                 'i5': {'state': 'Donor/Desynced', 'role': None},
                 'i6': {'state': 'Initialized', 'role': None},
                 'i7': exception.GuestTimeout(),
                 'i8': {'state': None, 'role': None}}
        build.return_value = ([{'id': i} for i in roles], [])
        instances, _ips = self._view(True, roles).build_instances()
        # i1 is still building and is not asked.
        self.assertEqual(['unknown', 'primary', 'secondary', 'recovering',
                          'donor/desynced', 'offline', 'unknown', 'unknown'],
                         [i['role'] for i in instances])

    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_a_list_asks_nobody(self, build):
        build.return_value = ([{'id': 'i2'}], [])
        view = self._view(False, {'i2': {'state': 'Synced',
                                         'role': 'PRIMARY'}})
        instances, _ips = view.build_instances()
        self.assertNotIn('role', instances[0])

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(galera_api.loadbalancer, 'OctaviaClient')
    @mock.patch.object(galera_api.loadbalancer, 'find_endpoint')
    def test_endpoint(self, find_endpoint, client, data):
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'mariadb'
        find_endpoint.return_value = {'address': '10.0.0.50', 'port': 3306}
        view = galera_api.GaleraCommonClusterView(cluster, load_servers=True)
        self.assertEqual({'address': '10.0.0.50', 'port': 3306},
                         view.data()['cluster']['endpoint'])
        find_endpoint.assert_called_once_with(
            client.return_value, 'trove-cluster-c1', 3306)
        # A list asks Octavia nothing.
        view = galera_api.GaleraCommonClusterView(cluster, load_servers=False)
        self.assertNotIn('endpoint', view.data()['cluster'])
        find_endpoint.assert_called_once()

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    def test_no_endpoint_without_the_load_balancer_option(self, data):
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'other'
        with mock.patch.object(galera_api.CONF, 'get',
                               return_value=mock.Mock(spec=[])), \
                mock.patch.object(galera_api.loadbalancer,
                                  'find_endpoint') as find_endpoint:
            result = galera_api.GaleraCommonClusterView(
                cluster, load_servers=True).data()
        self.assertNotIn('endpoint', result['cluster'])
        find_endpoint.assert_not_called()

    def test_the_guest_api_has_the_shared_calls(self):
        api = galera_guestagent.GaleraCommonGuestAgentAPI.__new__(
            galera_guestagent.GaleraCommonGuestAgentAPI)
        api.agent_low_timeout = 15
        api.agent_high_timeout = 600
        with mock.patch.object(api, '_call') as call:
            api.get_member_role()
            api.leave_cluster()
            api.is_writable_member()
        self.assertEqual(
            [mock.call('get_member_role', 15, version='1.0'),
             mock.call('leave_cluster', 600, version='1.0'),
             mock.call('is_writable_member', 600, version='1.0')],
            call.call_args_list)


class GaleraTemplateTest(trove_testtools.TestCase):

    def _render(self, manager, **values):
        datastore_version = mock.Mock(manager=manager, version='x')
        datastore_version.name = 'x'
        datastore_version.datastore_name = manager
        config = template.ClusterConfigTemplate(
            datastore_version, {'ram': 2048, 'vcpus': 1}, 'i1')
        rendered = config.render(
            replication_user_pass='clusterrepuser:pw',
            cluster_ips='10.0.0.1,10.0.0.2,10.0.0.3', cluster_name='c1',
            instance_ip='10.0.0.2', instance_name='m2', **values)
        parser = configparser.ConfigParser(strict=True)
        parser.read_string(rendered)
        return parser

    def test_the_writer_mode_is_in_the_configuration(self):
        for manager in ('pxc', 'mariadb'):
            self.assertEqual(
                'single',
                self._render(manager)['trove']['cluster_writer_mode'])
            self.assertEqual(
                'multi', self._render(manager, writer_mode='multi')[
                    'trove']['cluster_writer_mode'])


class GaleraCommonTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(GaleraCommonTasksTest, self).setUp()
        self.tasks = galera_taskmanager.GaleraCommonClusterTasks.__new__(
            galera_taskmanager.GaleraCommonClusterTasks)
        self.tasks._datastore_version = mock.Mock()
        self.tasks.ds_version = mock.Mock(manager='pxc')
        self.calls = []
        self.guests = {}

        def guest(instance):
            if instance.id not in self.guests:
                g = mock.Mock()
                for name in ('reset_admin_password', 'install_cluster',
                             'cluster_complete', 'leave_cluster',
                             'write_cluster_configuration_overrides'):
                    getattr(g, name).side_effect = (
                        lambda *a, _n=name, _i=instance.id, **k:
                        self.calls.append((_n, _i)))
                g.get_cluster_context.return_value = {
                    'cluster_name': 'g1', 'writer_mode': 'multi',
                    'replication_user': {'name': 'r', 'password': 'p'},
                    'admin_password': 'a'}
                self.guests[instance.id] = g
            return self.guests[instance.id]
        self.tasks.get_guest = guest
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
        self.tasks._delete_load_balancer = mock.Mock(
            side_effect=lambda cid: self.calls.append(('delete_lb', cid)))
        self.render = mock.patch.object(
            galera_taskmanager.GaleraCommonClusterTasks,
            '_render_cluster_config',
            side_effect=lambda ctx, inst, ips, name, user, writer_mode: (
                '%s|%s|%s' % (inst.id, ips, writer_mode))).start()
        self.addCleanup(mock.patch.stopall)

    def _instances(self, *ids):
        return [mock.Mock(id=i) for i in ids]

    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_create_renders_the_mode_and_ends_with_the_load_balancer(
            self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)

        self.tasks.create_cluster('ctx', 'c1')

        bootstraps = [self.guests[i].install_cluster.call_args[0][2]
                      for i in ('i1', 'i2', 'i3')]
        self.assertEqual([True, False, False], bootstraps)
        # The mode the members kept from their creation.
        for call in self.render.call_args_list:
            self.assertEqual('multi', call.kwargs['writer_mode'])
        self.assertEqual(('sync', ('i1', 'i2', 'i3')), self.calls[-1])
        self.assertEqual(3, len([c for c in self.calls[:-1]
                                 if c[0] == 'cluster_complete']))
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_create_fails_without_its_load_balancer(self, db_instance,
                                                    instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks._sync_load_balancer.side_effect = Exception('no octavia')

        self.tasks.create_cluster('ctx', 'c1')

        self.tasks.update_statuses_on_failure.assert_called_once_with('c1')

    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_grow_adds_the_member_to_the_load_balancer(self, db_instance,
                                                       instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3', 'i4'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)

        self.tasks.grow_cluster('ctx', 'c1', ['i4'])

        self.assertEqual(('sync', ('i1', 'i2', 'i3', 'i4')), self.calls[-1])
        for call in self.render.call_args_list:
            self.assertEqual('multi', call.kwargs['writer_mode'])
        # A failure there does not fail the grow.
        self.tasks._sync_load_balancer.side_effect = Exception('no octavia')
        self.tasks.grow_cluster('ctx', 'c1', ['i4'])
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.assertEqual(2, self.tasks.reset_task.call_count)

    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_a_failed_grow_leaves_the_cluster_as_it_was(
            self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3', 'i4'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks._all_instances_ready.return_value = False

        self.tasks.grow_cluster('ctx', 'c1', ['i4'])

        self.assertEqual([('fail', ('i4',))], self.calls)
        self.tasks.update_statuses_on_failure.assert_not_called()
        self.tasks.reset_task.assert_called_once()

    @mock.patch.object(galera_taskmanager.utils, 'poll_until')
    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_shrink_leaves_first_and_updates_the_load_balancer(
            self, db_instance, instance, poll_until):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        instance.delete.side_effect = lambda inst: self.calls.append(
            ('delete', inst.id))

        self.tasks.shrink_cluster('ctx', 'c1', ['i2'])

        self.assertLess(self.calls.index(('leave_cluster', 'i2')),
                        self.calls.index(('delete', 'i2')))
        self.assertEqual(('sync', ('i1', 'i3')), self.calls[-1])
        # A member that cannot leave is deleted all the same.
        self.calls.clear()
        self.guests['i2'].leave_cluster.side_effect = Exception('gone')
        self.tasks.shrink_cluster('ctx', 'c1', ['i2'])
        self.assertIn(('delete', 'i2'), self.calls)
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(galera_taskmanager.task_models.ClusterTasks,
                       'delete_cluster')
    def test_delete_takes_the_load_balancer_with_it(self, base_delete):
        base_delete.side_effect = lambda ctx, cid: self.calls.append(
            ('delete_cluster', cid))
        self.tasks.delete_cluster('ctx', 'c1')
        self.assertEqual([('delete_lb', 'c1'), ('delete_cluster', 'c1')],
                         self.calls)
        self.tasks._load_balancer_enabled.return_value = False
        self.tasks.delete_cluster('ctx', 'c1')
        self.assertEqual(('delete_cluster', 'c1'), self.calls[-1])

    def test_load_balancer_enabled_by_the_datastore_options(self):
        tasks = galera_taskmanager.GaleraCommonClusterTasks.__new__(
            galera_taskmanager.GaleraCommonClusterTasks)
        tasks.ds_version = mock.Mock(manager='mariadb')
        self.assertTrue(tasks._load_balancer_enabled())
        with mock.patch.object(galera_taskmanager.CONF, 'get',
                               return_value=mock.Mock(spec=[])):
            self.assertFalse(tasks._load_balancer_enabled())

    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_fail_new_members(self, db_instance):
        tasks = galera_taskmanager.GaleraCommonClusterTasks.__new__(
            galera_taskmanager.GaleraCommonClusterTasks)
        failed = mock.Mock()
        db_instance.find_by.return_value = failed
        tasks._fail_new_members(['i4'])
        db_instance.find_by.assert_called_once_with(id='i4')
        failed.set_task_status.assert_called_once_with(
            galera_taskmanager.inst_tasks.InstanceTasks.GROWING_ERROR)
        failed.save.assert_called_once()


class ClusterWriterRootTest(trove_testtools.TestCase):
    """Root of a cluster goes to a member that takes writes."""

    def setUp(self):
        super(ClusterWriterRootTest, self).setUp()
        from trove.extensions.mysql import service as mysql_ext
        self.ext = mysql_ext
        self.controller = mysql_ext.ClusterWriterRootController()
        self.req = mock.Mock(environ={'trove.context': 'ctx'})
        self.guests = {}

        def guest_client(context, member_id):
            return self.guests[member_id]
        mock.patch.object(mysql_ext.strategy, 'load_guestagent_strategy',
                          return_value=mock.Mock(
                              guest_client_class=guest_client)).start()
        mock.patch.object(mysql_ext.DBInstance, 'find_by',
                          return_value=mock.Mock(
                              datastore_version_id='v')).start()
        mock.patch.object(mysql_ext.datastore_models.DatastoreVersion,
                          'load_by_uuid',
                          return_value=mock.Mock(manager='pxc')).start()
        mock.patch.object(self.controller._cluster, '_find_cluster_node_ids',
                          return_value=['m1', 'm2', 'm3']).start()
        self.create = mock.patch.object(self.controller._cluster,
                                        'instance_root_create').start()
        self.addCleanup(mock.patch.stopall)

    def _member(self, writable=None, error=None):
        g = mock.Mock()
        if error:
            g.is_writable_member.side_effect = error
        else:
            g.is_writable_member.return_value = writable
        return g

    def test_the_first_member_that_takes_writes_gets_root(self):
        self.guests = {'m1': self._member(False), 'm2': self._member(True),
                       'm3': self._member(True)}
        self.controller.root_create(self.req, {}, 't', 'c1', True)
        self.create.assert_called_once_with(self.req, {}, 'm2',
                                            ['m1', 'm2', 'm3'])
        self.guests['m3'].is_writable_member.assert_not_called()

    def test_a_member_that_does_not_answer_is_skipped(self):
        self.guests = {'m1': self._member(error=Exception('down')),
                       'm2': self._member(True), 'm3': self._member(True)}
        self.controller.root_create(self.req, {}, 't', 'c1', True)
        self.create.assert_called_once_with(self.req, {}, 'm2',
                                            ['m1', 'm2', 'm3'])

    def test_no_writer_no_root(self):
        self.guests = {m: self._member(False) for m in ('m1', 'm2', 'm3')}
        self.assertRaises(exception.UnprocessableEntity,
                          self.controller.root_create,
                          self.req, {}, 't', 'c1', True)
        self.create.assert_not_called()

    def test_root_of_a_cluster_is_not_disabled(self):
        self.assertRaises(exception.ClusterOperationNotSupported,
                          self.controller.root_delete, self.req, 't', 'c1',
                          True)

    def test_the_old_name_is_kept(self):
        self.assertIs(self.ext.ClusterWriterRootController,
                      self.ext.GroupReplicationRootController)
