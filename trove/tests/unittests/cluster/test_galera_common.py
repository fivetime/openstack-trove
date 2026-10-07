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

"""What the Galera strategies hold for the clusters built on them: the
load balancer hooks, the member roles view and the mode of a cluster. A
Galera cluster itself (pxc, mariadb) uses none of it yet.
"""

from unittest import mock

from trove.cluster import views as cluster_views
from trove.common import cfg
from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guestagent)
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_taskmanager)
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class GaleraCommonConfigTest(trove_testtools.TestCase):

    def test_galera_clusters_have_no_probe_or_load_balancer_yet(self):
        for manager in ('pxc', 'mariadb'):
            conf = CONF.get(manager)
            self.assertTrue(conf.cluster_support)
            for name in ('cluster_load_balancer', 'cluster_ready_port',
                         'cluster_probe_interval', 'cluster_tcp_ports'):
                self.assertNotIn(name, conf, '[%s] has %s' % (manager, name))


class GaleraCommonAPITest(trove_testtools.TestCase):

    def test_a_cluster_without_a_mode(self):
        self.assertIsNone(galera_api.GaleraCommonCluster.MODE_KEY)
        properties = {'x': 1}
        self.assertIs(properties,
                      galera_api.GaleraCommonCluster._validate_mode(
                          properties))
        self.assertIsNone(galera_api.GaleraCommonCluster._validate_mode(None))
        self.assertEqual(
            {'id': 'c1', 'instance_type': 'member'},
            galera_api.GaleraCommonCluster._member_config(
                mock.Mock(id='c1'), {'group_replication_mode': 'x'}))

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_the_galera_view_shows_neither_roles_nor_endpoint(
            self, build, data):
        build.return_value = ([{'id': 'i1'}], [])
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'pxc'
        view = galera_api.GaleraCommonClusterView(cluster, load_servers=True)
        self.assertNotIn('endpoint', view.data()['cluster'])
        instances, _ips = view.build_instances()
        self.assertNotIn('role', instances[0])
        cluster.get_guest.assert_not_called()

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    def test_the_roles_mixin_without_a_load_balancer_option(self, data):
        class View(galera_api.MemberRolesMixin,
                   galera_api.GaleraCommonClusterView):
            pass
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'pxc'
        with mock.patch.object(galera_api.loadbalancer,
                               'find_endpoint') as find_endpoint:
            result = View(cluster, load_servers=True).data()
        self.assertNotIn('endpoint', result['cluster'])
        find_endpoint.assert_not_called()

    @mock.patch.object(cluster_views.ClusterView, '_build_instances')
    def test_the_roles_mixin_maps_the_roles_of_its_view(self, build):
        class View(galera_api.MemberRolesMixin,
                   galera_api.GaleraCommonClusterView):
            ROLES = {('Synced', 'PRIMARY'): 'primary'}
        roles = {'i1': {'state': 'Synced', 'role': 'PRIMARY'},
                 'i2': {'state': 'Joiner', 'role': None}}
        build.return_value = ([{'id': i} for i in roles], [])
        cluster = mock.Mock()
        cluster.instances = [mock.Mock(id=i, status='HEALTHY') for i in roles]
        cluster.get_guest = lambda instance: mock.Mock(**{
            'get_member_role.return_value': roles[instance.id]})
        instances, _ips = View(cluster, load_servers=True).build_instances()
        self.assertEqual(['primary', 'joiner'],
                         [i['role'] for i in instances])

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
                    'cluster_name': 'g1',
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
        self.tasks._sync_load_balancer = mock.Mock(
            side_effect=lambda ctx, cid, instances: self.calls.append(
                ('sync', tuple(i.id for i in instances))))
        self.tasks._delete_load_balancer = mock.Mock(
            side_effect=lambda cid: self.calls.append(('delete_lb', cid)))
        mock.patch.object(
            galera_taskmanager.GaleraCommonClusterTasks,
            '_render_cluster_config',
            side_effect=lambda ctx, inst, ips, name, user: (
                '%s|%s' % (inst.id, ips))).start()
        self.addCleanup(mock.patch.stopall)

    def _instances(self, *ids):
        return [mock.Mock(id=i) for i in ids]

    def test_a_galera_cluster_has_no_load_balancer(self):
        self.assertFalse(self.tasks._load_balancer_enabled())
        self.tasks.ds_version = mock.Mock(manager='mysql')
        self.assertTrue(self.tasks._load_balancer_enabled())

    @mock.patch.object(galera_taskmanager.utils, 'poll_until')
    @mock.patch.object(galera_taskmanager, 'Instance')
    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_create_grow_and_shrink_as_before(self, db_instance, instance,
                                              poll_until):
        # The hooks are there for the clusters that use them; a Galera
        # cluster is built as it always was.
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        self.tasks.create_cluster('ctx', 'c1')
        self.assertEqual([('install_cluster', 'i1'),
                          ('install_cluster', 'i2'),
                          ('install_cluster', 'i3')],
                         [c for c in self.calls if c[0] == 'install_cluster'])
        self.tasks.grow_cluster('ctx', 'c1', ['i3'])
        self.tasks.shrink_cluster('ctx', 'c1', ['i3'])
        self.assertEqual([], [c for c in self.calls
                              if c[0] in ('sync', 'leave_cluster')])
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(galera_taskmanager.task_models.ClusterTasks,
                       'delete_cluster')
    def test_delete_takes_the_load_balancer_with_it(self, base_delete):
        base_delete.side_effect = lambda ctx, cid: self.calls.append(
            ('delete_cluster', cid))
        self.tasks.delete_cluster('ctx', 'c1')
        # None to delete for a Galera cluster.
        self.assertEqual([('delete_cluster', 'c1')], self.calls)
        self.tasks.ds_version = mock.Mock(manager='mysql')
        self.tasks.delete_cluster('ctx', 'c1')
        self.assertEqual([('delete_lb', 'c1'), ('delete_cluster', 'c1')],
                         self.calls[1:])

    @mock.patch.object(galera_taskmanager, 'DBInstance')
    def test_fail_new_members(self, db_instance):
        failed = mock.Mock()
        db_instance.find_by.return_value = failed
        self.tasks._fail_new_members(['i4'])
        db_instance.find_by.assert_called_once_with(id='i4')
        failed.set_task_status.assert_called_once_with(
            galera_taskmanager.inst_tasks.InstanceTasks.GROWING_ERROR)
        failed.save.assert_called_once()
