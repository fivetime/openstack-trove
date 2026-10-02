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

import configparser
from unittest import mock

from trove.common import cfg
from trove.common import exception
from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)
from trove.common.strategies.cluster.experimental.group_replication import (
    api as gr_api)
from trove.common.strategies.cluster.experimental.group_replication import (
    guestagent as gr_guestagent)
from trove.common.strategies.cluster.experimental.group_replication import (
    taskmanager as gr_taskmanager)
from trove.common.strategies.cluster import strategy
from trove.common import template
from trove.extensions.mysql import service as mysql_ext
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class GroupReplicationConfigTest(trove_testtools.TestCase):

    def test_mysql_and_percona_have_group_replication(self):
        for manager in ('mysql', 'percona'):
            conf = CONF.get(manager)
            self.assertTrue(conf.cluster_support)
            self.assertEqual(3, conf.min_cluster_member_count)
            self.assertIn('clusterrepuser', conf.ignore_users)
            self.assertIsInstance(strategy.load_api_strategy(manager),
                                  gr_api.GroupReplicationAPIStrategy)
            self.assertIsInstance(
                strategy.load_taskmanager_strategy(manager),
                gr_taskmanager.GroupReplicationTaskManagerStrategy)
            self.assertIsInstance(
                strategy.load_guestagent_strategy(manager),
                gr_guestagent.GroupReplicationGuestAgentStrategy)
            self.assertEqual(
                'trove.extensions.mysql.service.'
                'GroupReplicationRootController', conf.root_controller)

    def test_pxc_keeps_galera(self):
        self.assertIsInstance(strategy.load_api_strategy('pxc'),
                              galera_api.GaleraCommonAPIStrategy)
        self.assertNotIsInstance(strategy.load_api_strategy('pxc'),
                                 gr_api.GroupReplicationAPIStrategy)
        self.assertEqual(
            'trove.extensions.common.service.DefaultRootController',
            CONF.pxc.root_controller)


class GroupReplicationAPITest(trove_testtools.TestCase):

    def test_member_config_carries_the_mode(self):
        db_info = mock.Mock(id='c1')
        self.assertEqual(
            {'id': 'c1', 'instance_type': 'member',
             'group_replication_mode': 'multi-primary'},
            gr_api.GroupReplicationCluster._member_config(
                db_info, {'group_replication_mode': 'multi-primary'}))
        # A grow gives none: the task manager gives the group's.
        self.assertEqual(
            {'id': 'c1', 'instance_type': 'member'},
            gr_api.GroupReplicationCluster._member_config(db_info, None))

    @mock.patch.object(galera_api.GaleraCommonCluster, 'create')
    def test_create_defaults_to_single_primary(self, create):
        gr_api.GroupReplicationCluster.create(
            'ctx', 'c', 'ds', 'dsv', [], None, None, None)
        self.assertEqual({'group_replication_mode': 'single-primary'},
                         create.call_args[0][5])

    @mock.patch.object(galera_api.GaleraCommonCluster, 'create')
    def test_create_refuses_an_unknown_mode(self, create):
        self.assertRaises(
            exception.BadRequest, gr_api.GroupReplicationCluster.create,
            'ctx', 'c', 'ds', 'dsv', [],
            {'group_replication_mode': 'both'}, None, None)
        create.assert_not_called()


class GroupReplicationTemplateTest(trove_testtools.TestCase):

    def _render(self, manager, single_primary):
        datastore_version = mock.Mock(manager=manager, version='8.4')
        datastore_version.name = '8.4'
        datastore_version.datastore_name = manager
        config = template.ClusterConfigTemplate(
            datastore_version, {'ram': 2048, 'vcpus': 1}, 'i1')
        rendered = config.render(
            cluster_ips='10.0.0.1,10.0.0.2,10.0.0.3',
            group_seeds='10.0.0.1:33061,10.0.0.2:33061,10.0.0.3:33061',
            group_port=33061, cluster_name='a-uuid',
            instance_ip='10.0.0.2', instance_name='m2',
            single_primary=single_primary)
        # The guest agent parses it as an INI file: no key twice.
        parser = configparser.ConfigParser(strict=True)
        parser.read_string(rendered)
        return parser['mysqld']

    def test_rendered_configuration(self):
        for manager in ('mysql', 'percona'):
            mysqld = self._render(manager, True)
            self.assertEqual('"a-uuid"',
                             mysqld['group_replication_group_name'])
            self.assertEqual('"10.0.0.2:33061"',
                             mysqld['group_replication_local_address'])
            self.assertEqual('"10.0.0.1,10.0.0.2,10.0.0.3"',
                             mysqld['group_replication_ip_allowlist'])
            # Until the cluster is complete the guest agent starts it.
            self.assertEqual('OFF',
                             mysqld['group_replication_start_on_boot'])
            self.assertEqual('ON',
                             mysqld['group_replication_single_primary_mode'])
            self.assertIn('mysql_clone.so', mysqld['plugin_load_add'])

    def test_multi_primary(self):
        mysqld = self._render('mysql', False)
        self.assertEqual('OFF',
                         mysqld['group_replication_single_primary_mode'])
        self.assertEqual(
            'ON', mysqld['group_replication_enforce_update_everywhere_checks'])


class GroupReplicationTasksTest(trove_testtools.TestCase):

    def setUp(self):
        super(GroupReplicationTasksTest, self).setUp()
        self.tasks = gr_taskmanager.GroupReplicationClusterTasks.__new__(
            gr_taskmanager.GroupReplicationClusterTasks)
        self.tasks._datastore_version = mock.Mock()
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
                    'mode': 'multi-primary', 'cluster_name': 'g1',
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
        self.render = mock.patch.object(
            gr_taskmanager.GroupReplicationClusterTasks,
            '_render_cluster_config',
            side_effect=lambda ctx, inst, ips, name, user, mode: (
                '%s|%s|%s' % (inst.id, ips, mode))).start()
        self.addCleanup(mock.patch.stopall)

    def _instances(self, *ids):
        return [mock.Mock(id=i) for i in ids]

    @mock.patch.object(gr_taskmanager, 'Instance')
    @mock.patch.object(gr_taskmanager, 'DBInstance')
    def test_create(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)

        self.tasks.create_cluster('ctx', 'c1')

        # The recovery channel takes passwords of 32 characters at most.
        replication_user = self.guests['i1'].install_cluster.call_args[0][0]
        self.assertEqual('clusterrepuser', replication_user['name'])
        self.assertLessEqual(len(replication_user['password']), 32)
        installs = [c for c in self.calls if c[0] == 'install_cluster']
        self.assertEqual([('install_cluster', 'i1'),
                          ('install_cluster', 'i2'),
                          ('install_cluster', 'i3')], installs)
        bootstraps = [self.guests[i].install_cluster.call_args[0][2]
                      for i in ('i1', 'i2', 'i3')]
        self.assertEqual([True, False, False], bootstraps)
        # The mode the members kept from their creation.
        for call in self.render.call_args_list:
            self.assertEqual('multi-primary', call.kwargs['mode'])
            self.assertEqual('10.0.0.1,10.0.0.2,10.0.0.3', call[0][2])
        # Every member has the cluster's admin password before any joins.
        first_install = self.calls.index(('install_cluster', 'i1'))
        self.assertEqual(3, len([c for c in self.calls[:first_install]
                                 if c[0] == 'reset_admin_password']))
        self.assertEqual(3, len([c for c in self.calls
                                 if c[0] == 'cluster_complete']))
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(gr_taskmanager, 'Instance')
    @mock.patch.object(gr_taskmanager, 'DBInstance')
    def test_grow_lets_the_new_members_in_first(self, db_instance, instance):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i2', 'i3', 'i4'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)

        self.tasks.grow_cluster('ctx', 'c1', ['i4'])

        updates = [c[1] for c in self.calls
                   if c[0] == 'write_cluster_configuration_overrides']
        self.assertEqual(['i1', 'i2', 'i3'], updates)
        self.assertLess(
            self.calls.index(('write_cluster_configuration_overrides',
                              'i3')),
            self.calls.index(('install_cluster', 'i4')))
        self.assertFalse(self.guests['i4'].install_cluster.call_args[0][2])
        self.guests['i4'].reset_admin_password.assert_called_once_with('a')
        for call in self.render.call_args_list:
            self.assertEqual('10.0.0.1,10.0.0.2,10.0.0.3,10.0.0.4',
                             call[0][2])
        self.tasks.update_statuses_on_failure.assert_not_called()

    @mock.patch.object(gr_taskmanager.utils, 'poll_until')
    @mock.patch.object(gr_taskmanager, 'Instance')
    @mock.patch.object(gr_taskmanager, 'DBInstance')
    def test_shrink_leaves_before_delete(self, db_instance, instance,
                                         poll_until):
        db_instance.find_all.return_value.all.return_value = (
            self._instances('i1', 'i3'))
        instance.load.side_effect = lambda ctx, i: mock.Mock(id=i)
        instance.delete.side_effect = lambda inst: self.calls.append(
            ('delete', inst.id))

        self.tasks.shrink_cluster('ctx', 'c1', ['i2'])

        self.assertLess(self.calls.index(('leave_cluster', 'i2')),
                        self.calls.index(('delete', 'i2')))
        updates = [c[1] for c in self.calls
                   if c[0] == 'write_cluster_configuration_overrides']
        self.assertEqual(['i1', 'i3'], updates)
        self.tasks.update_statuses_on_failure.assert_not_called()


class GroupReplicationRootTest(trove_testtools.TestCase):

    @mock.patch.object(mysql_ext, 'datastore_models')
    @mock.patch.object(mysql_ext, 'DBInstance')
    @mock.patch.object(mysql_ext, 'strategy')
    def test_root_is_enabled_on_the_writable_member(
            self, strategy_mock, db_instance, datastore_models):
        writable = {'i1': False, 'i2': True, 'i3': False}
        guest_class = strategy_mock.load_guestagent_strategy.return_value
        guest_class.guest_client_class.side_effect = (
            lambda ctx, i: mock.Mock(**{
                'is_writable_member.return_value': writable[i]}))
        controller = mysql_ext.GroupReplicationRootController()
        controller._cluster = mock.Mock()
        controller._cluster._find_cluster_node_ids.return_value = [
            'i1', 'i2', 'i3']
        req = mock.Mock(environ={'trove.context': 'ctx'})

        controller.root_create(req, {}, 't1', 'c1', True)

        controller._cluster.instance_root_create.assert_called_once_with(
            req, {}, 'i2', ['i1', 'i2', 'i3'])

    def test_root_of_a_cluster_cannot_be_disabled(self):
        self.assertRaises(
            exception.ClusterOperationNotSupported,
            mysql_ext.GroupReplicationRootController().root_delete,
            mock.Mock(), 't1', 'c1', True)
