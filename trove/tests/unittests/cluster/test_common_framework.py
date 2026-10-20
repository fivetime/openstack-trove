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

from unittest import mock

from trove.cluster import views as cluster_views
from trove.common import exception
from trove.common import loadbalancer
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.common.strategies.cluster.experimental.common import (
    taskmanager as common_taskmanager)
from trove.common.strategies.cluster.experimental.common import topology
from trove.tests.unittests import trove_testtools


class _DBInfo(object):
    """A cluster's row: its properties as the real one keeps them."""

    def __init__(self, properties=None):
        self.properties = dict(properties or {})
        self.saved = []

    def get_properties(self):
        return dict(self.properties)

    def save_properties(self, properties):
        self.properties = dict(properties)
        self.saved.append(dict(properties))


def _topology_class(name):
    class Topology(object):
        created = []

        def __init__(self, context, db_info, *args, **kwargs):
            self.context, self.db_info = context, db_info
            self.topology = name

        @classmethod
        def create(cls, context, name_, datastore, datastore_version,
                   instances, extended_properties, locality, configuration,
                   image_id=None):
            cls.created.append(dict(extended_properties))
            return cls(context, _DBInfo({'other': 1}))
    return Topology


class TopologyTest(trove_testtools.TestCase):
    """A datastore whose clusters come in more than one shape."""

    def setUp(self):
        super(TopologyTest, self).setUp()
        self.sharded = _topology_class('cluster')
        self.sentinel = _topology_class('sentinel')
        self.clusters = topology.clusters(
            {'cluster': self.sharded, 'sentinel': self.sentinel}, 'cluster')

    def _create(self, props):
        return self.clusters.create('ctx', 'c', 'ds', 'dsv', [], props,
                                    None, None)

    def test_a_cluster_without_a_topology_is_of_the_default(self):
        cluster = self._create({'replicas_per_master': '1'})
        self.assertEqual('cluster', cluster.topology)
        # The topology's own properties only, and none kept: a cluster
        # from before the datastore had topologies looks the same.
        self.assertEqual([{'replicas_per_master': '1'}], self.sharded.created)
        self.assertEqual([], cluster.db_info.saved)
        self.assertEqual('cluster',
                         self.clusters('ctx', cluster.db_info).topology)

    def test_the_topology_asked_for_is_kept(self):
        cluster = self._create({'topology': 'sentinel',
                                'replication_mode': 'sync'})
        self.assertEqual('sentinel', cluster.topology)
        # Given to the topology's class, to keep before the task manager
        # is asked (this fake does not: the dispatcher does it after).
        self.assertEqual([{'replication_mode': 'sync',
                           'topology': 'sentinel'}],
                         self.sentinel.created)
        # Added to what the topology keeps itself.
        self.assertEqual({'other': 1, 'topology': 'sentinel'},
                         cluster.db_info.properties)
        loaded = self.clusters('ctx', cluster.db_info)
        self.assertIsInstance(loaded, self.sentinel)

    def test_an_unknown_topology(self):
        self.assertRaisesRegex(exception.BadRequest,
                               'Unknown cluster topology ring',
                               self._create, {'topology': 'ring'})

    def test_views(self):
        class Sharded(object):
            def __init__(self, cluster, req=None, load_servers=True):
                self.args = (cluster, req, load_servers)

        class Sentinel(Sharded):
            pass
        views = topology.views({'cluster': Sharded, 'sentinel': Sentinel},
                               'cluster')
        cluster = mock.Mock(db_info=_DBInfo({'topology': 'sentinel'}))
        view = views(cluster, 'req', False)
        self.assertIsInstance(view, Sentinel)
        self.assertEqual((cluster, 'req', False), view.args)
        self.assertIsInstance(views(mock.Mock(db_info=_DBInfo())),
                              Sharded)


class _EntryTasks(common_taskmanager.ClusterEntryTasksMixin):
    def __init__(self):
        self.datastore_version = mock.Mock(manager='mysql')


class ExtraEndpointsTest(trove_testtools.TestCase):
    """Further endpoints of a cluster, each to every member's same port
    (a sentinel each member runs).
    """

    def setUp(self):
        super(ExtraEndpointsTest, self).setUp()
        self.patch_conf_property('cluster_load_balancer_extra_ports',
                                 {'sentinel_endpoint': '26379'},
                                 section='mysql')

    def test_the_ports_as_numbers(self):
        self.assertEqual({'sentinel_endpoint': 26379},
                         loadbalancer.extra_ports(
                             common_taskmanager.CONF.mysql))
        self.assertEqual({}, loadbalancer.extra_ports(mock.Mock(spec=[])))

    @mock.patch.object(common_taskmanager.loadbalancer, 'OctaviaClient')
    @mock.patch.object(common_taskmanager.loadbalancer,
                       'ensure_load_balancer')
    def test_a_listener_each(self, ensure, client):
        tasks = _EntryTasks()
        member = {'name': 'i1', 'address': '10.0.0.1', 'protocol_port': 3307,
                  'subnet_id': 'sub-4'}
        tasks._load_balancer_members = mock.Mock(return_value=[member])
        tasks._sync_load_balancer('ctx', 'c1', [mock.Mock(id='i1')])
        self.assertEqual(
            [(26379, [dict(member, protocol_port=26379)])],
            ensure.call_args[1]['listeners'])

    @mock.patch.object(cluster_views.ClusterView, 'data',
                       side_effect=lambda: {'cluster': {'id': 'c1'}})
    @mock.patch.object(common_api.loadbalancer, 'OctaviaClient')
    @mock.patch.object(common_api.loadbalancer, 'find_endpoint')
    def test_shown_by_name(self, find_endpoint, client, data):
        class View(common_api.MemberRolesMixin, cluster_views.ClusterView):
            pass
        find_endpoint.return_value = {'address': '10.0.0.50', 'port': 3306,
                                      'status': 'ONLINE'}
        cluster = mock.Mock(id='c1')
        cluster.datastore_version.manager = 'mysql'
        result = View(cluster, load_servers=True).data()['cluster']
        self.assertEqual({'address': '10.0.0.50', 'port': 26379,
                          'status': 'ONLINE'}, result['sentinel_endpoint'])
        self.assertEqual(3306, result['endpoint']['port'])
