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

import json
from unittest import mock

from trove.common import loadbalancer
from trove.tests.unittests import trove_testtools


class FakeOctavia(object):
    """Octavia's REST API, as far as Trove uses it: one load balancer at a
    time, ACTIVE as soon as it is looked at. A listener, pool or health
    monitor is made PENDING_CREATE and goes ACTIVE with the balancer,
    the children before it as Octavia writes a provider's status; with
    ``balancer_first`` the balancer goes ACTIVE on its own and a child
    only once it is looked at itself (the OVN provider reports the
    balancer ACTIVE on a member's health change). A request on a child
    that is not ACTIVE is refused as immutable, as Octavia does.
    """

    def __init__(self):
        self.lbs = {}
        self.listeners = {}
        self.pools = {}
        self.monitors = {}
        self.members = {}
        self.calls = []
        self.next_id = 0
        # Statuses the next GETs of a load balancer answer with.
        self.statuses = []
        self.balancer_first = False

    def _id(self, kind):
        self.next_id += 1
        return '%s-%s' % (kind, self.next_id)

    def request(self, url, method, **kwargs):
        path = url[len(loadbalancer.API):]
        self.calls.append((method, path))
        body = kwargs.get('json')
        params = kwargs.get('params') or {}
        return self._answer(method, path, body, params)

    def _response(self, status, data=None):
        content = json.dumps(data).encode() if data is not None else b''
        return mock.Mock(status_code=status, content=content,
                         text=content.decode(),
                         json=lambda: json.loads(content))

    def _answer(self, method, path, body, params):
        parts = path.strip('/').split('/')
        if method == 'GET' and path == '/loadbalancers':
            return self._response(200, {'loadbalancers': [
                lb for lb in self.lbs.values()
                if lb['name'] == params.get('name')]})
        if method == 'POST' and path == '/loadbalancers':
            lb = dict(body['loadbalancer'], id=self._id('lb'),
                      vip_address='10.0.0.50',
                      provisioning_status='PENDING_CREATE',
                      operating_status='OFFLINE', listeners=[], pools=[])
            self.lbs[lb['id']] = lb
            return self._response(201, {'loadbalancer': lb})
        if parts[0] == 'loadbalancers' and len(parts) == 2:
            lb = self.lbs.get(parts[1])
            if lb is None:
                return self._response(404)
            if method == 'DELETE':
                del self.lbs[parts[1]]
                return self._response(204)
            if self.statuses:
                lb['provisioning_status'] = self.statuses.pop(0)
            else:
                lb['provisioning_status'] = 'ACTIVE'
            if lb['provisioning_status'] == 'ACTIVE' and \
                    not self.balancer_first:
                self._children_active()
            return self._response(200, {'loadbalancer': lb})
        if method == 'POST' and path == '/listeners':
            listener = dict(body['listener'], id=self._id('li'),
                            default_pool_id=None,
                            provisioning_status='PENDING_CREATE')
            self.listeners[listener['id']] = listener
            self.lbs[listener['loadbalancer_id']]['listeners'].append(
                {'id': listener['id']})
            return self._response(201, {'listener': listener})
        if parts[0] == 'listeners' and len(parts) == 2:
            return self._response(200, {'listener': self._looked_at(
                self.listeners[parts[1]])})
        if method == 'POST' and path == '/pools':
            refused = self._immutable('Listener',
                                      self.listeners[body['pool'][
                                          'listener_id']])
            if refused:
                return refused
            pool = dict(body['pool'], id=self._id('pool'),
                        provisioning_status='PENDING_CREATE')
            self.pools[pool['id']] = pool
            self.listeners[pool['listener_id']]['default_pool_id'] = pool['id']
            self.members[pool['id']] = []
            return self._response(201, {'pool': pool})
        if parts[0] == 'pools' and len(parts) == 2:
            return self._response(200, {'pool': self._looked_at(
                self.pools[parts[1]])})
        if method == 'POST' and path == '/healthmonitors':
            refused = self._immutable(
                'Pool', self.pools[body['healthmonitor']['pool_id']])
            if refused:
                return refused
            monitor = dict(body['healthmonitor'], id=self._id('hm'),
                           provisioning_status='PENDING_CREATE')
            self.monitors[monitor['id']] = monitor
            return self._response(201, {'healthmonitor': monitor})
        if parts[0] == 'healthmonitors' and len(parts) == 2:
            return self._response(200, {'healthmonitor': self._looked_at(
                self.monitors[parts[1]])})
        if parts[0] == 'pools' and parts[-1] == 'members':
            if method == 'GET':
                return self._response(200, {'members': self.members[parts[1]]})
            refused = self._immutable('Pool', self.pools[parts[1]])
            if refused:
                return refused
            self.members[parts[1]] = body['members']
            self.pools[parts[1]]['provisioning_status'] = 'PENDING_UPDATE'
            return self._response(202)
        raise AssertionError('unexpected %s %s' % (method, path))

    def _children_active(self):
        for child in list(self.listeners.values()) + list(
                self.pools.values()) + list(self.monitors.values()):
            child['provisioning_status'] = 'ACTIVE'

    @staticmethod
    def _looked_at(child):
        child['provisioning_status'] = 'ACTIVE'
        return child

    def _immutable(self, kind, child):
        if child.get('provisioning_status', 'ACTIVE') == 'ACTIVE':
            return None
        return self._response(409, {
            'faultstring': '%s %s is immutable and cannot be updated.'
                           % (kind, child['id'])})

    def active_lb(self, name, port=3306, members=()):
        """A load balancer as Octavia would hold it after a create."""
        lb = {'id': self._id('lb'), 'name': name, 'vip_address': '10.0.0.50',
              'provisioning_status': 'ACTIVE', 'operating_status': 'ONLINE',
              'listeners': [], 'pools': []}
        self.lbs[lb['id']] = lb
        listener = {'id': self._id('li'), 'protocol_port': port,
                    'default_pool_id': self._id('pool')}
        self.listeners[listener['id']] = listener
        lb['listeners'].append({'id': listener['id']})
        self.pools[listener['default_pool_id']] = {
            'id': listener['default_pool_id'], 'provisioning_status': 'ACTIVE'}
        self.members[listener['default_pool_id']] = list(members)
        return lb


def _member(address, subnet='sub-1', port=3307):
    return {'name': address, 'address': address, 'protocol_port': port,
            'subnet_id': subnet}


class TestLoadBalancer(trove_testtools.TestCase):

    def setUp(self):
        super(TestLoadBalancer, self).setUp()
        self.octavia = FakeOctavia()
        session = mock.Mock()
        session.request = self.octavia.request
        mock.patch.object(loadbalancer.utils, 'poll_until',
                          side_effect=self._poll).start()
        self.addCleanup(mock.patch.stopall)
        self.client = loadbalancer.OctaviaClient(session=session,
                                                 region_name='r1')
        self.client.timeout = 10
        self.members = [_member('10.0.0.1'), _member('10.0.0.2')]

    @staticmethod
    def _poll(retriever, condition=lambda value: value, **kwargs):
        # Without sleeping; the fake answers at once.
        for _ in range(20):
            value = retriever()
            if condition(value):
                return value
        raise loadbalancer.exception.PollTimeOut()

    def test_name(self):
        self.assertEqual('trove-cluster-c1',
                         loadbalancer.cluster_load_balancer_name('c1'))

    def test_endpoint_filter(self):
        self.assertEqual('load-balancer',
                         self.client.endpoint_filter['service_type'])
        self.assertEqual('r1', self.client.endpoint_filter['region_name'])

    def test_creates_what_is_missing(self):
        lb = loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306,
            provider='ovn')
        self.assertEqual('ACTIVE', lb['provisioning_status'])
        self.assertEqual(
            ['POST /loadbalancers', 'POST /listeners', 'POST /pools',
             'POST /healthmonitors', 'PUT /pools/pool-3/members'],
            [' '.join(c) for c in self.octavia.calls
             if c[0] in ('POST', 'PUT')])
        created = list(self.octavia.lbs.values())[0]
        self.assertEqual('ovn', created['provider'])
        self.assertEqual('sub-1', created['vip_subnet_id'])
        listener = list(self.octavia.listeners.values())[0]
        self.assertEqual(3306, listener['protocol_port'])
        self.assertEqual({'10.0.0.1', '10.0.0.2'},
                         {m['address'] for m in self.octavia.members[
                             'pool-3']})
        self.assertEqual(3307, self.octavia.members['pool-3'][0][
            'protocol_port'])

    def test_a_second_listener_for_the_read_endpoint(self):
        # A datastore with a read endpoint: a second listener, with its
        # own pool and members on their read port.
        reads = [_member('10.0.0.2', port=5434),
                 _member('10.0.0.3', port=5434)]
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 5432,
            listeners=[(5433, reads)])
        listeners = sorted(li['protocol_port']
                           for li in self.octavia.listeners.values())
        self.assertEqual([5432, 5433], listeners)
        self.assertEqual(2, len(self.octavia.pools))
        read_pool = [li['default_pool_id'] for li in
                     self.octavia.listeners.values()
                     if li['protocol_port'] == 5433][0]
        self.assertEqual({('10.0.0.2', 5434), ('10.0.0.3', 5434)},
                         {(m['address'], m['protocol_port'])
                          for m in self.octavia.members[read_pool]})
        self.assertEqual(2, len([c for c in self.octavia.calls
                                 if c == ('POST', '/healthmonitors')]))
        # Whole already: nothing more is done.
        self.octavia.calls = []
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 5432,
            listeners=[(5433, reads)])
        self.assertEqual([], [c for c in self.octavia.calls
                              if c[0] in ('POST', 'PUT', 'DELETE')])

    def test_a_child_still_pending_when_the_balancer_says_active(self):
        # The OVN provider: the balancer ACTIVE on its own account while
        # the pool just made is PENDING_CREATE; the health monitor on it
        # (and the members into it) wait for the pool itself.
        self.octavia.balancer_first = True
        reads = [_member('10.0.0.2', port=5434)]
        lb = loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 5432,
            listeners=[(5433, reads)])
        self.assertEqual('ACTIVE', lb['provisioning_status'])
        self.assertEqual(2, len(self.octavia.monitors))
        calls = [' '.join(c) for c in self.octavia.calls]
        # Each child is looked at before the next request touches it.
        for child in ('li-2', 'pool-3', 'li-5', 'pool-6'):
            kind = 'listeners' if child.startswith('li') else 'pools'
            looked = calls.index('GET /%s/%s' % (kind, child))
            touched = next(i for i, c in enumerate(calls)
                           if i > looked and c.startswith(('POST', 'PUT')))
            self.assertLess(looked, touched)
        self.assertEqual(
            ['POST /loadbalancers', 'POST /listeners', 'POST /pools',
             'POST /healthmonitors', 'PUT /pools/pool-3/members',
             'POST /listeners', 'POST /pools', 'POST /healthmonitors',
             'PUT /pools/pool-6/members'],
            [c for c in calls if c.startswith(('POST', 'PUT'))])

    def test_a_child_that_fails(self):
        self.octavia.balancer_first = True
        self.octavia._looked_at = lambda child: dict(
            child, provisioning_status='ERROR')
        err = self.assertRaises(
            loadbalancer.LoadBalancerError,
            loadbalancer.ensure_load_balancer,
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        self.assertIn('listener li-2', str(err))
        self.assertIn('provisioning failed', str(err))

    def test_the_read_listener_is_added_to_a_balancer_that_lacks_it(self):
        self.octavia.active_lb('trove-cluster-c1', port=5432,
                               members=self.members)
        reads = [_member('10.0.0.2', port=5434)]
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 5432,
            listeners=[(5433, reads)])
        self.assertEqual(
            ['POST /listeners', 'POST /pools', 'POST /healthmonitors',
             'PUT /pools/pool-5/members'],
            [' '.join(c) for c in self.octavia.calls
             if c[0] in ('POST', 'PUT', 'DELETE')])
        self.assertEqual(1, len(self.octavia.lbs))

    def test_leaves_a_whole_one_alone(self):
        self.octavia.active_lb('trove-cluster-c1', members=self.members)
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        self.assertEqual([], [c for c in self.octavia.calls
                              if c[0] in ('POST', 'PUT', 'DELETE')])

    def test_replaces_the_members_that_differ(self):
        self.octavia.active_lb('trove-cluster-c1',
                               members=[_member('10.0.0.1'),
                                        _member('10.0.0.9')])
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        puts = [c for c in self.octavia.calls if c[0] == 'PUT']
        self.assertEqual(1, len(puts))
        self.assertEqual({'10.0.0.1', '10.0.0.2'},
                         {m['address'] for m in list(
                             self.octavia.members.values())[0]})

    def test_makes_a_failed_one_again(self):
        lb = self.octavia.active_lb('trove-cluster-c1')
        lb['provisioning_status'] = 'ERROR'
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        calls = [' '.join(c) for c in self.octavia.calls]
        self.assertIn('DELETE /loadbalancers/%s' % lb['id'], calls)
        self.assertIn('POST /loadbalancers', calls)
        self.assertNotIn(lb['id'], self.octavia.lbs)

    def test_makes_a_half_made_one_again(self):
        # A load balancer without the listener: left from a failure.
        lb = self.octavia.active_lb('trove-cluster-c1')
        lb['listeners'] = []
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        self.assertNotIn(lb['id'], self.octavia.lbs)
        self.assertEqual(1, len(self.octavia.lbs))

    def test_waits_for_a_pending_one(self):
        lb = self.octavia.active_lb('trove-cluster-c1', members=self.members)
        lb['provisioning_status'] = 'PENDING_UPDATE'
        self.octavia.statuses = ['PENDING_UPDATE', 'PENDING_UPDATE',
                                 'ACTIVE']
        loadbalancer.ensure_load_balancer(
            self.client, 'trove-cluster-c1', 'sub-1', self.members, 3306)
        self.assertEqual([], [c for c in self.octavia.calls
                              if c[0] in ('POST', 'PUT', 'DELETE')])

    def test_a_create_that_never_finishes(self):
        self.octavia.statuses = ['PENDING_CREATE'] * 30
        self.assertRaises(loadbalancer.LoadBalancerError,
                          loadbalancer.ensure_load_balancer, self.client,
                          'trove-cluster-c1', 'sub-1', self.members, 3306)

    def test_a_create_that_fails(self):
        self.octavia.statuses = ['PENDING_CREATE', 'ERROR']
        self.assertRaises(loadbalancer.LoadBalancerError,
                          loadbalancer.ensure_load_balancer, self.client,
                          'trove-cluster-c1', 'sub-1', self.members, 3306)

    def test_delete(self):
        lb = self.octavia.active_lb('trove-cluster-c1')
        loadbalancer.delete_load_balancer(self.client, 'trove-cluster-c1')
        self.assertNotIn(lb['id'], self.octavia.lbs)
        # Nothing to delete is fine.
        loadbalancer.delete_load_balancer(self.client, 'trove-cluster-c1')

    def test_endpoint(self):
        self.assertIsNone(loadbalancer.find_endpoint(
            self.client, 'trove-cluster-c1', 3306))
        self.octavia.active_lb('trove-cluster-c1')
        self.assertEqual({'address': '10.0.0.50', 'port': 3306,
                          'status': 'ONLINE'},
                         loadbalancer.find_endpoint(
                             self.client, 'trove-cluster-c1', 3306))

    def test_an_error_carries_octavia_s_reason(self):
        self.client.session.request = lambda *a, **k: mock.Mock(
            status_code=409, content=b'{"faultstring": "quota"}',
            text='{"faultstring": "quota"}',
            json=lambda: {'faultstring': 'quota'})
        err = self.assertRaises(loadbalancer.LoadBalancerError,
                                self.client.find_load_balancer, 'x')
        self.assertIn('409 quota', str(err))
