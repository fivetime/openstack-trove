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
    time, ACTIVE as soon as it is looked at.
    """

    def __init__(self):
        self.lbs = {}
        self.listeners = {}
        self.pools = {}
        self.members = {}
        self.calls = []
        self.next_id = 0
        # Statuses the next GETs of a load balancer answer with.
        self.statuses = []

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
            return self._response(200, {'loadbalancer': lb})
        if method == 'POST' and path == '/listeners':
            listener = dict(body['listener'], id=self._id('li'),
                            default_pool_id=None)
            self.listeners[listener['id']] = listener
            self.lbs[listener['loadbalancer_id']]['listeners'].append(
                {'id': listener['id']})
            return self._response(201, {'listener': listener})
        if parts[0] == 'listeners' and len(parts) == 2:
            return self._response(200, {'listener': self.listeners[parts[1]]})
        if method == 'POST' and path == '/pools':
            pool = dict(body['pool'], id=self._id('pool'))
            self.pools[pool['id']] = pool
            self.listeners[pool['listener_id']]['default_pool_id'] = pool['id']
            self.members[pool['id']] = []
            return self._response(201, {'pool': pool})
        if method == 'POST' and path == '/healthmonitors':
            return self._response(201, {'healthmonitor': dict(
                body['healthmonitor'], id=self._id('hm'))})
        if parts[0] == 'pools' and parts[-1] == 'members':
            if method == 'GET':
                return self._response(200, {'members': self.members[parts[1]]})
            self.members[parts[1]] = body['members']
            return self._response(202)
        raise AssertionError('unexpected %s %s' % (method, path))

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
        self.pools[listener['default_pool_id']] = {'id': listener[
            'default_pool_id']}
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
