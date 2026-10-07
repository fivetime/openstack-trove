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

"""Load balancers in front of clusters, through Octavia.

A cluster whose members do not all take writes, or whose clients should
not have to know which member does, gets a load balancer on the members'
subnet: its address is the cluster's endpoint. The members answer the
balancer's checks on a port they open only while they take writes, so the
balancer sends to no other.

The balancer belongs to the service project, as the members' ports do, and
is found by its name, so that nothing about it has to be kept in the
database. Octavia is reached with the service credentials' session, as
Neutron is; no client library is needed.
"""

from oslo_log import log as logging

from trove.common import cfg
from trove.common import clients_admin
from trove.common import exception
from trove.common.i18n import _
from trove.common import utils

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

API = '/v2.0/lbaas'
ACTIVE = 'ACTIVE'
ERROR = 'ERROR'
DELETED = 'DELETED'
PENDING_DELETE = 'PENDING_DELETE'
# How often a load balancer is looked at while it is being worked on.
POLL_INTERVAL = 2


class LoadBalancerError(exception.TroveError):
    message = _("Load balancer %(name)s: %(reason)s")


class LoadBalancerNotFound(LoadBalancerError):
    message = _("Load balancer %(name)s is not there.")


def cluster_load_balancer_name(cluster_id):
    return 'trove-cluster-%s' % cluster_id


class OctaviaClient(object):
    """What Trove asks of Octavia, over its REST API."""

    def __init__(self, session=None, region_name=None):
        self.session = session or clients_admin.get_keystone_session()
        self.endpoint_filter = {
            'service_type': CONF.octavia_service_type,
            'interface': CONF.octavia_endpoint_type,
            'region_name': region_name or CONF.service_credentials.region_name,
        }
        self.endpoint_override = CONF.octavia_url
        self.timeout = CONF.load_balancer_timeout

    def _request(self, method, path, json=None, params=None):
        kwargs = {'endpoint_filter': self.endpoint_filter,
                  'raise_exc': False}
        if self.endpoint_override:
            kwargs['endpoint_override'] = self.endpoint_override
        if json is not None:
            kwargs['json'] = json
        if params:
            kwargs['params'] = params
        response = self.session.request(API + path, method, **kwargs)
        if response.status_code == 404:
            raise LoadBalancerNotFound(name=path)
        if response.status_code >= 400:
            try:
                reason = response.json().get('faultstring') or response.text
            except Exception:
                reason = response.text
            raise LoadBalancerError(
                name=path, reason='%s %s' % (response.status_code, reason))
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # Load balancers

    def find_load_balancer(self, name):
        """The load balancer of the name, None when there is none. Should
        there be several, the newest: an older one is a leftover.
        """
        found = self._request('GET', '/loadbalancers',
                              params={'name': name})['loadbalancers']
        if len(found) > 1:
            LOG.warning("%(count)s load balancers are named %(name)s; "
                        "taking the newest.",
                        {'count': len(found), 'name': name})
            found.sort(key=lambda lb: lb.get('created_at') or '')
        return found[-1] if found else None

    def get_load_balancer(self, lb_id):
        return self._request('GET', '/loadbalancers/%s' % lb_id)[
            'loadbalancer']

    def create_load_balancer(self, name, vip_subnet_id, provider,
                             description=None):
        body = {'loadbalancer': {
            'name': name, 'provider': provider,
            'vip_subnet_id': vip_subnet_id, 'admin_state_up': True}}
        if description:
            body['loadbalancer']['description'] = description
        return self._request('POST', '/loadbalancers', json=body)[
            'loadbalancer']

    def delete_load_balancer(self, lb_id):
        try:
            self._request('DELETE', '/loadbalancers/%s' % lb_id,
                          params={'cascade': 'true'})
        except LoadBalancerNotFound:
            pass

    def create_listener(self, lb_id, name, port):
        body = {'listener': {
            'name': name, 'loadbalancer_id': lb_id, 'protocol': 'TCP',
            'protocol_port': port, 'admin_state_up': True}}
        return self._request('POST', '/listeners', json=body)['listener']

    def get_listener(self, listener_id):
        return self._request('GET', '/listeners/%s' % listener_id)[
            'listener']

    def create_pool(self, listener_id, name):
        body = {'pool': {
            'name': name, 'listener_id': listener_id, 'protocol': 'TCP',
            'lb_algorithm': 'SOURCE_IP_PORT', 'admin_state_up': True}}
        return self._request('POST', '/pools', json=body)['pool']

    def create_health_monitor(self, pool_id, name):
        # A member is out after two failed checks, back after two good
        # ones: a failover shows on the endpoint within seconds.
        body = {'healthmonitor': {
            'name': name, 'pool_id': pool_id, 'type': 'TCP',
            'delay': 2, 'timeout': 1, 'max_retries': 2,
            'max_retries_down': 2, 'admin_state_up': True}}
        return self._request('POST', '/healthmonitors', json=body)[
            'healthmonitor']

    def list_members(self, pool_id):
        return self._request('GET', '/pools/%s/members' % pool_id)['members']

    def replace_members(self, pool_id, members):
        """Make the pool's members these and no others, in one go."""
        self._request('PUT', '/pools/%s/members' % pool_id,
                      json={'members': members})

    # Waiting

    def wait_active(self, lb_id):
        """The load balancer once Octavia is done with it."""
        def done(lb):
            status = lb['provisioning_status']
            if status == ERROR:
                raise LoadBalancerError(name=lb.get('name') or lb_id,
                                        reason='provisioning failed')
            return status == ACTIVE
        try:
            return utils.poll_until(
                lambda: self.get_load_balancer(lb_id), done,
                sleep_time=POLL_INTERVAL, time_out=self.timeout)
        except exception.PollTimeOut:
            raise LoadBalancerError(
                name=lb_id, reason='not ACTIVE after %ss' % self.timeout)

    def wait_deleted(self, lb_id):
        def gone():
            try:
                return (self.get_load_balancer(lb_id)['provisioning_status']
                        == DELETED)
            except LoadBalancerNotFound:
                return True
        try:
            utils.poll_until(gone, sleep_time=POLL_INTERVAL,
                             time_out=self.timeout)
        except exception.PollTimeOut:
            raise LoadBalancerError(
                name=lb_id, reason='not deleted after %ss' % self.timeout)


def _member_key(member):
    return (member['address'], int(member['protocol_port']),
            member.get('subnet_id'))


def _pool_for_port(client, lb, port):
    """The id of the pool behind the listener on the port, None if the
    load balancer has no such listener.
    """
    for listener in lb.get('listeners') or []:
        details = client.get_listener(listener['id'])
        if details.get('protocol_port') == port:
            return details.get('default_pool_id')
    return None


def ensure_load_balancer(client, name, vip_subnet_id, members, port,
                         provider=None, description=None):
    """A load balancer of the name on the subnet, listening on the port,
    with these members and no others. Made whole whatever it was: not
    there, half made, failed, or with other members.

    :param members: [{'name', 'address', 'protocol_port', 'subnet_id'}]
    """
    provider = provider or CONF.load_balancer_provider
    lb = client.find_load_balancer(name)
    if lb is not None:
        status = lb['provisioning_status']
        if status == PENDING_DELETE:
            client.wait_deleted(lb['id'])
            lb = None
        elif status != ACTIVE:
            lb = (client.wait_active(lb['id']) if status != ERROR
                  else lb)
    if lb is not None:
        pool_id = (_pool_for_port(client, lb, port)
                   if lb['provisioning_status'] == ACTIVE else None)
        if pool_id is None:
            LOG.warning("Load balancer %s is %s; making it again.",
                        name, lb['provisioning_status'])
            delete_load_balancer(client, name)
            lb = None

    if lb is None:
        LOG.info("Creating load balancer %s on subnet %s.", name,
                 vip_subnet_id)
        lb = client.create_load_balancer(name, vip_subnet_id, provider,
                                         description)
        client.wait_active(lb['id'])
        listener = client.create_listener(lb['id'], name, port)
        client.wait_active(lb['id'])
        pool = client.create_pool(listener['id'], name)
        client.wait_active(lb['id'])
        client.create_health_monitor(pool['id'], name)
        client.wait_active(lb['id'])
        pool_id = pool['id']
        current = []
    else:
        current = client.list_members(pool_id)

    wanted = {_member_key(m) for m in members}
    if wanted != {_member_key(m) for m in current}:
        LOG.info("Load balancer %s: members %s.", name,
                 sorted(m['address'] for m in members))
        client.replace_members(pool_id, [
            {'name': m.get('name', m['address']), 'address': m['address'],
             'protocol_port': m['protocol_port'],
             'subnet_id': m['subnet_id'], 'admin_state_up': True}
            for m in members])
        client.wait_active(lb['id'])
    return client.get_load_balancer(lb['id'])


def delete_load_balancer(client, name):
    lb = client.find_load_balancer(name)
    if lb is None:
        return
    LOG.info("Deleting load balancer %s.", name)
    client.delete_load_balancer(lb['id'])
    client.wait_deleted(lb['id'])


def find_endpoint(client, name, port):
    """Where the clients of the cluster connect, None without a load
    balancer.
    """
    lb = client.find_load_balancer(name)
    if lb is None or lb['provisioning_status'] in (DELETED, PENDING_DELETE):
        return None
    return {'address': lb['vip_address'], 'port': port,
            'status': lb.get('operating_status')}
