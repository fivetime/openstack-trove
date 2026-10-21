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

"""The repair's plugin of the Redis family (Redis, Valkey, KeyDB): the
replacement of a lost member of a cluster of the sentinel topology.

The cluster's own shrink removes a member that is gone (it keeps it from
being elected if it answers, deletes it, then has the sentinels that stay
forget it), and its grow adds one; the plugin only says when the cluster
can lose the member: every other member answers, their sentinels know a
primary they do not hold down, and they hold the lost member out and no
other. A cluster of the cluster topology is not replaced (yet).
"""

from oslo_log import log as logging

from trove.common import cfg
from trove.common import clients
from trove.common import cluster_health
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import replace
from trove.common.strategies.cluster.experimental.common import topology

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

SENTINEL = 'sentinel'


class SentinelReplace(replace.ReplacePlugin):

    def __init__(self, manager, guest_factory=None):
        self.manager = manager
        self.guest_factory = guest_factory or clients.create_guest_client

    def can_replace(self, context, db_cluster, lost, others):
        if db_cluster.get_properties().get(topology.TOPOLOGY) != SENTINEL:
            return False, 'only a cluster of the sentinel topology'
        if len(others) < 2:
            return False, 'fewer than two members would be left'
        lost_ip = next((ip for ip, name in
                        health.known_members(db_cluster.id).items()
                        if name == lost.name), None)
        if not lost_ip:
            return False, 'its address is not known'
        timeout = CONF.get(self.manager).cluster_health_timeout
        for member in others:
            try:
                view = self.guest_factory(
                    context, member.id, self.manager).get_cluster_health(
                        timeout=timeout)
            except Exception as err:
                return False, 'member %s does not answer: %s' % (
                    member.name, err)
            if not view:
                return False, 'member %s tells nothing' % member.name
            if view.get('status') == cluster_health.DOWN:
                return False, 'member %s holds the cluster down: %s' % (
                    member.name, view.get('reason'))
            out = set(view.get('out') or [])
            if out != {lost_ip}:
                return False, 'member %s holds out %s, not %s alone' % (
                    member.name, ', '.join(sorted(out)) or 'none', lost_ip)
        return True, 'every other member holds %s out' % lost_ip
