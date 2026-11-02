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
replacement of a lost member of a cluster of either topology.

Of the sentinel topology, the cluster's own shrink removes a member that
is gone (it keeps it from being elected if it answers, deletes it, then
has the sentinels that stay forget it), and its grow adds one. Of the
cluster topology the shrink removes it too (the nodes that stay forget
it), but the grow takes whole groups of a master and its replicas: the
member in its place is made alone, of the lost one's kind and group in
the books, and joins as a replica of the master with the fewest (the
lost one's slots went to its replica with the failover: the cluster is
short of a replica, whichever the lost one was).

Either way the plugin says when the cluster can lose the member: every
other member answers, none holds the cluster down (the sentinels know a
primary they do not hold down; the cluster's state is ok, every slot
served), and each holds the lost member out and no other
(``common.replace.PeersHoldItOut``).
"""

import time

from oslo_log import log as logging

from trove.common import clients
from trove.common.strategies.cluster.experimental.common import placement
from trove.common.strategies.cluster.experimental.common import replace
from trove.common.strategies.cluster.experimental.common import topology
from trove.quota.quota import check_quotas

LOG = logging.getLogger(__name__)

CLUSTER = 'cluster'
SENTINEL = 'sentinel'
MEMBER = 'member'


class RedisReplace(replace.PeersHoldItOut):

    @staticmethod
    def _topology(db_cluster):
        return topology.topology_of(db_cluster, CLUSTER)

    def remember(self, context, db_cluster, lost, others):
        """Of the cluster topology: the lost member's kind and group, for
        the one in its place to take in the books.
        """
        if self._topology(db_cluster) != CLUSTER:
            return {}
        return {'type': lost.type or MEMBER, 'shard_id': lost.shard_id}

    def add(self, context, db_cluster, state):
        if self._topology(db_cluster) != CLUSTER:
            return super(RedisReplace, self).add(context, db_cluster, state)
        from trove.cluster import models as cluster_models
        from trove.common import glance as common_glance
        from trove.common import server_group as srv_grp
        from trove.instance import models as inst_models
        from trove.taskmanager import api as task_api
        cluster = cluster_models.Cluster.load(context, db_cluster.id)
        spec = dict(state['spec'])
        image_id = cluster.ds_version.image_id
        if not image_id:
            image_id = common_glance.get_image_id(
                clients.create_glance_client(context), None,
                cluster.ds_version.image_tags)
        deltas = {'instances': 1}
        if spec.get('volume_size'):
            deltas['volumes'] = spec['volume_size']
        check_quotas(context.project_id, deltas)
        cluster_config = {'id': cluster.id, 'instance_type': state['type'],
                          'shard_id': state['shard_id']}
        # The group of the lost member's master and replicas, when the
        # cluster is placed by group; else the cluster's one group.
        locality = placement.locality_for(
            context, cluster.db_info, cluster_config,
            srv_grp.ServerGroup.convert_to_hint(cluster.server_group))
        instance = inst_models.Instance.create(
            context, '%s-node-%d' % (cluster.name, int(time.time())),
            spec['flavor_id'], image_id, [], [], cluster.ds,
            cluster.ds_version, spec.get('volume_size'), None,
            spec.get('availability_zone'), spec.get('nics'),
            configuration_id=None,
            cluster_config=cluster_config,
            volume_type=spec.get('volume_type'), locality=locality)
        task_api.load(context, self.manager).add_node(
            self.manager, db_cluster.id, instance.id)

    # The flags of a node that is not to be counted on.
    FAILING = frozenset(['fail', 'fail?', 'noaddr', 'handshake'])

    def spread(self, context, db_cluster, members, host_of, candidates=None):
        """cluster_redis_spread_masters: of a cluster of the topology
        cluster whose host has more than one master, the replica that is
        to take one of their places: a replica of that master on a host
        without a master. Returns (member, reason), or None.

        With no such replica (every replica on a host with a master: on
        the Incus placement round, 2026-11-02, the six members of a
        cluster were on two hosts after an evacuation, Nova picking at
        random among the hosts its groups allow), a replica to move first:
        (member, reason, host), the host one ``candidates(member)`` gives
        (hosts that can take the member) without a master and without a
        member of the replica's shard, a host without any member first.
        The move is a cold migration; the failover follows on a later
        look.

        A failover needs most masters to vote, so a host with most of them
        takes the whole cluster down with it (Incus migration matrix,
        2026-11-01: 2 of 3 masters on one host after a failover); the
        failovers move the masters about, which no server group follows.
        ``host_of`` gives a member's host.
        """
        if self._topology(db_cluster) != CLUSTER:
            return None
        guests = {m.id: self.guest_factory(context, m.id, self.manager)
                  for m in members if m.compute_instance_id}
        nodes = None
        member_of = {}
        for member in members:
            guest = guests.get(member.id)
            if guest is None:
                continue
            try:
                member_of[guest.get_node_id()] = member
                if nodes is None:
                    nodes = guest.get_cluster_nodes()
            except Exception as err:
                # A member that does not tell: no move on a partial view.
                LOG.debug("Member %(member)s does not tell its node: "
                          "%(err)s", {'member': member.id, 'err': err})
                return None
        if not nodes:
            return None
        hosts = {}

        def host(node_id):
            member = member_of.get(node_id)
            if member is None:
                return None
            if member.id not in hosts:
                hosts[member.id] = host_of(member)
            return hosts[member.id]

        masters = [n for n in nodes if n['role'] == 'master' and
                   n['has_slots'] and not self.FAILING & set(n['flags'])]
        by_host = {}
        for node in masters:
            by_host.setdefault(host(node['id']), []).append(node)
        if None in by_host:
            # A master whose host is not known: no move.
            return None
        master_hosts = set(by_host)
        crowded_hosts = sorted(h for h, ns in by_host.items() if len(ns) > 1)
        if not crowded_hosts:
            return None
        for crowded in crowded_hosts:
            for master in sorted(by_host[crowded], key=lambda n: n['id']):
                for node in sorted(nodes, key=lambda n: n['id']):
                    if (node['role'] != 'replica' or
                            node['master_id'] != master['id'] or
                            self.FAILING & set(node['flags'])):
                        continue
                    where = host(node['id'])
                    if where is None or where in master_hosts:
                        continue
                    replica = member_of[node['id']]
                    return replica, (
                        '%d masters on %s; the replica %s on %s takes the '
                        'place of %s' % (
                            len(by_host[crowded]), crowded, replica.name,
                            where, member_of[master['id']].name))
        occupied = {host(n['id']) for n in nodes}
        for crowded in crowded_hosts:
            for master in sorted(by_host[crowded], key=lambda n: n['id']):
                shard = [master] + [n for n in nodes
                                    if n['master_id'] == master['id']]
                shard_hosts = {host(n['id']) for n in shard}
                for node in sorted(shard[1:], key=lambda n: n['id']):
                    if node['role'] != 'replica' or \
                            self.FAILING & set(node['flags']):
                        continue
                    replica = member_of[node['id']]
                    options = [h for h in (candidates(replica)
                                           if candidates else ())
                               if h not in master_hosts and
                               h not in shard_hosts]
                    if not options:
                        continue
                    target = sorted(options,
                                    key=lambda h: (h in occupied, h))[0]
                    return replica, (
                        '%d masters on %s and no replica of theirs on a host '
                        'without a master; the replica %s moves from %s to '
                        '%s first' % (len(by_host[crowded]), crowded,
                                      replica.name, host(node['id']),
                                      target)), target
        LOG.warning("Cluster %(cluster)s: more than one master on "
                    "%(hosts)s, and no replica of theirs that can take one's "
                    "place or move to a host without a master.",
                    {'cluster': db_cluster.id,
                     'hosts': ', '.join(crowded_hosts)})
        return None

    def failover(self, context, member):
        """The member, a replica, takes its master's place."""
        self.guest_factory(context, member.id, self.manager).cluster_failover()
