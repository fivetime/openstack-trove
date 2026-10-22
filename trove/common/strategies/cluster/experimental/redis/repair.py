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

from trove.common import clients
from trove.common.strategies.cluster.experimental.common import replace
from trove.common.strategies.cluster.experimental.common import topology
from trove.quota.quota import check_quotas

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
        locality = srv_grp.ServerGroup.convert_to_hint(cluster.server_group)
        instance = inst_models.Instance.create(
            context, '%s-node-%d' % (cluster.name, int(time.time())),
            spec['flavor_id'], image_id, [], [], cluster.ds,
            cluster.ds_version, spec.get('volume_size'), None,
            spec.get('availability_zone'), spec.get('nics'),
            configuration_id=None,
            cluster_config={'id': cluster.id,
                            'instance_type': state['type'],
                            'shard_id': state['shard_id']},
            volume_type=spec.get('volume_type'), locality=locality)
        task_api.load(context, self.manager).add_node(
            self.manager, db_cluster.id, instance.id)
