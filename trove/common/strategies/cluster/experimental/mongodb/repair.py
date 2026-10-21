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

"""The repair's plugin of MongoDB clusters: the replacement of a lost
member of a shard's replica set.

A shard is a replica set of three; the cluster's shrink and grow take
whole shards (and query routers), so a lost member is replaced within its
set: the set's primary drops it from the set's configuration and it is
deleted (the task manager's ``remove_member``), then a member like it is
made for the same shard and the primary adds it (``add_member``). The
members that vouch are the set's other members: each answers, their set
has a primary, and each holds the lost member out and no other
(``common.replace.PeersHoldItOut``); the members of other shards, the
config servers and the query routers see nothing of it. A member of the
config server set and a query router are not replaced (yet).
"""

import time

from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import replace

MEMBER = 'member'


class MongoDbReplace(replace.PeersHoldItOut):

    @staticmethod
    def _set_of(lost, others):
        return [m for m in others
                if m.shard_id == lost.shard_id and m.type == MEMBER]

    def can_replace(self, context, db_cluster, lost, others):
        if getattr(lost, 'type', None) != MEMBER:
            return False, 'only a member of a shard is replaced, not a %s' % (
                lost.type or 'member of no type')
        return super(MongoDbReplace, self).can_replace(
            context, db_cluster, lost, self._set_of(lost, others))

    def remember(self, context, db_cluster, lost, others):
        """Its shard, its address in the set (host:port, as the set's
        configuration names it) and the set's name, for the steps.
        """
        host = next((name for name, trove_name in
                     health.known_members(db_cluster.id).items()
                     if trove_name == lost.name), None)
        if not host:
            raise exception.TroveError(_("Its address is not known."))
        peers = self._set_of(lost, others)
        replica_set_name = self.guest_factory(
            context, peers[0].id, self.manager).get_replica_set_name()
        return {'shard_id': lost.shard_id, 'host': host,
                'replica_set_name': replica_set_name}

    def remove(self, context, db_cluster, state):
        from trove.taskmanager import api as task_api
        task_api.load(context, self.manager).mongodb_remove_member(
            db_cluster.id, state['lost'], state['host'])

    def add(self, context, db_cluster, state):
        from trove.cluster import models as cluster_models
        from trove.common import server_group as srv_grp
        from trove.taskmanager import api as task_api
        cluster = cluster_models.Cluster.load(context, db_cluster.id)
        spec = dict(state['spec'])
        spec.setdefault('name', '%s-%s-%d' % (
            cluster.name, state['replica_set_name'], int(time.time())))
        cluster_config = {'shard_id': state['shard_id'],
                          'instance_type': MEMBER,
                          'replica_set_name': state['replica_set_name']}
        locality = srv_grp.ServerGroup.convert_to_hint(cluster.server_group)
        instance_ids = cluster._create_instances(
            [spec], cluster_config, state['replica_set_name'], locality)
        task_api.load(context, self.manager).mongodb_add_member(
            db_cluster.id, instance_ids[0])
