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
member of a shard's replica set, of the config server set, or a lost
query router.

A shard is a replica set of three, and so are the config servers; the
cluster's shrink and grow take whole shards (and query routers), so a
lost member is replaced within its set: the set's primary drops it from
the set's configuration and it is deleted (the task manager's
``remove_member``), then a member like it is made for the same set and
the primary adds it (``add_member``; a new config server is told to the
query routers too). The members that vouch are the set's other members:
each answers, their set has a primary, and each holds the lost member
out and no other (``common.replace.PeersHoldItOut``); the members of
other sets and the query routers see nothing of it.

A query router is of no set and has no peers to ask: Nova's word on its
server is enough, it is deleted and one like it is made and pointed at
the config servers.
"""

import time

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import replace

CONF = cfg.CONF
MEMBER = 'member'
CONFIG_SERVER = 'config_server'
QUERY_ROUTER = 'query_router'
# The replica sets: a member of one is replaced within it.
OF_A_SET = (MEMBER, CONFIG_SERVER)


class MongoDbReplace(replace.PeersHoldItOut):

    @staticmethod
    def _set_of(lost, others):
        """The other members of the lost one's replica set: of its shard,
        or the other config servers.
        """
        return [m for m in others
                if m.type == lost.type and
                (lost.type != MEMBER or m.shard_id == lost.shard_id)]

    def can_replace(self, context, db_cluster, lost, others):
        kind = getattr(lost, 'type', None)
        if kind == QUERY_ROUTER:
            return True, ("a query router has no peers to ask: Nova's word "
                          "on its server is enough")
        if kind not in OF_A_SET:
            return False, 'a %s is not replaced' % (
                kind or 'member of no type')
        return super(MongoDbReplace, self).can_replace(
            context, db_cluster, lost, self._set_of(lost, others))

    def vouches(self, member, view, lost_name):
        # A set without a primary (the config servers' is DEGRADED then,
        # the cluster goes on without it) cannot drop or add a member.
        if 'has no primary' in (view.get('reason') or ''):
            return False, 'member %s: %s' % (member.name, view['reason'])
        return super(MongoDbReplace, self).vouches(member, view, lost_name)

    def remember(self, context, db_cluster, lost, others):
        """Of a set's member: its shard, its address in the set
        (host:port, as the set's configuration names it) and the set's
        name, for the steps. Of a query router: nothing.
        """
        if lost.type == QUERY_ROUTER:
            return {'type': QUERY_ROUTER}
        host = next((name for name, trove_name in
                     health.known_members(db_cluster.id).items()
                     if trove_name == lost.name), None)
        if not host:
            raise exception.TroveError(_("Its address is not known."))
        peers = self._set_of(lost, others)
        replica_set_name = self.guest_factory(
            context, peers[0].id, self.manager).get_replica_set_name()
        return {'type': lost.type, 'shard_id': lost.shard_id, 'host': host,
                'replica_set_name': replica_set_name}

    def remove(self, context, db_cluster, state):
        from trove.taskmanager import api as task_api
        task_api.load(context, self.manager).mongodb_remove_member(
            db_cluster.id, state['lost'], state.get('host'))

    def add(self, context, db_cluster, state):
        from trove.cluster import models as cluster_models
        from trove.common import server_group as srv_grp
        from trove.taskmanager import api as task_api
        cluster = cluster_models.Cluster.load(context, db_cluster.id)
        spec = dict(state['spec'])
        kind = state.get('type', MEMBER)
        locality = srv_grp.ServerGroup.convert_to_hint(cluster.server_group)
        # The cluster's key, for the new member: _create_instances asks
        # it of a query router the grow looks up first (_prep_resize),
        # which no step of a replacement does. Any member that stays has
        # it too.
        key = None
        if CONF.get(self.manager).cluster_secure:
            peer = next(i for i in cluster.db_instances
                        if i.id != state['lost'])
            key = self.guest_factory(context, peer.id, self.manager).get_key()
        if kind == QUERY_ROUTER:
            spec.setdefault('name', '%s-mongos-%d' % (cluster.name,
                                                      int(time.time())))
            instance_ids = cluster._create_query_router_instances(
                [spec], locality, key=key)
        elif kind == CONFIG_SERVER:
            spec.setdefault('name', '%s-configsvr-%d' % (cluster.name,
                                                         int(time.time())))
            instance_ids = cluster._create_instances(
                [spec], {'instance_type': CONFIG_SERVER}, 'configsvr',
                locality, key=key)
        else:
            spec.setdefault('name', '%s-%s-%d' % (
                cluster.name, state['replica_set_name'], int(time.time())))
            cluster_config = {'shard_id': state['shard_id'],
                              'instance_type': MEMBER,
                              'replica_set_name': state['replica_set_name']}
            instance_ids = cluster._create_instances(
                [spec], cluster_config, state['replica_set_name'], locality,
                key=key)
        task_api.load(context, self.manager).mongodb_add_member(
            db_cluster.id, instance_ids[0])
