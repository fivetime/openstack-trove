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

"""The placement of a cluster's members by group (cluster_placement_per_
shard): a server group for each set of members that must not share a host,
instead of one for the whole cluster.

A Redis family cluster of the topology cluster has a master and its
replicas to keep apart, a MongoDB cluster each replica set, its config
servers and its query routers. Such clusters have more members than hosts
often, so they are created soft-anti-affinity, and a soft group of the
whole cluster keeps a master and its replica apart no better than any two
members: one host took a whole shard with it (Incus migration matrix,
2026-11-01).

A member's group is the group of its key: the shard_id of a member of a
shard, else its instance_type. The groups are made as the members are,
and kept in the cluster's properties (``placement_groups``, a group id by
key), with the policy (``placement_policy``) and the locality the cluster
was created with (``placement_locality``). A cluster without them keeps
its one group: every helper here gives back the hint it was given then.
"""

from oslo_log import log as logging

from trove.common import cfg
from trove.common.clients import create_nova_client
from trove.common import exception
from trove.common.i18n import _
from trove.common import server_group as srv_grp

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

GROUPS = 'placement_groups'
POLICY = 'placement_policy'
LOCALITY = 'placement_locality'
STRICT = 'strict'
ANTI_AFFINITY = 'anti-affinity'

# The cluster class attribute that asks for it: True, or a callable of the
# cluster's extended properties.
ATTRIBUTE = 'SHARD_PLACEMENT'


class ShardPlacement(object):
    """What the cluster's create gets for its locality when its members are
    placed by group: the policy of the groups, and the locality asked for.
    """

    def __init__(self, locality):
        self.locality = locality
        self.policy = (ANTI_AFFINITY
                       if CONF.cluster_placement_shard_policy == STRICT
                       else locality)

    def __repr__(self):
        return 'ShardPlacement(%s, %s)' % (self.locality, self.policy)


def wanted(cluster_class, extended_properties):
    """Whether a cluster of the class, with these properties, is placed by
    group: the operator lets it and the class (the class of the topology
    the properties pick, for a class of several topologies) asks for it.
    """
    if not CONF.cluster_placement_per_shard:
        return False
    topologies = getattr(cluster_class, 'TOPOLOGIES', None)
    if topologies:
        from trove.common.strategies.cluster.experimental.common import (
            topology)
        name = ((extended_properties or {}).get(topology.TOPOLOGY)
                or cluster_class.DEFAULT)
        cluster_class = topologies.get(name)
        if cluster_class is None:
            return False
    asks = getattr(cluster_class, ATTRIBUTE, False)
    if callable(asks):
        return bool(asks(extended_properties or {}))
    return bool(asks)


def start(context, db_info, locality, groups):
    """At a cluster's create, once its record is: for a ShardPlacement,
    the properties that place its members by group, after a check that
    the project can have ``groups`` more server groups. Returns the hint
    the members are given otherwise: the locality itself, or None.
    """
    if not isinstance(locality, ShardPlacement):
        return locality
    check_quota(context, groups)
    properties = db_info.get_properties()
    properties.update({GROUPS: {}, POLICY: locality.policy,
                       LOCALITY: locality.locality})
    db_info.save_properties(properties)
    return None


def check_quota(context, groups):
    """A 400 unless the project can have ``groups`` more server groups: a
    cluster that cannot have them all would be built half placed.
    """
    try:
        limits = create_nova_client(context).limits.get().absolute
        values = {limit.name: limit.value for limit in limits}
    except Exception as err:
        LOG.warning("The server group quota could not be read: %s", err)
        return
    allowed = values.get('maxServerGroups')
    used = values.get('totalServerGroupsUsed')
    if allowed is None or used is None or allowed < 0:
        return
    if used + groups > allowed:
        raise exception.BadRequest(_(
            "The cluster's members are placed in %(groups)d server groups "
            "(cluster_placement_per_shard), and the project has "
            "%(left)d of its %(allowed)d left: its quota server_groups "
            "needs raising.") % {'groups': groups,
                                 'left': max(allowed - used, 0),
                                 'allowed': allowed})


def key_of(cluster_config):
    """The key of a member's group, from its cluster config."""
    return cluster_config.get('shard_id') or cluster_config.get(
        'instance_type')


def placed(db_info):
    """Whether the cluster's members are placed by group."""
    return GROUPS in db_info.get_properties()


def locality_for(context, db_info, cluster_config, fallback):
    """The scheduler hint of a member of the cluster: its group's, made
    and kept if the cluster has none for its key yet, for a cluster placed
    by group; ``fallback`` (the cluster's one group) otherwise.
    """
    properties = db_info.get_properties()
    groups = properties.get(GROUPS)
    if groups is None:
        return fallback
    key = key_of(cluster_config)
    group_id = groups.get(key)
    if not group_id:
        name = '%s_%s' % (db_info.name, str(key)[:8])
        group = srv_grp.ServerGroup.create(
            context, properties.get(POLICY) or ANTI_AFFINITY, name)
        group_id = group.id
        # Read again: another member's group may have been kept since.
        properties = db_info.get_properties()
        properties.setdefault(GROUPS, {})[key] = group_id
        db_info.save_properties(properties)
        LOG.info("Cluster %(cluster)s: server group %(group)s for the "
                 "members of %(key)s.", {'cluster': db_info.id,
                                         'group': group_id, 'key': key})
    return {'group': group_id}


def delete(context, db_info):
    """Delete the cluster's groups (each, its members or not)."""
    groups = db_info.get_properties().get(GROUPS) or {}
    if not groups:
        return
    client = create_nova_client(context)
    for key, group_id in sorted(groups.items()):
        try:
            client.server_groups.delete(group_id)
            LOG.info("Cluster %(cluster)s: deleted server group %(group)s "
                     "of %(key)s.", {'cluster': db_info.id,
                                     'group': group_id, 'key': key})
        except Exception as err:
            LOG.warning("Cluster %(cluster)s: server group %(group)s not "
                        "deleted: %(err)s", {'cluster': db_info.id,
                                             'group': group_id, 'err': err})


def locality(db_info):
    """The locality the cluster was created with, when placed by group."""
    return db_info.get_properties().get(LOCALITY)
