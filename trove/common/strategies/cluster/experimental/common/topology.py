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

"""A datastore whose clusters come in more than one shape: the Redis
family's sharded Redis Cluster, or a primary with replicas and sentinels.

Trove picks a cluster's classes by its datastore manager alone: the API
strategy's ``cluster_class``, ``cluster_view_class`` and
``mgmt_cluster_view_class``, the task manager strategy's
``task_manager_cluster_tasks_class``. A datastore with topologies gives
each of these as a dispatcher (``clusters``, ``views``) over a class per
topology. A new cluster takes its topology from the extended property
``topology`` (the default when it has none), which the dispatcher keeps
in the cluster's properties; a cluster loaded later takes the one it
keeps. A cluster from before the datastore had topologies keeps none and
is of the default topology: nothing changes for it.
"""

from trove.common import exception
from trove.common.i18n import _

TOPOLOGY = 'topology'


def topology_of(db_info, default):
    """The topology a cluster was created with."""
    get = getattr(db_info, 'get_properties', None)
    properties = (get() if get is not None else None) or {}
    return properties.get(TOPOLOGY) or default


def _pick(classes, topology):
    try:
        return classes[topology]
    except KeyError:
        raise exception.BadRequest(_(
            "Unknown cluster topology %(topology)s: one of %(known)s.") % {
                'topology': topology, 'known': ', '.join(sorted(classes))})


def clusters(classes, default):
    """A cluster class (the API's or the task manager's) that is the class
    of the cluster's topology: ``classes`` maps each topology to its own.
    """

    class TopologyCluster(object):
        TOPOLOGIES = dict(classes)
        DEFAULT = default

        def __new__(cls, context, db_info, *args, **kwargs):
            clazz = _pick(cls.TOPOLOGIES, topology_of(db_info, cls.DEFAULT))
            return clazz(context, db_info, *args, **kwargs)

        @classmethod
        def create(cls, context, name, datastore, datastore_version,
                   instances, extended_properties, locality,
                   configuration, image_id=None):
            props = dict(extended_properties or {})
            topology = props.pop(TOPOLOGY, None) or cls.DEFAULT
            clazz = _pick(cls.TOPOLOGIES, topology)
            cluster = clazz.create(context, name, datastore,
                                   datastore_version, instances, props,
                                   locality, configuration, image_id)
            if topology != cls.DEFAULT:
                kept = cluster.db_info.get_properties()
                kept[TOPOLOGY] = topology
                cluster.db_info.save_properties(kept)
            return cluster

    return TopologyCluster


def views(classes, default):
    """A cluster view class that is the view of the cluster's topology."""

    class TopologyView(object):
        TOPOLOGIES = dict(classes)
        DEFAULT = default

        def __new__(cls, cluster, *args, **kwargs):
            clazz = _pick(cls.TOPOLOGIES,
                          topology_of(cluster.db_info, cls.DEFAULT))
            return clazz(cluster, *args, **kwargs)

    return TopologyView
