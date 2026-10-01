# Copyright [2015] Hewlett-Packard Development Company, L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

from oslo_log import log as logging

from trove.cluster import models
from trove.cluster.tasks import ClusterTasks
from trove.cluster.views import ClusterView
from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common import server_group as srv_grp
from trove.common.strategies.cluster import base
from trove.common import utils
from trove.extensions.mgmt.clusters.views import MgmtClusterView
from trove.instance import models as inst_models
from trove.quota.quota import check_quotas
from trove.taskmanager import api as task_api
LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class RedisAPIStrategy(base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return RedisCluster

    @property
    def cluster_view_class(self):
        return RedisClusterView

    @property
    def mgmt_cluster_view_class(self):
        return RedisMgmtClusterView


def replica_groups(num_instances, replicas_per_master, existing_masters=0):
    """Split the instances into groups of a master and its replicas.

    A master without a replica takes the whole cluster down with it (no
    other node serves its slots). With replicas, a failover needs most
    masters to vote, so a cluster of fewer than three masters cannot have
    one.
    """
    try:
        replicas = int(replicas_per_master or 0)
    except (TypeError, ValueError):
        replicas = -1
    if replicas < 0:
        raise exception.BadRequest(_(
            "replicas_per_master must be a whole number, 0 or more."))
    if num_instances % (1 + replicas):
        raise exception.BadRequest(_(
            "With %(r)s replica(s) per master the instances come in groups "
            "of %(g)s: a master and its replicas.") %
            {'r': replicas, 'g': 1 + replicas})
    masters = num_instances // (1 + replicas)
    if replicas and existing_masters + masters < 3:
        raise exception.BadRequest(_(
            "A cluster with replicas needs at least 3 masters: a failover "
            "needs most of them to agree."))
    return replicas, masters


class RedisCluster(models.Cluster):

    @staticmethod
    def _create_instances(context, db_info, datastore, datastore_version,
                          instances, extended_properties, locality,
                          image_id=None, replicas_per_master=0):
        redis_conf = CONF.get(datastore_version.manager)
        ephemeral_enabled = redis_conf.device_path
        volume_enabled = redis_conf.volume_support

        num_instances = len(instances)

        models.validate_instance_flavors(
            context, instances, volume_enabled, ephemeral_enabled)

        total_volume_allocation = models.get_required_volume_size(
            instances, volume_enabled)

        models.assert_homogeneous_cluster(instances)

        models.validate_instance_nics(context, instances)

        # Number on from the members there are: a grown cluster's new
        # member was named member-1 again.
        name_index = 1 + max(
            [int(m.group(1)) for m in (
                re.search(r'-member-(\d+)$', db_instance.name or '')
                for db_instance in inst_models.DBInstance.find_all(
                    cluster_id=db_info.id, deleted=False).all())
             if m] or [0])
        # Each master comes first in its group, its replicas after it; a
        # group shares a shard_id, which tells the taskmanager whose
        # replica a node is.
        group_size = 1 + replicas_per_master
        configs = []
        for index, instance in enumerate(instances):
            position = index % group_size
            if position == 0:
                shard_id = utils.generate_uuid()
                master_name = "%s-member-%s" % (db_info.name, name_index)
                name_index += 1
            if not instance.get('name'):
                instance['name'] = (
                    master_name if position == 0 else
                    "%s-replica-%s" % (master_name, position))
            configs.append({"id": db_info.id,
                            "instance_type": ("member" if position == 0
                                              else "replica"),
                            "shard_id": shard_id})

        # Check quotas
        quota_request = {'instances': num_instances,
                         'volumes': total_volume_allocation}
        check_quotas(context.project_id, quota_request)

        # The version is registered by image tags, so its image_id is
        # empty; the API resolved the image from the tags.
        image_id = datastore_version.image_id or image_id

        # Creating member instances
        return [inst_models.Instance.create(context,
                                            instance['name'],
                                            instance['flavor_id'],
                                            image_id,
                                            [], [],
                                            datastore, datastore_version,
                                            instance.get('volume_size'),
                                            None,
                                            instance.get(
                                                'availability_zone', None),
                                            instance.get('nics', None),
                                            configuration_id=None,
                                            cluster_config=config,
                                            volume_type=instance.get(
                                                'volume_type', None),
                                            modules=instance.get('modules'),
                                            locality=locality,
                                            region_name=instance.get(
                                                'region_name')
                                            )
                for instance, config in zip(instances, configs)]

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality, configuration,
               image_id=None):
        LOG.debug("Initiating cluster creation.")

        if configuration:
            raise exception.ConfigurationNotSupported()

        replicas, _masters = replica_groups(
            len(instances),
            (extended_properties or {}).get('replicas_per_master'))

        # Updating Cluster Task

        db_info = models.DBCluster.create(
            name=name, tenant_id=context.project_id,
            datastore_version_id=datastore_version.id,
            task_status=ClusterTasks.BUILDING_INITIAL)

        cls._create_instances(context, db_info, datastore, datastore_version,
                              instances, extended_properties, locality,
                              image_id=image_id,
                              replicas_per_master=replicas)

        # Calling taskmanager to further proceed for cluster-configuration
        task_api.load(context, datastore_version.manager).create_cluster(
            db_info.id)

        return RedisCluster(context, db_info, datastore, datastore_version)

    def upgrade(self, datastore_version):
        self.rolling_upgrade(datastore_version)

    def grow(self, instances, image_id=None):
        LOG.debug("Growing cluster.")

        self.validate_cluster_available()

        context = self.context
        db_info = self.db_info
        datastore = self.ds
        datastore_version = self.ds_version

        # A cluster with replicas grows by whole groups, as many replicas
        # per master as it has.
        members = inst_models.DBInstance.find_all(
            cluster_id=db_info.id, deleted=False).all()
        masters = len([m for m in members if m.type != 'replica'])
        replicas, _new = replica_groups(
            len(instances), (len(members) - masters) // max(masters, 1),
            existing_masters=masters)

        db_info.update(task_status=ClusterTasks.GROWING_CLUSTER)

        locality = srv_grp.ServerGroup.convert_to_hint(self.server_group)
        new_instances = self._create_instances(context, db_info,
                                               datastore, datastore_version,
                                               instances, None, locality,
                                               image_id=image_id,
                                               replicas_per_master=replicas)

        task_api.load(context, datastore_version.manager).grow_cluster(
            db_info.id, [instance.id for instance in new_instances])

        return RedisCluster(context, db_info, datastore, datastore_version)

    def shrink(self, removal_ids):
        """Members holding slots can leave too: the taskmanager moves their
        slots, with the keys, onto the remaining members first. That takes
        as long as the data does, so it is not done in the API request.
        """
        LOG.debug("Shrinking cluster %s.", self.id)

        self.validate_cluster_available()

        all_ids = [inst.id for inst in inst_models.DBInstance.find_all(
            cluster_id=self.id, deleted=False).all()]
        if not set(removal_ids) < set(all_ids):
            raise exception.ClusterShrinkMustNotLeaveClusterEmpty()

        self.db_info.update(task_status=ClusterTasks.SHRINKING_CLUSTER)
        task_api.load(self.context, self.ds_version.manager).shrink_cluster(
            self.db_info.id, removal_ids)

        return RedisCluster(self.context, self.db_info,
                            self.ds, self.ds_version)


class RedisClusterView(ClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])


class RedisMgmtClusterView(MgmtClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])
