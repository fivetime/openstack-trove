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

from oslo_log import log as logging

from trove.cluster import models
from trove.cluster.tasks import ClusterTasks
from trove.cluster.views import ClusterView
from trove.common import cfg
from trove.common import exception
from trove.common import server_group as srv_grp
from trove.common.strategies.cluster import base
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


class RedisCluster(models.Cluster):

    @staticmethod
    def _create_instances(context, db_info, datastore, datastore_version,
                          instances, extended_properties, locality,
                          image_id=None):
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

        name_index = 1
        for instance in instances:
            if not instance.get('name'):
                instance['name'] = "%s-member-%s" % (db_info.name, name_index)
                name_index += 1

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
                                            cluster_config={
                                                "id": db_info.id,
                                                "instance_type": "member"},
                                            volume_type=instance.get(
                                                'volume_type', None),
                                            modules=instance.get('modules'),
                                            locality=locality,
                                            region_name=instance.get(
                                                'region_name')
                                            )
                for instance in instances]

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality, configuration,
               image_id=None):
        LOG.debug("Initiating cluster creation.")

        if configuration:
            raise exception.ConfigurationNotSupported()

        # Updating Cluster Task

        db_info = models.DBCluster.create(
            name=name, tenant_id=context.project_id,
            datastore_version_id=datastore_version.id,
            task_status=ClusterTasks.BUILDING_INITIAL)

        cls._create_instances(context, db_info, datastore, datastore_version,
                              instances, extended_properties, locality,
                              image_id=image_id)

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

        db_info.update(task_status=ClusterTasks.GROWING_CLUSTER)

        locality = srv_grp.ServerGroup.convert_to_hint(self.server_group)
        new_instances = self._create_instances(context, db_info,
                                               datastore, datastore_version,
                                               instances, None, locality,
                                               image_id=image_id)

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
