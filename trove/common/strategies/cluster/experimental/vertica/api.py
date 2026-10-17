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
from oslo_utils import strutils

from trove.cluster import models
from trove.cluster.tasks import ClusterTasks
from trove.cluster.views import ClusterView
from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.experimental.vertica import communal
from trove.common import utils
from trove.extensions.mgmt.clusters.views import MgmtClusterView
from trove.instance import models as inst_models
from trove.quota.quota import check_quotas
from trove.taskmanager import api as task_api


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# Vertica does not sync what it commits to disk: it relies on the other
# nodes holding the data. Members that all lose power at once lose recent
# commits, or come back unable to start. Mounted with sync, every write
# reaches the disk before it returns, at a cost to write throughput.
SYNC_WRITES = 'sync_writes'


# Where the data is: on the volumes of the members (Enterprise Mode), or
# in a bucket of the project's object storage with the members' volumes
# as its cache (Eon Mode, communal.py).
STORAGE_MODE = 'storage_mode'
ENTERPRISE = 'enterprise'
EON = 'eon'
# The user whose EC2 credential the communal storage is reached with.
EC2_USER = 'ec2_user_id'


def cluster_properties(extended_properties, conf=None):
    """The storage mode and sync writes the extended properties of a new
    cluster ask for. A property this datastore does not know is refused
    rather than ignored: a misspelled one would leave the writes unsynced
    without a word.
    """
    props = dict(extended_properties or {})
    known = (STORAGE_MODE, SYNC_WRITES)
    unknown = sorted(set(props) - set(known))
    if unknown:
        raise exception.BadRequest(_(
            "Unknown extended properties for a Vertica cluster: %(unknown)s."
            " Known: %(known)s.") % {'unknown': ', '.join(unknown),
                                     'known': ', '.join(known)})
    try:
        sync = strutils.bool_from_string(props.get(SYNC_WRITES, False),
                                         strict=True)
    except ValueError:
        raise exception.BadRequest(_(
            "%s must be true or false.") % SYNC_WRITES)
    mode = str(props.get(STORAGE_MODE, ENTERPRISE)).lower()
    if mode not in (ENTERPRISE, EON):
        raise exception.BadRequest(_(
            "%(name)s must be %(enterprise)s or %(eon)s.")
            % {'name': STORAGE_MODE, 'enterprise': ENTERPRISE, 'eon': EON})
    if mode == EON and not getattr(conf, 'eon_enabled', False):
        raise exception.BadRequest(_(
            "Eon Mode is not available for Vertica clusters in this "
            "cloud."))
    return {STORAGE_MODE: mode, SYNC_WRITES: sync}


def member_settings(properties):
    """What the guest of each member gets in its cluster configuration."""
    return {SYNC_WRITES: True} if properties.get(SYNC_WRITES) else {}


def shown_properties(properties):
    """What a cluster shows of its properties; nothing for a cluster
    created before they were kept.
    """
    if not properties:
        return None
    shown = {STORAGE_MODE: properties.get(STORAGE_MODE, ENTERPRISE),
             SYNC_WRITES: bool(properties.get(SYNC_WRITES))}
    if shown[STORAGE_MODE] == EON:
        shown['communal_bucket'] = properties.get('communal_bucket')
    return shown


class VerticaAPIStrategy(base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return VerticaCluster

    def _action_grow(self, cluster, body):
        nodes = body['grow']
        instances = []
        for node in nodes:
            instance = {
                'flavor_id': utils.get_id_from_href(node['flavorRef'])
            }
            if 'name' in node:
                instance['name'] = node['name']
            if 'volume' in node:
                instance['volume_size'] = int(node['volume']['size'])
            instances.append(instance)
        return cluster.grow(instances)

    def _action_shrink(self, cluster, body):
        nodes = body['shrink']
        instance_ids = [node['id'] for node in nodes]
        return cluster.shrink(instance_ids)

    @property
    def cluster_view_class(self):
        return VerticaClusterView

    @property
    def mgmt_cluster_view_class(self):
        return VerticaMgmtClusterView


class VerticaCluster(models.Cluster):

    @staticmethod
    def _create_instances(context, db_info, datastore, datastore_version,
                          instances, extended_properties, locality,
                          new_cluster=True, image_id=None):
        vertica_conf = CONF.get(datastore_version.manager)
        properties = cluster_properties(extended_properties, vertica_conf)
        num_instances = len(instances)

        existing = inst_models.DBInstance.find_all(cluster_id=db_info.id,
                                                   deleted=False).all()
        num_existing = len(existing)

        # Matching number of instances with configured cluster_member_count
        if (new_cluster and
                num_instances != vertica_conf.cluster_member_count):
            raise exception.ClusterNumInstancesNotSupported(
                num_instances=vertica_conf.cluster_member_count)

        models.validate_instance_flavors(
            context, instances, vertica_conf.volume_support,
            vertica_conf.device_path)

        req_volume_size = models.get_required_volume_size(
            instances, vertica_conf.volume_support)
        models.assert_homogeneous_cluster(instances)

        models.validate_instance_nics(context, instances)

        deltas = {'instances': num_instances, 'volumes': req_volume_size}

        check_quotas(context.project_id, deltas)

        flavor_id = instances[0]['flavor_id']
        volume_size = instances[0].get('volume_size', None)
        volume_type = instances[0].get('volume_type', None)

        nics = [instance.get('nics', None) for instance in instances]

        azs = [instance.get('availability_zone', None)
               for instance in instances]

        regions = [instance.get('region_name', None)
                   for instance in instances]

        # Creating member instances
        minstances = []
        for i in range(0, num_instances):
            if i == 0 and new_cluster:
                member_config = {"id": db_info.id, "instance_type": "master"}
            else:
                member_config = {"id": db_info.id, "instance_type": "member"}
            member_config.update(member_settings(properties))
            instance_name = "%s-member-%s" % (db_info.name,
                                              str(i + num_existing + 1))
            minstances.append(
                inst_models.Instance.create(
                    context, instance_name, flavor_id,
                    datastore_version.image_id or image_id, [], [], datastore,
                    datastore_version, volume_size, None,
                    nics=nics[i], availability_zone=azs[i],
                    configuration_id=None, cluster_config=member_config,
                    volume_type=volume_type,
                    modules=instances[i].get('modules'), locality=locality,
                    region_name=regions[i])
            )
        return minstances

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality, configuration,
               image_id=None):
        LOG.debug("Initiating cluster creation.")

        if configuration:
            raise exception.ConfigurationNotSupported()

        vertica_conf = CONF.get(datastore_version.manager)
        # Before anything is created.
        properties = cluster_properties(extended_properties, vertica_conf)
        num_instances = len(instances)

        # Matching number of instances with configured cluster_member_count
        if num_instances != vertica_conf.cluster_member_count:
            raise exception.ClusterNumInstancesNotSupported(
                num_instances=vertica_conf.cluster_member_count)

        db_info = models.DBCluster.create(
            name=name, tenant_id=context.project_id,
            datastore_version_id=datastore_version.id,
            task_status=ClusterTasks.BUILDING_INITIAL)
        if properties[STORAGE_MODE] == EON:
            # The bucket is named after the cluster; the task manager
            # creates it with an EC2 credential of this user.
            properties['communal_bucket'] = communal.bucket_name(db_info.id)
            properties[EC2_USER] = context.user_id
        db_info.save_properties(properties)

        cls._create_instances(context, db_info, datastore, datastore_version,
                              instances, extended_properties, locality,
                              new_cluster=True, image_id=image_id)
        # Calling taskmanager to further proceed for cluster-configuration
        task_api.load(context, datastore_version.manager).create_cluster(
            db_info.id)

        return VerticaCluster(context, db_info, datastore, datastore_version)

    @staticmethod
    def k_safety(n):
        """
        Vertica defines k-safety values of 0, 1 or 2:
        https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/Glossary/
        K-Safety.htm
        """
        if n < 3:
            return 0
        elif n < 5:
            return 1
        else:
            return 2

    # Not yet with vcluster; see the taskmanager strategy.
    def grow(self, instances, image_id=None):
        raise exception.BadRequest(_(
            "Growing a Vertica cluster is not supported."))

    def shrink(self, instance_ids):
        raise exception.BadRequest(_(
            "Shrinking a Vertica cluster is not supported."))


class VerticaClusterView(ClusterView):

    def build_instances(self):
        return self._build_instances(['member', 'master'],
                                     ['member', 'master'])

    def get_extended_properties(self):
        return shown_properties(self.cluster.db_info.get_properties())


class VerticaMgmtClusterView(MgmtClusterView):

    def build_instances(self):
        return self._build_instances(['member', 'master'],
                                     ['member', 'master'])

    def get_extended_properties(self):
        return shown_properties(self.cluster.db_info.get_properties())
