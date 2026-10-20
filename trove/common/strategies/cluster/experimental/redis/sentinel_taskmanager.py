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

"""The sentinel topology of a Redis family cluster, in the task manager.

A cluster is built once every member is up: the members get the
cluster's three passwords (the cluster admin the replicas sync as, the
sentinel user the sentinels log in to the servers as, the sentinels'
own admin), the first becomes the primary and the others its replicas,
each with its sentinel; then the cluster's load balancer, without which
the cluster is of no use. The sentinels elect the primary from then on:
the task manager only records the cluster's health
(``common.health.HealthWatch``), as for every Redis family cluster.
"""

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import (
    taskmanager as common_taskmanager)
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_api)
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import Instance
import trove.taskmanager.models as task_models

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


def cluster_passwords():
    """The passwords a new cluster's members share, by what they are for."""
    return {name: utils.generate_random_password()
            for name in ('admin_password', 'sentinel_password',
                         'sentinel_admin_password')}


class RedisSentinelClusterTasks(common_taskmanager.ClusterEntryTasksMixin,
                                task_models.ClusterTasks):

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster (sentinel) for id: %s.", cluster_id)

        def _create_cluster():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            instance_ids = [db_instance.id for db_instance in db_instances]
            if not self._all_instances_ready(instance_ids, cluster_id):
                raise TroveError(_("Instances in cluster did not report "
                                   "ACTIVE"))
            instances = [Instance.load(context, instance_id)
                         for instance_id in instance_ids]
            guests = [self.get_guest(instance) for instance in instances]
            ips = [guest.get_node_ip()[0] for guest in guests]
            mode = self.db_info.get_properties().get(
                sentinel_api.MODE_KEY, sentinel_api.SYNC)
            config = dict(cluster_passwords(), primary=ips[0], members=ips,
                          mode=mode)
            LOG.info("Cluster %s: primary %s, replicas %s, %s mode.",
                     cluster_id, ips[0], ', '.join(ips[1:]), mode)
            try:
                # The primary first: the replicas sync with it as they
                # join.
                for guest in guests:
                    guest.sentinel_install(config)
                for guest in guests:
                    guest.cluster_complete()
                # A cluster without its endpoint is of no use: a failure
                # here fails the cluster like any other step.
                if self._load_balancer_enabled():
                    self._sync_load_balancer(context, cluster_id, instances)
            except Exception:
                LOG.exception("Error creating cluster %s.", cluster_id)
                self.update_statuses_on_failure(cluster_id)
                return False
            return True

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            if _create_cluster():
                self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise
            LOG.exception("Timeout for building cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        except TroveError:
            LOG.exception("Error creating cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()
        LOG.debug("End create_cluster (sentinel) for id: %s.", cluster_id)

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        raise TroveError(_("A Redis cluster of the sentinel topology cannot "
                           "be grown yet."))

    def shrink_cluster(self, context, cluster_id, removal_ids):
        raise TroveError(_("A Redis cluster of the sentinel topology cannot "
                           "be shrunk yet."))

    def upgrade_cluster(self, context, cluster_id, datastore_version):
        self.rolling_upgrade_cluster(context, cluster_id, datastore_version)
