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

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common.strategies.cluster import base
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VerticaTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return VerticaTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return VerticaClusterTasks


class VerticaClusterTasks(task_models.ClusterTasks):

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s.", cluster_id)

        def _create_cluster():

            # Fetch instances by cluster_id against instances table.
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            instance_ids = [db_instance.id for db_instance in db_instances]

            # Wait for cluster members to get to cluster-ready status.
            if not self._all_instances_ready(instance_ids, cluster_id):
                return

            LOG.debug("All members ready, proceeding for cluster setup.")
            instances = [Instance.load(context, instance_id) for instance_id
                         in instance_ids]

            member_ips = [self.get_ip(instance) for instance in instances]
            guests = [self.get_guest(instance) for instance in instances]

            # vcluster on the first member talks to the agent of every
            # member: all must trust the authority of the first, and the
            # admin has one password for the one database.
            try:
                master_id = next(db_instance.id for db_instance
                                 in db_instances
                                 if db_instance.type == 'master')
                master_guest = next(guest for instance, guest
                                    in zip(instances, guests)
                                    if instance.id == master_id)
                secrets = master_guest.get_cluster_secrets()
                for instance, guest in zip(instances, guests):
                    if instance.id != master_id:
                        guest.install_cluster_secrets(secrets)

                LOG.debug("Installing cluster with members: %s.", member_ips)
                config = master_guest.install_cluster(member_ips)

                # Every member starts and stops its node with vcluster,
                # which needs the configuration the creation wrote.
                for instance, guest in zip(instances, guests):
                    if instance.id != master_id:
                        guest.set_cluster_config(config)

                LOG.debug("Finalizing cluster configuration.")
                for guest in guests:
                    guest.cluster_complete()
            except Exception:
                LOG.exception("Error creating cluster.")
                self.update_statuses_on_failure(cluster_id)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _create_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for building cluster.")
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s.", cluster_id)

    # Growing and shrinking went through adminTools over SSH, which the
    # image has neither of. The API refuses both until they are done with
    # vcluster (add_node, remove_node) and tried with a license: the
    # Community Edition allows three nodes, and three nodes with
    # K-safety 1 cannot lose one.
    def grow_cluster(self, context, cluster_id, new_instance_ids):
        raise TroveError(_("Growing a Vertica cluster is not supported."))

    def shrink_cluster(self, context, cluster_id, instance_ids):
        raise TroveError(_("Shrinking a Vertica cluster is not supported."))


class VerticaTaskManagerAPI(task_api.API):

    def _cast(self, method_name, version, **kwargs):
        LOG.debug("Casting %s", method_name)
        cctxt = self.client.prepare(version=version)
        cctxt.cast(self.context, method_name, **kwargs)
