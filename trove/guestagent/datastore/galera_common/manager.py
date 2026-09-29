#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from oslo_log import log as logging

from trove.instance import service_status

LOG = logging.getLogger(__name__)


class GaleraManagerMixin(object):
    """The cluster calls of the guest agent, for a MySQL based manager.

    These are the calls the Galera task manager strategy makes. List the
    mixin before the manager class it extends.
    """

    def _cluster_node_command(self):
        return self.get_start_db_params(self.app.get_data_dir())

    def install_cluster(self, context, replication_user,
                        cluster_configuration, bootstrap):
        try:
            self.app.install_cluster(
                replication_user, cluster_configuration,
                self._cluster_node_command(), bootstrap=bootstrap)
            LOG.info("The cluster configuration is installed.")
        except Exception:
            LOG.exception("Failed to install the cluster configuration.")
            self.status.set_status(service_status.ServiceStatuses.FAILED)
            raise

    def reset_admin_password(self, context, admin_password):
        LOG.debug("Storing the admin password of the cluster.")
        self.app.reset_admin_password(admin_password)

    def get_cluster_context(self, context):
        LOG.debug("Getting the cluster context.")
        return self.app.get_cluster_context()

    def write_cluster_configuration_overrides(self, context,
                                              cluster_configuration):
        LOG.debug("Applying the updated cluster configuration.")
        self.app.write_cluster_configuration_overrides(cluster_configuration)

    def cluster_complete(self, context):
        # Every member has joined by now, so the one that formed the
        # cluster can leave and come back without the cluster going down.
        self.app.leave_bootstrap(self._cluster_node_command())
        super(GaleraManagerMixin, self).cluster_complete(context)
