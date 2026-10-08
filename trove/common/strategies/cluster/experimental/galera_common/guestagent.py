# Copyright [2015] Hewlett-Packard Development Company, L.P.
# Copyright 2016 Tesora Inc.
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

from trove.common import cfg
from trove.common.strategies.cluster import base as cluster_base
from trove.guestagent import api as guest_api


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class GaleraCommonGuestAgentStrategy(cluster_base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return GaleraCommonGuestAgentAPI


class GaleraCommonGuestAgentAPI(guest_api.API):
    """Cluster Specific Datastore Guest API

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.  Whenever a change is made to
    any API method in this class, add a version number and comment
    to the top of guestagent/api.py and use the version number as
    appropriate in this file
    """

    def install_cluster(self, replication_user, cluster_configuration,
                        bootstrap):
        """Install the cluster."""
        LOG.debug("Installing Galera cluster.")
        version = guest_api.API.API_BASE_VERSION

        self._call("install_cluster", CONF.cluster_usage_timeout,
                   version=version,
                   replication_user=replication_user,
                   cluster_configuration=cluster_configuration,
                   bootstrap=bootstrap)

    def reset_admin_password(self, admin_password):
        """Store this password on the instance as the admin password."""
        version = guest_api.API.API_BASE_VERSION

        self._call("reset_admin_password", CONF.cluster_usage_timeout,
                   version=version,
                   admin_password=admin_password)

    def cluster_complete(self):
        """Set the status that the cluster is build is complete."""
        LOG.debug("Notifying cluster install completion.")
        version = guest_api.API.API_BASE_VERSION

        return self._call("cluster_complete", self.agent_high_timeout,
                          version=version)

    def get_cluster_context(self):
        """Get the context of the cluster."""
        LOG.debug("Getting the cluster context.")
        version = guest_api.API.API_BASE_VERSION

        return self._call("get_cluster_context", self.agent_high_timeout,
                          version=version)

    def write_cluster_configuration_overrides(self, cluster_configuration):
        """Write an updated the cluster configuration."""
        LOG.debug("Writing an updated the cluster configuration.")
        version = guest_api.API.API_BASE_VERSION

        self._call("write_cluster_configuration_overrides",
                   self.agent_high_timeout,
                   version=version,
                   cluster_configuration=cluster_configuration)

    def leave_cluster(self):
        """Leave the cluster, before the member is deleted."""
        LOG.debug("Leaving the cluster.")
        self._call("leave_cluster", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION)

    def is_writable_member(self):
        """Whether the member takes writes."""
        return self._call("is_writable_member", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_member_role(self):
        """The member's state and role in the cluster. For a view: a
        member that does not answer soon counts as unknown.
        """
        return self._call("get_member_role", self.agent_low_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_recovery_view(self, timeout=None):
        """Where the member stands, for the recovery of the cluster: a
        member that does not answer in time is unreachable.
        """
        return self._call("get_recovery_view",
                          timeout or self.agent_low_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def bootstrap_cluster(self):
        """Form the cluster again on this member: told, not waited for.
        Forming takes as long as a state transfer, and the task manager
        follows it through the members' recovery views.
        """
        LOG.debug("Forming the cluster again.")
        self._cast("bootstrap_cluster",
                   version=guest_api.API.API_BASE_VERSION)
