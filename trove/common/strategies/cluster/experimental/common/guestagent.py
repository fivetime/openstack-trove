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

from trove.guestagent import api as guest_api


LOG = logging.getLogger(__name__)


class ClusterGuestAgentAPI(guest_api.API):
    """The guest agent calls every cluster member answers: a datastore's
    cluster guest API adds its own to these.

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.  Whenever a change is made to
    any API method in this class, add a version number and comment
    to the top of guestagent/api.py and use the version number as
    appropriate in this file
    """

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

    def leave_cluster(self):
        """Leave the cluster, before the member is deleted."""
        LOG.debug("Leaving the cluster.")
        self._call("leave_cluster", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION)

    def is_writable_member(self):
        """Whether the member takes writes."""
        return self._call("is_writable_member", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_recovery_view(self, timeout=None):
        """Where the member stands, for the recovery of the cluster: a
        member that does not answer in time is unreachable.
        """
        return self._call("get_recovery_view",
                          timeout or self.agent_low_timeout,
                          version=guest_api.API.API_BASE_VERSION)
