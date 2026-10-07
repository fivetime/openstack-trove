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

from oslo_log import log as logging

from trove.common.strategies.cluster import base as cluster_base
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guestagent)
from trove.guestagent import api as guest_api

LOG = logging.getLogger(__name__)


class GroupReplicationGuestAgentStrategy(
        cluster_base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return GroupReplicationGuestAgentAPI


class GroupReplicationGuestAgentAPI(
        galera_guestagent.GaleraCommonGuestAgentAPI):
    """The Galera calls, which Group Replication answers as well, and the
    ones of its own.

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.
    """

    def leave_cluster(self):
        """Leave the group, before the member is deleted."""
        LOG.debug("Leaving the group.")
        self._call("leave_cluster", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION)

    def is_writable_member(self):
        """Whether the member takes writes: the primary, or any member of
        a multi-primary group.
        """
        return self._call("is_writable_member", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_member_role(self):
        """The member's state and role in the group. For a view: a member
        that does not answer soon counts as unknown.
        """
        return self._call("get_member_role", self.agent_low_timeout,
                          version=guest_api.API.API_BASE_VERSION)
