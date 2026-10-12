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

from trove.common import cfg
from trove.common.strategies.cluster import base as cluster_base
from trove.common.strategies.cluster.experimental.common import (
    guestagent as common_guestagent)
from trove.guestagent import api as guest_api

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class PostgresqlGuestAgentStrategy(cluster_base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return PostgresqlGuestAgentAPI


class PostgresqlGuestAgentAPI(common_guestagent.ClusterGuestAgentAPI):
    """The cluster calls of a PostgreSQL member: installed as the primary
    or as a standby of it, the slots the primary keeps for the standbys,
    the members list every member keeps, and the failover calls (the
    lease, fencing, promotion, following, rejoining).

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.  Whenever a change is made to
    any API method in this class, add a version number and comment
    to the top of guestagent/api.py and use the version number as
    appropriate in this file
    """

    def install_cluster(self, cluster_config):
        """Install the member as the primary (no primary in the
        configuration) or as a standby of it, which takes a base backup:
        as long as the data is.
        """
        LOG.debug("Installing the cluster configuration.")
        self._call("install_cluster", CONF.cluster_usage_timeout,
                   version=guest_api.API.API_BASE_VERSION,
                   cluster_config=cluster_config)

    def create_member_slot(self, member_id):
        """On the primary: the replication slot a standby streams
        through, kept for it while it is away.
        """
        self._call("create_member_slot", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION,
                   member_id=member_id)

    def drop_member_slot(self, member_id):
        """On the primary: the slot of a member that left."""
        self._call("drop_member_slot", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION,
                   member_id=member_id)

    def update_cluster_members(self, members, mode, primary_id=None):
        """The members of the cluster, by id and address, and its mode,
        kept by every member; and the primary, after a failover.
        """
        self._call("update_cluster_members", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION,
                   members=members, mode=mode, primary_id=primary_id)

    def renew_lease(self, ttl):
        """On the primary the task manager judges healthy: it takes
        writes for ``ttl`` seconds more, standbys streaming or not.
        """
        self._call("renew_lease", self.agent_low_timeout,
                   version=guest_api.API.API_BASE_VERSION, ttl=ttl)

    def remember_sync_set(self, primary_id, members):
        """On a member: the standbys the given primary last reported
        streaming, for a failover decided without the primary.
        """
        self._cast("remember_sync_set",
                   version=guest_api.API.API_BASE_VERSION,
                   primary_id=primary_id, members=members)

    def fence_member(self):
        """On a primary that is being replaced: no more writes, the
        server stops.
        """
        self._call("fence_member", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION)

    def promote_member(self, members, mode):
        """On the standby that takes over: it becomes the primary.
        Returns its position and timeline.
        """
        return self._call("promote_member", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          members=members, mode=mode)

    def follow_primary(self, primary):
        """On a standby: it streams from the given primary (id and host)
        from now on.
        """
        self._call("follow_primary", self.agent_high_timeout,
                   version=guest_api.API.API_BASE_VERSION, primary=primary)

    def rejoin_cluster(self, primary):
        """On a former primary, or a standby left behind: it becomes a
        standby of the given primary, by a rewind or a base backup. Not
        waited for: as long as the data is.
        """
        self._cast("rejoin_cluster", version=guest_api.API.API_BASE_VERSION,
                   primary=primary)
