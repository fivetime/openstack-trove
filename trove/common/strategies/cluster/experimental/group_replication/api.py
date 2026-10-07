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

"""MySQL Group Replication clusters, for MySQL and Percona Server.

A cluster is created, grown and shrunk as a Galera one is; what differs is
in the task manager and the guest agent. The tenant chooses the mode with
the extended property ``group_replication_mode``: ``single-primary`` (the
default), where one member takes writes and the group elects another when
it fails, or ``multi-primary``, where every member does.
"""

from oslo_log import log as logging

from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)

LOG = logging.getLogger(__name__)

MODE_KEY = 'group_replication_mode'
SINGLE_PRIMARY = 'single-primary'
MULTI_PRIMARY = 'multi-primary'
MODES = (SINGLE_PRIMARY, MULTI_PRIMARY)
# The roles a member has as the view shows them.
ROLES = {('ONLINE', 'PRIMARY'): 'primary',
         ('ONLINE', 'SECONDARY'): 'secondary'}


class GroupReplicationAPIStrategy(galera_api.GaleraCommonAPIStrategy):

    @property
    def cluster_class(self):
        return GroupReplicationCluster

    @property
    def cluster_view_class(self):
        return GroupReplicationClusterView

    @property
    def mgmt_cluster_view_class(self):
        return GroupReplicationMgmtClusterView


class GroupReplicationCluster(galera_api.GaleraCommonCluster):

    MODE_KEY = MODE_KEY
    MODES = MODES
    DEFAULT_MODE = SINGLE_PRIMARY


class GroupReplicationClusterView(galera_api.GaleraCommonClusterView):
    ROLES = ROLES


class GroupReplicationMgmtClusterView(galera_api.GaleraCommonMgmtClusterView):
    ROLES = ROLES
