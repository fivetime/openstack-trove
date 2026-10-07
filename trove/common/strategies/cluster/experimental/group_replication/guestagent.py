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

from trove.common.strategies.cluster import base as cluster_base
from trove.common.strategies.cluster.experimental.galera_common import (
    guestagent as galera_guestagent)


class GroupReplicationGuestAgentStrategy(
        cluster_base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return GroupReplicationGuestAgentAPI


class GroupReplicationGuestAgentAPI(
        galera_guestagent.GaleraCommonGuestAgentAPI):
    """The Galera calls, which Group Replication answers as well: leaving
    the group, the role a member has and whether it takes writes included.

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.
    """
