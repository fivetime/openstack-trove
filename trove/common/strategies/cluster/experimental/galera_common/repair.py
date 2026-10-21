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

"""The repair's plugin of Galera (MariaDB, Percona XtraDB Cluster): the
replacement of a lost member.

The cluster's own shrink removes a member that is gone (it deletes it,
then gives the members that stay the cluster address without it), and
its grow adds one that joins with a state transfer; the plugin only says
when the cluster can lose the member: every other member answers, is in
the primary component, and sees every member the configuration names in
it but the lost one (``common.replace.PeersHoldItOut``). The shrink
deletes the lost member's server before the others are given the new
address: it does not come back to join them.
"""

from trove.common.strategies.cluster.experimental.common import replace


class GaleraReplace(replace.PeersHoldItOut):
    pass
