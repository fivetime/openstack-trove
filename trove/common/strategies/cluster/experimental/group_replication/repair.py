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

"""The repair's plugin of MySQL Group Replication (MySQL, Percona
Server): the replacement of a lost member.

The cluster's own shrink removes a member that is gone (it deletes it,
then gives the members that stay the group seeds without it), and its
grow adds one that recovers from a donor; the plugin only says when the
group can lose the member: every other member answers, is ONLINE in a
group that keeps its majority, and sees every member the configuration
names ONLINE but the lost one (``common.replace.PeersHoldItOut``). The
shrink deletes the lost member's server before the others are given the
new seeds: it does not come back to join them.
"""

from trove.common.strategies.cluster.experimental.common import replace


class GroupReplicationReplace(replace.PeersHoldItOut):
    pass
