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

"""The repair's plugin of Cassandra: the replacement of a lost member.

Every node sees the whole ring (gossip): the cluster can lose a member
when every other member answers, none holds the cluster down (most of
the ring down: a quorum of some ranges gone), and each holds that member
out of the ring and no other (``common.replace.PeersHoldItOut``; a member
is out while it is not up and normal, as nodetool status has it).

The cluster's own shrink removes a member that is gone (a node that stays
takes it out of the ring by its host id, nodetool removenode, and the
nodes that stay stream its replicas among them; then it is deleted), and
its grow adds one, which bootstraps its share of the data: the ring has
to be whole by then (a node that bootstraps while one is down refuses),
which the order of the steps gives.
"""

from trove.common.strategies.cluster.experimental.common import replace


class CassandraReplace(replace.PeersHoldItOut):
    pass
