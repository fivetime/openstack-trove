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

"""The repair's plugin of the Redis family (Redis, Valkey, KeyDB): the
replacement of a lost member of a cluster of the sentinel topology.

The cluster's own shrink removes a member that is gone (it keeps it from
being elected if it answers, deletes it, then has the sentinels that stay
forget it), and its grow adds one; the plugin only says when the cluster
can lose the member: every other member answers, their sentinels know a
primary they do not hold down, and they hold the lost member out and no
other (``common.replace.PeersHoldItOut``). A cluster of the cluster
topology is not replaced (yet).
"""

from trove.common.strategies.cluster.experimental.common import replace
from trove.common.strategies.cluster.experimental.common import topology

SENTINEL = 'sentinel'


class SentinelReplace(replace.PeersHoldItOut):

    def applies(self, db_cluster):
        if db_cluster.get_properties().get(topology.TOPOLOGY) != SENTINEL:
            return False, 'only a cluster of the sentinel topology'
        return True, None
