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

"""The repair's plugin of PostgreSQL clusters: the replacement of a lost
member.

Only the primary sees the other members (a standby that does not stream
from it is out); a standby sees itself, streaming or not. The cluster can
lose a member when every other member answers, one of them is the primary
and holds that member out and no other, and every standby streams. A
primary that is lost is replaced once the recovery has promoted a standby
(Trove's lease): until then no member holds it out as a primary.

The cluster's own shrink removes the member (it drops its slot on the
primary, deletes it, and gives the members that stay the cluster without
it), and its grow adds a standby.
"""

from trove.common import cluster_health
from trove.common.strategies.cluster.experimental.common import replace


def _of_the_primary(view):
    # Only the primary tells of other members: a standby's view holds
    # none out, or itself.
    out = set(view.get('out') or [])
    return bool(out) and view.get('me') not in out


class PostgresqlReplace(replace.PeersHoldItOut):

    def vouches(self, member, view, lost_name):
        if _of_the_primary(view):
            return super(PostgresqlReplace, self).vouches(
                member, view, lost_name)
        if view.get('status') != cluster_health.HEALTHY:
            return False, 'member %s does not stream: %s' % (
                member.name, view.get('reason'))
        return True, None

    @staticmethod
    def name_of(db_cluster, lost, views):
        primaries = [(m, view) for m, view in views if _of_the_primary(view)]
        if len(primaries) != 1:
            return None
        return replace.PeersHoldItOut.name_of(db_cluster, lost, primaries)
