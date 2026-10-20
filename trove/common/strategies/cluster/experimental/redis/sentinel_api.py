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

"""The sentinel topology of a Redis family cluster, in the API: a primary
with replicas, a sentinel beside each server electing the primary, chosen
with the extended property ``topology=sentinel``.

The tenant chooses the replication mode (``replication_mode``): ``sync``
(the default) makes a primary that no replica follows refuse writes, so
that a primary cut off from the others takes none the cluster then loses;
``async`` keeps taking them. The cluster shows its endpoints (writes to
the primary, reads from the replicas that keep up, the sentinels) and the
name its sentinels know the primary by.
"""

from trove.cluster.views import ClusterView
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.extensions.mgmt.clusters.views import MgmtClusterView

TOPOLOGY = 'sentinel'
MODE_KEY = 'replication_mode'
SYNC, ASYNC = 'sync', 'async'
# The name the sentinels know the primary by (the guest agent's
# redis_common.sentinel.MASTER_NAME).
MASTER_NAME = 'trove'


class RedisSentinelCluster(common_api.HomogeneousCluster):

    MODE_KEY = MODE_KEY
    MODES = (SYNC, ASYNC)
    DEFAULT_MODE = SYNC
    KEPT_PROPERTIES = (MODE_KEY,)

    def grow(self, instances, image_id=None):
        raise exception.BadRequest(_(
            "A Redis cluster of the sentinel topology cannot be grown "
            "yet."))

    def shrink(self, instances):
        raise exception.BadRequest(_(
            "A Redis cluster of the sentinel topology cannot be shrunk "
            "yet."))


class SentinelViewMixin(common_api.MemberRolesMixin):
    """The members with the role each has now (master or replica), the
    endpoints, the mode and the primary's name for a client that speaks
    sentinel.
    """

    def _role_name(self, answer):
        return answer.get('role') or 'unknown'

    def build_instances(self):
        return self._build_instances(['member'], ['member'])

    def data(self):
        result = super(SentinelViewMixin, self).data()
        properties = self.cluster.db_info.get_properties()
        result['cluster'].update({
            'topology': TOPOLOGY,
            'replication_mode': properties.get(MODE_KEY, SYNC),
            'sentinel_master_name': MASTER_NAME,
        })
        return result


class RedisSentinelClusterView(SentinelViewMixin, ClusterView):
    pass


class RedisSentinelMgmtClusterView(SentinelViewMixin, MgmtClusterView):
    pass
