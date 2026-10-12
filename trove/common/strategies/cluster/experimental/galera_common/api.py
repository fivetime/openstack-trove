# Copyright [2015] Hewlett-Packard Development Company, L.P.
# Copyright 2016 Tesora Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_log import log as logging

from trove.cluster.views import ClusterView
from trove.common import cfg
from trove.common.strategies.cluster import base as cluster_base
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.extensions.mgmt.clusters.views import MgmtClusterView


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The names the modules built on this one have used.
ANSWERING_STATUSES = common_api.ANSWERING_STATUSES
MemberRolesMixin = common_api.MemberRolesMixin

# The tenant chooses with the extended property ``writer_mode``: single
# (the default), where one synced member takes writes and another takes
# over when it fails, or multi, where every synced member does.
WRITER_MODE_KEY = 'writer_mode'
SINGLE_WRITER = 'single'
MULTI_WRITER = 'multi'
WRITER_MODES = (SINGLE_WRITER, MULTI_WRITER)
# The roles a Galera member has as the view shows them: its wsrep state,
# and PRIMARY or SECONDARY when it is synced.
ROLES = {('Synced', 'PRIMARY'): 'primary',
         ('Synced', 'SECONDARY'): 'secondary',
         ('Joiner', None): 'recovering',
         ('Joined', None): 'recovering',
         ('Initialized', None): 'offline'}


class GaleraCommonAPIStrategy(cluster_base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return GaleraCommonCluster

    @property
    def cluster_view_class(self):
        return GaleraCommonClusterView

    @property
    def mgmt_cluster_view_class(self):
        return GaleraCommonMgmtClusterView


class GaleraCommonCluster(common_api.HomogeneousCluster):
    """A Galera cluster: members all alike, and a writer mode the tenant
    chooses.
    """

    MODE_KEY = WRITER_MODE_KEY
    MODES = WRITER_MODES
    DEFAULT_MODE = SINGLE_WRITER


class GaleraCommonClusterView(MemberRolesMixin, ClusterView):

    ROLES = ROLES

    def build_instances(self):
        return self._build_instances(['member'], ['member'])


class GaleraCommonMgmtClusterView(MemberRolesMixin, MgmtClusterView):

    ROLES = ROLES

    def build_instances(self):
        return self._build_instances(['member'], ['member'])
