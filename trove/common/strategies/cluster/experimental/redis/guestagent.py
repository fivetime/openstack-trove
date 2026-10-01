# Copyright [2015] Hewlett-Packard Development Company, L.P.
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

from trove.common import cfg
from trove.common.strategies.cluster import base
from trove.guestagent import api as guest_api


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class RedisGuestAgentStrategy(base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return RedisGuestAgentAPI


class RedisGuestAgentAPI(guest_api.API):
    """Cluster Specific Datastore Guest API

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.  Whenever a change is made to
    any API method in this class, add a version number and comment
    to the top of guestagent/api.py and use the version number as
    appropriate in this file
    """

    def cluster_init(self, password):
        LOG.debug("Adding the cluster admin account.")
        return self._call("cluster_init", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          password=password)

    def get_cluster_password(self):
        LOG.debug("Retrieve the cluster admin password.")
        return self._call("get_cluster_password", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_node_ip(self):
        LOG.debug("Retrieve ip info from node.")
        return self._call("get_node_ip", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def get_node_id(self):
        LOG.debug("Retrieve the cluster node id.")
        return self._call("get_node_id", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def cluster_meet(self, ip, port):
        LOG.debug("Joining node to cluster.")
        return self._call("cluster_meet", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          ip=ip, port=port)

    def cluster_addslots(self, first_slot, last_slot):
        LOG.debug("Adding slots %s-%s to cluster.", first_slot, last_slot)
        return self._call("cluster_addslots", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          first_slot=first_slot, last_slot=last_slot)

    def cluster_replicate(self, master_id):
        LOG.debug("Replicating master %s.", master_id)
        return self._call("cluster_replicate", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          master_id=master_id)

    def get_cluster_nodes(self):
        LOG.debug("Retrieve the nodes of the cluster.")
        return self._call("get_cluster_nodes", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    # The next three can take long: waiting for the cluster to agree, and
    # moving slots with their keys. The cluster task's own timeout bounds
    # them.
    def cluster_wait(self, expected_nodes):
        LOG.debug("Waiting for the cluster to see %s nodes.", expected_nodes)
        return self._call("cluster_wait", CONF.cluster_usage_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          expected_nodes=expected_nodes)

    def cluster_rebalance(self, weights=None, use_empty_masters=False):
        LOG.debug("Rebalancing the cluster.")
        return self._call("cluster_rebalance", CONF.cluster_usage_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          weights=weights,
                          use_empty_masters=use_empty_masters)

    def cluster_del_node(self, node_id):
        LOG.debug("Removing node %s from the cluster.", node_id)
        return self._call("cluster_del_node", CONF.cluster_usage_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          node_id=node_id)

    def cluster_complete(self):
        LOG.debug("Notifying cluster install completion.")
        return self._call("cluster_complete", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)
