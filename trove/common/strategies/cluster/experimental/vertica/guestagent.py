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


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VerticaGuestAgentStrategy(base.BaseGuestAgentStrategy):

    @property
    def guest_client_class(self):
        return VerticaGuestAgentAPI


class VerticaGuestAgentAPI(guest_api.API):
    """Cluster Specific Datastore Guest API

    **** VERSION CONTROLLED API ****

    The methods in this class are subject to version control as
    coordinated by guestagent/api.py.  Whenever a change is made to
    any API method in this class, add a version number and comment
    to the top of guestagent/api.py and use the version number as
    appropriate in this file
    """

    def get_cluster_secrets(self):
        LOG.debug("Getting the cluster secrets of the first member.")
        return self._call("get_cluster_secrets", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)

    def install_cluster_secrets(self, secrets):
        LOG.debug("Installing the cluster secrets.")
        return self._call("install_cluster_secrets",
                          self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          secrets=secrets)

    def install_cluster(self, members):
        LOG.debug("Creating the database on %s.", members)
        return self._call("install_cluster", CONF.cluster_usage_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          members=members)

    def set_cluster_config(self, config):
        LOG.debug("Setting the cluster configuration.")
        return self._call("set_cluster_config", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION,
                          config=config)

    def cluster_complete(self):
        LOG.debug("Notifying cluster install completion.")
        return self._call("cluster_complete", self.agent_high_timeout,
                          version=guest_api.API.API_BASE_VERSION)
