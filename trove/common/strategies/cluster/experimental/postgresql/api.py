#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from oslo_log import log as logging

from trove.cluster.views import ClusterView
from trove.common import clients
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster import base as cluster_base
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.extensions.mgmt.clusters.views import MgmtClusterView

LOG = logging.getLogger(__name__)

# The tenant chooses with the extended property ``replication_mode`` how a
# commit on the primary waits for the standbys.
MODE_KEY = 'replication_mode'
ASYNC = 'async'
SYNC = 'sync'
STRICT = 'strict'
MODES = (ASYNC, SYNC, STRICT)
# The roles a member has as the view shows them: its state, and for a
# standby whether the primary counts it as synchronous.
ROLES = {('primary', 'PRIMARY'): 'primary',
         ('standby', 'SYNC'): 'sync-standby',
         ('standby', 'ASYNC'): 'standby',
         ('standby', 'STREAMING'): 'standby',
         ('standby', 'DETACHED'): 'disconnected',
         ('rebuilding', None): 'recovering',
         ('stopped', None): 'offline'}
# What the primary reports a standby's sync_state as when a commit waits
# for it.
SYNCHRONOUS_STATES = ('sync', 'quorum')


class PostgresqlAPIStrategy(cluster_base.BaseAPIStrategy):

    @property
    def cluster_class(self):
        return PostgresqlCluster

    @property
    def cluster_view_class(self):
        return PostgresqlClusterView

    @property
    def mgmt_cluster_view_class(self):
        return PostgresqlMgmtClusterView


class PostgresqlCluster(common_api.HomogeneousCluster):
    """A PostgreSQL cluster: members all alike, and a replication mode the
    tenant chooses.
    """

    MODE_KEY = MODE_KEY
    MODES = MODES
    DEFAULT_MODE = SYNC

    def _primary_ids(self, instance_ids):
        """Those of the members that are the primary, asked."""
        primaries = []
        for instance_id in instance_ids:
            try:
                answer = clients.create_guest_client(
                    self.context, instance_id,
                    self.ds_version.manager).get_member_role()
            except Exception as err:
                LOG.info("Member %s did not tell its role: %s", instance_id,
                         err)
                continue
            if answer.get('state') == 'primary':
                primaries.append(instance_id)
        return primaries

    def shrink(self, instances):
        # The primary is where the data is written: it cannot go until a
        # standby has taken over, which the cluster does not do on request
        # yet.
        primaries = self._primary_ids(instances)
        if primaries:
            raise exception.BadRequest(
                _("Member %(ids)s is the primary of the cluster and cannot "
                  "be removed; remove a standby.") % {
                    'ids': ', '.join(primaries)})
        return super(PostgresqlCluster, self).shrink(instances)


class PostgresqlRolesMixin(common_api.MemberRolesMixin):
    """The members' roles, with the standbys' as the primary sees them:
    a streaming standby is synchronous when the primary's commits wait
    for it. The replication mode comes with the first member that answers.
    """

    ROLES = ROLES

    def _member_roles(self, instances):
        answers = {}
        for instance in instances:
            if instance.status not in common_api.ANSWERING_STATUSES:
                continue
            try:
                answers[instance.id] = self.cluster.get_guest(
                    instance).get_member_role()
            except Exception as err:
                LOG.info("Member %s did not tell its role: %s", instance.id,
                         err)
        standbys = {}
        self._mode = None
        for answer in answers.values():
            if answer.get('state') == 'primary':
                standbys = answer.get('standbys') or {}
            self._mode = self._mode or answer.get('mode')
        roles = {}
        for instance in instances:
            answer = answers.get(instance.id)
            if not answer:
                roles[instance.id] = 'unknown'
                continue
            if (answer.get('state') == 'standby' and
                    answer.get('role') == 'STREAMING'):
                seen = standbys.get(instance.id)
                if seen:
                    answer = dict(answer, role='SYNC' if seen.get(
                        'sync_state') in SYNCHRONOUS_STATES else 'ASYNC')
            roles[instance.id] = self._role_name(answer)
        return roles

    def data(self):
        result = super(PostgresqlRolesMixin, self).data()
        if self.load_servers and getattr(self, '_mode', None):
            result['cluster'][MODE_KEY] = self._mode
        return result


class PostgresqlClusterView(PostgresqlRolesMixin, ClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])


class PostgresqlMgmtClusterView(PostgresqlRolesMixin, MgmtClusterView):

    def build_instances(self):
        return self._build_instances(['member'], ['member'])
