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

from oslo_log import log as logging

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster import strategy
from trove.common import wsgi
from trove.datastore import models as datastore_models
from trove.extensions.common.service import ClusterRootController
from trove.extensions.common.service import DefaultRootController
from trove.instance.models import DBInstance

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class GroupReplicationRootController(DefaultRootController):
    """Root of a single MySQL instance as before; root of a Group
    Replication cluster is enabled on a member that takes writes, and the
    group replicates it to the others.
    """

    def __init__(self):
        self._cluster = ClusterRootController()

    def root_index(self, req, tenant_id, instance_id, is_cluster):
        if is_cluster:
            return self._cluster.root_index(req, tenant_id, instance_id,
                                            is_cluster)
        return super(GroupReplicationRootController, self).root_index(
            req, tenant_id, instance_id, is_cluster)

    def root_create(self, req, body, tenant_id, instance_id, is_cluster):
        if not is_cluster:
            return super(GroupReplicationRootController, self).root_create(
                req, body, tenant_id, instance_id, is_cluster)
        context = req.environ[wsgi.CONTEXT_KEY]
        member_ids = self._cluster._find_cluster_node_ids(tenant_id,
                                                          instance_id)
        writable_id = self._writable_member(context, member_ids)
        LOG.info("Enabling root for cluster '%(cluster)s' on member "
                 "%(member)s.", {'cluster': instance_id,
                                 'member': writable_id})
        return self._cluster.instance_root_create(req, body, writable_id,
                                                  member_ids)

    def root_delete(self, req, tenant_id, instance_id, is_cluster):
        if is_cluster:
            raise exception.ClusterOperationNotSupported(
                operation='disable_root')
        return super(GroupReplicationRootController, self).root_delete(
            req, tenant_id, instance_id, is_cluster)

    @staticmethod
    def _writable_member(context, member_ids):
        for member_id in member_ids:
            manager = datastore_models.DatastoreVersion.load_by_uuid(
                DBInstance.find_by(id=member_id).datastore_version_id).manager
            guest = strategy.load_guestagent_strategy(
                manager).guest_client_class(context, member_id)
            try:
                if guest.is_writable_member():
                    return member_id
            except Exception:
                LOG.exception("Could not ask member %s.", member_id)
        raise exception.UnprocessableEntity(
            _("No member of the cluster takes writes now."))
