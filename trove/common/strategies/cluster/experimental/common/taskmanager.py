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

"""The task manager side of what clusters share: the load balancer in
front of the members, the cluster's endpoint; deleting the cluster with
it; failing a grow without failing the cluster; and the strategy's
periodic recovery.
"""

from oslo_log import log as logging

from trove.common import cfg
from trove.common import clients
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common import loadbalancer
from trove.common.strategies.cluster import base as cluster_base
from trove.instance.models import DBInstance
from trove.instance import tasks as inst_tasks


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class RecoverableTaskManagerStrategy(cluster_base.BaseTaskManagerStrategy):
    """A task manager strategy whose clusters the task manager looks at
    periodically; by default it does not.
    """

    def cluster_recovery(self, manager):
        """What brings the clusters of a datastore manager back, run by
        the task manager periodically (a ``ClusterRecovery``); None for a
        datastore whose clusters need none.
        """
        return None


class ClusterEntryTasksMixin(object):
    """The load balancer of a cluster: its endpoint, in front of the
    members that take writes. A cluster whose datastore has no
    cluster_load_balancer option has none. List it before the cluster
    tasks class it extends.
    """

    def _load_balancer_enabled(self):
        conf = CONF.get(self.datastore_version.manager)
        return getattr(conf, 'cluster_load_balancer', False)

    def _member_subnet_id(self, context, instance, address):
        """The subnet of the member's port on the user's network, where the
        load balancer reaches it and lives.
        """
        ports = clients.create_neutron_client(context).list_ports(
            name='trove-%s' % instance.id).get('ports', [])
        for port in ports:
            for fixed_ip in port.get('fixed_ips', []):
                if fixed_ip.get('ip_address') == address:
                    return fixed_ip['subnet_id']
        if ports and ports[0].get('fixed_ips'):
            return ports[0]['fixed_ips'][0]['subnet_id']
        raise TroveError(_("Member %(id)s has no port on the user's "
                           "network.") % {'id': instance.id})

    def _load_balancer_members(self, context, instances):
        conf = CONF.get(self.datastore_version.manager)
        members = []
        for instance in instances:
            address = self.get_ip(instance)
            members.append({
                'name': instance.id, 'address': address,
                'protocol_port': conf.cluster_ready_port,
                'subnet_id': self._member_subnet_id(context, instance,
                                                    address)})
        return members

    def _sync_load_balancer(self, context, cluster_id, instances):
        """The load balancer of the cluster, with these members."""
        conf = CONF.get(self.datastore_version.manager)
        members = self._load_balancer_members(context, instances)
        loadbalancer.ensure_load_balancer(
            loadbalancer.OctaviaClient(),
            loadbalancer.cluster_load_balancer_name(cluster_id),
            members[0]['subnet_id'], members, conf.cluster_load_balancer_port,
            description='Endpoint of Trove cluster %s' % cluster_id)

    def _delete_load_balancer(self, cluster_id):
        loadbalancer.delete_load_balancer(
            loadbalancer.OctaviaClient(),
            loadbalancer.cluster_load_balancer_name(cluster_id))

    def _sync_load_balancer_or_log(self, context, cluster_id, instances):
        """For a grow or a shrink: the cluster works without, and the next
        change tries again.
        """
        if not self._load_balancer_enabled():
            return
        try:
            self._sync_load_balancer(context, cluster_id, instances)
        except Exception:
            LOG.exception("The load balancer of cluster %s could not be "
                          "brought up to date.", cluster_id)

    def delete_cluster(self, context, cluster_id):
        if self._load_balancer_enabled():
            try:
                self._delete_load_balancer(cluster_id)
            except Exception:
                LOG.exception("The load balancer of cluster %s could not "
                              "be deleted.", cluster_id)
        super(ClusterEntryTasksMixin, self).delete_cluster(
            context, cluster_id)

    def _fail_new_members(self, new_instance_ids):
        """A grow that failed: the members that were to join are failed,
        the cluster is left as it was. A member that found no host (strict
        anti-affinity) is the usual cause, and the cluster is whole
        without it.
        """
        for instance_id in new_instance_ids:
            try:
                db_instance = DBInstance.find_by(id=instance_id)
                db_instance.set_task_status(
                    inst_tasks.InstanceTasks.GROWING_ERROR)
                db_instance.save()
            except Exception:
                LOG.exception("Could not mark member %s as failed.",
                              instance_id)
