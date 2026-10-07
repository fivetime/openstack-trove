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

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common import clients
from trove.common.clients import create_nova_client
from trove.common.exception import PollTimeOut
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common import loadbalancer
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_taskmanager)
from trove.common.strategies.cluster.experimental.group_replication import (
    api as gr_api)
from trove.common.template import ClusterConfigTemplate
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.instance import tasks as inst_tasks
from trove.taskmanager import api as task_api

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The port the members talk to each other on (group_replication_local_address).
GROUP_PORT = 33061
# The longest password the recovery channel takes (error 3056): shorter
# than the passwords Trove generates by default.
REPLICATION_PASSWORD_LENGTH = 32


class GroupReplicationTaskManagerStrategy(
        galera_taskmanager.GaleraCommonTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return task_api.API

    @property
    def task_manager_cluster_tasks_class(self):
        return GroupReplicationClusterTasks


class GroupReplicationClusterTasks(
        galera_taskmanager.GaleraCommonClusterTasks):
    """Create, grow and shrink a Group Replication cluster.

    The order is Galera's: the members get the admin password of the
    cluster and the cluster configuration, the first forms the group and
    the others join it one at a time, and ``cluster_complete`` ends it. A
    member that is to join is let in by the others first, and one that is
    removed leaves the group before it is deleted.
    """

    def _render_cluster_config(self, context, instance, cluster_ips,
                               cluster_name, replication_user,
                               mode=gr_api.SINGLE_PRIMARY):
        client = create_nova_client(context)
        flavor = client.flavors.get(instance.flavor_id)
        instance_ip = self.get_ip(instance)
        ips = cluster_ips.split(',')
        config = ClusterConfigTemplate(
            self.datastore_version, flavor, instance.id)
        return config.render(
            cluster_ips=','.join(ips),
            group_seeds=','.join('%s:%s' % (ip, GROUP_PORT) for ip in ips),
            group_port=GROUP_PORT,
            cluster_name=cluster_name,
            instance_ip=instance_ip,
            instance_name=instance.name,
            single_primary=(mode == gr_api.SINGLE_PRIMARY),
        )

    # The load balancer: the cluster's endpoint, in front of the members
    # that take writes.

    def _load_balancer_enabled(self):
        return CONF.get(self.datastore_version.manager).cluster_load_balancer

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
                'protocol_port': conf.group_replication_ready_port,
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
        super(GroupReplicationClusterTasks, self).delete_cluster(
            context, cluster_id)

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s.", cluster_id)

        def _create_cluster():
            db_instances = DBInstance.find_all(cluster_id=cluster_id).all()
            instance_ids = [db_instance.id for db_instance in db_instances]

            LOG.debug("Waiting for instances to get to cluster-ready status.")
            if not self._all_instances_ready(instance_ids, cluster_id):
                raise TroveError(_("Instances in cluster did not report "
                                   "ACTIVE"))

            LOG.debug("All members ready, proceeding for cluster setup.")
            instances = [Instance.load(context, instance_id) for instance_id
                         in instance_ids]
            cluster_ips = ",".join(self.get_ip(instance)
                                   for instance in instances)
            guests = [self.get_guest(instance) for instance in instances]

            replication_user = {
                "name": self.CLUSTER_REPLICATION_USER,
                "password": utils.generate_random_password(
                    REPLICATION_PASSWORD_LENGTH),
            }
            # The group name is a UUID, which a member must know to join.
            group_name = utils.generate_uuid()
            # Kept by every member from its creation.
            mode = guests[0].get_cluster_context().get(
                'mode', gr_api.SINGLE_PRIMARY)
            LOG.info("Forming a %s group of %s.", mode, cluster_ips)

            try:
                # A member that joins gets the accounts of the group, so
                # the admin password every guest agent keeps has to be the
                # group's before.
                admin_password = str(utils.generate_random_password())
                for guest in guests:
                    guest.reset_admin_password(admin_password)

                bootstrap = True
                for instance, guest in zip(instances, guests):
                    configuration = self._render_cluster_config(
                        context, instance, cluster_ips, group_name,
                        replication_user, mode=mode)
                    guest.install_cluster(replication_user, configuration,
                                          bootstrap)
                    bootstrap = False

                LOG.debug("Finalizing cluster configuration.")
                for guest in guests:
                    guest.cluster_complete()

                # A cluster without its endpoint is of no use: a failure
                # here fails the cluster like any other step.
                if self._load_balancer_enabled():
                    self._sync_load_balancer(context, cluster_id, instances)
            except Exception:
                LOG.exception("Error creating cluster.")
                self.update_statuses_on_failure(cluster_id)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _create_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for building cluster.")
            self.update_statuses_on_failure(cluster_id)
        except TroveError:
            LOG.exception("Error creating cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s.", cluster_id)

    def _update_members(self, context, instances, cluster_ips,
                        cluster_context):
        """Give the members the members of the group: whom they let in and
        look for, also while running.
        """
        for instance in instances:
            configuration = self._render_cluster_config(
                context, instance, cluster_ips,
                cluster_context['cluster_name'],
                cluster_context['replication_user'],
                mode=cluster_context['mode'])
            self.get_guest(instance).write_cluster_configuration_overrides(
                configuration)

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        LOG.debug("Begin Group Replication grow_cluster for id: %s.",
                  cluster_id)

        def _grow_cluster():
            db_instances = DBInstance.find_all(
                cluster_id=cluster_id, deleted=False).all()
            existing_instances = [Instance.load(context, db_inst.id)
                                  for db_inst in db_instances
                                  if db_inst.id not in new_instance_ids]
            if not existing_instances:
                raise TroveError(_("Unable to determine existing cluster "
                                   "member(s)"))
            cluster_context = self.get_guest(
                existing_instances[0]).get_cluster_context()

            if not self._all_instances_ready(new_instance_ids, cluster_id):
                raise TroveError(_("Instances in cluster did not report "
                                   "ACTIVE"))

            LOG.debug("All members ready, proceeding for cluster setup.")
            new_instances = [Instance.load(context, instance_id)
                             for instance_id in new_instance_ids]
            cluster_ips = ",".join(
                self.get_ip(instance)
                for instance in existing_instances + new_instances)

            # The group lets the new members in before they try.
            self._update_members(context, existing_instances, cluster_ips,
                                 cluster_context)

            for instance in new_instances:
                guest = self.get_guest(instance)
                guest.reset_admin_password(cluster_context['admin_password'])
                configuration = self._render_cluster_config(
                    context, instance, cluster_ips,
                    cluster_context['cluster_name'],
                    cluster_context['replication_user'],
                    mode=cluster_context['mode'])
                guest.install_cluster(cluster_context['replication_user'],
                                      configuration, False)

            self._check_cluster_for_root(context, existing_instances,
                                         new_instances)

            for instance in new_instances:
                self.get_guest(instance).cluster_complete()

            self._sync_load_balancer_or_log(
                context, cluster_id, existing_instances + new_instances)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _grow_cluster()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for growing cluster.")
            self._fail_new_members(new_instance_ids)
        except Exception:
            LOG.exception("Error growing cluster %s.", cluster_id)
            self._fail_new_members(new_instance_ids)
        finally:
            timeout.cancel()
            # The cluster goes on, with or without the new members.
            self.reset_task()

        LOG.debug("End grow_cluster for id: %s.", cluster_id)

    def _fail_new_members(self, new_instance_ids):
        """A grow that failed: the members that were to join are failed,
        the cluster is left as it was. Galera's handling marks every
        member failed and leaves the cluster growing for good; a member
        that found no host (strict anti-affinity) is the usual cause, and
        the cluster is whole without it.
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

    def shrink_cluster(self, context, cluster_id, removal_instance_ids):
        LOG.debug("Begin Group Replication shrink_cluster for id: %s.",
                  cluster_id)

        def _shrink_cluster():
            removal_instances = [Instance.load(context, instance_id)
                                 for instance_id in removal_instance_ids]
            for instance in removal_instances:
                # Leaving is quicker and cleaner than being expelled once
                # gone; a member that cannot is expelled all the same.
                try:
                    self.get_guest(instance).leave_cluster()
                except Exception:
                    LOG.exception("Instance %s could not leave the group.",
                                  instance.id)
                Instance.delete(instance)

            def all_instances_marked_deleted():
                non_deleted_ids = [db_instance.id for db_instance in
                                   DBInstance.find_all(
                                       cluster_id=cluster_id,
                                       deleted=False).all()]
                return not set(removal_instance_ids).intersection(
                    non_deleted_ids)
            try:
                LOG.info("Deleting instances (%s)", removal_instance_ids)
                utils.poll_until(all_instances_marked_deleted,
                                 sleep_time=2,
                                 time_out=CONF.cluster_delete_time_out)
            except PollTimeOut:
                LOG.error("timeout for instances to be marked as deleted.")
                return

            leftover_instances = [
                Instance.load(context, db_inst.id)
                for db_inst in DBInstance.find_all(
                    cluster_id=cluster_id, deleted=False).all()
                if db_inst.id not in removal_instance_ids]
            cluster_ips = ",".join(self.get_ip(instance)
                                   for instance in leftover_instances)
            cluster_context = self.get_guest(
                leftover_instances[0]).get_cluster_context()
            self._update_members(context, leftover_instances, cluster_ips,
                                 cluster_context)
            self._sync_load_balancer_or_log(context, cluster_id,
                                            leftover_instances)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _shrink_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for shrinking cluster.")
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.SHRINKING_ERROR)
        except Exception:
            LOG.exception("Error shrinking cluster %s.", cluster_id)
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.SHRINKING_ERROR)
        finally:
            timeout.cancel()

        LOG.debug("End shrink_cluster for id: %s.", cluster_id)
