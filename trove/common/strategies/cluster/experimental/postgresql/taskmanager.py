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

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import PollTimeOut
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import (
    taskmanager as common_taskmanager)
from trove.common.strategies.cluster.experimental.postgresql import (
    api as pg_api)
from trove.common.strategies.cluster.experimental.postgresql import (
    recovery as pg_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    topology)
from trove.common import utils
from trove.guestagent.common import cluster_probe
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The account the standbys stream with, on every member; its password is
# the cluster's.
REPLICATION_USER = 'replicator'


class PostgresqlTaskManagerStrategy(
        common_taskmanager.RecoverableTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return task_api.API

    @property
    def task_manager_cluster_tasks_class(self):
        return PostgresqlClusterTasks

    def cluster_recovery(self, manager):
        return pg_recovery.PostgresqlClusterRecovery(manager)


class PostgresqlClusterTasks(common_taskmanager.ClusterEntryTasksMixin,
                             task_models.ClusterTasks):
    """Create, grow, shrink, restart and upgrade a PostgreSQL cluster.

    The member with the lowest address is the primary; the others are
    installed as standbys of it, each with a slot on the primary, and
    every member keeps the list of members and the mode. A standby that
    leaves takes its slot with it; a primary that is removed hands over
    to the standby that holds the most first (a switchover). Restarts
    and upgrades go standbys first, the primary last.
    """

    def _members(self, instances):
        return {instance.id: self.get_ip(instance) for instance in instances}

    def _roles(self, instances):
        """Each member's role answer, for those that answer."""
        answers = {}
        for instance in instances:
            try:
                answers[instance.id] = self.get_guest(
                    instance).get_member_role()
            except Exception as err:
                LOG.info("Member %s did not tell its role: %s", instance.id,
                         err)
        return answers

    def _primary(self, instances):
        """The primary among the members, and its cluster context."""
        answers = self._roles(instances)
        primaries = [instance for instance in instances
                     if answers.get(instance.id, {}).get('state') == 'primary']
        if len(primaries) != 1:
            raise TroveError(_("The cluster has %(n)s primaries, not one: "
                               "%(ids)s.") % {
                                   'n': len(primaries),
                                   'ids': [i.id for i in primaries]})
        primary = primaries[0]
        return primary, self.get_guest(primary).get_cluster_context()

    @staticmethod
    def _config(members, mode, replication_user, self_id, primary):
        """What a member is installed with: ``primary`` is None on the
        primary, the primary's id and host on a standby.
        """
        return {'members': members, 'mode': mode,
                'replication_user': replication_user, 'self_id': self_id,
                'primary': primary}

    def _install_standby(self, primary, primary_guest, instance, members,
                         mode, replication_user):
        primary_guest.create_member_slot(instance.id)
        self.get_guest(instance).install_cluster(self._config(
            members, mode, replication_user, instance.id,
            {'id': primary.id, 'host': self.get_ip(primary)}))

    def _update_members(self, instances, members, mode, primary_id=None):
        for instance in instances:
            self.get_guest(instance).update_cluster_members(
                members, mode, primary_id=primary_id)

    def _switchover(self, context, cluster_id, primary, standbys, members,
                    mode):
        """The primary hands over to a standby: writes stop on the primary
        (its fast stop sends the standbys every last WAL record), the
        standby that received the most is promoted, the other standbys
        follow it. Returns the new primary.
        """
        LOG.info("Cluster %s: switchover from %s.", cluster_id, primary.id)
        self.get_guest(primary).fence_member()
        views = {}
        for instance in standbys:
            try:
                views[instance.id] = self.get_guest(
                    instance).get_recovery_view()
            except Exception as err:
                LOG.info("Member %s did not answer: %s", instance.id, err)
        top = topology.Topology(standbys, [(i, views.get(i.id))
                                           for i in standbys])
        timelines = [top.timeline(i) for i in top.standbys
                     if top.timeline(i) is not None]
        candidates = topology.candidates(
            top, max(timelines) if timelines else None)
        if not candidates:
            raise TroveError(_("No standby can take over from the primary "
                               "%s.") % primary.id)
        chosen = topology.choose(top, candidates)
        target = [i for i in standbys if i.id == chosen][0]
        LOG.info("Cluster %s: switchover: promote %s (%s, lsn %s).",
                 cluster_id, chosen, self.get_ip(target),
                 top.answer(chosen).get('lsn'))
        self.get_guest(target).promote_member(members, mode)
        new_primary = {'id': chosen, 'host': self.get_ip(target)}
        for instance in standbys:
            if instance is not target:
                try:
                    self.get_guest(instance).follow_primary(new_primary)
                except Exception:
                    LOG.exception("Standby %s could not follow the new "
                                  "primary.", instance.id)
        self._update_members(standbys, members, mode, primary_id=chosen)
        return target

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
            instances = [Instance.load(context, instance_id)
                         for instance_id in instance_ids]
            members = self._members(instances)
            # The mode the members kept from their creation.
            mode = self.get_guest(instances[0]).get_cluster_context().get(
                'mode') or pg_api.SYNC
            replication_user = {
                'name': REPLICATION_USER,
                'password': utils.generate_random_password()}
            primary = min(instances,
                          key=lambda i: cluster_probe.ip_key(self.get_ip(i)))
            LOG.info("Forming a %s cluster of %s, primary %s.", mode,
                     sorted(members.values()), self.get_ip(primary))
            try:
                primary_guest = self.get_guest(primary)
                primary_guest.install_cluster(self._config(
                    members, mode, replication_user, primary.id, None))
                for instance in instances:
                    if instance is not primary:
                        self._install_standby(primary, primary_guest,
                                              instance, members, mode,
                                              replication_user)
                LOG.debug("Finalizing cluster configuration.")
                for instance in instances:
                    self.get_guest(instance).cluster_complete()
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

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        LOG.debug("Begin PostgreSQL grow_cluster for id: %s.", cluster_id)

        def _grow_cluster():
            db_instances = DBInstance.find_all(
                cluster_id=cluster_id, deleted=False).all()
            existing_instances = [Instance.load(context, db_inst.id)
                                  for db_inst in db_instances
                                  if db_inst.id not in new_instance_ids]
            if not existing_instances:
                raise TroveError(_("Unable to determine existing cluster "
                                   "member(s)"))
            primary, cluster_context = self._primary(existing_instances)
            primary_guest = self.get_guest(primary)
            mode = cluster_context.get('mode') or pg_api.SYNC
            replication_user = cluster_context['replication_user']

            if not self._all_instances_ready(new_instance_ids, cluster_id):
                raise TroveError(_("Instances in cluster did not report "
                                   "ACTIVE"))
            LOG.debug("All members ready, proceeding for cluster setup.")
            new_instances = [Instance.load(context, instance_id)
                             for instance_id in new_instance_ids]
            members = self._members(existing_instances + new_instances)
            for instance in new_instances:
                self._install_standby(primary, primary_guest, instance,
                                      members, mode, replication_user)
            self._check_cluster_for_root(context, existing_instances,
                                         new_instances)
            self._update_members(existing_instances + new_instances,
                                 members, mode)
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

    def shrink_cluster(self, context, cluster_id, removal_instance_ids):
        LOG.debug("Begin PostgreSQL shrink_cluster for id: %s.", cluster_id)

        def _shrink_cluster():
            db_instances = DBInstance.find_all(
                cluster_id=cluster_id, deleted=False).all()
            leftover_instances = [Instance.load(context, db_inst.id)
                                  for db_inst in db_instances
                                  if db_inst.id not in removal_instance_ids]
            removal_instances = [Instance.load(context, instance_id)
                                 for instance_id in removal_instance_ids]
            primary, cluster_context = self._primary(
                leftover_instances + removal_instances)
            mode = cluster_context.get('mode') or pg_api.SYNC
            if primary.id in removal_instance_ids:
                # The primary goes: a standby that stays takes over
                # first.
                primary = self._switchover(
                    context, cluster_id, primary, leftover_instances,
                    self._members(leftover_instances), mode)
            primary_guest = self.get_guest(primary)
            for instance in removal_instances:
                # Its ports close before it goes: nothing new is sent to
                # it.
                try:
                    self.get_guest(instance).leave_cluster()
                except Exception:
                    LOG.exception("Instance %s could not leave the "
                                  "cluster.", instance.id)
                try:
                    primary_guest.drop_member_slot(instance.id)
                except Exception:
                    LOG.exception("The slot of instance %s could not be "
                                  "dropped.", instance.id)
                Instance.delete(instance)

            def all_instances_marked_deleted():
                non_deleted_ids = [
                    db_instance.id for db_instance in DBInstance.find_all(
                        cluster_id=cluster_id, deleted=False).all()]
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

            self._update_members(leftover_instances,
                                 self._members(leftover_instances), mode)
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
            self.update_statuses_on_failure(cluster_id)
        except Exception:
            LOG.exception("Error shrinking cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End shrink_cluster for id: %s.", cluster_id)

    def _standbys_first(self, context, cluster_id):
        """A sort key: the primary after the standbys, so that a rolling
        restart or upgrade interrupts writes once, at the end.
        """
        instances = [Instance.load(context, db_inst.id)
                     for db_inst in DBInstance.find_all(
                         cluster_id=cluster_id, deleted=False).all()]
        primaries = {instance_id for instance_id, answer in
                     self._roles(instances).items()
                     if answer.get('state') == 'primary'}
        return lambda instance: (1 if instance.id in primaries else 0,
                                 instance.id)

    def restart_cluster(self, context, cluster_id):
        self.rolling_restart_cluster(
            context, cluster_id,
            ordering_function=self._standbys_first(context, cluster_id))

    def upgrade_cluster(self, context, cluster_id, datastore_version):
        self.rolling_upgrade_cluster(
            context, cluster_id, datastore_version,
            ordering_function=self._standbys_first(context, cluster_id))
