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

"""The sentinel topology of a Redis family cluster, in the task manager.

A cluster is built once every member is up: the members get the
cluster's three passwords (the cluster admin the replicas sync as, the
sentinel user the sentinels log in to the servers as, the sentinels'
own admin), the first becomes the primary and the others its replicas,
each with its sentinel; then the cluster's load balancer, without which
the cluster is of no use. The sentinels elect the primary from then on:
the task manager only records the cluster's health
(``common.health.HealthWatch``), as for every Redis family cluster.
"""

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import (
    taskmanager as common_taskmanager)
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_api)
from trove.common import utils
from trove.extensions.common import models as ext_models
from trove.instance.models import DBInstance
from trove.instance.models import Instance
import trove.taskmanager.models as task_models

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


def cluster_passwords():
    """The passwords a new cluster's members share, by what they are for."""
    return {name: utils.generate_random_password()
            for name in ('admin_password', 'sentinel_password',
                         'sentinel_admin_password')}


class RedisSentinelClusterTasks(common_taskmanager.ClusterEntryTasksMixin,
                                task_models.ClusterTasks):

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster (sentinel) for id: %s.", cluster_id)

        def _create_cluster():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            instance_ids = [db_instance.id for db_instance in db_instances]
            if not self._all_instances_ready(instance_ids, cluster_id):
                raise TroveError(_("Instances in cluster did not report "
                                   "ACTIVE"))
            instances = [Instance.load(context, instance_id)
                         for instance_id in instance_ids]
            guests = [self.get_guest(instance) for instance in instances]
            ips = [guest.get_node_ip()[0] for guest in guests]
            mode = self.db_info.get_properties().get(
                sentinel_api.MODE_KEY, sentinel_api.SYNC)
            config = dict(cluster_passwords(), primary=ips[0], members=ips,
                          mode=mode)
            LOG.info("Cluster %s: primary %s, replicas %s, %s mode.",
                     cluster_id, ips[0], ', '.join(ips[1:]), mode)
            try:
                # The primary first: the replicas sync with it as they
                # join.
                for guest in guests:
                    guest.sentinel_install(config)
                for guest in guests:
                    guest.cluster_complete()
                # A cluster without its endpoint is of no use: a failure
                # here fails the cluster like any other step.
                if self._load_balancer_enabled():
                    self._sync_load_balancer(context, cluster_id, instances)
            except Exception:
                LOG.exception("Error creating cluster %s.", cluster_id)
                self.update_statuses_on_failure(cluster_id)
                return False
            return True

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            if _create_cluster():
                self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise
            LOG.exception("Timeout for building cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        except TroveError:
            LOG.exception("Error creating cluster %s.", cluster_id)
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()
        LOG.debug("End create_cluster (sentinel) for id: %s.", cluster_id)

    # After the build

    def _roles(self, instances):
        """{instance id: master | replica | None} as the members tell."""
        roles = {}
        for instance in instances:
            try:
                answer = self.get_guest(instance).get_member_role() or {}
                roles[instance.id] = answer.get('role')
            except Exception as err:
                LOG.info("Member %s did not tell its role: %s",
                         instance.id, err)
                roles[instance.id] = None
        return roles

    def _members(self, context, cluster_id, but=()):
        return [Instance.load(context, db_instance.id)
                for db_instance in DBInstance.find_all(
                    cluster_id=cluster_id, deleted=False).all()
                if db_instance.id not in but]

    def _primary_of(self, instances):
        for instance_id, role in self._roles(instances).items():
            if role == 'master':
                return next(i for i in instances if i.id == instance_id)
        raise TroveError(_("No member of the cluster is its primary."))

    def _set_quorum(self, instances):
        quorum = len(instances) // 2 + 1
        for instance in instances:
            self.get_guest(instance).sentinel_set_quorum(quorum)

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        """The new members join as replicas of the primary, each with its
        sentinel; the sentinels agree by a majority of them all; root, if
        it is enabled, is enabled on them with the cluster's password.
        """
        LOG.debug("Begin grow_cluster (sentinel) for id: %s.", cluster_id)

        def _grow_cluster():
            if not self._all_instances_ready(new_instance_ids, cluster_id):
                raise TroveError(_("The new members did not report "
                                   "ACTIVE."))
            existing = self._members(context, cluster_id,
                                     but=new_instance_ids)
            new = [Instance.load(context, instance_id)
                   for instance_id in new_instance_ids]
            primary = self._primary_of(existing)
            primary_guest = self.get_guest(primary)
            members = existing + new
            config = dict(primary_guest.get_sentinel_config(),
                          primary=primary_guest.get_node_ip()[0],
                          members=[self.get_guest(i).get_node_ip()[0]
                                   for i in members])
            LOG.info("Cluster %s: %s join as replicas of %s.", cluster_id,
                     ', '.join(i.id for i in new), config['primary'])
            for instance in new:
                self.get_guest(instance).sentinel_install(config)
            for instance in new:
                self.get_guest(instance).cluster_complete()
            self._set_quorum(members)
            self._enable_root_on(context, existing, new)
            if self._load_balancer_enabled():
                self._sync_load_balancer(context, cluster_id, members)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _grow_cluster()
        except Timeout as t:
            if t is not timeout:
                raise
            LOG.exception("Timeout for growing cluster %s.", cluster_id)
            self._fail_new_members(new_instance_ids)
        except Exception:
            LOG.exception("Error growing cluster %s.", cluster_id)
            self._fail_new_members(new_instance_ids)
        finally:
            timeout.cancel()
            self.reset_task()
        LOG.debug("End grow_cluster (sentinel) for id: %s.", cluster_id)

    def _enable_root_on(self, context, existing, new):
        """Root of a Redis server is a user of its own, not data the
        replicas get: the members that join get the cluster's root.
        """
        if not any(ext_models.Root.load(context, i.id) for i in existing):
            return
        password = self.get_guest(existing[0]).get_root_password()
        for instance in new:
            self.get_guest(instance).enable_root_with_password(password)
            ext_models.RootHistory.create(context, instance.id)

    def shrink_cluster(self, context, cluster_id, removal_ids):
        """The members that go are never to be elected; the primary, if it
        is one of them, hands over first; they are deleted; the sentinels
        that stay agree by a majority of them, and learn the members again
        one at a time.
        """
        LOG.debug("Begin shrink_cluster (sentinel) for id: %s.", cluster_id)

        def _shrink_cluster():
            leaving = [Instance.load(context, instance_id)
                       for instance_id in removal_ids]
            staying = self._members(context, cluster_id, but=removal_ids)
            for instance in leaving:
                try:
                    self.get_guest(instance).sentinel_leave()
                except Exception:
                    LOG.exception("Member %s cannot be kept from being "
                                  "elected (it is down?).", instance.id)
            if self._primary_of(leaving + staying).id in removal_ids:
                self.get_guest(staying[0]).sentinel_failover()
            for instance in leaving:
                Instance.delete(instance)

            def gone():
                ids = {db.id for db in DBInstance.find_all(
                    cluster_id=cluster_id, deleted=False).all()}
                return not ids.intersection(removal_ids)
            utils.poll_until(gone, sleep_time=2,
                             time_out=CONF.cluster_delete_time_out)
            self._set_quorum(staying)
            for instance in staying:
                self.get_guest(instance).sentinel_reset(len(staying))
            if self._load_balancer_enabled():
                self._sync_load_balancer(context, cluster_id, staying)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _shrink_cluster()
        except Timeout as t:
            if t is not timeout:
                raise
            LOG.exception("Timeout for shrinking cluster %s.", cluster_id)
        except Exception:
            LOG.exception("Error shrinking cluster %s.", cluster_id)
        finally:
            timeout.cancel()
            self.reset_task()
        LOG.debug("End shrink_cluster (sentinel) for id: %s.", cluster_id)

    # Rolling actions: the replicas first, the primary last and only once
    # it has handed over, so that writes stop once, for a failover.

    def _replicas_first(self, context, cluster_id):
        roles = self._roles(self._members(context, cluster_id))
        return lambda instance: (1 if roles.get(instance.id) == 'master'
                                 else 0, instance.id)

    def _hand_over(self, instance):
        """Before the primary is restarted or upgraded: another member's
        sentinels elect another primary.
        """
        if self._roles([instance]).get(instance.id) != 'master':
            return
        others = self._members(instance.context, self.id, but=(instance.id,))
        if others:
            self.get_guest(others[0]).sentinel_failover()

    def restart_cluster(self, context, cluster_id):
        self.rolling_restart_cluster(
            context, cluster_id,
            ordering_function=self._replicas_first(context, cluster_id),
            before_member=self._hand_over)

    def upgrade_cluster(self, context, cluster_id, datastore_version):
        self.rolling_upgrade_cluster(
            context, cluster_id, datastore_version,
            ordering_function=self._replicas_first(context, cluster_id),
            before_member=self._hand_over)
