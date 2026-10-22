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

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import topology
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_api)
from trove.common.strategies.cluster.experimental.redis import (
    sentinel_taskmanager)
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.instance.models import load_any_instance
from trove.instance import tasks as inst_tasks
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models


LOG = logging.getLogger(__name__)
CONF = cfg.CONF
# The flags of a node the cluster holds failing, or about to.
FAILING = frozenset(['fail', 'fail?', 'noaddr'])


class RedisTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return RedisTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return CLUSTER_TASKS

    @property
    def task_manager_manager_actions(self):
        return {'add_node': self._manager_add_node}

    def _manager_add_node(self, context, cluster_id, instance_id):
        cluster_tasks = task_models.ClusterTasks.load(
            context, cluster_id, RedisClusterTasks)
        cluster_tasks.add_node(context, cluster_id, instance_id)


class RedisClusterTasks(task_models.ClusterTasks):

    # A Redis Cluster spreads 16384 hash slots over its masters.
    TOTAL_SLOTS = 16384

    @staticmethod
    def slot_ranges(num_nodes):
        """[(first, last), ...]: an even share of the slots for each node,
        the first ones taking one more while there are leftovers.
        """
        per_node, leftover = divmod(RedisClusterTasks.TOTAL_SLOTS, num_nodes)
        ranges = []
        first = 0
        for index in range(num_nodes):
            count = per_node + (1 if index < leftover else 0)
            ranges.append((first, first + count - 1))
            first += count
        return ranges

    def _groups(self, context, db_instances):
        """([master], [(replica, its master)]) as the instances were made:
        a master and its replicas share a shard_id.
        """
        masters = {}
        for db_instance in db_instances:
            if db_instance.type != 'replica':
                masters[db_instance.shard_id or db_instance.id] = (
                    Instance.load(context, db_instance.id))
        replicas = [(Instance.load(context, db_instance.id),
                     masters[db_instance.shard_id])
                    for db_instance in db_instances
                    if db_instance.type == 'replica']
        return list(masters.values()), replicas

    def _replicate(self, replicas):
        for replica, master in replicas:
            self.get_guest(replica).cluster_replicate(
                self.get_guest(master).get_node_id())

    def _join(self, head_guest, new_insts, expected_nodes):
        """The new members join the cluster at its head: the cluster's
        admin account, the root user the others have, the meet. Returns
        their guests once each sees the whole cluster.
        """
        (cluster_head_ip, cluster_head_port) = head_guest.get_node_ip()
        # A list: the guests are gone through more than once (a map
        # was, and every loop after the first did nothing).
        new_guests = [self.get_guest(inst) for inst in new_insts]

        password = head_guest.get_cluster_password()
        for guest in new_guests:
            guest.cluster_init(password)

        # A client is sent to the new members too: they need the root
        # user the others have, if root is enabled.
        root_password = head_guest.get_root_password()
        if root_password:
            for guest in new_guests:
                guest.enable_root_with_password(root_password)

        # Connect nodes to the cluster head
        for guest in new_guests:
            guest.cluster_meet(cluster_head_ip, cluster_head_port)

        # A node that has just met the cluster refuses slots until it
        # sees all of it.
        for guest in new_guests:
            guest.cluster_wait(expected_nodes)
        return new_guests

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s.", cluster_id)

        def _create_cluster():

            # Fetch instances by cluster_id against instances table.
            db_instances = DBInstance.find_all(cluster_id=cluster_id).all()
            instance_ids = [db_instance.id for db_instance in db_instances]

            # Wait for cluster members to get to cluster-ready status.
            if not self._all_instances_ready(instance_ids, cluster_id):
                return

            LOG.debug("All members ready, proceeding for cluster setup.")
            masters, replicas = self._groups(context, db_instances)
            instances = masters + [r for r, _master in replicas]
            guests = [self.get_guest(instance) for instance in instances]
            try:
                # Every member gets the account the cluster is managed with.
                password = utils.generate_random_password()
                for guest in guests:
                    guest.cluster_init(password)

                # Connect nodes to the first node
                cluster_head = masters[0]
                cluster_head_port = '6379'
                cluster_head_ip = self.get_ip(cluster_head)
                for guest in guests[1:]:
                    guest.cluster_meet(cluster_head_ip, cluster_head_port)

                # The masters share the slots; a replica holds none.
                for master, (first_slot, last_slot) in zip(
                        masters, self.slot_ranges(len(masters))):
                    self.get_guest(master).cluster_addslots(first_slot,
                                                            last_slot)

                for guest in guests:
                    guest.cluster_wait(len(guests))
                self._replicate(replicas)
                for guest in guests:
                    guest.cluster_complete()
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
        finally:
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s.", cluster_id)

    def grow_cluster(self, context, cluster_id, new_instance_ids):
        LOG.debug("Begin grow_cluster for id: %s.", cluster_id)

        def _grow_cluster():

            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            cluster_head = next(Instance.load(context, db_inst.id)
                                for db_inst in db_instances
                                if db_inst.id not in new_instance_ids)
            if not cluster_head:
                raise TroveError(_("Unable to determine existing Redis cluster"
                                   " member"))
            head_guest = self.get_guest(cluster_head)

            # Wait for cluster members to get to cluster-ready status.
            if not self._all_instances_ready(new_instance_ids, cluster_id):
                return

            LOG.debug("All members ready, proceeding for cluster setup.")
            new_masters, new_replicas = self._groups(
                context, [db_inst for db_inst in db_instances
                          if db_inst.id in new_instance_ids])
            new_insts = new_masters + [r for r, _master in new_replicas]
            new_guests = self._join(head_guest, new_insts, len(db_instances))

            # Replicas first: an empty node that is still a master would
            # take slots in the rebalance.
            self._replicate(new_replicas)

            # The new masters are empty: give them their share of the
            # slots, with the keys in them.
            head_guest.cluster_rebalance(use_empty_masters=True)

            for guest in new_guests:
                guest.cluster_complete()

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _grow_cluster()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for growing cluster.")
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.GROWING_ERROR)
        except Exception:
            LOG.exception("Error growing cluster %s.", cluster_id)
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.GROWING_ERROR)
        finally:
            timeout.cancel()
            # Failed or not, the cluster can be acted on again: the members
            # carry the error, and a task left set would refuse even a
            # delete.
            self.reset_task()

        LOG.debug("End grow_cluster for id: %s.", cluster_id)

    def add_node(self, context, cluster_id, instance_id):
        """The repair: the node in place of a lost one joins the cluster
        and follows the master with the fewest replicas (the lost one's
        slots went to its replica with the failover: the cluster is short
        of a replica, whichever the lost one was). Its kind and group in
        the books are the lost one's, which a grow counts the masters by.
        """
        LOG.debug("Begin add_node %(instance)s to cluster %(cluster)s.",
                  {'instance': instance_id, 'cluster': cluster_id})

        def _add_node():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            cluster_head = next(Instance.load(context, db_inst.id)
                                for db_inst in db_instances
                                if db_inst.id != instance_id)
            head_guest = self.get_guest(cluster_head)
            if not self._all_instances_ready([instance_id], cluster_id):
                return
            new = Instance.load(context, instance_id)
            guest = self._join(head_guest, [new], len(db_instances))[0]
            nodes = head_guest.get_cluster_nodes()
            replicas = {n['id']: 0 for n in nodes
                        if n['role'] == 'master' and n['has_slots']
                        and not FAILING & set(n['flags'])}
            if not replicas:
                raise TroveError(_("No master holds slots."))
            for n in nodes:
                if n['master_id'] in replicas:
                    replicas[n['master_id']] += 1
            guest.cluster_replicate(min(replicas, key=replicas.get))
            guest.cluster_complete()

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _add_node()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout adding node %s to cluster %s.",
                          instance_id, cluster_id)
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.GROWING_ERROR,
                instance_ids=[instance_id])
        except Exception:
            LOG.exception("Error adding node %s to cluster %s.",
                          instance_id, cluster_id)
            self.update_statuses_on_failure(
                cluster_id, status=inst_tasks.InstanceTasks.GROWING_ERROR,
                instance_ids=[instance_id])
        finally:
            timeout.cancel()
            self.reset_task()

        LOG.debug("End add_node %s.", instance_id)

    def _nodes_of(self, cluster_id, removed, nodes):
        """The node ids of the members that leave, and among them those
        of the members without a server (one the repair replaces): such
        a member is asked nothing, its node is the one at the address
        the cluster's health learnt of it, and it must hold no slots (its
        replica took them with the failover; otherwise nothing serves
        them, and a rebalance cannot move them off a node that is gone).
        """
        at = {name: address for address, name in
              health.known_members(cluster_id).items()}
        removed_ids, gone_ids = set(), set()
        for inst in removed:
            if inst.server is not None:
                removed_ids.add(self.get_guest(inst).get_node_id())
                continue
            node = next((n for n in nodes
                         if n['address'] == at.get(inst.name)), None)
            if node is None:
                raise TroveError(_("Member %s has no server, and the "
                                   "cluster knows no node of it.") %
                                 inst.name)
            if node['has_slots']:
                raise TroveError(_("Member %s has no server and still "
                                   "holds slots: nothing serves them.") %
                                 inst.name)
            removed_ids.add(node['id'])
            gone_ids.add(node['id'])
        return removed_ids, gone_ids

    def shrink_cluster(self, context, cluster_id, removal_ids):
        LOG.debug("Begin shrink_cluster for id: %s.", cluster_id)

        def _shrink_cluster():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            remaining = [Instance.load(context, db_inst.id)
                         for db_inst in db_instances
                         if db_inst.id not in removal_ids]
            # A member whose server is gone (one the repair replaces)
            # goes too: loaded without its server.
            removed = [load_any_instance(context, instance_id)
                       for instance_id in removal_ids]
            guest_at = {self.get_ip(inst): self.get_guest(inst)
                        for inst in remaining}
            head_guest = self.get_guest(remaining[0])

            # The roles as they are: a failover swaps a master and its
            # replica, which the instances' types do not follow.
            nodes = head_guest.get_cluster_nodes()
            removed_ids, gone_ids = self._nodes_of(cluster_id, removed,
                                                   nodes)
            kept_masters = [n for n in nodes if n['id'] not in removed_ids
                            and n['role'] == 'master' and n['has_slots']]
            if not kept_masters:
                raise TroveError(_("Removing these members would leave no "
                                   "master to hold the slots."))

            # Move every slot, with its keys, off the leaving masters.
            draining = {n['id']: 0 for n in nodes
                        if n['id'] in removed_ids and n['role'] == 'master'
                        and n['has_slots']}
            if draining:
                head_guest.cluster_rebalance(weights=draining)

            # A replica that stays follows a master that stays: the one with
            # the fewest replicas.
            followers = {m['id']: 0 for m in kept_masters}
            for n in nodes:
                if n['master_id'] in followers and n['id'] not in removed_ids:
                    followers[n['master_id']] += 1
            for n in nodes:
                if (n['id'] not in removed_ids and n['role'] == 'replica'
                        and n['master_id'] in removed_ids):
                    target = min(followers, key=followers.get)
                    guest_at[n['address']].cluster_replicate(target)
                    followers[target] += 1

            # Then take them out of the cluster, replicas before the
            # masters they follow, and delete them.
            for n in sorted((n for n in nodes if n['id'] in removed_ids),
                            key=lambda n: n['role'] != 'replica'):
                if n['id'] in gone_ids:
                    # Forgotten by every node that stays: del-node loads
                    # the cluster from the nodes that answer and asks the
                    # node to reset itself.
                    for guest in guest_at.values():
                        guest.cluster_forget(n['id'])
                else:
                    head_guest.cluster_del_node(n['id'])
            for inst in removed:
                inst.update_db(cluster_id=None)
                Instance.delete(inst)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _shrink_cluster()
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
            # Failed or not, the cluster can be acted on again: the members
            # carry the error, and a task left set would refuse even a
            # delete.
            self.reset_task()

        LOG.debug("End shrink_cluster for id: %s.", cluster_id)

    def upgrade_cluster(self, context, cluster_id, datastore_version):
        self.rolling_upgrade_cluster(context, cluster_id, datastore_version)


class RedisTaskManagerAPI(task_api.API):

    def add_node(self, manager, cluster_id, instance_id):
        """The repair: a node in place of a lost one joins the cluster.
        The strategy serves three datastores, so the cast names the
        manager: the task manager takes the strategy from the first word
        of the method's name.
        """
        LOG.debug("Making async call to add node %s to cluster %s",
                  instance_id, cluster_id)
        cctxt = self.client.prepare(version=task_api.API.API_BASE_VERSION)
        cctxt.cast(self.context, "%s_add_node" % manager,
                   cluster_id=cluster_id, instance_id=instance_id)


CLUSTER_TASKS = topology.clusters(
    {'cluster': RedisClusterTasks,
     sentinel_api.TOPOLOGY: sentinel_taskmanager.RedisSentinelClusterTasks},
    'cluster')
