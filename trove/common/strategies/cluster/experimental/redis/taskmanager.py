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
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.instance import tasks as inst_tasks
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class RedisTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return RedisTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return RedisClusterTasks


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
            instances = [Instance.load(context, instance_id) for instance_id
                         in instance_ids]

            guests = [self.get_guest(instance) for instance in instances]
            try:
                # Every member gets the account the cluster is managed with.
                password = utils.generate_random_password()
                for guest in guests:
                    guest.cluster_init(password)

                # Connect nodes to the first node
                cluster_head = instances[0]
                cluster_head_port = '6379'
                cluster_head_ip = self.get_ip(cluster_head)
                for guest in guests[1:]:
                    guest.cluster_meet(cluster_head_ip, cluster_head_port)

                for guest, (first_slot, last_slot) in zip(
                        guests, self.slot_ranges(len(guests))):
                    guest.cluster_addslots(first_slot, last_slot)

                for guest in guests:
                    guest.cluster_wait(len(guests))
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
            (cluster_head_ip, cluster_head_port) = head_guest.get_node_ip()

            # Wait for cluster members to get to cluster-ready status.
            if not self._all_instances_ready(new_instance_ids, cluster_id):
                return

            LOG.debug("All members ready, proceeding for cluster setup.")
            new_insts = [Instance.load(context, instance_id)
                         for instance_id in new_instance_ids]
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
                guest.cluster_wait(len(db_instances))

            # The new members are empty masters: give them their share of
            # the slots, with the keys in them.
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

    def shrink_cluster(self, context, cluster_id, removal_ids):
        LOG.debug("Begin shrink_cluster for id: %s.", cluster_id)

        def _shrink_cluster():
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            remaining = [Instance.load(context, db_inst.id)
                         for db_inst in db_instances
                         if db_inst.id not in removal_ids]
            removed = [Instance.load(context, instance_id)
                       for instance_id in removal_ids]
            head_guest = self.get_guest(remaining[0])
            node_ids = [self.get_guest(inst).get_node_id()
                        for inst in removed]

            # Move every slot, with its keys, off the leaving members,
            # then take them out of the cluster and delete them.
            head_guest.cluster_rebalance(
                weights={node_id: 0 for node_id in node_ids})
            for node_id in node_ids:
                head_guest.cluster_del_node(node_id)
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

    pass
