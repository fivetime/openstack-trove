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

import time
from unittest import mock

from oslo_utils import timeutils

from trove.cluster.models import DBCluster
from trove.cluster.tasks import ClusterTasks
from trove.common import coordination
from trove.common import exception
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import DBInstanceFault
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import manager as tm_manager
from trove.taskmanager import ownership
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util

# A task manager before this one on the same host (its container started
# again): gone, with or without a coordination backend.
BEFORE = '%s:1:0badf00d' % ownership._host(ownership.PROCESS)
# A task manager on another host: alive as far as can be told without a
# backend.
ELSEWHERE = 'taskmanager-elsewhere:1:0badf00d'


class OwnershipTest(trove_testtools.TestCase):
    """On the real tables."""

    def setUp(self):
        super(OwnershipTest, self).setUp()
        try:
            util.init_db()
        except exception.BadRequest:
            pass
        self.addCleanup(self._clean)
        self.made = []
        self.patch_conf_property('backend_url', None, section='coordination')
        ownership._liveness = None
        self.addCleanup(setattr, ownership, '_liveness', None)

    def _clean(self):
        for row in reversed(self.made):
            try:
                row.delete()
            except Exception:
                pass

    def _instance(self, task=InstanceTasks.NONE, owner=None,
                  cluster_id=None):
        instance = DBInstance(task, id=utils.generate_uuid(), name='i',
                              tenant_id='t', cluster_id=cluster_id,
                              datastore_version_id='v1', deleted=False,
                              created=timeutils.utcnow(), task_owner=owner)
        instance.save()
        self.made.append(instance)
        return instance.id

    def _cluster(self, task=ClusterTasks.NONE, owner=None):
        cluster = DBCluster(task, id=utils.generate_uuid(), name='c',
                            tenant_id='t', datastore_version_id='v1',
                            created=timeutils.utcnow(), task_owner=owner)
        cluster.save()
        self.made.append(cluster)
        return cluster.id

    def _fault(self, instance_id):
        try:
            return DBInstanceFault.find_by(instance_id=instance_id)
        except exception.ModelNotFoundError:
            return None

    def test_a_task_is_its_task_managers_while_it_runs(self):
        instance_id = self._instance(InstanceTasks.RESIZING)
        seen = []

        class Manager(object):
            @ownership.owning('instance')
            def resize_volume(self, context, instance_id, new_size):
                seen.append(DBInstance.find_by(id=instance_id).task_owner)

        Manager().resize_volume(None, instance_id=instance_id, new_size=2)
        self.assertEqual([ownership.PROCESS], seen)
        self.assertIsNone(DBInstance.find_by(id=instance_id).task_owner)

    def test_a_task_that_fails_is_let_go_too(self):
        cluster_id = self._cluster(ClusterTasks.GROWING_CLUSTER)

        @ownership.owning('cluster')
        def grow(manager, context, cluster_id):
            raise RuntimeError('no')

        self.assertRaises(RuntimeError, grow, None, None, cluster_id)
        self.assertIsNone(DBCluster.find_by(id=cluster_id).task_owner)

    def test_a_task_taken_over_meanwhile_stays_taken(self):
        instance_id = self._instance(InstanceTasks.REBOOTING)

        @ownership.owning('instance')
        def reboot(manager, context, instance_id):
            ownership._set_owner('instance', instance_id, ELSEWHERE)

        reboot(None, None, instance_id)
        self.assertEqual(ELSEWHERE,
                         DBInstance.find_by(id=instance_id).task_owner)

    def test_the_actions_of_a_strategy_are_owned(self):
        cluster_id = self._cluster(ClusterTasks.ADDING_SHARD)
        seen = []

        def add_shard(context, cluster_id, shard_id, replica_set_name):
            seen.append(DBCluster.find_by(id=cluster_id).task_owner)

        strategy = mock.Mock(task_manager_manager_actions={
            'add_shard_cluster': add_shard})
        with mock.patch.object(tm_manager.strategy,
                               'load_taskmanager_strategy',
                               return_value=strategy):
            manager = tm_manager.Manager.__new__(tm_manager.Manager)
            action = manager.mongodb_add_shard_cluster
        action(None, cluster_id=cluster_id, shard_id='s',
               replica_set_name='rs2')
        self.assertEqual([ownership.PROCESS], seen)
        self.assertIsNone(DBCluster.find_by(id=cluster_id).task_owner)

    def test_a_build_given_up_ends_in_error(self):
        instance_id = self._instance(InstanceTasks.BUILDING, owner=BEFORE)

        self.assertEqual(1, ownership.take_over_abandoned())

        instance = DBInstance.find_by(id=instance_id)
        self.assertEqual(InstanceTasks.BUILDING_ERROR_ABANDONED,
                         instance.task_status)
        self.assertTrue(instance.task_status.is_error)
        self.assertIsNone(instance.task_owner)
        fault = self._fault(instance_id)
        self.assertEqual('The build was not finished.', fault.message)
        self.assertIn(BEFORE, fault.details)

    def test_the_tasks_of_a_live_task_manager_are_left(self):
        instance_id = self._instance(InstanceTasks.BUILDING,
                                     owner=ELSEWHERE)
        cluster_id = self._cluster(ClusterTasks.BUILDING_INITIAL,
                                   owner=ELSEWHERE)
        mine = self._instance(InstanceTasks.BUILDING,
                              owner=ownership.PROCESS)

        self.assertEqual(0, ownership.take_over_abandoned())

        self.assertEqual(InstanceTasks.BUILDING,
                         DBInstance.find_by(id=instance_id).task_status)
        self.assertEqual(ClusterTasks.BUILDING_INITIAL,
                         DBCluster.find_by(id=cluster_id).task_status)
        self.assertEqual(ownership.PROCESS,
                         DBInstance.find_by(id=mine).task_owner)

    def test_another_task_given_up_is_cleared(self):
        instance_id = self._instance(InstanceTasks.RESIZING, owner=BEFORE)
        ownership.take_over_abandoned()
        instance = DBInstance.find_by(id=instance_id)
        self.assertEqual(InstanceTasks.NONE, instance.task_status)
        self.assertEqual('Resizing was not finished: check the instance '
                         'before going on.',
                         self._fault(instance_id).message)

    def test_a_delete_given_up_is_left_to_be_asked_again(self):
        instance_id = self._instance(InstanceTasks.DELETING, owner=BEFORE)
        cluster_id = self._cluster(ClusterTasks.DELETING, owner=BEFORE)
        ownership.take_over_abandoned()
        self.assertEqual(InstanceTasks.DELETING,
                         DBInstance.find_by(id=instance_id).task_status)
        self.assertEqual(ClusterTasks.DELETING,
                         DBCluster.find_by(id=cluster_id).task_status)
        self.assertIsNone(DBCluster.find_by(id=cluster_id).task_owner)

    def test_a_cluster_build_given_up(self):
        cluster_id = self._cluster(ClusterTasks.BUILDING_INITIAL,
                                   owner=BEFORE)
        # Its members built (their own task manager finished) but were
        # never formed into the cluster.
        members = [self._instance(InstanceTasks.NONE, cluster_id=cluster_id)
                   for _ in range(3)]

        ownership.take_over_abandoned()

        cluster = DBCluster.find_by(id=cluster_id)
        self.assertEqual(ClusterTasks.NONE, cluster.task_status)
        for member in members:
            self.assertEqual(InstanceTasks.BUILDING_ERROR_ABANDONED,
                             DBInstance.find_by(id=member).task_status)
            self.assertEqual('The cluster task was not finished.',
                             self._fault(member).message)

    def test_a_cluster_grow_given_up(self):
        cluster_id = self._cluster(ClusterTasks.GROWING_CLUSTER,
                                   owner=BEFORE)
        old = self._instance(InstanceTasks.NONE, cluster_id=cluster_id)
        new = self._instance(InstanceTasks.BUILDING, cluster_id=cluster_id)

        ownership.take_over_abandoned()

        self.assertEqual(ClusterTasks.NONE,
                         DBCluster.find_by(id=cluster_id).task_status)
        self.assertEqual(InstanceTasks.NONE,
                         DBInstance.find_by(id=old).task_status)
        self.assertEqual(InstanceTasks.GROWING_ERROR,
                         DBInstance.find_by(id=new).task_status)

    def test_another_task_manager_took_it_first(self):
        instance_id = self._instance(InstanceTasks.BUILDING, owner=BEFORE)
        real = ownership._set_owner

        def first(kind, target_id, owner, only_if=None):
            if only_if == BEFORE:
                # Another task manager took it between the query and here.
                real(kind, target_id, None, only_if=BEFORE)
            return real(kind, target_id, owner, only_if=only_if)

        with mock.patch.object(ownership, '_set_owner', side_effect=first):
            self.assertEqual(0, ownership.take_over_abandoned())
        self.assertEqual(InstanceTasks.BUILDING,
                         DBInstance.find_by(id=instance_id).task_status)


class FakeLock(object):
    """A lock as the etcd driver has it: a key with a lease on the
    backend, and a flag in the process that a lapsed lease does not
    clear (the refresh of a lapsed lease raises nothing).
    """

    def __init__(self, backend, name):
        self.backend = backend
        self.name = name
        self.acquired = False

    def acquire(self, blocking=False):
        self.backend.tried.append(self.name)
        if self.name in self.backend.keys:
            return False
        self.backend.keys[self.name] = self
        self.acquired = True
        return True

    def release(self):
        self.acquired = False
        if self.backend.keys.get(self.name) is self:
            del self.backend.keys[self.name]
            return True
        return False

    def heartbeat(self):
        return self.acquired


class FakeBackend(object):

    def __init__(self):
        self.keys = {}
        self.tried = []
        self.started = None
        self.stopped = False

    def start(self, start_heart=False):
        self.started = start_heart

    def stop(self):
        self.stopped = True

    def heartbeat(self):
        return 30

    def get_lock(self, name):
        return FakeLock(self, name)

    def lapse(self, name):
        """The lease of the lock lapsed: the key is gone, the holder's
        flag is not.
        """
        del self.keys[name]

    def take(self, name):
        """Another process holds the lock."""
        self.keys[name] = 'another'


class LivenessTest(trove_testtools.TestCase):

    def setUp(self):
        super(LivenessTest, self).setUp()
        self.patch_conf_property('backend_url', 'etcd3+http://etcd:2379',
                                 section='coordination')
        self.backend = FakeBackend()
        patcher = mock.patch('tooz.coordination.get_coordinator',
                             return_value=self.backend)
        patcher.start()
        self.addCleanup(patcher.stop)
        # No beating thread: the looks are the test's.
        mock.patch.object(coordination.Heart, '_spawn').start()
        self.addCleanup(mock.patch.stopall)
        self.liveness = ownership.Liveness()
        self.own = (ownership.LOCK % ownership.PROCESS).encode()
        self.other = (ownership.LOCK % ELSEWHERE).encode()

    def _later(self, seconds):
        later = time.monotonic() + seconds
        return mock.patch.object(ownership.time, 'monotonic',
                                 return_value=later)

    def test_a_task_manager_holds_its_lock(self):
        self.liveness.start()
        self.assertIn(self.own, self.backend.keys)
        # tooz's heart beats once per lease: Trove's beats three times.
        self.assertFalse(self.backend.started)
        self.assertEqual(10, self.liveness._heart.period)
        self.assertEqual(20, self.liveness.grace)
        self.liveness.stop()
        self.assertNotIn(self.own, self.backend.keys)
        self.assertTrue(self.backend.stopped)

    def test_an_owner_whose_lock_stays_free_is_gone(self):
        self.liveness.start()
        # The first look: free, but a lease that lapsed is held again by
        # a live owner within the grace period.
        self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.assertNotIn(self.other, self.backend.keys)
        with self._later(self.liveness.grace - 1):
            self.assertFalse(self.liveness.gone(ELSEWHERE))
        with self._later(self.liveness.grace + 1):
            self.assertTrue(self.liveness.gone(ELSEWHERE))

    def test_an_owner_that_holds_its_lock_again_lives(self):
        self.liveness.start()
        self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.backend.take(self.other)
        with self._later(self.liveness.grace + 1):
            self.assertFalse(self.liveness.gone(ELSEWHERE))
        # Free again: the looks start over.
        self.backend.lapse(self.other)
        with self._later(self.liveness.grace + 2):
            self.assertFalse(self.liveness.gone(ELSEWHERE))

    def test_an_owner_holding_its_lock_lives(self):
        self.liveness.start()
        self.backend.take(self.other)
        self.assertFalse(self.liveness.gone(ELSEWHERE))

    def test_a_lapsed_lock_is_held_again(self):
        self.liveness.start()
        lock = self.liveness._lock
        self.backend.lapse(self.own)
        self.assertTrue(lock.heartbeat())  # the driver tells nothing
        self.liveness._heart.beat()
        self.assertIn(self.own, self.backend.keys)
        self.assertIsNot(lock, self.liveness._lock)
        self.assertIs(self.liveness._lock, self.backend.keys[self.own])
        # Stopping releases the lock that holds it now.
        self.liveness.stop()
        self.assertNotIn(self.own, self.backend.keys)

    def test_a_lock_another_holds_is_held_again_when_it_is_free(self):
        self.liveness.start()
        lock = self.liveness._lock
        self.backend.lapse(self.own)
        self.backend.take(self.own)
        # Lost, as the driver tells it.
        lock.acquired = False
        self.liveness._heart.beat()
        self.assertIsNone(self.liveness._lock)
        # Without its own lock it tells nothing of the others.
        self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.assertNotIn(self.other, self.backend.tried)
        self.backend.lapse(self.own)
        self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.assertIs(self.liveness._lock, self.backend.keys[self.own])
        self.assertIn(self.other, self.backend.tried)

    def test_what_cannot_be_told_lives(self):
        self.liveness.start()
        with mock.patch.object(self.backend, 'get_lock',
                               side_effect=RuntimeError('down')):
            self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.assertFalse(self.liveness.gone(ownership.PROCESS))

    def test_without_its_own_lock_it_tells_nothing(self):
        # A task manager that lost its own lock may be the one cut off.
        self.backend.take(self.own)
        self.liveness.start()
        self.assertIsNone(self.liveness._lock)
        self.assertFalse(self.liveness.gone(ELSEWHERE))
        self.assertNotIn(self.other, self.backend.tried)

    def test_the_process_before_on_this_host_is_gone(self):
        self.assertTrue(self.liveness.gone(BEFORE))
