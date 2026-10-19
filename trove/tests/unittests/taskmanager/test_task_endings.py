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

"""How tasks end: a cluster whose build failed, a task manager told to
stop.
"""

from unittest import mock

from oslo_utils import timeutils

from trove.cluster.models import DBCluster
from trove.cluster.tasks import ClusterTasks
from trove.common import exception
from trove.common.rpc import service as rpc_service
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import models as tm_models
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class ClusterFailureTest(trove_testtools.TestCase):
    """On the real tables."""

    def setUp(self):
        super(ClusterFailureTest, self).setUp()
        try:
            util.init_db()
        except exception.BadRequest:
            pass
        self.made = []
        self.addCleanup(self._clean)

    def _clean(self):
        for row in reversed(self.made):
            try:
                row.delete()
            except Exception:
                pass

    def _cluster(self, task):
        cluster = DBCluster(task, id=utils.generate_uuid(), name='c',
                            tenant_id='t', datastore_version_id='v1',
                            created=timeutils.utcnow())
        cluster.save()
        self.made.append(cluster)
        return cluster.id

    def _instance(self, task, cluster_id):
        instance = DBInstance(task, id=utils.generate_uuid(), name='i',
                              tenant_id='t', cluster_id=cluster_id,
                              datastore_version_id='v1', deleted=False,
                              created=timeutils.utcnow())
        instance.save()
        self.made.append(instance)
        return instance.id

    def test_a_failed_build_can_be_deleted(self):
        # The API deletes a cluster only with no task or deleting.
        cluster_id = self._cluster(ClusterTasks.BUILDING_INITIAL)
        member = self._instance(InstanceTasks.NONE, cluster_id=cluster_id)
        tasks = tm_models.ClusterTasks(mock.Mock(),
                                       DBCluster.find_by(id=cluster_id),
                                       mock.Mock(), mock.Mock())

        tasks.update_statuses_on_failure(cluster_id)

        self.assertEqual(ClusterTasks.NONE,
                         DBCluster.find_by(id=cluster_id).task_status)
        self.assertEqual(InstanceTasks.BUILDING_ERROR_SERVER,
                         DBInstance.find_by(id=member).task_status)


class StopTest(trove_testtools.TestCase):

    def test_the_requests_under_way_finish_before_the_lock_goes(self):
        calls = mock.Mock()
        service = rpc_service.RpcService.__new__(rpc_service.RpcService)
        service.topic = 'taskmanager'
        service.rpcserver = calls.rpcserver
        service.manager_impl = calls.manager
        with mock.patch.object(rpc_service.service.Service, 'stop',
                               calls.super_stop):
            service.stop()
        self.assertEqual(
            ['manager.service_stopping', 'rpcserver.stop', 'rpcserver.wait',
             'manager.service_stopped', 'super_stop'],
            [c[0] for c in calls.mock_calls])
