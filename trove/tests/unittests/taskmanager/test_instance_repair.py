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

import datetime
from unittest import mock

from novaclient import exceptions as nova_exceptions
from oslo_utils import timeutils

from trove.common import cfg
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import instance_repair
from trove.tests.unittests.taskmanager import test_health
from trove.tests.unittests.taskmanager import test_repair

CONF = cfg.CONF


class InstanceRepairTest(test_health.HealthTestBase):
    """On the real tables, with Nova's answers as test_repair captured
    them on production.
    """

    def setUp(self):
        super(InstanceRepairTest, self).setUp()
        self.nova = mock.Mock()
        self.nova.servers.get.return_value = test_repair._server()
        self.nova.services.list.return_value = [test_repair._compute()]
        self.nova.instance_action.list.return_value = [
            test_repair.NOVA_STOP, test_repair.CREATE]
        self.now = timeutils.normalize_time(
            timeutils.parse_isotime('2026-10-20T13:45:00'))
        # Only the instances this test made: the tables are shared with
        # every other test of the run, whose instances may linger.
        self.mine = set()
        self.covered = mock.Mock(
            side_effect=lambda context, ids: set(ids) & self.mine)
        self.repair = instance_repair.InstanceRepair(
            nova_factory=lambda context: self.nova, now=lambda: self.now,
            covered=self.covered)
        self.notifier = mock.patch.object(
            instance_repair.rpc, 'get_notifier').start().return_value
        self.addCleanup(mock.patch.stopall)

    def _instance(self, age=200, server=test_repair.SERVER, cluster_id=None,
                  task=InstanceTasks.NONE, deleted=False, heard=True):
        """A standalone instance whose heartbeat is ``age`` seconds old
        as of self.now (None: never heard from).
        """
        instance = DBInstance(task, id=utils.generate_uuid(), name='i',
                              tenant_id='t', cluster_id=cluster_id,
                              compute_instance_id=server,
                              datastore_version_id='v1',
                              created=timeutils.utcnow())
        # The constructor sets deleted False.
        instance.deleted = deleted
        instance.save()
        self.made.append(instance)
        self.mine.add(instance.id)
        if heard:
            status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                           id=utils.generate_uuid(),
                                           instance_id=instance.id)
            status.save()
            self.made.append(status)
            with InstanceServiceStatus.query() as query:
                query.filter_by(instance_id=instance.id).update(
                    {'updated_at': self.now -
                     datetime.timedelta(seconds=age)})
        return instance

    def _events(self):
        return [(call.args[1], call.args[2]['instance_id'],
                 call.args[2]['reason'])
                for call in self.notifier.info.call_args_list]

    def test_the_suspects(self):
        stale = self._instance()
        self._instance(age=0)
        self._instance(cluster_id='c')
        self._instance(task=InstanceTasks.BUILDING)
        self._instance(deleted=True)
        self._instance(server=None)
        unheard = self._instance(heard=False)
        restart = self._instance(task=InstanceTasks.RESTART_REQUIRED)
        stale_before = self.now - datetime.timedelta(seconds=90)
        rows = {row[0]: row
                for row in instance_repair.suspect_instances(stale_before)}
        self.assertEqual({stale.id, unheard.id, restart.id},
                         set(rows) & self.mine)
        self.assertEqual((stale.id, 'i', 't', test_repair.SERVER),
                         rows[stale.id])

    def test_a_quiet_instance_nova_shut_off_is_started(self):
        instance = self._instance()
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_called_once_with(test_repair.SERVER)
        self.assertEqual([(instance_repair.EVENT_START, instance.id,
                           'Nova shut it off by itself at '
                           '2026-10-20T13:41:06.000000')], self._events())
        self.assertIn(instance.id, self.repair.started)
        self.assertIn(instance.id, self.covered.call_args.args[1])

    def test_an_instance_stopped_by_a_user_stays_stopped(self):
        self._instance()
        self.nova.instance_action.list.return_value = [
            test_repair.ADMIN_STOP, test_repair.CREATE]
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_not_called()
        self.assertEqual([], self._events())

    def test_a_running_server_is_left_alone(self):
        self._instance()
        self.nova.servers.get.return_value = test_repair._server('ACTIVE')
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_not_called()

    def test_given_up_after_the_attempts(self):
        self._instance()
        self.nova.instance_action.list.return_value = [
            test_repair._start('2026-10-20T13:44:00.000000'),
            test_repair._start('2026-10-20T13:43:00.000000'),
            test_repair._start('2026-10-20T13:42:00.000000'),
            test_repair.NOVA_STOP, test_repair.CREATE]
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_not_called()

    def test_an_instance_this_replica_does_not_cover_is_not_its(self):
        self._instance()
        self.covered.side_effect = lambda context, ids: set()
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_not_called()
        self.nova.servers.get.assert_not_called()

    def test_a_cluster_member_is_the_cluster_repairs(self):
        member = self._instance(cluster_id='c')
        self.repair.check_now(mock.Mock())
        self.nova.servers.get.assert_not_called()
        for call in self.covered.call_args_list:
            self.assertNotIn(member.id, call.args[1])

    def test_the_end_is_told_when_the_instance_is_heard_from_again(self):
        instance = self._instance()
        self.repair.check_now(mock.Mock())
        with InstanceServiceStatus.query() as query:
            query.filter_by(instance_id=instance.id).update(
                {'updated_at': self.now})
        self.repair.check_now(mock.Mock())
        self.assertEqual([instance_repair.EVENT_START,
                          instance_repair.EVENT_END],
                         [e[0] for e in self._events()])
        self.assertNotIn(instance.id, self.repair.started)
        self.nova.servers.start.assert_called_once()

    def test_a_server_that_is_gone_is_not_this_repairs(self):
        instance = self._instance()
        self.repair.started[instance.id] = (mock.Mock(), 0)
        self.nova.servers.get.side_effect = nova_exceptions.NotFound(404)
        self.repair.check_now(mock.Mock())
        self.nova.servers.start.assert_not_called()
        self.assertEqual({}, self.repair.started)

    def test_a_start_by_the_other_task_manager_is_no_error(self):
        self._instance()
        self.nova.servers.start.side_effect = nova_exceptions.Conflict(409)
        self.repair.check_now(mock.Mock())
        self.assertEqual([], self._events())
        self.assertEqual({}, self.repair.started)

    def test_a_failed_start_is_told(self):
        instance = self._instance()
        self.nova.servers.start.side_effect = nova_exceptions.ClientException(
            500, 'boom')
        self.repair.check_now(mock.Mock())
        self.assertEqual(instance_repair.EVENT_ERROR, self._events()[0][0])
        self.assertEqual(instance.id, self._events()[0][1])
        self.assertEqual({}, self.repair.started)

    def test_the_switch(self):
        self._instance()
        self.patch_conf_property('instance_repair_start', False)
        self.repair.check_now(mock.Mock())
        self.nova.servers.get.assert_not_called()
        self.covered.assert_not_called()
        self.assertEqual([], self._events())

    def test_the_task_manager_scans(self):
        from trove.taskmanager import manager
        self.assertTrue(hasattr(manager.Manager, 'repair_instances'))
        with mock.patch.object(instance_repair, '_repair', None):
            with mock.patch.object(instance_repair, 'InstanceRepair') as cls:
                instance_repair.scan('ctx')
                instance_repair.scan('ctx')
        cls.assert_called_once_with()
        self.assertEqual(2, cls.return_value.check_now.call_count)
