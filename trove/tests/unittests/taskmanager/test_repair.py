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
from novaclient.v2 import instance_action
from novaclient.v2 import servers
from novaclient.v2 import services
from oslo_utils import timeutils

from trove.cluster.tasks import ClusterTasks
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.common.strategies.cluster.experimental.common import repair
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import recovery
from trove.tests.unittests.taskmanager import test_health

SERVER = '0692f3f6-5e6b-4ab2-a9b9-5e70243f8915'
TROVE_USER = '6f094d383238433581d1b93eed36d2e3'
ADMIN_USER = '1de29bd6e5db4fabba9de9a14fdde80b'
SERVICE_PROJECT = '7e92eb25f709471583f1bedf125239ac'


def _action(action, start_time, user_id, request_id):
    # As os-instance-actions (microversion 2.57) gave them on production
    # (2026-10-20) for a member powered off with virsh destroy: Nova's
    # own stop from _sync_instance_power_state has no user.
    return instance_action.InstanceAction(mock.Mock(), {
        'action': action, 'instance_uuid': SERVER, 'message': None,
        'project_id': SERVICE_PROJECT if user_id else None,
        'request_id': request_id, 'start_time': start_time,
        'user_id': user_id, 'updated_at': start_time})


CREATE = _action('create', '2026-10-20T13:16:18.000000', TROVE_USER,
                 'req-7ba04d92-794f-4a90-8a4c-3931cd695415')
NOVA_STOP = _action('stop', '2026-10-20T13:41:06.000000', None,
                    'req-2bdefbfc-ff66-42a2-8e5e-274c7de702db')
ADMIN_STOP = _action('stop', '2026-10-20T13:41:06.000000', ADMIN_USER,
                     'req-2bdefbfc-ff66-42a2-8e5e-274c7de702db')


def _start(at):
    return _action('start', at, TROVE_USER, 'req-%s' % at)


def _server(status='SHUTOFF', host='kvm-worker3.cloud.local'):
    return servers.Server(mock.Mock(), {
        'id': SERVER, 'status': status, 'OS-EXT-SRV-ATTR:host': host})


def _compute(state='up'):
    # As GET /os-services gave it on production (2026-10-21).
    return services.Service(mock.Mock(), {
        'id': 'cd4b7685-f1c4-41e0-8231-ffee3fc86a20',
        'binary': 'nova-compute', 'host': 'kvm-worker3.cloud.local',
        'zone': 'nova', 'status': 'enabled', 'state': state,
        'updated_at': '2026-10-21T00:02:17.000000'})


class RepairTestBase(test_health.HealthTestBase):

    def setUp(self):
        super(RepairTestBase, self).setUp()
        self.nova = mock.Mock()
        self.nova.servers.get.return_value = _server()
        self.nova.services.list.return_value = [_compute()]
        self.nova.instance_action.list.return_value = [NOVA_STOP, CREATE]
        self.now = timeutils.normalize_time(
            timeutils.parse_isotime('2026-10-20T13:45:00'))
        self.repair = repair.ClusterRepair(
            nova_factory=lambda context: self.nova, now=lambda: self.now)
        self.notifier = mock.patch.object(
            repair.rpc, 'get_notifier').start().return_value
        self.addCleanup(mock.patch.stopall)

    def _quiet_cluster(self, stale=(0,), task=ClusterTasks.NONE):
        cluster, members = self._cluster(3, stale=stale, task=task)
        for member in members:
            member.compute_instance_id = (SERVER if members.index(member)
                                          in stale else 'other')
            member.save()
        return cluster, members

    def _events(self):
        return [c.args[1] for c in self.notifier.info.call_args_list]


class TestStartDecision(RepairTestBase):
    """Whether a quiet member's server is started."""

    def _decide(self):
        member = mock.Mock(compute_instance_id=SERVER)
        return self.repair.start_decision(self.nova, member)

    def test_a_server_nova_shut_off_by_itself_is_started(self):
        decision = self._decide()
        self.assertTrue(decision.start)
        self.assertIn('Nova shut it off by itself', decision.reason)
        self.nova.services.list.assert_called_once_with(
            host='kvm-worker3.cloud.local', binary='nova-compute')

    def test_a_server_stopped_through_the_api_stays_off(self):
        self.nova.instance_action.list.return_value = [ADMIN_STOP, CREATE]
        decision = self._decide()
        self.assertFalse(decision.start)
        self.assertEqual('stopped by user %s' % ADMIN_USER, decision.reason)

    def test_a_server_that_runs_is_left_alone(self):
        for status in ('ACTIVE', 'ERROR', 'BUILD', 'REBOOT'):
            self.nova.servers.get.return_value = _server(status)
            self.assertFalse(self._decide().start)

    def test_nothing_on_a_host_whose_compute_service_is_down(self):
        self.nova.services.list.return_value = [_compute('down')]
        decision = self._decide()
        self.assertFalse(decision.start)
        self.assertIn('not up', decision.reason)
        # Or that cannot be looked up.
        self.nova.services.list.side_effect = RuntimeError('403')
        self.assertFalse(self._decide().start)

    def test_once_more_after_the_interval_then_given_up(self):
        self.nova.instance_action.list.return_value = [
            _start('2026-10-20T13:42:00.000000'), NOVA_STOP, CREATE]
        # 180 s after the first start, the interval is 300 s.
        decision = self._decide()
        self.assertFalse(decision.start)
        self.assertEqual(121, decision.again_in)
        self.now += datetime.timedelta(seconds=150)
        decision = self._decide()
        self.assertTrue(decision.start)
        self.assertIn('start 2', decision.reason)
        self.nova.instance_action.list.return_value = [
            _start('2026-10-20T13:52:00.000000'),
            _start('2026-10-20T13:47:00.000000'),
            _start('2026-10-20T13:42:00.000000'), NOVA_STOP, CREATE]
        self.now += datetime.timedelta(hours=1)
        decision = self._decide()
        self.assertFalse(decision.start)
        self.assertIn('given up', decision.reason)

    def test_starts_before_the_stop_do_not_count(self):
        self.nova.instance_action.list.return_value = [
            NOVA_STOP, _start('2026-10-20T13:30:00.000000'),
            _start('2026-10-20T13:20:00.000000'),
            _start('2026-10-20T13:17:00.000000'), CREATE]
        self.assertTrue(self._decide().start)

    def test_off_without_a_stop_is_left_alone(self):
        self.nova.instance_action.list.return_value = [CREATE]
        self.assertFalse(self._decide().start)


class TestClusterRepair(RepairTestBase):
    """A look at a cluster: which members are started, and when not."""

    def test_the_quiet_member_is_started_and_told(self):
        cluster, members = self._quiet_cluster()
        again = self.repair.check_now(mock.Mock(), cluster)
        self.nova.servers.start.assert_called_once_with(SERVER)
        self.assertEqual(300, again)
        self.assertEqual([repair.EVENT_START], self._events())
        payload = self.notifier.info.call_args.args[2]
        self.assertEqual((cluster.id, members[0].id, 'start'),
                         (payload['cluster_id'], payload['instance_id'],
                          payload['action']))
        # Only the quiet one is looked up.
        self.nova.servers.get.assert_called_once_with(SERVER)

    def test_its_end_is_told_when_it_is_heard_from(self):
        cluster, members = self._quiet_cluster()
        self.repair.check_now(mock.Mock(), cluster)
        self._heard_from(members[0])
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        self.assertEqual([repair.EVENT_START, repair.EVENT_END],
                         self._events())
        self.assertEqual({}, self.repair.started)

    def _heard_from(self, member):
        from trove.instance.models import InstanceServiceStatus
        with InstanceServiceStatus.query() as query:
            query.filter_by(instance_id=member.id).update(
                {'updated_at': timeutils.utcnow()})

    def test_a_cluster_whose_members_all_answer_costs_no_nova_call(self):
        cluster, _members = self._quiet_cluster(stale=())
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        self.nova.servers.get.assert_not_called()

    def test_nothing_while_the_cluster_or_a_member_has_a_task(self):
        cluster, members = self._quiet_cluster(
            task=ClusterTasks.RECOVERING_CLUSTER)
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        cluster, members = self._quiet_cluster()
        members[1].task_status = InstanceTasks.RESIZING
        members[1].save()
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        self.nova.servers.start.assert_not_called()

    def test_off_for_the_operator_or_the_cluster(self):
        cluster, _members = self._quiet_cluster()
        self.patch_conf_property('cluster_repair_start', False)
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        self.patch_conf_property('cluster_repair_start', True)
        cluster.save_properties({'topology': 'sentinel',
                                 repair.AUTO_REPAIR: repair.OFF})
        self.assertIsNone(self.repair.check_now(mock.Mock(), cluster))
        self.nova.servers.start.assert_not_called()

    def test_a_start_another_task_manager_made_is_no_error(self):
        # Both task managers that watch the cluster looked on the same
        # report and started it 48 ms apart (production, 2026-10-21).
        cluster, _members = self._quiet_cluster()
        self.nova.servers.start.side_effect = nova_exceptions.Conflict(
            409, "Cannot 'start' instance 49ed7829-69a7-4e34-a3c7-"
                 "7c75e2ea9be2 while it is in task_state powering-on",
            request_id='req-55acd4aa-dd20-405d-8cf4-6b2bd9988c31')
        self.repair.check_now(mock.Mock(), cluster)
        self.assertEqual([], self._events())
        self.assertEqual({}, self.repair.started)

    def test_a_start_nova_refuses_is_told(self):
        cluster, _members = self._quiet_cluster()
        self.nova.servers.start.side_effect = RuntimeError(
            "Cannot 'start' instance while it is in vm_state active")
        self.repair.check_now(mock.Mock(), cluster)
        self.assertEqual([repair.EVENT_ERROR], self._events())
        self.assertEqual({}, self.repair.started)


class TestTheHubRepairs(RepairTestBase):
    """The recovery hub runs the repair on every look, also at a cluster
    whose datastore has no recovery, and looks again when it asks.
    """

    def test_every_look_repairs(self):
        cluster, _members = self._quiet_cluster()
        hub = recovery.RecoveryHub(coordinator=mock.Mock())
        hub.context = mock.Mock()
        hub.repair = self.repair
        hub._recovery_of = mock.Mock(return_value=None)
        hub._look(cluster.id, 'scan')
        self.nova.servers.start.assert_called_once_with(SERVER)
        due, reason = hub.pending[cluster.id]
        self.assertEqual('repair', reason)

    def test_a_failing_repair_does_not_fail_the_look(self):
        cluster, _members = self._quiet_cluster()
        hub = recovery.RecoveryHub(coordinator=mock.Mock())
        hub.context = mock.Mock()
        hub.repair = mock.Mock()
        hub.repair.check_now.side_effect = RuntimeError('nova')
        watch = mock.Mock()
        watch.still_watching.return_value = False
        hub._recovery_of = mock.Mock(return_value=watch)
        hub._look(cluster.id, 'scan')
        watch.check_now.assert_called_once()
        self.assertNotIn(cluster.id, hub.pending)


class TestAutoRepairProperty(test_health.HealthTestBase):
    """The tenant's word on the repair, kept with the cluster."""

    def test_kept_and_checked(self):
        kept = common_api.HomogeneousCluster._kept_properties(
            {'topology': 'sentinel', 'auto_repair': 'off', 'other': 1})
        self.assertEqual({'topology': 'sentinel', 'auto_repair': 'off'},
                         kept)
        common_api.HomogeneousCluster._validate_repair(
            {'auto_repair': 'start'})
        common_api.HomogeneousCluster._validate_repair(None)
        self.assertRaises(exception.BadRequest,
                          common_api.HomogeneousCluster._validate_repair,
                          {'auto_repair': 'replace-everything'})
