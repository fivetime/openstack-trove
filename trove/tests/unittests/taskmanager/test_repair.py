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

from trove.cluster import models as cluster_models
from trove.cluster.models import DBCluster
from trove.cluster.tasks import ClusterTasks
from trove.common import cluster_health as ch
from trove.common import context as trove_context
from trove.common import exception
from trove.common.notification import EndNotification
from trove.common.strategies.cluster.experimental.common import (
    api as common_api)
from trove.common.strategies.cluster.experimental.common import (
    health as common_health)
from trove.common.strategies.cluster.experimental.common import repair
from trove.common.strategies.cluster.experimental.common import replace
from trove.common.strategies.cluster.experimental.galera_common import (
    repair as galera_repair)
from trove.common.strategies.cluster.experimental.group_replication import (
    repair as gr_repair)
from trove.common.strategies.cluster.experimental.mongodb import (
    repair as mongodb_repair)
from trove.common.strategies.cluster.experimental.postgresql import (
    repair as pg_repair)
from trove.common.strategies.cluster.experimental.redis import (
    repair as redis_repair)
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import api as task_api
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


def _peer_server(server_id):
    # As GET /servers/{id} gave a member's metadata on production
    # (2026-10-21): the user the cluster was made for.
    return servers.Server(mock.Mock(), {
        'id': server_id, 'status': 'ACTIVE',
        'OS-EXT-SRV-ATTR:host': 'kvm-worker4.cloud.local',
        'metadata': {'trove_project_id': 'ac19c812536b4b06bd8c394353d683f3',
                     'trove_user_id': ADMIN_USER,
                     'trove_instance_id':
                         '045d957c-fce3-421a-b5e2-6ff711a3db94'}})


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
        # The repair's: a health that changes is told through the same
        # notifier (trove.cluster.health).
        return [c.args[1] for c in self.notifier.info.call_args_list
                if c.args[1].startswith('trove.cluster.repair.')]


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


class _Plugin(replace.ReplacePlugin):

    def __init__(self, ok=True, why='every other member holds it out'):
        super(_Plugin, self).__init__('valkey')
        self.ok, self.why = ok, why
        self.asked = []

    def can_replace(self, context, db_cluster, lost, others):
        self.asked.append((lost.id, sorted(m.id for m in others)))
        return self.ok, self.why


class ReplaceTestBase(RepairTestBase):

    def setUp(self):
        super(ReplaceTestBase, self).setUp()
        self.patch_conf_property('cluster_repair_replace', True)
        self.plugin = _Plugin()
        self.repair.replacement.plugins = lambda manager: self.plugin
        self.task_api = mock.patch.object(
            task_api, 'load').start()
        self.cinder = mock.patch.object(
            replace.clients, 'create_cinder_client').start().return_value
        self.cinder.volumes.get.return_value = mock.Mock(
            volume_type='nvme-rep3', availability_zone='nova')
        self.neutron = mock.patch.object(
            replace.clients, 'create_neutron_client').start().return_value
        # A member's two ports, named alike, the management network's
        # first (production, 2026-10-21: the new member got two ports on
        # the management network and never reached its primary).
        self.patch_conf_property('management_networks',
                                 ['e70354a0-3f16-45ef-81e4-5d815b43a765'])
        self.neutron.list_ports.return_value = {'ports': [
            {'network_id': 'e70354a0-3f16-45ef-81e4-5d815b43a765'},
            {'network_id': '68ade15f-b2f1-4d58-903a-e5477c9755b8'}]}
        mock.patch.object(replace.clients, 'create_nova_client',
                          return_value=self.nova).start()
        # The member's server is gone; Nova lists it among the deleted,
        # on a host whose compute service is up. Its peers' servers run.
        self.nova.servers.get.side_effect = (
            lambda server_id: _peer_server(server_id)
            if server_id != SERVER else self._gone())
        self.nova.servers.list.return_value = [_server('DELETED')]

    @staticmethod
    def _gone():
        raise nova_exceptions.NotFound(404)

    def _lost_cluster(self):
        cluster, members = self._quiet_cluster()
        for member in members:
            member.flavor_id = '854b3e43-493b-4e14-830d-449edd211928'
            member.volume_size = 2
            member.volume_id = 'vol-%s' % member.id[:8]
            member.save()
        return cluster, members

    def _state(self, cluster):
        return DBCluster.find_by(id=cluster.id).get_properties().get(
            replace.STATE)


class TestLostDecision(ReplaceTestBase):
    """Nova's side of whether a quiet member is lost for good."""

    def _decide(self, start_decision=None, up=True):
        member = mock.Mock(compute_instance_id=SERVER)
        return self.repair.replacement.lost_decision(
            self.nova, member, start_decision, lambda host: up)

    def test_a_server_gone_on_a_host_that_is_up(self):
        self.assertTrue(self._decide().lost)
        self.nova.servers.list.assert_called_once_with(search_opts={
            'uuid': SERVER, 'deleted': True, 'all_tenants': True})

    def test_a_server_gone_on_a_host_that_is_down_may_run_on(self):
        verdict = self._decide(up=False)
        self.assertFalse(verdict.lost)
        self.assertIn('not up', verdict.reason)
        self.nova.servers.list.return_value = []
        self.assertFalse(self._decide().lost)

    def test_in_error_or_off_and_given_up(self):
        self.nova.servers.get.side_effect = None
        self.nova.servers.get.return_value = _server('ERROR')
        self.assertTrue(self._decide().lost)
        self.assertFalse(self._decide(up=False).lost)
        self.nova.servers.get.return_value = _server('SHUTOFF')
        given_up = repair.StartDecision(
            False, 'started 3 times since Nova shut it off; given up')
        self.assertTrue(self._decide(given_up).lost)
        waiting = repair.StartDecision(False, 'started 30 s ago', 271)
        self.assertFalse(self._decide(waiting).lost)
        for status in ('ACTIVE', 'BUILD', 'REBOOT'):
            self.nova.servers.get.return_value = _server(status)
            self.assertFalse(self._decide().lost)


class TestReplacement(ReplaceTestBase):
    """The replacement of a lost member, step by step."""

    def test_it_begins_with_the_clusters_shrink(self):
        cluster, members = self._lost_cluster()
        again = self.repair.check_now(mock.Mock(), cluster,
                                      manager='valkey')
        self.assertEqual(replace.FOLLOW_EVERY, again)
        self.assertEqual([(members[0].id, sorted(m.id for m in members[1:]))],
                         self.plugin.asked)
        state = self._state(cluster)
        self.assertEqual(replace.REMOVING, state['phase'])
        self.assertEqual(members[0].id, state['lost'])
        self.assertEqual({'flavor_id': '854b3e43-493b-4e14-830d-449edd211928',
                          'volume_size': 2, 'volume_type': 'nvme-rep3',
                          'availability_zone': 'nova',
                          'nics': [{'network_id':
                                    '68ade15f-b2f1-4d58-903a-e5477c9755b8'}]},
                         state['spec'])
        # As the cluster's grow checks the members it adds (production:
        # 'net-id' failed it, TypeError).
        neutron = mock.patch.object(cluster_models.clients,
                                    'create_neutron_client').start()
        cluster_models.validate_instance_nics(mock.Mock(), [state['spec']])
        neutron.return_value.find_resource.assert_called_once_with(
            'network', '68ade15f-b2f1-4d58-903a-e5477c9755b8')
        self.assertEqual(ClusterTasks.SHRINKING_CLUSTER,
                         DBCluster.find_by(id=cluster.id).task_status)
        self.task_api.return_value.shrink_cluster.assert_called_once_with(
            cluster.id, [members[0].id])
        # The user the cluster was made for, as its members' servers tell
        # (production: a new member's server without one, HTTP 400).
        self.assertEqual(ADMIN_USER, state['user'])
        self.assertEqual(ADMIN_USER,
                         self.task_api.call_args.args[0].user_id)
        self.assertEqual([repair.EVENT_START], self._events())
        self.assertEqual('replace', [
            c.args[2] for c in self.notifier.info.call_args_list
            if c.args[1] == repair.EVENT_START][0]['action'])

    def test_nothing_when_no_member_tells_its_user(self):
        cluster, _members = self._lost_cluster()
        self.nova.servers.get.side_effect = (
            lambda server_id: servers.Server(mock.Mock(), {
                'id': server_id, 'status': 'ACTIVE', 'metadata': {}})
            if server_id != SERVER else self._gone())
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self.assertIsNone(self._state(cluster))
        self.task_api.return_value.shrink_cluster.assert_not_called()

    def test_nothing_when_off_without_a_plugin_or_refused(self):
        cluster, _members = self._lost_cluster()
        self.patch_conf_property('cluster_repair_replace', False)
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self.patch_conf_property('cluster_repair_replace', True)
        cluster.save_properties({repair.AUTO_REPAIR: repair.START})
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        cluster.save_properties({})
        self.repair.replacement.plugins = lambda manager: None
        self.repair.check_now(mock.Mock(), cluster, manager='mongodb')
        self.repair.replacement.plugins = lambda manager: self.plugin
        self.plugin.ok = False
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self.assertIsNone(self._state(cluster))
        self.task_api.return_value.shrink_cluster.assert_not_called()
        self.assertEqual([], self._events())

    def test_the_breaker_holds_when_many_clusters_lose_members(self):
        self.patch_conf_property('cluster_repair_breaker', 1)
        first, _m = self._lost_cluster()
        second, _m = self._lost_cluster()
        self.repair.check_now(mock.Mock(), first, manager='valkey')
        self.repair.check_now(mock.Mock(), second, manager='valkey')
        self.assertIsNotNone(self._state(first))
        self.assertIsNone(self._state(second))
        self.assertEqual(1, self.task_api.return_value.
                         shrink_cluster.call_count)

    def test_one_task_manager_of_two_begins(self):
        cluster, _members = self._lost_cluster()
        other = repair.ClusterRepair(nova_factory=lambda c: self.nova,
                                     now=lambda: self.now)
        other.replacement.plugins = lambda manager: self.plugin
        stale_view = DBCluster.find_by(id=cluster.id)
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        # The other read the cluster before the first took it.
        stale_view.task_status = ClusterTasks.NONE
        stale_view.properties = None
        self.assertIsNone(other.replacement.begin(
            mock.Mock(), stale_view, 'valkey',
            [m for m in DBInstance_all(cluster.id)],
            DBInstance_all(cluster.id)[0], 'its server is gone'))
        self.assertEqual(1, self.task_api.return_value.
                         shrink_cluster.call_count)

    def _removed(self, cluster, members):
        members[0].deleted = True
        members[0].save()
        db = DBCluster.find_by(id=cluster.id)
        db.update(task_status=ClusterTasks.NONE)
        return DBCluster.find_by(id=cluster.id)

    def test_then_the_grow_then_done_when_healthy(self):
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        db = self._removed(cluster, members)
        load = mock.patch.object(cluster_models.Cluster,
                                 'load').start()
        load.return_value.ds_version.image_id = 'image-1'
        self.assertEqual(replace.FOLLOW_EVERY, self.repair.check_now(
            mock.Mock(), db, manager='valkey'))
        spec = self._state(cluster)['spec']
        load.return_value.grow.assert_called_once_with([spec], 'image-1',
                                                       held=True)
        # The grow's task, taken by the step (of two task managers, one).
        self.assertEqual(ClusterTasks.GROWING_CLUSTER,
                         DBCluster.find_by(id=cluster.id).task_status)
        self.assertEqual(ADMIN_USER, load.call_args.args[0].user_id)
        DBCluster.find_by(id=cluster.id).update(task_status=ClusterTasks.NONE)
        self.assertEqual(replace.ADDING, self._state(cluster)['phase'])
        # The tenant's cluster, as the service: its project.
        context = load.call_args.args[0]
        self.assertEqual(('t', True), (context.project_id, context.is_admin))
        # The new member, built; the cluster not healthy yet.
        new = self._member_of(cluster)
        db = DBCluster.find_by(id=cluster.id)
        self.assertEqual(replace.FOLLOW_EVERY, self.repair.check_now(
            mock.Mock(), db, manager='valkey'))
        common_health.record(cluster.id, ch.view(ch.HEALTHY))
        self.assertIsNone(self.repair.check_now(mock.Mock(), db,
                                                manager='valkey'))
        self.assertIsNone(self._state(cluster))
        self.assertEqual([repair.EVENT_START, repair.EVENT_END],
                         self._events())
        ended = [c.args[2] for c in self.notifier.info.call_args_list
                 if c.args[1] == repair.EVENT_END]
        self.assertIn(new.name, ended[0]['reason'])

    def test_one_task_manager_of_two_adds(self):
        # Both hear the shrink end, and look at once (production).
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self._removed(cluster, members)
        other = repair.ClusterRepair(nova_factory=lambda c: self.nova,
                                     now=lambda: self.now)
        other.replacement.plugins = lambda manager: self.plugin
        first = DBCluster.find_by(id=cluster.id)
        second = DBCluster.find_by(id=cluster.id)
        load = mock.patch.object(cluster_models.Cluster, 'load').start()
        load.return_value.ds_version.image_id = 'image-1'
        self.repair.check_now(mock.Mock(), first, manager='valkey')
        other.check_now(mock.Mock(), second, manager='valkey')
        load.return_value.grow.assert_called_once()
        self.assertEqual(replace.ADDING, self._state(cluster)['phase'])

    def test_a_step_whose_grow_runs_is_not_ended(self):
        # Production: the other task manager looked between the step
        # taken and the grow's first member, and ended it ('the grow
        # added no member'); the grow went on.
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self._removed(cluster, members)
        load = mock.patch.object(cluster_models.Cluster, 'load').start()
        load.return_value.ds_version.image_id = 'image-1'
        other = repair.ClusterRepair(nova_factory=lambda c: self.nova,
                                     now=lambda: self.now)
        other.replacement.plugins = lambda manager: self.plugin

        def a_look_meanwhile(*args, **kwargs):
            self.assertEqual(replace.FOLLOW_EVERY, other.check_now(
                mock.Mock(), DBCluster.find_by(id=cluster.id),
                manager='valkey'))
        load.return_value.grow.side_effect = a_look_meanwhile
        self.repair.check_now(mock.Mock(), DBCluster.find_by(id=cluster.id),
                              manager='valkey')
        self.assertEqual(replace.ADDING, self._state(cluster)['phase'])
        self.assertEqual([repair.EVENT_START], self._events())

    def test_one_task_manager_of_two_tells_the_end(self):
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        DBCluster.find_by(id=cluster.id).update(task_status=ClusterTasks.NONE)
        other = repair.ClusterRepair(nova_factory=lambda c: self.nova,
                                     now=lambda: self.now)
        first = DBCluster.find_by(id=cluster.id)
        second = DBCluster.find_by(id=cluster.id)
        self.repair.check_now(mock.Mock(), first, manager='valkey')
        other.check_now(mock.Mock(), second, manager='valkey')
        self.assertEqual([repair.EVENT_START, repair.EVENT_ERROR],
                         self._events())

    def _member_of(self, cluster):
        member = DBInstance(InstanceTasks.NONE, id=utils.generate_uuid(),
                            name='c-member-new', tenant_id='t',
                            cluster_id=cluster.id, datastore_version_id='v1',
                            deleted=False, created=timeutils.utcnow())
        member.save()
        self.made.append(member)
        status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                       id=utils.generate_uuid(),
                                       instance_id=member.id)
        status.save()
        self.made.append(status)
        return member

    def test_a_shrink_that_left_the_member_ends_it(self):
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        DBCluster.find_by(id=cluster.id).update(task_status=ClusterTasks.NONE)
        db = DBCluster.find_by(id=cluster.id)
        self.assertIsNone(self.repair.check_now(mock.Mock(), db,
                                                manager='valkey'))
        self.assertIsNone(self._state(cluster))
        self.assertEqual([repair.EVENT_START, repair.EVENT_ERROR],
                         self._events())
        # Not again by itself: the operator's (production: every two
        # minutes, a shrink that could not remove it).
        self.now += datetime.timedelta(seconds=120)
        self.assertIsNone(self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey'))
        self.assertEqual(1, self.task_api.return_value.
                         shrink_cluster.call_count)
        failed = DBCluster.find_by(id=cluster.id).get_properties()[
            replace.FAILED]
        self.assertEqual((members[0].id, 'the shrink did not remove it'),
                         (failed['lost'], failed['error']))

    def test_another_member_lost_after_a_failure_is_replaced(self):
        cluster, members = self._lost_cluster()
        cluster.save_properties({replace.FAILED: {
            'lost': 'a member gone since', 'error': 'the grow failed'}})
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self.assertEqual(members[0].id, self._state(cluster)['lost'])

    def test_a_new_member_that_failed_ends_it(self):
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        db = self._removed(cluster, members)
        load = mock.patch.object(cluster_models.Cluster,
                                 'load').start()
        load.return_value.ds_version.image_id = 'image-1'
        self.repair.check_now(mock.Mock(), db, manager='valkey')
        new = self._member_of(cluster)
        new.task_status = InstanceTasks.BUILDING_ERROR_SERVER
        new.save()
        # The grow ended its task.
        DBCluster.find_by(id=cluster.id).update(task_status=ClusterTasks.NONE)
        self.assertIsNone(self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey'))
        self.assertEqual(repair.EVENT_ERROR, self._events()[-1])
        self.assertIsNone(self._state(cluster))

    def test_too_long_ends_it(self):
        cluster, _members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        DBCluster.find_by(id=cluster.id).update(task_status=ClusterTasks.NONE)
        self.now += datetime.timedelta(seconds=3601)
        self.assertIsNone(self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey'))
        self.assertIn('not done within',
                      self.notifier.info.call_args.args[2]['reason'])

    def test_a_step_whose_task_never_ended_is_let_go_in_time(self):
        cluster, _members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        # The shrink failed before it could end its task.
        self.now += datetime.timedelta(seconds=3601)
        self.assertIsNone(self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey'))
        self.assertIsNone(self._state(cluster))
        self.assertEqual(ClusterTasks.NONE,
                         DBCluster.find_by(id=cluster.id).task_status)
        self.assertEqual([repair.EVENT_START, repair.EVENT_ERROR],
                         self._events())

    def test_a_task_not_the_steps_is_left_alone_in_time(self):
        cluster, _members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        DBCluster.find_by(id=cluster.id).update(
            task_status=ClusterTasks.DELETING)
        self.now += datetime.timedelta(seconds=3601)
        self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey')
        self.assertEqual(ClusterTasks.DELETING,
                         DBCluster.find_by(id=cluster.id).task_status)

    def _ended_by_the_task_manager(self, context, **kwargs):
        # The cast carries the context to the task manager, whose step
        # of the action ends its notification (manager.shrink_cluster).
        carried = trove_context.TroveContext.from_dict(context.to_dict())
        with EndNotification(carried, **kwargs):
            pass
        return [c.args[1] for c in self.notifier.info.call_args_list]

    def test_the_shrink_and_the_grow_are_the_clusters_actions(self):
        cluster, members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        context = self.task_api.call_args.args[0]
        self.assertEqual('taskmanager', context.notification.server_type)
        sent = self._ended_by_the_task_manager(
            context, cluster_id=cluster.id, instance_ids=[members[0].id])
        self.assertIn('dbaas.cluster_shrink.start', sent)
        self.assertIn('dbaas.cluster_shrink.end', sent)
        db = self._removed(cluster, members)
        load = mock.patch.object(cluster_models.Cluster, 'load').start()
        load.return_value.ds_version.image_id = 'image-1'
        self.repair.check_now(mock.Mock(), db, manager='valkey')
        context = load.call_args.args[0]
        sent = self._ended_by_the_task_manager(
            context, cluster_id=cluster.id, instance_ids=['new'])
        self.assertIn('dbaas.cluster_grow.start', sent)
        self.assertIn('dbaas.cluster_grow.end', sent)

    def test_the_plugins_own_steps(self):
        # A datastore whose steps are not the cluster's shrink and grow
        # (MongoDB: a replica set loses and gets one member): what it
        # remembers of the lost member is kept, its steps are called with
        # the tenant's context and the state, the tasks taken as ever.
        cluster, members = self._lost_cluster()
        self.plugin.remember = lambda c, db, lost, others: {
            'extra': lost.name}
        self.plugin.remove = mock.Mock()
        self.plugin.add = mock.Mock()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        state = self._state(cluster)
        self.assertEqual(members[0].name, state['extra'])
        context, db, given = self.plugin.remove.call_args.args
        self.assertEqual(('t', members[0].id, ADMIN_USER),
                         (context.project_id, given['lost'], context.user_id))
        self.task_api.return_value.shrink_cluster.assert_not_called()
        self.assertEqual(ClusterTasks.SHRINKING_CLUSTER,
                         DBCluster.find_by(id=cluster.id).task_status)
        db = self._removed(cluster, members)
        self.repair.check_now(mock.Mock(), db, manager='valkey')
        context, db, given = self.plugin.add.call_args.args
        self.assertEqual((replace.ADDING, members[0].name),
                         (given['phase'], given['extra']))
        self.assertEqual(ClusterTasks.GROWING_CLUSTER,
                         DBCluster.find_by(id=cluster.id).task_status)

    def test_a_plugin_that_cannot_remember_holds(self):
        cluster, _members = self._lost_cluster()
        self.plugin.remember = mock.Mock(side_effect=RuntimeError('no rs'))
        self.assertEqual(replace.FOLLOW_EVERY * 4, self.repair.check_now(
            mock.Mock(), cluster, manager='valkey'))
        self.assertIsNone(self._state(cluster))
        self.assertEqual(ClusterTasks.NONE,
                         DBCluster.find_by(id=cluster.id).task_status)
        self.assertEqual([], self._events())

    def test_a_replacement_is_followed_while_its_task_runs(self):
        cluster, _members = self._lost_cluster()
        self.repair.check_now(mock.Mock(), cluster, manager='valkey')
        self.assertEqual(replace.FOLLOW_EVERY, self.repair.check_now(
            mock.Mock(), DBCluster.find_by(id=cluster.id), manager='valkey'))
        self.assertIsNotNone(self._state(cluster))


class TestAFailedGrow(test_health.HealthTestBase):
    """A grow whose new member fails marks the new member, not the members
    that serve (production: all three of a cluster in ERROR).
    """

    def test_only_the_members_waited_for(self):
        from trove.taskmanager import models as task_models
        cluster, members = self._cluster(3)
        with InstanceServiceStatus.query() as query:
            query.filter_by(instance_id=members[2].id).update(
                {'status_id': ServiceStatuses.FAILED._code})
        tasks = task_models.ClusterTasks.__new__(task_models.ClusterTasks)
        tasks.reset_task = mock.Mock()
        mock.patch.object(task_models.utils, 'poll_until').start()
        self.addCleanup(mock.patch.stopall)
        self.assertFalse(tasks._all_instances_ready([members[2].id],
                                                    cluster.id))
        self.assertEqual(
            [InstanceTasks.NONE, InstanceTasks.NONE,
             InstanceTasks.BUILDING_ERROR_SERVER],
            [DBInstance.find_by(id=m.id).task_status for m in members])


class _PeerViews(test_health.HealthTestBase):
    """A plugin of PeersHoldItOut asked about the first of three members,
    with the views the other two tell (as the guests' own tests pin them
    from captured status).
    """

    manager = None
    ips = ()

    def setUp(self):
        super(_PeerViews, self).setUp()
        self.guests = {}
        self.cluster, self.members = self._cluster(3, stale=(0,))
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY, me=ip))
             for m, ip in zip(self.members, self.ips)], []))
        for m in self.members[1:]:
            self.guests[m.id] = mock.Mock()

    def _tell(self, member, view):
        self.guests[self.members[member].id].get_cluster_health.\
            return_value = view

    def _ask(self):
        plugin = replace.plugin_for(self.manager)
        plugin.guest_factory = lambda c, i, m: self.guests[i]
        return plugin.can_replace(mock.Mock(), self.cluster,
                                  self.members[0], self.members[1:])


class TestGaleraReplace(_PeerViews):
    """Galera's word on a lost member (MariaDB 11.4, wsrep status)."""

    manager = 'mariadb'
    ips = ('172.31.250.100', '172.31.250.98', '172.31.250.99')

    def test_out_of_the_primary_component_of_both_others(self):
        self._tell(1, ch.view(ch.DEGRADED, ['172.31.250.100'],
                              'not in the primary component',
                              '172.31.250.98'))
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.100'],
                              'not in the primary component',
                              '172.31.250.99'))
        self.assertEqual(
            (True, 'every other member holds 172.31.250.100 out'),
            self._ask())

    def test_a_cluster_whose_health_its_recovery_records(self):
        # Production (gl1, 2026-10-21): Galera's health is recorded from
        # the recovery's views, which carry no 'me': the members' names
        # never learnt, the lost one is the one both others hold out.
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY)) for m in self.members], []))
        self.assertEqual({}, common_health.known_members(self.cluster.id))
        self.test_out_of_the_primary_component_of_both_others()
        # Not when they hold out different members, or one of them.
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.98'],
                              'not in the primary component',
                              '172.31.250.99'))
        self.assertEqual((False, 'its address is not known'), self._ask())

    def test_not_while_another_is_alone_or_out_itself(self):
        self._tell(1, ch.view(ch.DEGRADED, ['172.31.250.100'],
                              'not in the primary component',
                              '172.31.250.98'))
        # The last one standing: both others out.
        self._tell(2, ch.view(ch.DEGRADED,
                              ['172.31.250.100', '172.31.250.98'],
                              'not in the primary component',
                              '172.31.250.99'))
        self.assertFalse(self._ask()[0])
        # Out of a primary component: it sees itself out.
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.99'],
                              'out of the primary component',
                              '172.31.250.99'))
        self.assertFalse(self._ask()[0])
        # In with it: not lost.
        self._tell(2, ch.view(ch.HEALTHY, me='172.31.250.99'))
        self.assertFalse(self._ask()[0])


class TestGroupReplicationReplace(_PeerViews):
    """Group Replication's word on a lost member (MySQL 8.4)."""

    manager = 'mysql'
    ips = ('172.31.250.116', '172.31.250.114', '172.31.250.115')

    def test_not_online_for_both_others(self):
        self._tell(1, ch.view(ch.DEGRADED, ['172.31.250.116'],
                              'not ONLINE in the group', '172.31.250.114'))
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.116'],
                              'not ONLINE in the group', '172.31.250.115'))
        self.assertTrue(self._ask()[0])

    def test_not_while_the_group_lost_its_majority(self):
        self._tell(1, ch.view(ch.DOWN, ['172.31.250.115', '172.31.250.116'],
                              'the group has lost the majority of its '
                              'members', '172.31.250.114'))
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.116'],
                              'not ONLINE in the group', '172.31.250.115'))
        self.assertFalse(self._ask()[0])

    def test_the_plugins_are_found_by_their_entry_points(self):
        replace._plugins.clear()
        for manager, cls in (
                ('mariadb', galera_repair.GaleraReplace),
                ('pxc', galera_repair.GaleraReplace),
                ('mysql', gr_repair.GroupReplicationReplace),
                ('percona', gr_repair.GroupReplicationReplace),
                ('postgresql', pg_repair.PostgresqlReplace)):
            plugin = replace.plugin_for(manager)
            self.assertIsInstance(plugin, cls)
            self.assertEqual(manager, plugin.manager)


class TestPostgresqlReplace(_PeerViews):
    """PostgreSQL's word on a lost member: only the primary sees the
    others (PostgreSQL 18; the views the guest's tests pin).
    """

    manager = 'postgresql'
    ips = ('172.31.250.132', '172.31.250.130', '172.31.250.131')

    def setUp(self):
        super(TestPostgresqlReplace, self).setUp()
        # As its recovery records it: the names never learnt.
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY)) for m in self.members], []))

    def _primary_sees_it_gone(self):
        self._tell(1, ch.view(ch.DEGRADED, ['172.31.250.132'],
                              'not streaming from the primary',
                              '172.31.250.130'))

    def test_the_primary_holds_it_out_the_standby_streams(self):
        self._primary_sees_it_gone()
        self._tell(2, ch.view(ch.HEALTHY, me='172.31.250.131'))
        self.assertEqual(
            (True, 'every other member holds 172.31.250.132 out'),
            self._ask())

    def test_not_while_the_standby_does_not_stream(self):
        self._primary_sees_it_gone()
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.131'],
                              'not streaming', '172.31.250.131'))
        self.assertFalse(self._ask()[0])

    def test_not_without_a_primary_that_holds_it_out(self):
        # The lost one was the primary, no standby promoted yet: both
        # see themselves out.
        self._tell(1, ch.view(ch.DEGRADED, ['172.31.250.130'],
                              'not streaming', '172.31.250.130'))
        self._tell(2, ch.view(ch.DEGRADED, ['172.31.250.131'],
                              'not streaming', '172.31.250.131'))
        self.assertEqual((False, 'its address is not known'), self._ask())
        # A primary that sees every standby streaming: nothing is lost.
        self._tell(1, ch.view(ch.HEALTHY, me='172.31.250.130'))
        self._tell(2, ch.view(ch.HEALTHY, me='172.31.250.131'))
        self.assertFalse(self._ask()[0])
        # Even where the health learnt its name.
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY, me=ip))
             for m, ip in zip(self.members, self.ips)], []))
        self.assertFalse(self._ask()[0])


class TestMongoDbReplace(_PeerViews):
    """MongoDB's word on a lost member of a shard: the set's other
    members vouch (names as the set has them, host:port); the config
    servers and the query routers see nothing of it; the steps are the
    set's own, on the task manager.
    """

    manager = 'mongodb'
    ips = ('10.0.0.11:27017', '10.0.0.12:27017', '10.0.0.13:27017')

    def setUp(self):
        super(TestMongoDbReplace, self).setUp()
        for member in self.members:
            member.shard_id = 'shard-1'
            member.type = 'member'
            member.save()
        self.rest = []
        for name, kind, shard in (('c-configsvr-1', 'config_server', 'cfg'),
                                  ('c-mongos-1', 'query_router', None)):
            db = DBInstance(InstanceTasks.NONE, id=utils.generate_uuid(),
                            name=name, tenant_id='t',
                            cluster_id=self.cluster.id,
                            datastore_version_id='v1', deleted=False,
                            created=timeutils.utcnow(), shard_id=shard,
                            type=kind)
            db.save()
            self.made.append(db)
            self.rest.append(db)
        self._tell(1, ch.view(ch.DEGRADED, ['10.0.0.11:27017'],
                              'replica set rs1: 10.0.0.11:27017 out',
                              '10.0.0.12:27017'))
        self._tell(2, ch.view(ch.DEGRADED, ['10.0.0.11:27017'],
                              'replica set rs1: 10.0.0.11:27017 out',
                              '10.0.0.13:27017'))

    def _ask(self, lost=None):
        plugin = replace.plugin_for(self.manager)
        # No guest for the config server or the router: asking one fails.
        plugin.guest_factory = lambda c, i, m: self.guests[i]
        lost = lost or self.members[0]
        others = [m for m in self.members + self.rest if m.id != lost.id]
        return plugin.can_replace(mock.Mock(), self.cluster, lost, others)

    def test_the_sets_other_members_vouch(self):
        self.assertEqual(
            (True, 'every other member holds 10.0.0.11:27017 out'),
            self._ask())

    def test_not_a_config_server_or_a_query_router(self):
        for lost in self.rest:
            ok, why = self._ask(lost)
            self.assertFalse(ok)
            self.assertIn(lost.type, why)

    def test_not_while_the_set_has_no_primary(self):
        self._tell(1, ch.view(ch.DOWN, ['10.0.0.11:27017'],
                              'replica set rs1 has no primary',
                              '10.0.0.12:27017'))
        self.assertFalse(self._ask()[0])

    def test_what_the_steps_need(self):
        plugin = replace.plugin_for(self.manager)
        plugin.guest_factory = lambda c, i, m: self.guests[i]
        self.guests[self.members[1].id].get_replica_set_name.\
            return_value = 'rs1'
        self.assertEqual(
            {'shard_id': 'shard-1', 'host': '10.0.0.11:27017',
             'replica_set_name': 'rs1'},
            plugin.remember(mock.Mock(), self.cluster, self.members[0],
                            self.members[1:] + self.rest))
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY)) for m in self.members], []))
        self.assertRaises(exception.TroveError, plugin.remember,
                          mock.Mock(), self.cluster, self.members[0],
                          self.members[1:])

    def test_the_sets_own_steps(self):
        plugin = replace.plugin_for(self.manager)
        api = mock.patch.object(task_api, 'load').start().return_value
        state = {'lost': self.members[0].id, 'host': '10.0.0.11:27017',
                 'shard_id': 'shard-1', 'replica_set_name': 'rs1',
                 'spec': {'flavor_id': 'f1', 'volume_size': 10}}
        plugin.remove('ctx', self.cluster, state)
        api.mongodb_remove_member.assert_called_once_with(
            self.cluster.id, self.members[0].id, '10.0.0.11:27017')
        load = mock.patch.object(cluster_models.Cluster, 'load').start()
        cluster = load.return_value
        cluster.name = 'c'
        cluster._create_instances.return_value = ['new']
        with mock.patch.object(mongodb_repair.time, 'time',
                               return_value=1792600000.0):
            plugin.add('ctx', self.cluster, state)
        (spec,), config, tag, _locality = (
            cluster._create_instances.call_args.args)
        self.assertEqual({'flavor_id': 'f1', 'volume_size': 10,
                          'name': 'c-rs1-1792600000'}, spec)
        self.assertEqual({'shard_id': 'shard-1', 'instance_type': 'member',
                          'replica_set_name': 'rs1'}, config)
        self.assertEqual('rs1', tag)
        api.mongodb_add_member.assert_called_once_with(self.cluster.id,
                                                       'new')

    def test_the_plugin_is_found_by_its_entry_point(self):
        replace._plugins.clear()
        self.assertIsInstance(replace.plugin_for('mongodb'),
                              mongodb_repair.MongoDbReplace)


def DBInstance_all(cluster_id):
    return DBInstance.find_all(cluster_id=cluster_id, deleted=False).all()


class TestSentinelReplace(test_health.HealthTestBase):
    """The Redis family's word on a lost member of a sentinel cluster,
    from what the other members' sentinels see (common sentinel_health
    reasons).
    """

    def setUp(self):
        super(TestSentinelReplace, self).setUp()
        self.guests = {}
        self.plugin = redis_repair.SentinelReplace(
            'valkey', guest_factory=lambda c, i, m: self.guests[i])
        self.cluster, self.members = self._cluster(3, stale=(0,))
        self.cluster.save_properties({'topology': 'sentinel'})
        common_health.record(self.cluster.id, ch.combine(
            [(m.name, ch.view(ch.HEALTHY, me='192.168.1.%d' % (11 + i)))
             for i, m in enumerate(self.members)], []))
        for i, m in enumerate(self.members[1:], start=1):
            self.guests[m.id] = mock.Mock()
            self.guests[m.id].get_cluster_health.return_value = ch.view(
                ch.DEGRADED, ['192.168.1.11'], 'down as the sentinel sees it',
                me='192.168.1.%d' % (11 + i))

    def _ask(self):
        return self.plugin.can_replace(mock.Mock(), self.cluster,
                                       self.members[0], self.members[1:])

    def test_held_out_by_every_other_member(self):
        ok, why = self._ask()
        self.assertTrue(ok)
        self.assertEqual('every other member holds 192.168.1.11 out', why)

    def test_not_while_a_peer_does_not_answer_or_sees_more(self):
        guest = self.guests[self.members[2].id]
        guest.get_cluster_health.side_effect = RuntimeError('timeout')
        self.assertFalse(self._ask()[0])
        guest.get_cluster_health.side_effect = None
        guest.get_cluster_health.return_value = ch.view(
            ch.DEGRADED, ['192.168.1.11', '192.168.1.12'],
            'down as the sentinel sees it', me='192.168.1.13')
        self.assertFalse(self._ask()[0])
        guest.get_cluster_health.return_value = ch.view(
            ch.DOWN, ['192.168.1.11'],
            'the sentinels hold the primary down', me='192.168.1.13')
        self.assertFalse(self._ask()[0])
        # A peer that sees it in: not lost.
        guest.get_cluster_health.return_value = ch.view(
            ch.HEALTHY, me='192.168.1.13')
        self.assertFalse(self._ask()[0])

    def test_only_the_sentinel_topology(self):
        self.cluster.save_properties({'topology': 'cluster'})
        self.assertFalse(self._ask()[0])

    def test_the_plugin_is_found_by_its_entry_point(self):
        replace._plugins.clear()
        for manager in ('redis', 'valkey', 'keydb'):
            plugin = replace.plugin_for(manager)
            self.assertIsInstance(plugin, redis_repair.SentinelReplace)
            self.assertEqual(manager, plugin.manager)
        self.assertIsNone(replace.plugin_for('cassandra'))
