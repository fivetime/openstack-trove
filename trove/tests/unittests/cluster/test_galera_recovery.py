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

"""The task manager bringing a Galera cluster back after every member went
down: what the members answer, what it decides, whom it tells.
"""

from unittest import mock

from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common.strategies.cluster.experimental.common import (
    recovery as common_recovery)
from trove.common.strategies.cluster.experimental.galera_common import (
    recovery)
from trove.common.strategies.cluster.experimental.galera_common import (
    taskmanager as galera_taskmanager)
from trove.common.strategies.cluster.experimental.group_replication import (
    taskmanager as gr_taskmanager)
from trove.common.strategies.cluster import strategy
from trove.instance.tasks import InstanceTasks
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
U = 'u1'


def _answer(ip, waiting=True, in_group=False, seqno=10, uuid=U, safe=False):
    return {'ip': ip, 'waiting': waiting, 'in_group': in_group,
            'position': [uuid, seqno] if seqno is not None else None,
            'safe_to_bootstrap': safe, 'bootstrapped': False}


class GaleraRecoveryTest(trove_testtools.TestCase):

    def setUp(self):
        super(GaleraRecoveryTest, self).setUp()
        self.members = [mock.Mock(id=i, task_status=InstanceTasks.NONE)
                        for i in ('i1', 'i2', 'i3')]
        self.answers = {'i1': _answer('10.0.0.1'), 'i2': _answer('10.0.0.2'),
                        'i3': _answer('10.0.0.3')}
        self.calls = []
        self.guests = {}

        def guest(context, member):
            if member.id not in self.guests:
                g = mock.Mock()

                def view(timeout=None, _id=member.id):
                    self.calls.append(('view', _id))
                    answer = self.answers.get(_id)
                    if answer is None:
                        raise Exception('timed out')
                    return answer
                g.get_recovery_view.side_effect = view
                g.bootstrap_cluster.side_effect = (
                    lambda _id=member.id: self.calls.append(
                        ('bootstrap', _id)))
                self.guests[member.id] = g
            return self.guests[member.id]
        self.recovery = recovery.GaleraClusterRecovery('pxc', guest)
        self.recovery._members = mock.Mock(return_value=self.members)
        self.recovery.conf = mock.Mock(
            cluster_peer_timeout=3, cluster_recovery_grace=60,
            cluster_recovery_interval=30, cluster_auto_bootstrap=True,
            cluster_bootstrap_needs_all_members=False,
            cluster_bootstrap_jitter=0, cluster_recovery_timeout=3600)
        self.db_cluster = mock.Mock(id='c1', task_status=ClusterTasks.NONE,
                                    updated=None)
        mock.patch.object(common_recovery.cluster_models.DBCluster, 'find_by',
                          return_value=self.db_cluster).start()
        mock.patch.object(common_recovery.time, 'sleep').start()

        # Freeing the task is a compare-and-set too; here it frees the
        # mock cluster's task the way the real one does.
        def release(cluster_id, db_cluster=None):
            self.recovery.forming_since.pop(cluster_id, None)
            self.recovery.waiting_since.pop(cluster_id, None)
            if self.db_cluster.task_status == ClusterTasks.RECOVERING_CLUSTER:
                self.db_cluster.update(task_status=ClusterTasks.NONE)
                self.db_cluster.task_status = ClusterTasks.NONE
        mock.patch.object(common_recovery.ClusterRecovery, '_release',
                          side_effect=release).start()

        # The lock is a compare-and-set on the clusters table; here it
        # takes the mock cluster's task the way the real one does.
        def take(cluster_id):
            if self.db_cluster.task_status != ClusterTasks.NONE:
                return False
            self.db_cluster.update(
                task_status=ClusterTasks.RECOVERING_CLUSTER)
            self.db_cluster.task_status = ClusterTasks.RECOVERING_CLUSTER
            self.recovery.forming_since[cluster_id] = \
                common_recovery.time.monotonic()
            return True
        mock.patch.object(common_recovery.ClusterRecovery, '_take',
                          side_effect=take).start()
        self.addCleanup(mock.patch.stopall)

    def _check_at(self, seconds):
        with mock.patch.object(common_recovery.time, 'monotonic',
                               return_value=seconds):
            self.recovery.check('ctx', self.db_cluster)

    def _bootstraps(self):
        return [c[1] for c in self.calls if c[0] == 'bootstrap']

    def test_nothing_while_a_primary_component_is_up(self):
        self.answers['i2'] = _answer('10.0.0.2', waiting=False,
                                     in_group=True)
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())
        self.assertEqual({}, self.recovery.waiting_since)

    def test_nothing_while_nobody_waits(self):
        for i in self.answers:
            self.answers[i]['waiting'] = False
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def test_nothing_while_the_cluster_has_a_task(self):
        self.db_cluster.task_status = ClusterTasks.GROWING_CLUSTER
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self.calls)

    def test_nothing_while_a_member_has_a_task(self):
        # A member being resized, restarted or upgraded is left alone,
        # and so is its cluster; once it is done the recovery goes on.
        self.members[1].task_status = InstanceTasks.RESIZING
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self.calls)
        self.members[1].task_status = InstanceTasks.NONE
        self._check_at(200)
        self._check_at(300)
        self.assertEqual(['i1'], self._bootstraps())

    def test_the_grace_period_and_the_interval(self):
        self._check_at(0)
        # Looked at again when the grace period ends, not after the
        # interval; a cluster not waiting after the interval.
        self.assertEqual(30, self.recovery.look_again_in('c1'))
        self._check_at(59)
        self.assertEqual(1, self.recovery.look_again_in('c1'))
        self.assertEqual(30, self.recovery.look_again_in('c9'))
        self._check_at(61)
        self.assertEqual(['i1'], self._bootstraps())
        self.assertEqual(30, self.recovery.look_again_in('c1'))
        # Were the cluster freed and still waiting: not before the
        # interval.
        self.db_cluster.task_status = ClusterTasks.NONE
        self.recovery.waiting_since['c1'] = 0
        self._check_at(80)
        self.assertEqual(['i1'], self._bootstraps())
        self._check_at(92)
        self.assertEqual(['i1', 'i1'], self._bootstraps())

    def test_the_most_advanced_member_forms_the_cluster(self):
        self.answers['i3'] = _answer('10.0.0.3', seqno=12)
        self._check_at(0)
        self._check_at(100)
        self.assertEqual(['i3'], self._bootstraps())

    def test_every_waiting_member_is_decided_and_logged(self):
        # The member behind comes after the one that forms the cluster:
        # its decision is logged all the same.
        self.answers['i1'] = _answer('10.0.0.1', seqno=12)
        with mock.patch.object(recovery.LOG, 'info') as info:
            self._check_at(0)
            self._check_at(100)
        decided = [c[0][2] for c in info.call_args_list
                   if c[0][0].startswith('Cluster %s, member')]
        self.assertEqual(['i1', 'i2', 'i3'], decided)
        self.assertEqual(['i1'], self._bootstraps())

    def test_the_lowest_address_among_equals(self):
        self.answers['i1'] = _answer('10.0.0.1', seqno=12)
        self.answers['i2'] = _answer('10.0.0.2', seqno=12)
        self._check_at(0)
        self._check_at(100)
        self.assertEqual(['i1'], self._bootstraps())

    def test_a_member_whose_position_is_not_known_yet_holds_everyone(self):
        # It may hold the most: its recovery run comes after the grace
        # period. Nothing is decided until it answers its position.
        self.answers['i1'] = _answer('10.0.0.1', seqno=None)
        with mock.patch.object(recovery.LOG, 'info') as info:
            self._check_at(0)
            self._check_at(100)
        self.assertEqual([], self._bootstraps())
        reasons = [c[0][-1] for c in info.call_args_list
                   if c[0][0].startswith('Cluster %s, member')]
        self.assertEqual(['10.0.0.1'] * 3, reasons)
        # Neither does the last to leave, while another's is unknown.
        self.answers['i3'] = _answer('10.0.0.3', safe=True)
        self._check_at(200)
        self.assertEqual([], self._bootstraps())
        # Known: the one holding the most goes.
        self.answers['i1'] = _answer('10.0.0.1', seqno=12)
        self._check_at(300)
        self.assertEqual(['i1'], self._bootstraps())

    def test_nobody_knowing_a_position_nobody_forms_the_cluster(self):
        for i in ('i1', 'i2', 'i3'):
            self.answers[i] = _answer('10.0.0.%s' % i[1], seqno=None)
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def test_different_histories_wait(self):
        self.answers['i1'] = _answer('10.0.0.1', uuid='other')
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def test_every_member_when_galera_asks(self):
        # Galera goes on committing after members leave: two of three
        # may lack what the third holds.
        self.recovery.conf.cluster_bootstrap_needs_all_members = True
        del self.answers['i3']
        with mock.patch.object(recovery.LOG, 'info') as info:
            self._check_at(0)
            self._check_at(100)
        self.assertEqual([], self._bootstraps())
        reasons = [c[0][-1] for c in info.call_args_list
                   if c[0][0].startswith('Cluster %s, member')]
        self.assertEqual(['2 of 3 members here, 3 needed'] * 2, reasons)
        self.answers['i3'] = _answer('10.0.0.3')
        self._check_at(200)
        self.assertEqual(['i1'], self._bootstraps())

    def test_the_last_to_leave_forms_the_cluster_alone(self):
        self.recovery.conf.cluster_bootstrap_needs_all_members = True
        del self.answers['i1']
        del self.answers['i2']
        self.answers['i3'] = _answer('10.0.0.3', safe=True)
        with mock.patch.object(recovery.LOG, 'info') as info:
            self._check_at(0)
            self._check_at(100)
        self.assertEqual(['i3'], self._bootstraps())
        self.assertIn('the last to leave the cluster',
                      [c[0][0] for c in info.call_args_list
                       if 'bootstrap' in c[0][0]][0])

    def test_the_last_to_leave_is_not_trusted_when_another_holds_more(self):
        self.recovery.conf.cluster_bootstrap_needs_all_members = True
        self.answers['i3'] = _answer('10.0.0.3', safe=True, seqno=10)
        self.answers['i1'] = _answer('10.0.0.1', seqno=12)
        self._check_at(0)
        self._check_at(100)
        # Everyone is here and i1 holds the most: i1 goes, not i3.
        self.assertEqual(['i1'], self._bootstraps())

    def test_the_last_to_leave_without_a_position_waits_for_it(self):
        self.recovery.conf.cluster_bootstrap_needs_all_members = True
        del self.answers['i1']
        self.answers['i3'] = _answer('10.0.0.3', safe=True, seqno=None)
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())
        self.answers['i3'] = _answer('10.0.0.3', safe=True, seqno=10)
        self._check_at(200)
        self.assertEqual(['i3'], self._bootstraps())

    def test_no_majority_waits(self):
        del self.answers['i2']
        del self.answers['i3']
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())
        # A majority is enough.
        self.answers['i2'] = _answer('10.0.0.2')
        self._check_at(200)
        self.assertEqual(['i1'], self._bootstraps())

    def test_every_member_when_asked(self):
        self.recovery.conf.cluster_bootstrap_needs_all_members = True
        del self.answers['i3']
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def test_auto_bootstrap_off(self):
        self.recovery.conf.cluster_auto_bootstrap = False
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def test_not_when_the_members_changed_meanwhile(self):
        answers = [dict(self.answers), dict(self.answers)]
        answers[1]['i2'] = _answer('10.0.0.2', waiting=False, in_group=True)
        self.recovery._views = mock.Mock(side_effect=[
            [(m, a.get(m.id)) for m in self.members] for a in answers])
        self._check_at(0)
        self.recovery.waiting_since['c1'] = -100
        self._check_at(100)
        self.assertEqual([], self._bootstraps())

    def _lock_updates(self):
        updates = []

        def update(**kw):
            updates.append(kw['task_status'])
            self.db_cluster.task_status = kw['task_status']
        self.db_cluster.update.side_effect = update
        return updates

    def test_the_cluster_is_locked_while_it_is_formed_again(self):
        # The member is told and not waited for; the cluster keeps its
        # task, and is not decided again, until a member is in a primary
        # component.
        updates = self._lock_updates()
        self._check_at(0)
        self._check_at(100)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER], updates)
        self.assertEqual(['i1'], self._bootstraps())
        self._check_at(200)
        self._check_at(300)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER], updates)
        self.assertEqual(['i1'], self._bootstraps())
        self.answers['i1'] = _answer('10.0.0.1', waiting=False,
                                     in_group=True)
        self._check_at(400)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER,
                          ClusterTasks.NONE], updates)
        self.assertNotIn('c1', self.recovery.forming_since)
        self.assertNotIn('c1', self.recovery.waiting_since)
        # Not decided again while the component is up.
        self._check_at(500)
        self.assertEqual(['i1'], self._bootstraps())

    def test_a_cluster_formed_by_another_task_manager_is_followed(self):
        updates = self._lock_updates()
        self.db_cluster.task_status = ClusterTasks.RECOVERING_CLUSTER
        self._check_at(0)
        self.assertEqual([], self._bootstraps())
        self.assertEqual([], updates)
        self.answers['i2'] = _answer('10.0.0.2', waiting=False,
                                     in_group=True)
        self._check_at(30)
        self.assertEqual([ClusterTasks.NONE], updates)

    def test_the_cluster_is_freed_after_the_recovery_timeout(self):
        updates = self._lock_updates()
        self._check_at(0)
        self._check_at(100)
        self._check_at(100 + 3599)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER], updates)
        self._check_at(100 + 3600)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER,
                          ClusterTasks.NONE], updates)
        # Decided anew: the grace period runs again first.
        self._check_at(100 + 3600 + 30)
        self.assertEqual(['i1'], self._bootstraps())
        self._check_at(100 + 3600 + 100)
        self.assertEqual(['i1', 'i1'], self._bootstraps())

    def test_a_member_that_cannot_be_told_frees_the_cluster(self):
        updates = self._lock_updates()
        self._check_at(0)
        self.guests['i1'].bootstrap_cluster.side_effect = Exception('no')
        self._check_at(100)
        self.assertEqual([ClusterTasks.RECOVERING_CLUSTER,
                          ClusterTasks.NONE], updates)
        self.assertNotIn('c1', self.recovery.forming_since)

    def test_run_goes_on_after_a_failure(self):
        other = mock.Mock(id='c2', task_status=ClusterTasks.NONE)
        self.recovery._members.side_effect = [Exception('db'), self.members]
        with mock.patch.object(common_recovery.time, 'monotonic',
                               return_value=0):
            self.recovery.run('ctx', [other, self.db_cluster])
        self.assertIn('c1', self.recovery.waiting_since)


class RecoveryMembersTest(trove_testtools.TestCase):
    """The task manager's context owns no tenant: the members come
    straight from the database, the guest client from the member's id.
    """

    @mock.patch.object(common_recovery.DBInstance, 'find_all')
    def test_members_are_the_db_rows_of_the_cluster(self, find_all):
        rows = [mock.Mock(id='i1'), mock.Mock(id='i2')]
        find_all.return_value.all.return_value = rows
        r = recovery.GaleraClusterRecovery('pxc')
        self.assertEqual(rows, r._members(mock.Mock(project_id=None), 'c1'))
        find_all.assert_called_once_with(cluster_id='c1', deleted=False)

    @mock.patch.object(common_recovery.clients, 'create_guest_client')
    def test_guest_client_from_the_member_id_and_the_manager(self, create):
        r = recovery.GaleraClusterRecovery('mariadb')
        self.assertIs(create.return_value, r._guest('ctx', mock.Mock(id='i1')))
        create.assert_called_once_with('ctx', 'i1', 'mariadb')


class RecoveryWiringTest(trove_testtools.TestCase):

    def test_the_galera_strategies_have_a_recovery_and_gr_has_none(self):
        for manager in ('pxc', 'mariadb'):
            strat = strategy.load_taskmanager_strategy(manager)
            self.assertIsInstance(strat, galera_taskmanager.
                                  GaleraCommonTaskManagerStrategy)
            self.assertIsInstance(strat.cluster_recovery(manager),
                                  recovery.GaleraClusterRecovery)
        strat = strategy.load_taskmanager_strategy('mysql')
        self.assertIsInstance(
            strat, gr_taskmanager.GroupReplicationTaskManagerStrategy)
        self.assertIsNone(strat.cluster_recovery('mysql'))
        # The poll is off by default: the members report, the scan runs.
        self.assertEqual(0, CONF.cluster_recovery_check_interval)
        self.assertEqual(300, CONF.cluster_recovery_scan_interval)
