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
import json
from unittest import mock

from oslo_utils import timeutils

from trove.cluster.models import DBCluster
from trove.cluster.models import DBClusterHealth
from trove.cluster.tasks import ClusterTasks
from trove.cluster import views as cluster_views
from trove.common import cluster_health as ch
from trove.common import exception
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.galera_common import (
    recovery as galera_recovery)
from trove.common import utils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.taskmanager import recovery
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class HealthTestBase(trove_testtools.TestCase):
    """On the real tables."""

    def setUp(self):
        super(HealthTestBase, self).setUp()
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

    def _cluster(self, n=3, stale=(), task=ClusterTasks.NONE):
        cluster = DBCluster(task, id=utils.generate_uuid(), name='c',
                            tenant_id='t', datastore_version_id='v1',
                            created=timeutils.utcnow())
        cluster.save()
        self.made.append(cluster)
        members = []
        for i in range(n):
            member = DBInstance(InstanceTasks.NONE, id=utils.generate_uuid(),
                                name='c-member-%d' % (i + 1), tenant_id='t',
                                cluster_id=cluster.id,
                                datastore_version_id='v1', deleted=False,
                                created=timeutils.utcnow())
            member.save()
            self.made.append(member)
            status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                           id=utils.generate_uuid(),
                                           instance_id=member.id)
            status.save()
            self.made.append(status)
            if i in stale:
                with InstanceServiceStatus.query() as query:
                    query.filter_by(instance_id=member.id).update(
                        {'updated_at': timeutils.utcnow() -
                         datetime.timedelta(seconds=600)})
            members.append(member)
        return cluster, members

    def _recorded(self, cluster_id):
        row = DBClusterHealth.find_by(id=cluster_id)
        self.made.append(row)
        return row.status, json.loads(row.detail), row.since


class TestHealthWatch(HealthTestBase):

    def _watch(self, answers):
        """A watch whose members answer as given, by name: a view, None
        (watches nothing) or an exception (does not answer).
        """
        def guest(context, member):
            client = mock.Mock()
            answer = answers[member.name]
            if isinstance(answer, Exception):
                client.get_cluster_health.side_effect = answer
            else:
                client.get_cluster_health.return_value = answer
            return client
        return health.HealthWatch('redis', guest_factory=guest)

    def test_a_cluster_short_of_a_member(self):
        cluster, members = self._cluster(stale=(2,))
        seen = ch.view(ch.DEGRADED, ['10.0.0.3'], 'failing: 10.0.0.3')
        watch = self._watch({
            'c-member-1': dict(seen, me='10.0.0.1'),
            'c-member-2': dict(seen, me='10.0.0.2'),
            # Its heartbeat is stale: not asked.
            'c-member-3': AssertionError('asked')})

        watch.check_now(None, cluster, 'test')

        status, detail, since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['10.0.0.3', 'c-member-3'], detail['out'])
        self.assertEqual('not answering: c-member-3; failing: 10.0.0.3 '
                         '(c-member-1, c-member-2)', detail['reason'])
        self.assertEqual({'10.0.0.1': 'c-member-1',
                          '10.0.0.2': 'c-member-2'}, detail['members'])
        self.assertTrue(watch.still_watching(cluster.id))
        self.assertEqual(120, watch.look_again_in(cluster.id))

    def test_whole_again_and_the_names_learnt(self):
        cluster, members = self._cluster()
        watch = self._watch({
            'c-member-%d' % i: ch.view(ch.HEALTHY, me='10.0.0.%d' % i)
            for i in (1, 2, 3)})
        watch.check_now(None, cluster, 'test')
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])
        self.assertFalse(watch.still_watching(cluster.id))
        # The third is gone now: the others name it by its address, which
        # the record maps to its name.
        seen = ch.view(ch.DEGRADED, ['10.0.0.3'], 'failing: 10.0.0.3')
        watch = self._watch({'c-member-1': seen, 'c-member-2': seen,
                             'c-member-3': RuntimeError('timed out')})
        watch.check_now(None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(['c-member-3'], detail['out'])

    def test_written_only_when_it_changes(self):
        cluster, members = self._cluster()
        answers = {'c-member-%d' % i: ch.view(ch.HEALTHY)
                   for i in (1, 2, 3)}
        self.assertTrue(health.record(cluster.id, ch.combine(
            list(answers.items()), [])))
        since = self._recorded(cluster.id)[2]
        self.assertFalse(health.record(cluster.id, ch.combine(
            list(answers.items()), [])))
        self.assertEqual(since, self._recorded(cluster.id)[2])

    def test_no_member_answers(self):
        cluster, members = self._cluster(stale=(0, 1, 2))
        self._watch({}).check_now(None, cluster, 'test')
        self.assertEqual(ch.DOWN, self._recorded(cluster.id)[0])

    def test_a_query_router_watches_nothing(self):
        cluster, members = self._cluster(n=2)
        self._watch({'c-member-1': ch.view(ch.HEALTHY),
                     'c-member-2': None}).check_now(None, cluster, 'test')
        self.assertEqual(ch.HEALTHY, self._recorded(cluster.id)[0])

    def test_guest_agents_from_before_the_watch(self):
        # They answer the call with an error: up, as their heartbeats say;
        # a timeout is no answer.
        cluster, members = self._cluster()
        self._watch({
            'c-member-1': exception.GuestError(original_message='NoSuch'),
            'c-member-2': exception.GuestError(original_message='NoSuch'),
            'c-member-3': exception.GuestTimeout()}).check_now(
                None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['c-member-3'], detail['out'])

    def test_a_member_whose_database_does_not_answer(self):
        cluster, members = self._cluster(n=2)
        self._watch({'c-member-1': ch.view(ch.HEALTHY),
                     'c-member-2': {'down': True, 'reason': 'no'}}
                    ).check_now(None, cluster, 'test')
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(['c-member-2'], detail['out'])

    def test_a_cluster_with_a_task_is_left(self):
        cluster, members = self._cluster(task=ClusterTasks.GROWING_CLUSTER)
        self._watch({}).check_now(None, cluster, 'test')
        self.assertRaises(exception.NotFound, DBClusterHealth.find_by,
                          id=cluster.id)

    def test_the_hub_gives_the_clusters_that_heal_themselves_a_watch(self):
        hub = recovery.RecoveryHub(coordinator=mock.Mock())
        hub.cluster_manager = {'c1': 'redis', 'c2': 'couchbase'}
        self.assertIsInstance(hub._recovery_of(mock.Mock(id='c1')),
                              health.HealthWatch)
        # No clusters that watch: nothing.
        with mock.patch.object(recovery.strategy,
                               'load_taskmanager_strategy') as load:
            del load.return_value.cluster_recovery
            self.assertIsNone(hub._recovery_of(mock.Mock(id='c2')))


class TestRecoveredHealth(HealthTestBase):
    """The health of a Galera cluster, from the views its recovery gets."""

    def test_from_the_views(self):
        cluster, members = self._cluster()
        views = [(members[0], {'in_group': True}),
                 (members[1], {'in_group': False, 'waiting': True}),
                 (members[2], None)]
        galera = galera_recovery.GaleraClusterRecovery('mariadb')
        galera._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DEGRADED, status)
        self.assertEqual(['c-member-2', 'c-member-3'], detail['out'])

        # No member in a primary component: down.
        views = [(m, {'in_group': False}) for m in members]
        galera._record_health(cluster.id, members, views)
        status, detail, _since = self._recorded(cluster.id)
        self.assertEqual(ch.DOWN, status)
        self.assertTrue(detail['reason'].startswith('no member serves'))

    def test_the_scan_looks_at_a_cluster_not_looked_at_yet(self):
        new, _m = self._cluster()
        seen, _m = self._cluster()
        health.record(seen.id, ch.combine([('c-member-1',
                                            ch.view(ch.HEALTHY))], []))
        self.made.append(DBClusterHealth.find_by(id=seen.id))
        found = {row[0] for row in recovery.suspect_clusters(
            timeutils.utcnow() - datetime.timedelta(seconds=90))}
        self.assertIn(new.id, found)
        self.assertNotIn(seen.id, found)


class TestHealthView(HealthTestBase):

    def test_unknown_until_looked_at_then_recorded(self):
        cluster, _m = self._cluster()
        view = mock.Mock(spec=cluster_views.ClusterView)
        view.cluster = mock.Mock(health=None)
        self.assertEqual({'status': ch.UNKNOWN},
                         cluster_views.ClusterView._build_health(view))
        health.record(cluster.id, ch.combine([('c-member-1',
                                               ch.view(ch.HEALTHY))],
                                             ['c-member-2']))
        row = DBClusterHealth.find_by(id=cluster.id)
        self.made.append(row)
        view.cluster = mock.Mock(health=row)
        shown = cluster_views.ClusterView._build_health(view)
        self.assertEqual(ch.DEGRADED, shown['status'])
        self.assertEqual(['c-member-2'], shown['members_out'])
        self.assertEqual('not answering: c-member-2', shown['reason'])
        self.assertEqual(row.since, shown['since'])

    def test_a_list_loads_them_at_once(self):
        one, _m = self._cluster()
        two, _m = self._cluster()
        health.record(one.id, ch.combine([('c-member-1',
                                           ch.view(ch.HEALTHY))], []))
        self.made.append(DBClusterHealth.find_by(id=one.id))
        clusters = [mock.Mock(id=one.id), mock.Mock(id=two.id)]
        listed = cluster_views.ClustersView(clusters)
        with mock.patch.object(listed, 'data_for_cluster',
                               side_effect=lambda c: c.health):
            rows = listed.data()['clusters']
        self.assertEqual(ch.HEALTHY, rows[0].status)
        self.assertIsNone(rows[1])
