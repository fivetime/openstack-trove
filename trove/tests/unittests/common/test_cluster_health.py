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

from trove.common import cluster_health as ch
from trove.tests.unittests import trove_testtools


class TestCombine(trove_testtools.TestCase):

    def test_every_member_healthy(self):
        health = ch.combine([('m-1', ch.view(ch.HEALTHY, me='10.0.0.1')),
                             ('m-2', ch.view(ch.HEALTHY, me='10.0.0.2'))], [])
        self.assertEqual(ch.HEALTHY, health['status'])
        self.assertEqual([], health['out'])
        self.assertIsNone(health['reason'])
        self.assertEqual({'10.0.0.1': 'm-1', '10.0.0.2': 'm-2'},
                         health['members'])

    def test_a_member_not_answering(self):
        health = ch.combine([('m-1', ch.view(ch.HEALTHY))], ['m-2'])
        self.assertEqual(ch.DEGRADED, health['status'])
        self.assertEqual(['m-2'], health['out'])
        self.assertEqual('not answering: m-2', health['reason'])

    def test_no_member_answering(self):
        health = ch.combine([], ['m-1', 'm-2'])
        self.assertEqual(ch.DOWN, health['status'])
        self.assertEqual(['m-1', 'm-2'], health['out'])

    def test_what_the_members_see_with_trove_s_names(self):
        # Two members see the third out, by its address: said once, named
        # as Trove names it from what an earlier look learnt.
        seen = ch.view(ch.DEGRADED, ['10.0.0.3:6379'], 'failing: 10.0.0.3')
        health = ch.combine([('m-1', seen), ('m-2', seen)], ['m-3'],
                            known={'10.0.0.3:6379': 'm-3'})
        self.assertEqual(ch.DEGRADED, health['status'])
        self.assertEqual(['m-3'], health['out'])
        self.assertEqual('not answering: m-3; failing: 10.0.0.3 (m-1, m-2)',
                         health['reason'])

    def test_the_worst_a_member_says(self):
        health = ch.combine([
            ('cfg-1', ch.view(ch.DEGRADED, ['c:27019'], 'set cfg: c out')),
            ('rs1-1', ch.view(ch.DOWN, ['a:27017', 'b:27017'],
                              'replica set rs1 has no primary'))], [])
        self.assertEqual(ch.DOWN, health['status'])
        self.assertEqual(['a:27017', 'b:27017', 'c:27019'], health['out'])

    def test_not_down_while_a_member_takes_the_writes(self):
        # kc9-keydb-s (production, 2026-10-26): the sentinels promoted a
        # replica that took the writes 16 s after the primary was killed;
        # both members' sentinels held the old primary down for 50 s
        # more, until the other replica followed the new one, and the
        # cluster showed DOWN all along.
        said = 'the sentinels hold the primary down'
        health = ch.combine([
            ('r-1', ch.view(ch.DOWN, ['10.0.0.1'], said, '10.0.0.2',
                            writable=True)),
            ('r-3', ch.view(ch.DOWN, ['10.0.0.1'], said, '10.0.0.3'))],
            ['r-2'], {'10.0.0.1': 'r-2'})
        self.assertEqual(ch.DEGRADED, health['status'])
        self.assertEqual(['r-2'], health['out'])
        # No member takes them: down.
        health = ch.combine([
            ('r-1', ch.view(ch.DOWN, ['10.0.0.1'], said, '10.0.0.2')),
            ('r-3', ch.view(ch.DOWN, ['10.0.0.1'], said, '10.0.0.3'))],
            ['r-2'], {'10.0.0.1': 'r-2'})
        self.assertEqual(ch.DOWN, health['status'])
        # Nor when the member that takes them sees the cluster degraded
        # already (its sentinel switched) while another sees it down.
        health = ch.combine([
            ('r-1', ch.view(ch.DEGRADED, ['10.0.0.1'],
                            'down as the sentinel sees it', '10.0.0.2',
                            writable=True)),
            ('r-3', ch.view(ch.DOWN, ['10.0.0.1'], said, '10.0.0.3'))],
            ['r-2'], {'10.0.0.1': 'r-2'})
        self.assertEqual(ch.DEGRADED, health['status'])
        # A view that does not say it is as before.
        self.assertNotIn('writable', ch.view(ch.HEALTHY))

    def test_the_members_that_take_the_writes(self):
        # A Redis sentinel primary just failed over (2026-10-27): the
        # promoted replica takes the writes, the old primary does not.
        health = ch.combine([
            ('r-1', ch.view(ch.HEALTHY, me='10.0.0.1', writable=True)),
            ('r-2', ch.view(ch.HEALTHY, me='10.0.0.2')),
            ('r-3', ch.view(ch.HEALTHY, me='10.0.0.3'))], [])
        self.assertEqual(['r-1'], health['writers'])
        # Every member of a multi-primary group; one that is out, and one
        # that does not answer, are not writers.
        health = ch.combine([
            ('g-1', ch.view(ch.HEALTHY, writable=True)),
            ('g-2', ch.view(ch.DEGRADED, ['g-3'], 'out', writable=True))],
            ['g-3'])
        self.assertEqual(['g-1', 'g-2'], health['writers'])
        # A datastore that names none (Cassandra), or no member answers.
        self.assertEqual([], ch.combine(
            [('c-1', ch.view(ch.HEALTHY))], [])['writers'])
        self.assertEqual([], ch.combine([], ['c-1'])['writers'])

    def test_a_member_that_has_not_caught_up_does_not_take_it_down(self):
        # ic-cass2 (production, Incus, 2026-10-28): a Cassandra node back
        # from a hard reboot held both other nodes down until it heard
        # from them; they answered, and the cluster showed DOWN for 21 s.
        late = ch.view(ch.DOWN, ['10.0.0.1', '10.0.0.3'],
                       '2 of 3 nodes down: 10.0.0.1, 10.0.0.3', '10.0.0.2')
        for other in (ch.view(ch.HEALTHY, me='10.0.0.1'),
                      ch.view(ch.DEGRADED, ['10.0.0.2'],
                              'not up and normal: 10.0.0.2 UJ', '10.0.0.1')):
            third = dict(other, me='10.0.0.3')
            health = ch.combine([('c-1', other), ('c-2', late),
                                 ('c-3', third)], [])
            self.assertEqual(ch.DEGRADED, health['status'])
            self.assertEqual(['c-2'], health['out'])
            self.assertIn('sees the cluster down, while c-1, c-3 answer '
                          '(c-2)', health['reason'])
        # One of them answering is enough: the other is silent.
        health = ch.combine([('c-1', ch.view(ch.HEALTHY, me='10.0.0.1')),
                             ('c-2', late)], ['c-3'], {'10.0.0.3': 'c-3'})
        self.assertEqual(ch.DEGRADED, health['status'])
        self.assertEqual(['c-2', 'c-3'], health['out'])

    def test_down_when_the_members_held_out_do_not_say_otherwise(self):
        late = ch.view(ch.DOWN, ['10.0.0.1', '10.0.0.3'],
                       '2 of 3 nodes down: 10.0.0.1, 10.0.0.3', '10.0.0.2')
        # They do not answer.
        health = ch.combine([('c-2', late)], ['c-1', 'c-3'],
                            {'10.0.0.1': 'c-1', '10.0.0.3': 'c-3'})
        self.assertEqual(ch.DOWN, health['status'])
        # Every node cut off from the others sees the others down.
        views = [('c-%d' % i, ch.view(
            ch.DOWN, ['10.0.0.%d' % j for j in (1, 2, 3) if j != i],
            'nodes down', '10.0.0.%d' % i)) for i in (1, 2, 3)]
        self.assertEqual(ch.DOWN, ch.combine(views, [])['status'])
        # A member answering that a DOWN view does not hold out says
        # nothing of it (a shard without a primary, the other shard
        # healthy).
        health = ch.combine([
            ('rs1-1', ch.view(ch.DOWN, ['a:27017'],
                              'replica set rs1 has no primary', 'b:27017')),
            ('rs2-1', ch.view(ch.HEALTHY, me='c:27017'))], ['rs1-2'],
            {'a:27017': 'rs1-2'})
        self.assertEqual(ch.DOWN, health['status'])
