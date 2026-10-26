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
        # A view that does not say it is as before.
        self.assertNotIn('writable', ch.view(ch.HEALTHY))
