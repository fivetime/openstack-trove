# Copyright 2026 Simon Zhou
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import uuid

from unittest import mock

from trove.common import cfg
from trove.common import exception
from trove.common import message_partition
from trove.db.sqlalchemy import session
from trove.instance.models import DBInstance
from trove.instance.models import get_instance_message_partition
from trove.instance.tasks import InstanceTasks
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util

CONF = cfg.CONF

GUEST_CONF = """[DEFAULT]
debug = True
transport_url = rabbit://trove:pw@10.224.18.8:5672/trove
control_exchange = trove

[oslo_messaging_rabbit]
rabbit_quorum_queue = True
"""


class MessagePartitionTestBase(trove_testtools.TestCase):

    def override(self, name, value, group=None):
        CONF.set_override(name, value, group=group)
        self.addCleanup(CONF.clear_override, name, group=group)

    def configure(self, partitions, default_max=100, default_zones=(),
                  default_accepts=True):
        """Configure partitions through the real options.

        partitions: {name: {max_instances, availability_zones, accepts_new}}
        """
        self.override('guest_message_partitions', list(partitions))
        self.override('default_partition_max_instances', default_max)
        self.override('default_partition_availability_zones',
                      list(default_zones))
        self.override('default_partition_accepts_new', default_accepts)
        for name, opts in partitions.items():
            message_partition._register(name)
            group = message_partition.GROUP_PREFIX + name
            self.override('transport_url',
                          'rabbit://trove:pw@%s-0:5672/trove' % name, group)
            self.override('guest_transport_url',
                          'rabbit://trove:pw@lb-%s:5672/trove' % name, group)
            for key, value in opts.items():
                self.override(key, value, group)


class TestPartitionConfig(MessagePartitionTestBase):

    def test_disabled_by_default(self):
        self.assertEqual([], CONF.guest_message_partitions)
        self.assertFalse(message_partition.enabled())

    def test_choose_without_partitions_counts_nothing(self):
        with mock.patch.object(message_partition, 'usage') as usage:
            self.assertIsNone(message_partition.choose('nova'))
        usage.assert_not_called()

    def test_get_default(self):
        self.configure({'p2': {}}, default_max=7, default_zones=['az1'])
        p = message_partition.get(None)
        self.assertIsNone(p.name)
        self.assertIsNone(p.transport_url)
        self.assertEqual(7, p.max_instances)
        self.assertEqual(['az1'], p.availability_zones)

    def test_get_partition(self):
        self.configure({'p2': {'max_instances': 3,
                               'availability_zones': ['az2']}})
        p = message_partition.get('p2')
        self.assertEqual('rabbit://trove:pw@p2-0:5672/trove', p.transport_url)
        self.assertEqual('rabbit://trove:pw@lb-p2:5672/trove',
                         p.guest_transport_url)
        self.assertEqual(3, p.max_instances)
        self.assertEqual(['az2'], p.availability_zones)
        self.assertTrue(p.accepts_new)

    def test_no_maximum_by_default(self):
        self.override('guest_message_partitions', ['p5'])
        message_partition._register('p5')
        group = message_partition.GROUP_PREFIX + 'p5'
        self.override('transport_url', 'rabbit://u:p@p5:5672/trove', group)
        self.override('guest_transport_url', 'rabbit://u:p@lb5:5672/trove',
                      group)
        self.assertIsNone(CONF.default_partition_max_instances)
        self.assertIsNone(message_partition.get(None).max_instances)
        self.assertIsNone(message_partition.get('p5').max_instances)

    def test_get_unconfigured(self):
        self.configure({'p2': {}})
        self.assertRaises(exception.TroveError, message_partition.get, 'p3')

    def test_get_without_urls(self):
        self.override('guest_message_partitions', ['p4'])
        message_partition._register('p4')
        self.assertRaises(exception.TroveError, message_partition.get, 'p4')

    def test_invalid_name(self):
        self.override('guest_message_partitions', ['bad name'])
        self.assertRaises(exception.TroveError, message_partition.get,
                          'bad name')


class TestChoose(MessagePartitionTestBase):

    def choose(self, usage, zone=None):
        with mock.patch.object(message_partition, 'usage',
                               side_effect=lambda name: usage[name]):
            return message_partition.choose(zone)

    def test_least_used_share_wins(self):
        self.configure({'p2': {'max_instances': 100}}, default_max=10)
        # default 5/10 = 50 %, p2 40/100 = 40 %
        self.assertEqual('p2', self.choose({None: 5, 'p2': 40}))
        # default 3/10 = 30 %, p2 40/100 = 40 %
        self.assertIsNone(self.choose({None: 3, 'p2': 40}))

    def test_unlimited_partitions_by_count(self):
        self.configure({'p2': {}, 'p3': {}}, default_max=None)
        self.assertEqual('p3', self.choose({None: 7, 'p2': 5, 'p3': 4}))
        self.assertIsNone(self.choose({None: 3, 'p2': 5, 'p3': 4}))

    def test_mixed_limits_by_count_with_caps(self):
        # p2 is capped and full; the rest are compared by count.
        self.configure({'p2': {'max_instances': 3}, 'p3': {}},
                       default_max=None)
        self.assertEqual('p3', self.choose({None: 9, 'p2': 3, 'p3': 8}))
        # p2 not full and the emptiest: it wins on count, not on share.
        self.assertEqual('p2', self.choose({None: 9, 'p2': 2, 'p3': 8}))

    def test_report_names_unlimited(self):
        self.configure({'p2': {'max_instances': 0}}, default_max=None,
                       default_accepts=False)
        e = self.assertRaises(exception.GuestMessagePartitionsFull,
                              self.choose, {None: 4, 'p2': 0})
        self.assertIn('default 4/unlimited draining', str(e))
        self.assertIn('p2 0/0', str(e))

    def test_full_partition_skipped(self):
        self.configure({'p2': {'max_instances': 4}}, default_max=10)
        self.assertIsNone(self.choose({None: 9, 'p2': 4}))

    def test_all_full(self):
        self.configure({'p2': {'max_instances': 4}}, default_max=10)
        e = self.assertRaises(exception.GuestMessagePartitionsFull,
                              self.choose, {None: 10, 'p2': 4})
        self.assertIn('default 10/10', str(e))
        self.assertIn('p2 4/4', str(e))

    def test_zero_max_takes_nothing(self):
        self.configure({'p2': {}}, default_max=0)
        self.assertEqual('p2', self.choose({None: 0, 'p2': 0}))

    def test_draining_partition_skipped(self):
        self.configure({'p2': {'accepts_new': False}})
        self.assertIsNone(self.choose({None: 50, 'p2': 0}))

    def test_draining_default_skipped(self):
        self.configure({'p2': {}}, default_accepts=False)
        self.assertEqual('p2', self.choose({None: 0, 'p2': 2000}))

    def test_zone_named_partition_preferred(self):
        self.configure({'p2': {'availability_zones': ['az2']},
                        'p3': {}})
        self.assertEqual('p2', self.choose({None: 0, 'p2': 2000, 'p3': 0},
                                           zone='az2'))

    def test_zone_named_partitions_full(self):
        # A zone with partitions of its own does not spill elsewhere.
        self.configure({'p2': {'availability_zones': ['az2'],
                               'max_instances': 1}})
        self.assertRaises(exception.GuestMessagePartitionsFull,
                          self.choose, {None: 0, 'p2': 1}, 'az2')

    def test_zone_without_partition_uses_unzoned(self):
        self.configure({'p2': {'availability_zones': ['az2']},
                        'p3': {}})
        self.assertEqual('p3', self.choose({None: 1, 'p2': 0, 'p3': 0},
                                           zone='nova'))
        self.assertIsNone(self.choose({None: 0, 'p2': 0, 'p3': 1}))


class TestGuestConfig(MessagePartitionTestBase):

    def setUp(self):
        super(TestGuestConfig, self).setUp()
        self.configure({'p2': {}})

    def test_default_partition_unchanged(self):
        self.assertEqual(GUEST_CONF,
                         message_partition.guest_config(GUEST_CONF, None))

    def test_transport_url_replaced(self):
        out = message_partition.guest_config(GUEST_CONF, 'p2')
        self.assertIn('transport_url = rabbit://trove:pw@lb-p2:5672/trove',
                      out)
        self.assertNotIn('10.224.18.8', out)
        self.assertEqual(1, out.count('transport_url'))
        self.assertIn('rabbit_quorum_queue = True', out)

    def test_transport_url_added_to_default(self):
        conf = GUEST_CONF.replace(
            'transport_url = rabbit://trove:pw@10.224.18.8:5672/trove\n', '')
        out = message_partition.guest_config(conf, 'p2')
        lines = out.splitlines()
        self.assertEqual('[DEFAULT]', lines[0])
        self.assertEqual(
            'transport_url = rabbit://trove:pw@lb-p2:5672/trove', lines[1])

    def test_other_section_not_touched(self):
        conf = ("[oslo_messaging_notifications]\n"
                "transport_url = kafka://redpanda:9092\n")
        out = message_partition.guest_config(conf, 'p2')
        self.assertIn('transport_url = kafka://redpanda:9092', out)
        self.assertTrue(out.startswith(
            '[DEFAULT]\ntransport_url = rabbit://trove:pw@lb-p2:5672/trove'))


class TestUsage(MessagePartitionTestBase):

    @classmethod
    def setUpClass(cls):
        super(TestUsage, cls).setUpClass()
        # An earlier test class may have emptied the tables of the test
        # database (util.cleanup_db, alembic_version included) with its
        # engine still configured: syncing it again would replay every
        # migration onto the existing tables. Only set it up when nothing
        # has yet.
        if session._FACADE is None:
            util.init_db()

    def make(self, partition, deleted=False):
        db_info = DBInstance.create(
            name='mp-%s' % uuid.uuid4().hex[:8], flavor_id='1',
            tenant_id='t', volume_size=1,
            datastore_version_id=str(uuid.uuid4()),
            task_status=InstanceTasks.BUILDING,
            message_partition=partition)
        if deleted:
            db_info.deleted = True
            db_info.save()
        self.addCleanup(db_info.delete)
        return db_info

    def test_usage_counts_live_instances_of_a_partition(self):
        name = 'u%s' % uuid.uuid4().hex[:8]
        self.make(name)
        self.make(name)
        self.make(name, deleted=True)
        self.make('other-%s' % name)
        self.assertEqual(2, message_partition.usage(name))

    def test_default_partition_is_null(self):
        before = message_partition.usage(None)
        self.make(None)
        self.assertEqual(before + 1, message_partition.usage(None))

    def test_instance_partition_lookup(self):
        db_info = self.make('p9')
        self.assertEqual('p9', get_instance_message_partition(db_info.id))
        other = self.make(None)
        self.assertIsNone(get_instance_message_partition(other.id))
