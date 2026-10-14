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

"""The keep-alive heartbeats written in batches, on the real tables."""

import datetime
from unittest import mock

from oslo_utils import timeutils

from trove.common import exception
from trove.common import utils
from trove.conductor import heartbeats
from trove.conductor import manager as conductor_manager
from trove.conductor.models import LastSeen
from trove.db.sqlalchemy import mappers
from trove.instance import models as t_models
from trove.instance.service_status import ServiceStatuses
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util


class KeepalivesTest(trove_testtools.TestCase):

    def setUp(self):
        super(KeepalivesTest, self).setUp()
        # Not set up when the module runs alone; set up (and maybe
        # cleaned, which keeps the mapping) when the whole suite runs.
        if not mappers.mapping_exists(t_models.InstanceServiceStatus):
            util.init_db()
        self.patch_conf_property('conductor_heartbeat_flush_interval', 5)
        mock.patch.object(heartbeats.Keepalives, '_ensure_ticker').start()
        self.addCleanup(mock.patch.stopall)
        self.manager = conductor_manager.Manager()
        self.keepalives = self.manager.keepalives
        self.now = timeutils.utcnow_ts(microsecond=True)
        self.rows = []

    def _instance(self, status=ServiceStatuses.HEALTHY, age=600):
        instance_id = utils.generate_uuid()
        row = t_models.InstanceServiceStatus(status, id=utils.generate_uuid(),
                                             instance_id=instance_id)
        row.save()
        self.rows.append(row)
        self.addCleanup(self._drop, row)
        self._age(instance_id, age)
        return instance_id

    def _drop(self, row):
        try:
            row.delete()
        except Exception:
            # Deleted by the test.
            pass

    def _age(self, instance_id, age):
        with t_models.InstanceServiceStatus.query() as query:
            query.filter_by(instance_id=instance_id).update(
                {'updated_at': timeutils.utcnow() -
                 datetime.timedelta(seconds=age)})

    def _row(self, instance_id):
        return t_models.InstanceServiceStatus.find_by(instance_id=instance_id)

    def _heartbeat(self, instance_id, status=ServiceStatuses.HEALTHY,
                   sent=None):
        self.manager.heartbeat(None, instance_id,
                               {'service_status': status.description},
                               sent=sent or self.now)

    def _age_of(self, instance_id):
        return (timeutils.utcnow() - self._row(instance_id).updated_at
                ).total_seconds()

    def test_a_repeated_status_waits_for_the_flush(self):
        i1 = self._instance()
        # The heartbeats of the status the row holds are batched:
        # nothing written yet, the first one included.
        self._heartbeat(i1, sent=self.now)
        self._heartbeat(i1, sent=self.now + 30)
        self._heartbeat(i1, sent=self.now + 60)
        self.assertGreater(self._age_of(i1), 500)
        self.assertEqual({i1: (ServiceStatuses.HEALTHY.code, self.now + 60)},
                         self.keepalives.pending)
        self.assertEqual({i1: (ServiceStatuses.HEALTHY.code, self.now + 60)},
                         self.keepalives.known)
        # The flush writes their time; one statement per status.
        i2 = self._instance(ServiceStatuses.RUNNING)
        self._heartbeat(i2, ServiceStatuses.RUNNING, sent=self.now)
        self.assertEqual(2, self.keepalives.flush())
        self.assertLess(self._age_of(i1), 5)
        self.assertLess(self._age_of(i2), 5)
        self.assertEqual({}, self.keepalives.pending)
        self.assertEqual(0, self.keepalives.flush())

    def test_a_row_the_control_plane_changed_is_written_in_full(self):
        # The task manager set SHUTDOWN (a resize); the guest's
        # heartbeat wins as it always did, a flush later.
        i1 = self._instance(ServiceStatuses.SHUTDOWN)
        self._heartbeat(i1, sent=self.now)
        self._heartbeat(i1, sent=self.now + 30)
        self.assertEqual(ServiceStatuses.SHUTDOWN, self._row(i1).status)
        self.assertEqual(0, self.keepalives.flush())
        self.assertEqual(ServiceStatuses.HEALTHY, self._row(i1).status)
        self.assertLess(self._age_of(i1), 5)
        self.assertEqual({i1: (ServiceStatuses.HEALTHY.code, self.now + 30)},
                         self.keepalives.known)

    def test_restart_required_is_kept(self):
        i1 = self._instance(ServiceStatuses.RESTART_REQUIRED, age=600)
        self._heartbeat(i1, sent=self.now)
        self._heartbeat(i1, sent=self.now + 30)
        self.assertEqual(0, self.keepalives.flush())
        self.assertEqual(ServiceStatuses.RESTART_REQUIRED,
                         self._row(i1).status)
        self.assertGreater(self._age_of(i1), 500)
        # Not written: forgotten, so that the next one is tried again.
        self.assertEqual({}, self.keepalives.known)

    def test_a_status_change_is_written_at_once(self):
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now)
        self._heartbeat(i1, sent=self.now + 30)
        self._heartbeat(i1, ServiceStatuses.SHUTDOWN, sent=self.now + 60)
        self.assertEqual(ServiceStatuses.SHUTDOWN, self._row(i1).status)
        self.assertEqual({}, self.keepalives.pending)
        self.assertEqual(self.now + 60,
                         LastSeen.load(i1, 'heartbeat').sent)

    def test_a_stale_heartbeat_after_a_batched_one_is_discarded(self):
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now + 60)
        # Sent before the batched one, delivered after it: the
        # database's last seen does not know the batched one.
        self._heartbeat(i1, ServiceStatuses.SHUTDOWN, sent=self.now + 30)
        self.assertEqual(ServiceStatuses.HEALTHY, self._row(i1).status)
        self._heartbeat(i1, sent=self.now + 30)
        self.assertEqual({i1: (ServiceStatuses.HEALTHY.code, self.now + 60)},
                         self.keepalives.pending)

    def test_without_an_interval_every_heartbeat_is_written(self):
        self.patch_conf_property('conductor_heartbeat_flush_interval', 0)
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now)
        self.assertLess(self._age_of(i1), 5)
        self.assertEqual({}, self.keepalives.pending)

    def test_a_heartbeat_without_a_time_or_a_status_is_written(self):
        i1 = self._instance()
        self.manager.heartbeat(None, i1, {}, sent=self.now + 30)
        self.assertLess(self._age_of(i1), 5)
        self._age(i1, 600)
        self.manager.heartbeat(
            None, i1, {'service_status': ServiceStatuses.HEALTHY.description})
        self.assertLess(self._age_of(i1), 5)
        self.assertEqual({}, self.keepalives.pending)

    def test_a_deleted_instance_leaves_the_batch(self):
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now + 30)
        self.rows[-1].delete()
        self.assertEqual(0, self.keepalives.flush())
        self.assertEqual({}, self.keepalives.known)
        # Its heartbeats are batched and dropped at each flush until
        # they stop (the guest goes with the instance).
        self._heartbeat(i1, sent=self.now + 60)
        self.assertEqual(0, self.keepalives.flush())
        self.assertEqual({}, self.keepalives.known)
        # The full path still says so.
        self.assertRaises(exception.ModelNotFoundError,
                          self.manager.heartbeat, None, i1, {})

    def test_a_full_batch_is_written_before_the_timer(self):
        with mock.patch.object(heartbeats, 'MAX_PENDING', 2):
            i1, i2 = self._instance(), self._instance()
            self._heartbeat(i1, sent=self.now + 30)
            self.assertGreater(self._age_of(i1), 500)
            self._heartbeat(i2, sent=self.now + 30)
        self.assertLess(self._age_of(i1), 5)
        self.assertLess(self._age_of(i2), 5)
        self.assertEqual({}, self.keepalives.pending)

    def test_a_failed_batch_falls_back_to_the_full_path(self):
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now + 30)
        with mock.patch.object(self.keepalives, 'flush',
                               side_effect=Exception('gone away')):
            self.keepalives._flush_safely()
        self.assertEqual({}, self.keepalives.known)
        self.assertEqual({}, self.keepalives.pending)
        # The next heartbeat is batched again (the batch may work now).
        self._heartbeat(i1, sent=self.now + 60)
        self.assertEqual(1, self.keepalives.flush())
        self.assertLess(self._age_of(i1), 5)

    def test_long_unheard_instances_are_forgotten(self):
        i1 = self._instance()
        self._heartbeat(i1, sent=self.now - 20 * 90)
        self.keepalives.flush()
        self.assertEqual(1, len(self.keepalives.known))
        self.keepalives._flushes = 99
        self.keepalives.flush()
        self.assertEqual({}, self.keepalives.known)
