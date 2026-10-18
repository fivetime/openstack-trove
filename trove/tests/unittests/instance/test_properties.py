# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from trove.common import exception
from trove.instance import properties
from trove.tests.unittests import trove_testtools


class InstancePropertiesTest(trove_testtools.TestCase):

    def test_a_vertica_instance_keeps_its_defaults(self):
        self.assertEqual({'sync_writes': False},
                         properties.instance_properties('vertica', None))
        self.assertEqual({'sync_writes': False},
                         properties.instance_properties('vertica', {}))

    def test_a_vertica_instance_takes_sync_writes(self):
        # As the client sends it: a string.
        self.assertEqual(
            {'sync_writes': True},
            properties.instance_properties('vertica',
                                           {'sync_writes': 'true'}))
        self.assertEqual(
            {'sync_writes': False},
            properties.instance_properties('vertica',
                                           {'sync_writes': False}))

    def test_sync_writes_is_true_or_false(self):
        self.assertRaisesRegex(
            exception.BadRequest, 'sync_writes must be true or false',
            properties.instance_properties, 'vertica',
            {'sync_writes': 'sometimes'})

    def test_an_unknown_property_is_refused(self):
        # A misspelled one would leave the writes unsynced unseen.
        self.assertRaisesRegex(
            exception.BadRequest, 'sync_write',
            properties.instance_properties, 'vertica',
            {'sync_write': 'true'})

    def test_other_datastores_take_none(self):
        self.assertEqual({}, properties.instance_properties('mysql', None))
        self.assertRaisesRegex(
            exception.BadRequest, 'mysql take no extended properties',
            properties.instance_properties, 'mysql', {'sync_writes': 'true'})

    def test_the_guest_is_told_of_sync_writes_only(self):
        self.assertEqual({'sync_writes': True},
                         properties.guest_properties({'sync_writes': True}))
        self.assertEqual({},
                         properties.guest_properties({'sync_writes': False}))
        self.assertEqual({}, properties.guest_properties({}))
