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

from unittest import mock

from trove.common import cfg
from trove.common.strategies.cluster.experimental.vertica import communal
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class TestCommunal(trove_testtools.TestCase):

    def _conf(self, **values):
        for name, value in values.items():
            CONF.set_override(name, value, group='vertica')
            self.addCleanup(CONF.clear_override, name, group='vertica')

    def test_bucket_and_location(self):
        self.assertEqual('trove-vertica-c1', communal.bucket_name('c1'))
        self.assertEqual('s3://trove-vertica-c1/db',
                         communal.location('trove-vertica-c1'))

    def test_the_guests_reach_the_s3_service_at_its_address(self):
        storage = mock.Mock()
        storage.address.return_value = ('s3.example.net:443', True)
        self.assertEqual(('s3.example.net:443', True),
                         communal.guest_endpoint(storage, 'vertica'))

    def test_or_elsewhere(self):
        storage = mock.Mock()
        storage.address.return_value = ('s3.example.net:443', True)
        self._conf(eon_communal_endpoint='10.0.0.10:80',
                   eon_communal_https=False)
        self.assertEqual(('10.0.0.10:80', False),
                         communal.guest_endpoint(storage, 'vertica'))
