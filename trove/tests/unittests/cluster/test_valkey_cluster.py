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

from oslo_config.cfg import NoSuchOptError

from trove.common import cfg
from trove.common.strategies.cluster.experimental.redis import api
from trove.common.strategies.cluster.experimental.redis import guestagent
from trove.common.strategies.cluster.experimental.redis import taskmanager
from trove.common.strategies.cluster import strategy
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class ValkeyClusterConfigTest(trove_testtools.TestCase):
    """Valkey clusters run on the Redis strategies; KeyDB has none."""

    def test_valkey_clusters_are_enabled(self):
        self.assertTrue(CONF.valkey.cluster_support)
        self.assertEqual(CONF.redis.cluster_tcp_ports,
                         CONF.valkey.cluster_tcp_ports)

    def test_valkey_loads_the_redis_strategies(self):
        self.assertIsInstance(strategy.load_api_strategy('valkey'),
                              api.RedisAPIStrategy)
        self.assertIsInstance(strategy.load_taskmanager_strategy('valkey'),
                              taskmanager.RedisTaskManagerStrategy)
        self.assertIsInstance(strategy.load_guestagent_strategy('valkey'),
                              guestagent.RedisGuestAgentStrategy)

    def test_keydb_has_no_clusters(self):
        self.assertRaises(NoSuchOptError, CONF.keydb.get, 'cluster_support')
        self.assertIsNone(strategy.load_taskmanager_strategy('keydb'))
