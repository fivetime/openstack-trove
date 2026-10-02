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

from trove.common import cfg
from trove.common.strategies.cluster.experimental.redis import api
from trove.common.strategies.cluster.experimental.redis import guestagent
from trove.common.strategies.cluster.experimental.redis import taskmanager
from trove.common.strategies.cluster import strategy
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class RedisFamilyClusterConfigTest(trove_testtools.TestCase):
    """Valkey and KeyDB clusters run on the Redis strategies."""

    def test_clusters_are_enabled(self):
        for manager in ('valkey', 'keydb'):
            self.assertTrue(CONF.get(manager).cluster_support)
            self.assertEqual(CONF.redis.cluster_tcp_ports,
                             CONF.get(manager).cluster_tcp_ports)

    def test_the_redis_strategies_are_loaded(self):
        for manager in ('valkey', 'keydb'):
            self.assertIsInstance(strategy.load_api_strategy(manager),
                                  api.RedisAPIStrategy)
            self.assertIsInstance(
                strategy.load_taskmanager_strategy(manager),
                taskmanager.RedisTaskManagerStrategy)
            self.assertIsInstance(
                strategy.load_guestagent_strategy(manager),
                guestagent.RedisGuestAgentStrategy)
