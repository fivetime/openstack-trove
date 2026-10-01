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

from unittest.mock import MagicMock
from unittest.mock import patch

from trove.common import exception
from trove.common import wsgi
from trove.extensions.redis import service
from trove.tests.unittests import trove_testtools


class RedisClusterRootTest(trove_testtools.TestCase):
    """Every member of a Redis Cluster has its own users."""

    def setUp(self):
        super(RedisClusterRootTest, self).setUp()
        self.controller = service.RedisRootController()
        self.req = MagicMock(environ={wsgi.CONTEXT_KEY: 'ctx'})
        patcher = patch.object(service.DBInstance, 'find_all')
        self.find_all = patcher.start()
        self.addCleanup(patcher.stop)
        self.find_all.return_value.all.return_value = [
            MagicMock(id=i) for i in ('m1', 'm2', 'm3')]

    @patch.object(service.RedisRoot, 'create')
    def test_enable_gives_every_member_one_password(self, create):
        create.return_value = MagicMock(password='p')
        self.controller.root_create(self.req, None, 'tenant', 'cluster',
                                    is_cluster=True)
        self.assertEqual(['m1', 'm2', 'm3'],
                         [c[0][1] for c in create.call_args_list])
        self.assertEqual(1, len({c[0][2] for c in create.call_args_list}))
        self.find_all.assert_called_once_with(
            tenant_id='tenant', cluster_id='cluster', deleted=False)

    @patch.object(service.models.Root, 'delete')
    def test_disable_on_every_member(self, delete):
        self.controller.root_delete(self.req, 'tenant', 'cluster',
                                    is_cluster=True)
        self.assertEqual(['m1', 'm2', 'm3'],
                         [c[0][1] for c in delete.call_args_list])

    @patch.object(service.models.Root, 'load',
                  side_effect=[False, True, False])
    def test_index_of_a_cluster(self, load):
        result = self.controller.root_index(self.req, 'tenant', 'cluster',
                                            is_cluster=True)
        self.assertTrue(result.data(None)['rootEnabled'])

    @patch.object(service.DBInstance, 'find_by',
                  return_value=MagicMock(cluster_id='cluster'))
    def test_a_member_alone_is_refused(self, find_by):
        self.assertRaises(exception.ClusterInstanceOperationNotSupported,
                          self.controller.root_create, self.req, None,
                          'tenant', 'm1', is_cluster=False)
