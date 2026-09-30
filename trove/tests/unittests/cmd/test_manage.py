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

from unittest import mock

from novaclient import exceptions as nova_exceptions

from trove.cmd import manage
from trove.tests.unittests import trove_testtools


def _instance(compute_id):
    return mock.Mock(id='instance-of-%s' % compute_id,
                     compute_instance_id=compute_id)


class TestNovaTagsUpgradeTask(trove_testtools.TestCase):

    def setUp(self):
        super(TestNovaTagsUpgradeTask, self).setUp()
        with mock.patch.object(manage, 'get_db_api'):
            self.commands = manage.Commands()
        self.nova = mock.Mock()
        patcher = mock.patch.object(manage.Commands, '_get_nova_client',
                                    return_value=self.nova)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _find_all(self, instances):
        return mock.patch.object(
            manage.DBInstance, 'find_all',
            return_value=mock.Mock(all=mock.Mock(return_value=instances)))

    def test_tags_present(self):
        self.nova.servers.get.return_value = mock.Mock(
            tags=['trove_instance'])
        with self._find_all([_instance('s1')]):
            self.assertTrue(self.commands._check_nova_tags_exists())

    def test_tags_missing(self):
        self.nova.servers.get.return_value = mock.Mock(tags=[])
        with self._find_all([_instance('s1')]):
            self.assertFalse(self.commands._check_nova_tags_exists())

    def test_instance_without_a_server_is_not_the_sample(self):
        # A delete that did not finish leaves the instance without its
        # server; db_sync used to stop at the 404.
        self.nova.servers.get.side_effect = [
            nova_exceptions.NotFound(404),
            mock.Mock(tags=['trove_instance'])]
        with self._find_all([_instance(None), _instance('gone'),
                             _instance('s2')]):
            self.assertTrue(self.commands._check_nova_tags_exists())
        self.assertEqual([mock.call('gone'), mock.call('s2')],
                         self.nova.servers.get.call_args_list)

    def test_no_server_left(self):
        self.nova.servers.get.side_effect = nova_exceptions.NotFound(404)
        with self._find_all([_instance('gone')]):
            self.assertFalse(self.commands._check_nova_tags_exists())

    def test_setting_tags_survives_a_missing_server(self):
        self.nova.api_version = manage.api_versions.APIVersion('2.96')
        self.nova.servers.get.side_effect = nova_exceptions.NotFound(404)
        with self._find_all([_instance('gone')]):
            # The error report used to reference the server that was
            # never loaded.
            self.commands.set_nova_tags(force=True)
        self.nova.servers.set_tags.assert_not_called()
