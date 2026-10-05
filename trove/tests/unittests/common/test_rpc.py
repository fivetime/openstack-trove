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

from unittest import mock

import oslo_messaging as messaging

from trove import rpc
from trove.tests.unittests import trove_testtools


class DeleteServerQueuesTest(trove_testtools.TestCase):
    def setUp(self):
        super(DeleteServerQueuesTest, self).setUp()
        self.target = messaging.Target(topic='topic', server='server')
        patcher = mock.patch.object(rpc, 'TRANSPORT', mock.sentinel.transport)
        patcher.start()
        self.addCleanup(patcher.stop)

    @mock.patch.object(messaging, 'delete_rpc_server_queues', create=True)
    def test_delete_server_queues(self, delete):
        rpc.delete_server_queues(self.target, retry=3)

        delete.assert_called_once_with(
            mock.sentinel.transport, self.target, retry=3)

    def test_delete_server_queues_old_oslo_messaging(self):
        # Without delete_rpc_server_queues there is nothing to call.
        with mock.patch.object(rpc, 'messaging', mock.Mock(spec=[])):
            rpc.delete_server_queues(self.target, retry=3)
