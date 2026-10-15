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


class PartitionTransportTest(trove_testtools.TestCase):
    def setUp(self):
        super(PartitionTransportTest, self).setUp()
        patcher = mock.patch.object(rpc, 'TRANSPORT', mock.sentinel.default)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.dict(rpc.PARTITION_TRANSPORTS, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_default_partition(self):
        self.assertIs(mock.sentinel.default, rpc.get_transport(None))
        self.assertIs(mock.sentinel.default, rpc.get_transport())

    @mock.patch('trove.common.message_partition.get')
    @mock.patch.object(messaging, 'get_rpc_transport')
    def test_partition_transport_made_once(self, get_rpc_transport, get):
        get.return_value.transport_url = 'rabbit://u:p@p2:5672/trove'

        first = rpc.get_transport('p2')
        second = rpc.get_transport('p2')

        self.assertIs(get_rpc_transport.return_value, first)
        self.assertIs(first, second)
        get.assert_called_once_with('p2')
        get_rpc_transport.assert_called_once_with(
            rpc.CONF, url='rabbit://u:p@p2:5672/trove',
            allowed_remote_exmods=rpc.get_allowed_exmods())

    @mock.patch.object(messaging, 'get_rpc_client')
    def test_client_on_a_given_transport(self, get_rpc_client):
        target = messaging.Target(topic='t', server='s')
        rpc.get_client(target, key=None, transport=mock.sentinel.p2)
        self.assertIs(mock.sentinel.p2, get_rpc_client.call_args.args[0])
        rpc.get_client(target, key=None)
        self.assertIs(mock.sentinel.default,
                      get_rpc_client.call_args.args[0])

    @mock.patch.object(messaging, 'get_rpc_server')
    def test_server_on_a_given_transport(self, get_rpc_server):
        target = messaging.Target(topic='t', server='s')
        rpc.get_server(target, [], key=None, transport=mock.sentinel.p2)
        self.assertIs(mock.sentinel.p2, get_rpc_server.call_args.args[0])

    @mock.patch.object(messaging, 'delete_rpc_server_queues', create=True)
    def test_delete_server_queues_on_a_given_transport(self, delete):
        target = messaging.Target(topic='t', server='s')
        rpc.delete_server_queues(target, retry=3, transport=mock.sentinel.p2)
        delete.assert_called_once_with(mock.sentinel.p2, target, retry=3)

    def test_cleanup_closes_partition_transports(self):
        p2 = mock.Mock()
        rpc.PARTITION_TRANSPORTS['p2'] = p2
        with mock.patch.object(rpc, 'TRANSPORT', mock.Mock()), \
                mock.patch.object(rpc, 'NOTIFICATION_TRANSPORT',
                                  mock.Mock()), \
                mock.patch.object(rpc, 'NOTIFIER', mock.Mock()):
            rpc.cleanup()
        p2.cleanup.assert_called_once_with()
        self.assertEqual({}, rpc.PARTITION_TRANSPORTS)
