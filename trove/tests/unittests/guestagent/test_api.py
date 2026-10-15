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

from trove.guestagent import api
from trove.tests.unittests import trove_testtools


class GuestAPIDeleteQueueTest(trove_testtools.TestCase):
    def setUp(self):
        super(GuestAPIDeleteQueueTest, self).setUp()
        # An instance on the default guest message partition.
        patcher = mock.patch(
            'trove.instance.models.get_instance_message_partition',
            return_value=None)
        patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(api.rpc, 'TRANSPORT',
                                    mock.sentinel.transport)
        patcher.start()
        self.addCleanup(patcher.stop)

    @mock.patch.object(api.API, 'get_client')
    @mock.patch.object(api.rpc, 'delete_server_queues')
    def test_delete_queue(self, delete_server_queues, get_client):
        guest = api.API(mock.Mock(), 'instance-id')

        guest.delete_queue()

        target = delete_server_queues.call_args.args[0]
        # The topic of a guest of an older image: its queue is the server
        # queue of the shared topic too, so this removes what either kind
        # of guest left, and leaves the shared topic's queue alone.
        self.assertEqual('guestagent.instance-id', target.topic)
        self.assertEqual('instance-id', target.server)
        self.assertEqual(3, delete_server_queues.call_args.kwargs['retry'])

    @mock.patch.object(api.API, 'get_client')
    def test_the_guest_is_a_server_of_the_shared_topic(self, get_client):
        guest = api.API(mock.Mock(), 'instance-id')
        self.assertEqual('guestagent', guest.target.topic)
        self.assertEqual('instance-id', guest.target.server)
        get_client.assert_called_once_with(guest.target, mock.ANY)


class GuestAPIMessagePartitionTest(trove_testtools.TestCase):
    """A guest on a guest message partition is reached through its broker."""

    def setUp(self):
        super(GuestAPIMessagePartitionTest, self).setUp()
        for target, kwargs in [
                ('trove.common.message_partition.enabled',
                 {'return_value': True}),
                ('trove.instance.models.get_instance_message_partition',
                 {'return_value': 'p2'}),
                ('trove.instance.models.get_instance_encryption_key',
                 {'return_value': 'key'})]:
            patcher = mock.patch(target, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = mock.patch.object(api.rpc, 'get_transport',
                                    return_value=mock.sentinel.p2)
        self.get_transport = patcher.start()
        self.addCleanup(patcher.stop)

    @mock.patch.object(api.rpc, 'get_client')
    def test_client_uses_partition_transport(self, get_client):
        guest = api.API(mock.Mock(), 'instance-id')

        self.get_transport.assert_called_once_with('p2')
        self.assertIs(mock.sentinel.p2,
                      get_client.call_args.kwargs['transport'])
        self.assertEqual('key', get_client.call_args.kwargs['key'])
        self.assertIs(get_client.return_value, guest.client)

    @mock.patch.object(api.rpc, 'get_client')
    @mock.patch.object(api.rpc, 'delete_server_queues')
    def test_delete_queue_on_partition(self, delete_server_queues,
                                       get_client):
        api.API(mock.Mock(), 'instance-id').delete_queue()

        self.assertIs(mock.sentinel.p2,
                      delete_server_queues.call_args.kwargs['transport'])
        # The transport is looked up once per guest client.
        self.get_transport.assert_called_once_with('p2')

    @mock.patch('trove.instance.models.DBInstance.get_by')
    @mock.patch.object(api.rpc, 'get_client')
    @mock.patch.object(api.rpc, 'get_server')
    def test_guest_queue_declared_on_partition(self, get_server, get_client,
                                               get_by):
        get_by.return_value.key = 'key'
        api.API(mock.Mock(), 'instance-id')._create_guest_queue()

        self.assertIs(mock.sentinel.p2,
                      get_server.call_args.kwargs['transport'])
        target = get_server.call_args.args[0]
        self.assertEqual('guestagent', target.topic)
        self.assertEqual('instance-id', target.server)
        get_server.return_value.start.assert_called_once_with()
        get_server.return_value.stop.assert_called_once_with()


class GuestAPIWithoutPartitionsTest(trove_testtools.TestCase):
    """Without partitions configured nothing is looked up."""

    @mock.patch('trove.instance.models.get_instance_message_partition')
    @mock.patch.object(api.rpc, 'get_transport')
    @mock.patch('trove.instance.models.get_instance_encryption_key',
                return_value='key')
    @mock.patch.object(api.rpc, 'get_client')
    def test_default_transport(self, get_client, key, get_transport,
                               get_partition):
        api.API(mock.Mock(), 'instance-id')
        self.assertIsNone(get_client.call_args.kwargs['transport'])
        get_transport.assert_not_called()
        get_partition.assert_not_called()

    @mock.patch('trove.common.message_partition.enabled', return_value=True)
    @mock.patch('trove.instance.models.get_instance_message_partition',
                return_value=None)
    @mock.patch.object(api.rpc, 'get_transport')
    @mock.patch('trove.instance.models.get_instance_encryption_key',
                return_value='key')
    @mock.patch.object(api.rpc, 'get_client')
    def test_default_partition(self, get_client, key, get_transport,
                               get_partition, enabled):
        api.API(mock.Mock(), 'instance-id')
        self.assertIsNone(get_client.call_args.kwargs['transport'])
        get_transport.assert_not_called()
        get_partition.assert_called_once_with('instance-id')
