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
