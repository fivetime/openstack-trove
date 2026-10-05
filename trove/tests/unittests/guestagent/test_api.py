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
        # The topic _create_guest_queue and the guest agent listen on, and
        # the guest agent's own server queue.
        self.assertEqual('guestagent.instance-id', target.topic)
        self.assertEqual('instance-id', target.server)
        self.assertEqual(3, delete_server_queues.call_args.kwargs['retry'])
