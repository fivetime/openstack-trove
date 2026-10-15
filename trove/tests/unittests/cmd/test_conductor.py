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

from trove.cmd import conductor
from trove.tests.unittests import trove_testtools


class ConductorMainTest(trove_testtools.TestCase):
    """trove-conductor listens on every guest message partition."""

    def run_main(self, partitions):
        conf = mock.Mock(conductor_queue='trove-conductor',
                         conductor_manager='trove.conductor.manager.Manager',
                         trove_conductor_workers=2)
        with mock.patch('trove.cmd.common.initialize',
                        return_value=conf), \
                mock.patch('trove.common.rpc.service.RpcService') as service, \
                mock.patch('oslo_service.service.launch') as launch, \
                mock.patch('trove.common.message_partition.names',
                           return_value=partitions), \
                mock.patch('trove.rpc.get_transport',
                           side_effect=lambda name: 'transport-%s' % name):
            conductor.main()
        return service, launch

    def test_default_partition_only(self):
        service, launch = self.run_main([])
        self.assertEqual(1, service.call_count)
        self.assertIsNone(service.call_args.kwargs['transport'])
        launch.return_value.launch_service.assert_not_called()
        launch.return_value.wait.assert_called_once_with()

    def test_a_server_per_partition(self):
        service, launch = self.run_main(['p2', 'p3'])
        transports = [c.kwargs['transport'] for c in service.call_args_list]
        self.assertEqual([None, 'transport-p2', 'transport-p3'], transports)
        for call in service.call_args_list:
            self.assertEqual('trove-conductor', call.kwargs['topic'])
        launched = launch.return_value.launch_service.call_args_list
        self.assertEqual(2, len(launched))
        for call in launched:
            self.assertEqual(2, call.kwargs['workers'])
        self.assertEqual(2, launch.call_args.kwargs['workers'])
