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

import os
from unittest import mock

import trove
from trove.cmd import guest
from trove.common import constants
from trove.guestagent.common import guestagent_utils
from trove.tests.unittests import trove_testtools


class Stop(Exception):
    pass


class TestUserPortSettled(trove_testtools.TestCase):
    """The image loader is started once the agent says the user-defined
    port is as it stays (trove-image-loader.path): pulled before the port
    is taken down, dockerd keeps connections that die with it and the
    agent's own pull hangs on them (Incus guests, 2026-10-23).
    """

    def _main(self, network_isolation, disable=None):
        self.patch_conf_property('guest_id', 'guest-1')
        self.patch_conf_property('datastore_manager', 'mysql')
        self.patch_conf_property('network_isolation', network_isolation)
        calls = mock.Mock()
        calls.disable.side_effect = disable
        with mock.patch.object(guest.cfg, 'parse_args'), \
                mock.patch.object(guest.logging, 'setup'), \
                mock.patch.object(guest.debug_utils, 'setup'), \
                mock.patch.object(guestagent_utils,
                                  'disable_user_defined_port',
                                  calls.disable), \
                mock.patch.object(guestagent_utils, 'mark_user_port_settled',
                                  calls.mark), \
                mock.patch.object(guest.operating_system, 'create_user',
                                  side_effect=Stop):
            # Stopped where the agent goes on to its own work.
            self.assertRaises(Stop, guest.main)
        return [c[0] for c in calls.mock_calls]

    def test_marked_after_the_port_is_taken_down(self):
        self.assertEqual(['disable', 'mark'], self._main(True))

    def test_marked_when_the_port_stays_up(self):
        self.assertEqual(['mark'], self._main(False))

    def test_marked_when_taking_the_port_down_failed(self):
        # The agent goes on and pulls itself; the loader may as well.
        self.assertEqual(['disable', 'mark'],
                         self._main(True, disable=OSError('no eth1.json')))

    def test_a_failed_mark_does_not_stop_the_agent(self):
        self.patch_conf_property('guest_id', 'guest-1')
        self.patch_conf_property('datastore_manager', 'mysql')
        self.patch_conf_property('network_isolation', False)
        with mock.patch.object(guest.cfg, 'parse_args'), \
                mock.patch.object(guest.logging, 'setup'), \
                mock.patch.object(guest.debug_utils, 'setup'), \
                mock.patch.object(guestagent_utils, 'mark_user_port_settled',
                                  side_effect=OSError('read-only /run')), \
                mock.patch.object(guest.operating_system, 'create_user',
                                  side_effect=Stop):
            self.assertRaises(Stop, guest.main)

    def test_the_mark_is_the_file_the_path_unit_waits_for(self):
        with mock.patch.object(guestagent_utils.operating_system,
                               'ensure_directory') as ensure, \
                mock.patch.object(guestagent_utils.operating_system,
                                  'write_file') as write:
            guestagent_utils.mark_user_port_settled()
        ensure.assert_called_once_with('/run/trove', as_root=True)
        write.assert_called_once_with(constants.USER_PORT_SETTLED_PATH, '',
                                      as_root=True)
        unit = os.path.join(
            os.path.dirname(os.path.dirname(trove.__file__)),
            'integration/scripts/files/elements/guest-agent/install.d/'
            'guest-agent-source-install/trove-image-loader')
        with open(unit + '.path') as f:
            self.assertIn('PathExists=%s\n' % constants.USER_PORT_SETTLED_PATH,
                          f.read())
        with open(unit + '.service') as f:
            service = f.read()
        self.assertIn('ConditionPathExists=%s\n'
                      % constants.USER_PORT_SETTLED_PATH, service)
        # Once a boot, and a failed pull is no failure: else the path unit
        # starts it again and again until its start limit (tried,
        # 2026-10-27: five runs, then the path unit failed).
        self.assertIn('RemainAfterExit=yes\n', service)
        self.assertIn('ExecStart=-/usr/local/sbin/trove-image-loader\n',
                      service)
