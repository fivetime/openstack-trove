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

"""The user-facing port's interface at the guest's start: taken down for
the docker-hostnic plugin (the default), or kept in the guest without
addresses for an ipvlan sub-interface (user_port_driver=ipvlan)."""

import json
from unittest import mock

from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.tests.unittests import trove_testtools

ETH1 = {"mac_address": "fa:16:3e:7c:9c:57", "ipv4_address": "10.111.0.8",
        "ipv4_cidr": "10.111.0.0/26", "ipv4_gateway": "10.111.0.1"}


class UserPortTest(trove_testtools.TestCase):

    def setUp(self):
        super().setUp()
        self.config_patch = mock.patch.object(
            guestagent_utils, 'user_port_config', return_value=dict(ETH1))
        self.config = self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        for name, attr in (('shell', 'execute_shell_cmd'),
                           ('write', 'write_file')):
            patcher = mock.patch.object(operating_system, attr)
            setattr(self, name, patcher.start())
            self.addCleanup(patcher.stop)

    def _link(self, name):
        link = mock.MagicMock()
        link.get_attr.return_value = name
        return link

    def commands(self):
        return [c.args[0] for c in self.shell.call_args_list]

    def test_ifname_by_mac(self):
        with mock.patch.object(guestagent_utils, 'IPRoute') as ipr:
            ipr.return_value.get_links.return_value = [self._link('nic01')]
            self.assertEqual('nic01', guestagent_utils.user_port_ifname())
            ipr.return_value.get_links.assert_called_once_with(
                address=ETH1["mac_address"])
            ipr.return_value.get_links.return_value = []
            self.assertIsNone(guestagent_utils.user_port_ifname())

    def test_ifname_without_the_config(self):
        self.config.return_value = None
        with mock.patch.object(guestagent_utils, 'IPRoute') as ipr:
            self.assertIsNone(guestagent_utils.user_port_ifname())
            ipr.assert_not_called()

    def test_hostnic_takes_the_interface_down(self):
        with mock.patch.object(guestagent_utils, 'user_port_ifname',
                               return_value='nic01'):
            guestagent_utils.prepare_user_defined_port()
        self.assertEqual(["ip link set nic01 down"], self.commands())
        self.write.assert_not_called()

    def test_ipvlan_keeps_the_interface_bare_and_up(self):
        self.patch_conf_property('user_port_driver', 'ipvlan')
        with mock.patch.object(guestagent_utils, 'user_port_ifname',
                               return_value='nic01'), \
                mock.patch.object(guestagent_utils.os.path, 'isdir',
                                  return_value=True):
            guestagent_utils.prepare_user_defined_port()
        self.write.assert_called_once_with(
            guestagent_utils.NETWORKD_USER_PORT_FILE,
            "[Match]\nMACAddress=fa:16:3e:7c:9c:57\n\n[Link]\nUnmanaged=yes\n",
            as_root=True)
        self.assertEqual(
            ["networkctl reload",
             "sysctl -w net.ipv6.conf.nic01.accept_ra=0",
             "ip -4 addr flush dev nic01",
             "ip -6 addr flush dev nic01 scope global",
             "ip -4 route flush dev nic01",
             "ip -6 route flush dev nic01",
             "ip link set nic01 up"], self.commands())
        for c in self.shell.call_args_list:
            self.assertEqual(dict(shell=True, as_root=True), c.kwargs)

    def test_ipvlan_without_networkd(self):
        self.patch_conf_property('user_port_driver', 'ipvlan')
        with mock.patch.object(guestagent_utils, 'user_port_ifname',
                               return_value='nic01'), \
                mock.patch.object(guestagent_utils.os.path, 'isdir',
                                  return_value=False):
            guestagent_utils.prepare_user_defined_port()
        self.write.assert_not_called()
        self.assertNotIn("networkctl reload", self.commands())
        self.assertEqual("ip link set nic01 up", self.commands()[-1])

    def test_ipvlan_without_the_interface(self):
        self.patch_conf_property('user_port_driver', 'ipvlan')
        with mock.patch.object(guestagent_utils, 'user_port_ifname',
                               return_value=None):
            guestagent_utils.prepare_user_defined_port()
        self.shell.assert_not_called()
        self.write.assert_not_called()

    def test_config_is_the_injected_file(self):
        self.config_patch.stop()
        mo = mock.mock_open(read_data=json.dumps(ETH1))
        with mock.patch.object(guestagent_utils.os.path, 'exists',
                               return_value=True), \
                mock.patch.object(guestagent_utils, 'open', mo, create=True):
            self.assertEqual(ETH1, guestagent_utils.user_port_config())
        with mock.patch.object(guestagent_utils.os.path, 'exists',
                               return_value=False):
            self.assertIsNone(guestagent_utils.user_port_config())
        self.config_patch.start()
