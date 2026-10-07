# Copyright 2016 Tesora, Inc.
# All Rights Reserved.
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
#

import copy
from unittest.mock import Mock, patch

from trove.common import server_group as srv_grp
from trove.tests.unittests import trove_testtools


class TestServerGroup(trove_testtools.TestCase):

    def setUp(self):
        super(TestServerGroup, self).setUp()
        self.ServerGroup = srv_grp.ServerGroup()
        self.context = trove_testtools.TroveTestContext(self)
        self.sg_id = 'sg-1234'
        self.locality = 'affinity'
        self.expected_hints = {'group': self.sg_id}
        self.server_group = Mock()
        self.server_group.id = self.sg_id
        self.server_group.policies = [self.locality]
        self.server_group.members = ['id-1', 'id-2']
        self.empty_server_group = copy.copy(self.server_group)
        self.empty_server_group.members = ['id-1']

    @patch.object(srv_grp, 'create_nova_client')
    def test_create(self, mock_client):
        mock_create = Mock(return_value=self.server_group)
        mock_client.return_value.server_groups.create = mock_create
        server_group = self.ServerGroup.create(
            self.context, self.locality, "name_suffix")
        mock_create.assert_called_with(name="locality_name_suffix",
                                       policies=[self.locality])
        self.assertEqual(self.server_group, server_group)

    @patch.object(srv_grp, 'create_nova_client')
    def test_delete(self, mock_client):
        mock_delete = Mock()
        mock_client.return_value.server_groups.delete = mock_delete
        self.ServerGroup.delete(self.context, self.empty_server_group)
        mock_delete.assert_called_with(self.sg_id)

    @patch.object(srv_grp, 'create_nova_client')
    def test_delete_non_empty(self, mock_client):
        mock_delete = Mock()
        mock_client.return_value.server_groups.delete = mock_delete
        srv_grp.ServerGroup.delete(self.context, self.server_group)
        mock_delete.assert_not_called()

    @patch.object(srv_grp, 'create_nova_client')
    def test_delete_force(self, mock_client):
        mock_delete = Mock()
        mock_client.return_value.server_groups.delete = mock_delete
        self.ServerGroup.delete(self.context, self.server_group, force=True)
        mock_delete.assert_called_with(self.sg_id)

    def test_convert_to_hint(self):
        hint = srv_grp.ServerGroup.convert_to_hint(self.server_group)
        self.assertEqual(self.expected_hints, hint, "Unexpected hint")

    def test_convert_to_hints(self):
        hints = {'hint': 'myhint'}
        hints = srv_grp.ServerGroup.convert_to_hint(self.server_group, hints)
        self.expected_hints.update(hints)
        self.assertEqual(self.expected_hints, hints, "Unexpected hints")

    def test_convert_to_hint_none(self):
        self.assertIsNone(srv_grp.ServerGroup.convert_to_hint(None))

    @patch.object(srv_grp, 'create_nova_client')
    def test_build_scheduler_hint(self, mock_client):
        mock_create = Mock(return_value=self.server_group)
        mock_client.return_value.server_groups.create = mock_create
        expected_hint = {'get_back': 'same_dict'}
        scheduler_hint = self.ServerGroup.build_scheduler_hint(
            self.context, expected_hint, "name_suffix")
        self.assertEqual(expected_hint, scheduler_hint, "Unexpected hint")

    @patch.object(srv_grp, 'create_nova_client')
    def test_build_scheduler_hint_from_locality(self, mock_client):
        mock_create = Mock(return_value=self.server_group)
        mock_client.return_value.server_groups.create = mock_create
        expected_hint = {'group': 'sg-1234'}
        scheduler_hint = self.ServerGroup.build_scheduler_hint(
            self.context, self.locality, "name_suffix")
        self.assertEqual(expected_hint, scheduler_hint, "Unexpected hint")

    def test_build_scheduler_hint_none(self):
        self.assertIsNone(srv_grp.ServerGroup.build_scheduler_hint(
            self.context, None, None))

    def test_get_locality(self):
        locality = srv_grp.ServerGroup.get_locality(self.server_group)
        self.assertEqual(self.locality, locality, "Unexpected locality")

    def test_get_locality_none(self):
        self.assertIsNone(srv_grp.ServerGroup.get_locality(None))

    def _nova_with_groups(self, mock_client, *groups):
        mock_client.return_value.server_groups.list = Mock(
            return_value=list(groups))

    def _group(self, name, members):
        group = Mock()
        group.name = name
        group.members = members
        return group

    @patch.object(srv_grp, 'create_nova_client')
    def test_load_for_cluster_by_a_member(self, mock_client):
        # The members' servers tell the group of a cluster, whatever its
        # name, and apart from a group of another cluster with that name.
        other = self._group('locality_c1', ['id-9'])
        ours = self._group('locality_old-name', ['id-1', 'id-2'])
        self._nova_with_groups(mock_client, other, ours)
        self.assertEqual(ours, srv_grp.ServerGroup.load_for_cluster(
            self.context, 'c1', ['id-2', 'id-3']))

    @patch.object(srv_grp, 'create_nova_client')
    def test_load_for_cluster_by_name_before_any_server(self, mock_client):
        # No member has a server yet: the one empty group of the name.
        used = self._group('locality_c1', ['id-9'])
        empty = self._group('locality_c1', [])
        self._nova_with_groups(mock_client, used, empty)
        self.assertEqual(empty, srv_grp.ServerGroup.load_for_cluster(
            self.context, 'c1', []))

    @patch.object(srv_grp, 'create_nova_client')
    def test_load_for_cluster_ambiguous_names(self, mock_client):
        self._nova_with_groups(mock_client, self._group('locality_c1', []),
                               self._group('locality_c1', []))
        self.assertIsNone(srv_grp.ServerGroup.load_for_cluster(
            self.context, 'c1', ['id-1']))

    @patch.object(srv_grp, 'create_nova_client')
    def test_load_for_cluster_without_nova(self, mock_client):
        mock_client.return_value.server_groups.list = Mock(
            side_effect=Exception('nova down'))
        self.assertIsNone(srv_grp.ServerGroup.load_for_cluster(
            self.context, 'c1', ['id-1']))
