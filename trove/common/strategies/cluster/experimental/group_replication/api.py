# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""MySQL Group Replication clusters, for MySQL and Percona Server.

A cluster is created, grown and shrunk as a Galera one is; what differs is
in the task manager and the guest agent. The tenant chooses the mode with
the extended property ``group_replication_mode``: ``single-primary`` (the
default), where one member takes writes and the group elects another when
it fails, or ``multi-primary``, where every member does.
"""

from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.galera_common import (
    api as galera_api)

MODE_KEY = 'group_replication_mode'
SINGLE_PRIMARY = 'single-primary'
MULTI_PRIMARY = 'multi-primary'
MODES = (SINGLE_PRIMARY, MULTI_PRIMARY)


class GroupReplicationAPIStrategy(galera_api.GaleraCommonAPIStrategy):

    @property
    def cluster_class(self):
        return GroupReplicationCluster


class GroupReplicationCluster(galera_api.GaleraCommonCluster):

    @classmethod
    def _member_config(cls, db_info, extended_properties):
        config = super(GroupReplicationCluster, cls)._member_config(
            db_info, extended_properties)
        # The members of a grown cluster get the group's mode from the
        # task manager instead.
        mode = (extended_properties or {}).get(MODE_KEY)
        if mode:
            config[MODE_KEY] = mode
        return config

    @classmethod
    def create(cls, context, name, datastore, datastore_version,
               instances, extended_properties, locality, configuration,
               image_id=None):
        properties = dict(extended_properties or {})
        mode = properties.setdefault(MODE_KEY, SINGLE_PRIMARY)
        if mode not in MODES:
            raise exception.BadRequest(
                _("The extended property %(key)s must be one of "
                  "%(modes)s.") % {'key': MODE_KEY,
                                   'modes': ', '.join(MODES)})
        return super(GroupReplicationCluster, cls).create(
            context, name, datastore, datastore_version, instances,
            properties, locality, configuration, image_id=image_id)
