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

from oslo_log import log as logging

from trove.guestagent.datastore.galera_common import manager as galera_manager

LOG = logging.getLogger(__name__)

MODE_KEY = 'group_replication_mode'


class GroupReplicationManagerMixin(galera_manager.GaleraManagerMixin):
    """The cluster calls of the guest agent for Group Replication, on a
    MySQL based manager whose app has ``GroupReplicationAppMixin``.

    The calls are Galera's, which the task manager strategy makes in the
    same order, and the cluster probe is Galera's; what is Group
    Replication's own is the mode kept from the creation. List the mixin
    before the manager class it extends.
    """

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        super(GroupReplicationManagerMixin, self).do_prepare(
            context, packages, databases, memory_mb, users, device_path,
            mount_point, backup_info, config_contents, root_password,
            overrides, cluster_config, snapshot, ds_version=ds_version)
        if cluster_config and cluster_config.get(MODE_KEY):
            # For the task manager to render the cluster configuration
            # with; the members of a grown cluster get the group's.
            self.app.keep_group_mode(cluster_config[MODE_KEY])
