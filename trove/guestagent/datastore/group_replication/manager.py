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
from trove.guestagent.datastore.group_replication import probe
from trove.guestagent.datastore import manager as base_manager

LOG = logging.getLogger(__name__)

MODE_KEY = 'group_replication_mode'


class GroupReplicationManagerMixin(galera_manager.GaleraManagerMixin):
    """The cluster calls of the guest agent for Group Replication, on a
    MySQL based manager whose app has ``GroupReplicationAppMixin``.

    The calls are those of Galera, which the task manager strategy makes in
    the same order, and a few more: a member leaving the group, the role a
    member has, and the mode kept from the creation. List the mixin before
    the manager class it extends.

    A role probe watches the member's role and keeps the ready port open
    on a member that takes writes; the cluster calls tell it where the
    member stands.
    """

    group_probe = None

    def init_group_probe(self):
        """Start the role probe; a manager without one is still a manager."""
        try:
            self.group_probe = probe.RoleProbe(self.app, self.docker_client)
            self.group_probe.start()
        except Exception:
            LOG.exception("The role probe could not be set up.")

    def install_cluster(self, context, replication_user,
                        cluster_configuration, bootstrap):
        super(GroupReplicationManagerMixin, self).install_cluster(
            context, replication_user, cluster_configuration, bootstrap)
        if self.group_probe:
            self.group_probe.enable_member()

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

    def cluster_complete(self, context):
        # Not Galera's, which starts the first member again: from now on
        # every member joins the group when it starts.
        self.app.complete_cluster()
        base_manager.Manager.cluster_complete(self, context)
        if self.group_probe:
            self.group_probe.enable_complete()

    def leave_cluster(self, context):
        # The probe first: the port closes, and nothing brings the member
        # back into the group it is leaving.
        if self.group_probe:
            self.group_probe.disable()
        self.app.leave_group()

    def is_writable_member(self, context):
        return self.app.is_writable_member()

    def get_member_role(self, context):
        return self.app.get_member_role()
