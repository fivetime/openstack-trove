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

from oslo_log import log as logging

from trove.common import cfg
from trove.common.notification import EndNotification
from trove.guestagent.datastore.couchbase import service
from trove.guestagent.datastore import manager
from trove.guestagent.datastore import service as base_service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.Manager):

    def __init__(self):
        super(Manager, self).__init__('couchbase')
        self.status = base_service.BaseDbStatus(self.docker_client)
        self.app = service.CouchbaseApp(self.status, self.docker_client)
        self.adm = self.app.adm

    @property
    def configuration_manager(self):
        return self.app.configuration_manager

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        """Decide the settings and the admin, then start the server and
        make it a cluster of its one node.

        A backup is unpacked onto the volume before the first start. It
        brings the buckets and the users of the instance it was taken
        from; the admin password is this instance's own either way.
        """
        LOG.info('Preparing database config files')
        self.app.configuration_manager.reset_configuration(config_contents)
        self.app.update_overrides(overrides)

        if not self.app.has_admin():
            self.app.secure()

        if backup_info:
            self.perform_restore(context, self.app.mount_point, backup_info)

        self.app.start_db(ds_version=ds_version)

    def restart(self, context):
        self.app.restart()

    def stop_db(self, context):
        self.app.stop_db()

    def apply_overrides(self, context, overrides):
        """The server takes its settings at runtime, over its API."""
        self.app.apply_settings()

    #########
    # Users
    #########

    def change_passwords(self, context, users):
        with EndNotification(context):
            self.adm.change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        with EndNotification(context):
            self.adm.update_attributes(username, hostname, user_attrs)

    def create_database(self, context, databases):
        with EndNotification(context):
            self.adm.create_database(databases)

    def create_user(self, context, users):
        with EndNotification(context):
            self.adm.create_user(users)

    def delete_database(self, context, database):
        with EndNotification(context):
            self.adm.delete_database(database)

    def delete_user(self, context, user):
        with EndNotification(context):
            self.adm.delete_user(user)

    def get_user(self, context, username, hostname):
        return self.adm.get_user(username, hostname)

    def grant_access(self, context, username, hostname, databases):
        self.adm.grant_access(username, hostname, databases)

    def revoke_access(self, context, username, hostname, database):
        self.adm.revoke_access(username, hostname, database)

    def list_access(self, context, username, hostname):
        return self.adm.list_access(username, hostname)

    def list_databases(self, context, limit=None, marker=None,
                       include_marker=False):
        return self.adm.list_databases(limit, marker, include_marker)

    def list_users(self, context, limit=None, marker=None,
                   include_marker=False):
        return self.adm.list_users(limit, marker, include_marker)

    def enable_root(self, context):
        return self.adm.enable_root()

    def enable_root_with_password(self, context, root_password=None):
        return self.adm.enable_root(root_password)

    def disable_root(self, context):
        self.adm.disable_root()

    def is_root_enabled(self, context):
        return self.adm.is_root_enabled()

    ##########
    # Backup
    ##########

    def create_backup(self, context, backup_info):
        LOG.info("Creating backup %s", backup_info['id'])
        with EndNotification(context):
            self.app.create_backup(context, backup_info)
