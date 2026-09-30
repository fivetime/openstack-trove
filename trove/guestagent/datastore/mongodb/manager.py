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
from trove.guestagent.datastore import manager
from trove.guestagent.datastore.mongodb import service
from trove.guestagent.datastore import service as base_service
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.Manager):

    def __init__(self):
        super(Manager, self).__init__('mongodb')
        self.status = base_service.BaseDbStatus(self.docker_client)
        self.app = service.MongoDBApp(self.status, self.docker_client)
        self.adm = self.app.adm

    @property
    def configuration_manager(self):
        return self.app.configuration_manager

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        """Configure the instance and, unless it is part of a cluster,
        start it with its admin user.

        A replica set member and a config server start too, but get their
        admin user from the taskmanager: the primary creates it when the
        replica set is initiated and the other members receive it with
        the data. A query router is not started before the taskmanager
        sends the config servers.
        """
        LOG.info('Preparing database config files')
        self.app.configuration_manager.reset_configuration(config_contents)
        self.app.apply_initial_guestagent_configuration(cluster_config)
        self.app.update_overrides(overrides)

        if backup_info:
            self._restore(context, backup_info, ds_version)

        if self.app.is_query_router:
            return

        self.app.start_db(ds_version=ds_version)

        if not cluster_config and not backup_info:
            self.app.secure()

        if backup_info and self.adm.is_root_enabled():
            self.status.report_root(context)

    def _restore(self, context, backup_info, ds_version):
        """Load the backup into a server without access control and give
        the admin user this instance's password: the backup carries the
        users of the instance it was taken from, the admin user included.
        """
        self.app.start_temporary_db(ds_version=ds_version)
        try:
            self.perform_restore(context, self.app.datadir, backup_info)
            self.app.reset_admin_password()
        finally:
            self.app.stop_temporary_db()

    def restart(self, context):
        LOG.debug("Restarting MongoDB.")
        self.app.restart()

    def stop_db(self, context):
        LOG.debug("Stopping MongoDB.")
        self.app.stop_db()

    def apply_overrides(self, context, overrides):
        LOG.debug("Overrides will be applied after restart.")

    #########
    # Users
    #########

    def change_passwords(self, context, users):
        with EndNotification(context):
            return self.adm.change_passwords(users)

    def update_attributes(self, context, username, hostname, user_attrs):
        with EndNotification(context):
            return self.adm.update_attributes(username, user_attrs)

    def create_database(self, context, databases):
        with EndNotification(context):
            return self.adm.create_database(databases)

    def create_user(self, context, users):
        with EndNotification(context):
            return self.adm.create_users(users)

    def delete_database(self, context, database):
        with EndNotification(context):
            return self.adm.delete_database(database)

    def delete_user(self, context, user):
        with EndNotification(context):
            return self.adm.delete_user(user)

    def get_user(self, context, username, hostname):
        return self.adm.get_user(username, hostname)

    def grant_access(self, context, username, hostname, databases):
        return self.adm.grant_access(username, databases)

    def revoke_access(self, context, username, hostname, database):
        return self.adm.revoke_access(username, database)

    def list_access(self, context, username, hostname):
        return self.adm.list_access(username)

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

    def is_root_enabled(self, context):
        return self.adm.is_root_enabled()

    ##########
    # Backup
    ##########

    def create_backup(self, context, backup_info):
        LOG.info("Creating backup %s", backup_info['id'])
        with EndNotification(context):
            self.app.create_backup(context, backup_info)

    ###########
    # Cluster
    ###########

    def _cluster_action(self, name, action, *args):
        LOG.debug("%s called with %s.", name, args)
        try:
            return action(*args)
        except Exception:
            LOG.exception("%s failed.", name)
            self.status.set_status(service_status.ServiceStatuses.FAILED)
            raise

    def add_members(self, context, members):
        self._cluster_action('add_members', self.app.add_members, members)

    def add_config_servers(self, context, config_servers):
        self._cluster_action('add_config_servers',
                             self.app.add_config_servers, config_servers)

    def add_shard(self, context, replica_set_name, replica_set_member):
        self._cluster_action('add_shard', self.app.add_shard,
                             replica_set_name, replica_set_member)

    def prep_primary(self, context):
        self._cluster_action('prep_primary', self.app.prep_primary)

    def get_key(self, context):
        return self.app.get_key()

    def create_admin_user(self, context, password):
        """Kept for the taskmanager API; every instance that has users of
        its own creates the admin user itself.
        """
        self.app.secure(password)

    def store_admin_password(self, context, password):
        self.app.store_admin_password(password)

    def get_replica_set_name(self, context):
        return self.app.replica_set_name

    def get_admin_password(self, context):
        return self.app.admin_password

    def is_shard_active(self, context, replica_set_name):
        return self.app.is_shard_active(replica_set_name)
