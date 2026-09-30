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
from trove.guestagent.datastore.cassandra import service
from trove.guestagent.datastore import manager
from trove.guestagent.datastore import service as base_service
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class Manager(manager.Manager):

    def __init__(self):
        super(Manager, self).__init__('cassandra')
        self.status = base_service.BaseDbStatus(self.docker_client)
        self.app = service.CassandraApp(self.status, self.docker_client)
        self.adm = self.app.adm

    @property
    def configuration_manager(self):
        return self.app.configuration_manager

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        """Configure the instance and, unless it is a member of a cluster,
        start it with its superuser.

        A server records the name of its cluster, its tokens and its peers
        on its first start, so a member is only started once it has the
        seeds of its cluster: the taskmanager sets them and then starts
        the members in order, with restart.
        """
        LOG.info('Preparing database config files')
        self.app.configuration_manager.reset_configuration(config_contents)
        cluster_name = cluster_config.get('id') if cluster_config else None
        self.app.apply_initial_guestagent_configuration(
            cluster_name=cluster_name)
        self.app.update_overrides(overrides)

        if cluster_config:
            self.app.write_cluster_topology(
                cluster_config['dc'], cluster_config['rack'])
            return

        self.app.write_cluster_topology(
            service.DEFAULT_DATA_CENTER, service.DEFAULT_RACK)

        if backup_info:
            self.perform_restore(context, self.app.datadir, backup_info)
            self.app.apply_post_restore_updates(backup_info,
                                                ds_version=ds_version)

        self.app.start_db(ds_version=ds_version)

        if not backup_info:
            self.app.secure()
        elif self.adm.is_root_enabled():
            self.status.report_root(context)

    def restart(self, context):
        self.app.restart()

    def stop_db(self, context):
        self.app.stop_db()

    def apply_overrides(self, context, overrides):
        LOG.debug("Overrides will be applied after restart.")

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

    ###########
    # Cluster
    ###########

    def get_data_center(self, context):
        return self.app.get_data_center()

    def get_rack(self, context):
        return self.app.get_rack()

    def set_seeds(self, context, seeds):
        self.app.set_seeds(seeds)

    def get_seeds(self, context):
        return self.app.get_seeds()

    def set_auto_bootstrap(self, context, enabled):
        self.app.set_auto_bootstrap(enabled)

    def node_cleanup_begin(self, context):
        self.app.node_cleanup_begin()

    def node_cleanup(self, context):
        self.app.node_cleanup()

    def node_decommission(self, context):
        self.app.node_decommission()

    def cluster_secure(self, context, password):
        try:
            return self.app.cluster_secure(password)
        except Exception:
            LOG.exception("Could not secure the cluster.")
            self.status.set_status(service_status.ServiceStatuses.FAILED)
            raise

    def get_admin_credentials(self, context):
        return self.app.get_admin_credentials()

    def store_admin_credentials(self, context, admin_credentials):
        self.app.store_admin_credentials(admin_credentials)

    def cluster_complete(self, context):
        """Every member brings its replicas of the roles up to date before
        it reports itself ready: the first node has raised their number.
        """
        self.app.repair_auth()
        super(Manager, self).cluster_complete(context)
