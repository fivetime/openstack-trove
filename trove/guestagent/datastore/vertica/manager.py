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
from oslo_serialization import base64
from oslo_service import periodic_task

from trove.common import cfg
from trove.common.notification import EndNotification
from trove.guestagent.common import cluster_member
from trove.guestagent.datastore import manager
from trove.guestagent.datastore import service as base_service
from trove.guestagent.datastore.vertica import service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

LICENSE_MODULE_TYPE = 'vertica_license'


class Manager(cluster_member.ClusterWatchMixin, manager.Manager):

    def __init__(self):
        super(Manager, self).__init__('vertica')
        self.status = base_service.BaseDbStatus(self.docker_client)
        self.app = service.VerticaApp(self.status, self.docker_client)
        self.adm = self.app.adm
        self.init_cluster_watch()

    @property
    def configuration_manager(self):
        return self.app.configuration_manager

    def prepare(self, context, packages, databases, memory_mb, users,
                device_path=None, mount_point=None, backup_info=None,
                config_contents=None, root_password=None, overrides=None,
                cluster_config=None, snapshot=None, modules=None,
                ds_version=None, properties=None):
        """Keep the license of a license module before anything else: the
        database is created with it. The module is then applied as any
        other, after the database exists. The extended properties of a
        single instance (trove.instance.properties) are for do_prepare.
        """
        self._properties = properties or {}
        for module in [data.get('module', {}) for data in modules or []]:
            if module.get('type') == LICENSE_MODULE_TYPE and \
                    module.get('contents'):
                LOG.info("Keeping the license of module '%s' for the "
                         "database.", module.get('name'))
                # Nothing here may stop the preparation: it has not
                # reported its start yet, and the instance would stay in
                # BUILD with no fault. A version that needs the license
                # fails to create its database, and says so.
                try:
                    self.app.write_license(
                        base64.decode_as_bytes(module['contents']))
                except Exception:
                    LOG.exception("Could not keep the license of module "
                                  "'%s'.", module.get('name'))
        super(Manager, self).prepare(
            context, packages, databases, memory_mb, users,
            device_path=device_path, mount_point=mount_point,
            backup_info=backup_info, config_contents=config_contents,
            root_password=root_password, overrides=overrides,
            cluster_config=cluster_config, snapshot=snapshot,
            modules=modules, ds_version=ds_version)

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        """Decide the admin and the settings, start the container, and
        create the database on it or bring the restored one up. A member
        of a cluster stops at its agent: the taskmanager has the first
        member create the database on all of them.
        """
        # Asked for by the cluster, or by a single instance; the server
        # syncs its commits once the database answers (apply_sync_writes).
        if (cluster_config or getattr(self, '_properties', {})).get(
                'sync_writes'):
            self.app.plan_sync_writes(cluster=bool(cluster_config))

        LOG.info('Preparing database config files')
        self.app.configuration_manager.reset_configuration(config_contents)
        self.app.update_overrides(overrides)

        if not self.app.has_admin():
            self.app.secure()

        if backup_info:
            self.perform_restore(context, self.app.mount_point, backup_info)

        self.app.start_db(ds_version=ds_version)
        if cluster_config:
            return
        if backup_info:
            self.app.reset_admin_password()
        elif not self.app.database_exists():
            self.app.create_database()
        self.app.apply_settings()
        # Before the instance is ACTIVE: no commit of the user unsynced.
        self.app.apply_sync_writes()

    def restart(self, context):
        self.app.restart()

    ##########
    # Cluster
    ##########

    def get_cluster_secrets(self, context):
        return self.app.cluster_secrets()

    def install_cluster_secrets(self, context, secrets):
        self.app.install_cluster_secrets(secrets)

    def install_cluster(self, context, members, communal=None):
        LOG.info("Creating the database on %(members)s%(eon)s.",
                 {'members': members,
                  'eon': ', in Eon Mode on %s' % communal['location']
                  if communal else ''})
        return self.app.install_cluster(members, communal=communal)

    def set_cluster_config(self, context, config):
        self.app.set_cluster_config(config)

    def set_communal_credential(self, context, access, secret):
        self.app.set_communal_credential(access, secret)

    def cluster_complete(self, context):
        # The database exists on every member by now.
        self.app.apply_sync_writes()
        super(Manager, self).cluster_complete(context)

    @periodic_task.periodic_task
    def keep_sync_writes(self, context):
        """The commits synced by the server when sync writes were asked
        for, after a reboot, a restart or a restore too, and the sync mount
        of an earlier build taken off.
        """
        try:
            self.app.apply_sync_writes()
        except Exception:
            LOG.exception("Failed to apply sync writes.")

    def stop_db(self, context):
        self.app.stop_db()

    def apply_overrides(self, context, overrides):
        """The parameters are set on the running server; those that need a
        restart take effect with the next one.
        """
        self.app.apply_overrides(overrides)

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
