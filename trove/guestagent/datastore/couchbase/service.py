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

import json
import os
import urllib.parse

from oslo_log import log as logging

from trove.common import cfg
from trove.common import constants
from trove.common.db.couchbase import models
from trove.common import exception
from trove.common.i18n import _
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent.common import configuration
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore import service
from trove.guestagent.utils import docker as docker_util
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The directory of the guest with the files Trove writes, mounted into the
# database container at the same path. The server reads none of them: what
# Trove decides is applied to it over its API.
HOST_CONF_DIR = '/etc/couchbase'
CONTAINER_CONF_DIR = HOST_CONF_DIR
# The settings, as the configuration template renders them and the
# configuration manager overrides them: a flat key=value file.
CONFIG_FILE = f'{HOST_CONF_DIR}/couchbase.conf'
# The credentials of the admin for the commands the guest agent runs in the
# container, and the password alone for the requests that set it.
NETRC_FILE_NAME = 'trove.netrc'
NETRC_FILE = f'{HOST_CONF_DIR}/{NETRC_FILE_NAME}'
PASSWORD_FILE_NAME = 'trove.secret'
PASSWORD_FILE = f'{HOST_CONF_DIR}/{PASSWORD_FILE_NAME}'
CONTAINER_PASSWORD_FILE = f'{CONTAINER_CONF_DIR}/{PASSWORD_FILE_NAME}'
# The volume, as the container sees it: everything the server keeps, the
# data, the configuration, the users and the logs.
CONTAINER_VAR_DIR = '/opt/couchbase/var'
# A token the server writes at every start, with which a process on the
# node is the administrator without a password.
LOCAL_TOKEN_FILE_NAME = 'lib/couchbase/localtoken'
LOCAL_TOKEN_FILE = f'{CONTAINER_VAR_DIR}/{LOCAL_TOKEN_FILE_NAME}'
LOCAL_TOKEN_USER = '@localtoken'

ADMIN_USER = 'os_admin'
PORT = 8091
# The services of the node. The Community Edition has no others worth a
# quota.
SERVICES = 'kv,index,n1ql'
# The role a user has on each of its buckets, and the one root has on the
# whole server. The Community Edition knows these two and the read-only
# admin.
BUCKET_ROLE = 'bucket_full_access'
ROOT_ROLE = 'admin'
# The settings the configuration file holds, with the names the API of the
# server knows them by. The rest are settings of the guest agent.
SERVER_SETTINGS = {
    'memory_quota': 'memoryQuota',
    'index_memory_quota': 'indexMemoryQuota',
}
COMPACTION_SETTINGS = {
    'compaction_database_threshold':
        'databaseFragmentationThreshold[percentage]',
    'compaction_view_threshold': 'viewFragmentationThreshold[percentage]',
}
BUCKET_DEFAULTS = {
    'bucket_ramsize': 100,
    'bucket_replicas': 0,
    'bucket_eviction_policy': 'valueOnly',
}


class CouchbaseApp(service.BaseDbApp):
    _configuration_manager = None

    # The token makes the check independent of the admin password: on a
    # restored instance the data brings the password of the instance it was
    # taken from, until the guest agent replaces it.
    HEALTHCHECK = {
        "test": [
            "CMD-SHELL",
            'curl -sf -o /dev/null -u "%s:$(cat %s)" '
            'http://127.0.0.1:%d/pools/default/buckets'
            % (LOCAL_TOKEN_USER, LOCAL_TOKEN_FILE, PORT)
        ],
        "start_period": 30 * 1000000000,
        "interval": 10 * 1000000000,
        "timeout": 5 * 1000000000,
        "retries": 3
    }

    @property
    def configuration_manager(self):
        if self._configuration_manager:
            return self._configuration_manager

        self._configuration_manager = configuration.ConfigurationManager(
            CONFIG_FILE,
            self.database_service_uid,
            self.database_service_gid,
            stream_codecs.KeyValueCodec(line_terminator='\n'),
            requires_root=True,
            override_strategy=configuration.OneFileOverrideStrategy(
                HOST_CONF_DIR)
        )
        return self._configuration_manager

    def __init__(self, status, docker_client):
        super(CouchbaseApp, self).__init__(status, docker_client)
        self.mount_point = cfg.get_configuration_property('mount_point')
        self.local_token_path = f'{self.mount_point}/{LOCAL_TOKEN_FILE_NAME}'
        self.adm = CouchbaseAdmin(self)

    ################
    # Configuration
    ################

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def setting(self, name, default=None):
        """A setting as the configuration file has it, or the default."""
        value = self.configuration_manager.get_value(name)
        if value is None:
            return default
        return value.strip() if isinstance(value, str) else value

    def _settings(self, names):
        """The settings of the file among the named, by their API names."""
        settings = {}
        for name, api_name in names.items():
            value = self.setting(name)
            if value is not None:
                settings[api_name] = value
        return settings

    def apply_settings(self):
        """Tell the running server the settings of the configuration file.

        The server keeps what it is told, so a restart needs nothing; the
        data of a restored backup brings the settings of the instance it
        was taken from, and these are replaced.
        """
        quotas = self._settings(SERVER_SETTINGS)
        if quotas:
            LOG.info('Applying the memory quotas %s.', quotas)
            self.adm.request('POST', '/pools/default', data=quotas)
        compaction = self._settings(COMPACTION_SETTINGS)
        if compaction:
            LOG.info('Applying the compaction settings %s.', compaction)
            # The server insists on being told whether the data and the
            # views are compacted at the same time.
            compaction['parallelDBAndViewCompaction'] = 'false'
            self.adm.request('POST', '/controller/setAutoCompaction',
                             data=compaction)

    def bucket_settings(self):
        """What a new bucket is created with."""
        return {name: self.setting(name, default)
                for name, default in BUCKET_DEFAULTS.items()}

    ##############
    # Admin user
    ##############

    def has_admin(self):
        return operating_system.exists(PASSWORD_FILE, as_root=True)

    def secure(self, password=None):
        """Decide the password of the admin. The server learns it when it
        is initialized, or, restored from a backup, when the password of
        the backup is replaced.
        """
        password = password or utils.generate_random_password()
        LOG.info('Configuring the Trove admin.')
        self._write_owned(
            NETRC_FILE,
            f'machine 127.0.0.1 login {ADMIN_USER} password {password}\n')
        self._write_owned(PASSWORD_FILE, password)
        self.save_password(ADMIN_USER, password)

    def _write_owned(self, path, content):
        """A file of the database user alone: it holds a password."""
        operating_system.write_file(path, content, as_root=True)
        operating_system.chown(
            path, self.database_service_uid, self.database_service_gid,
            as_root=True)
        operating_system.chmod(path, FileMode.SET_USR_RW, as_root=True)

    @property
    def admin_password(self):
        return self.get_auth_password()

    ############
    # Commands
    ############

    def execute(self, command):
        """Run a command in the database container and return what it
        wrote to its standard output.
        """
        container = self.docker_client.containers.get('database')
        code, output = container.exec_run(command, demux=True)
        stdout, stderr = (stream.decode() if stream else ''
                          for stream in output)
        if code != 0:
            raise exception.TroveError(
                _("Command %(command)s failed with %(code)s: %(error)s")
                % {'command': command[0], 'code': code,
                   'error': (stderr or stdout).strip()[-500:]})
        return stdout

    ############
    # Lifecycle
    ############

    def _ensure_directories(self):
        for folder in (HOST_CONF_DIR, self.mount_point):
            operating_system.ensure_directory(
                folder, user=self.database_service_uid,
                group=self.database_service_gid, force=True,
                as_root=True)

    def start_db(self, update_db=False, ds_version=None, command=None,
                 extra_volumes=None):
        """Start and wait for the database service."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        ds_version = ds_version or CONF.datastore_version
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')
        command = command or 'couchbase-server'

        self._ensure_directories()

        volumes = {
            HOST_CONF_DIR: {'bind': CONTAINER_CONF_DIR, 'mode': 'rw'},
            self.mount_point: {'bind': CONTAINER_VAR_DIR, 'mode': 'rw'},
        }
        if extra_volumes:
            volumes.update(extra_volumes)

        ports = {}
        for port_range in cfg.get_configuration_property('tcp_ports'):
            for port in port_range:
                ports[f'{port}/tcp'] = port

        if CONF.network_isolation and \
                os.path.exists(constants.ETH1_CONFIG_PATH):
            network_mode = constants.DOCKER_HOST_NIC_MODE
        else:
            network_mode = constants.DOCKER_BRIDGE_MODE

        user = "%s:%s" % (self.database_service_uid, self.database_service_gid)
        try:
            docker_util.start_container(
                self.docker_client,
                image,
                volumes=volumes,
                network_mode=network_mode,
                ports=ports,
                user=user,
                healthcheck=self.HEALTHCHECK,
                command=command
            )
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.state_change_wait_time, update_db
        ):
            raise exception.TroveError("Failed to start database service")

        self.initialize()

    def initialize(self):
        """Make the running server the one Trove decided.

        A server that has never been initialized becomes a cluster of its
        one node with the Trove admin. One restored from a backup is
        already a cluster, with the admin password of the instance the
        backup was taken from; that is replaced. Either way the server is
        then told the settings of the configuration file.
        """
        if not self.adm.is_initialized():
            LOG.info('Initializing the cluster.')
            self.adm.init_cluster(
                memory_quota=self.setting('memory_quota'),
                index_memory_quota=self.setting('index_memory_quota'))
        else:
            LOG.info('Setting the password of the admin.')
            self.adm.reset_admin_password()
        self.apply_settings()

    def restart(self):
        LOG.info("Restarting database")
        self._ensure_directories()
        try:
            docker_util.restart_container(self.docker_client)
        except Exception:
            LOG.exception("Failed to restart database")
            raise exception.TroveError("Failed to restart database")

        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.state_change_wait_time, update_db=True
        ):
            raise exception.TroveError("Failed to start database")

        LOG.info("Finished restarting database")

    ##########
    # Backup
    ##########

    def _backup_volumes(self):
        return {self.mount_point: {'bind': self.mount_point, 'mode': 'rw'}}

    def create_backup(self, context, backup_info):
        """A backup is a copy of the volume: the data, the configuration
        and the users of the server. The files of the data are only ever
        appended to, so a copy taken while the server runs is a database
        as of some moment during the copy.
        """
        super(CouchbaseApp, self).create_backup(
            context, backup_info,
            volumes_mapping=self._backup_volumes(),
            need_dbuser=False,
            extra_params=f'--db-datadir={self.mount_point}')

    def restore_backup(self, context, backup_info, restore_location):
        """Unpack a backup onto the empty volume.

        The restored server is the one of the backup, its buckets and
        users included, and gets this instance's admin password when it
        starts.
        """
        backup_id = backup_info['id']
        storage_driver = CONF.storage_strategy
        backup_driver = self.get_backup_strategy()
        user_token = context.auth_token
        swift_url = backup_info.get('swift_url')
        if not swift_url:
            raise exception.TroveError(
                "Missing swift_url in backup metadata.")
        image = self.get_backup_image()
        name = 'db_restore'

        command = (
            f'python3 main.py --nobackup '
            f'--storage-driver={storage_driver} --driver={backup_driver} '
            f'--os-token={user_token} --swift-url={swift_url} '
            f'--restore-from={backup_info["location"]} '
            f'--restore-checksum={backup_info["checksum"]} '
            f'--db-datadir={self.mount_point}'
        )
        if CONF.swift_api_insecure:
            command = f"{command} --swift-api-insecure"
        if CONF.backup_aes_cbc_key:
            command = (f"{command} "
                       f"--backup-encryption-key={CONF.backup_aes_cbc_key}")

        self._ensure_directories()
        LOG.info('Starting to restore backup %s, command: %s', backup_id,
                 command)
        output, ret = docker_util.run_container(
            self.docker_client, image, name,
            volumes=self._backup_volumes(), command=command)
        result = output[-1]
        if not ret:
            msg = f'Failed to run restore container, error: {result}'
            LOG.error(msg)
            raise Exception(msg)

        operating_system.chown(
            self.mount_point, self.database_service_uid,
            self.database_service_gid, force=True, as_root=True)


class CouchbaseAdmin(object):
    """Administrative operations, over the REST API of the server.

    The API is called from inside the database container: with the user's
    network interface handed to the container the guest agent cannot reach
    the address of the server, and the server has no socket to share.
    """

    def __init__(self, app):
        self.app = app

    #######
    # API
    #######

    def request(self, method, path, data=None, files=None,
                expected=(200, 201, 202), local=False):
        """Call the API as the admin, or with the token of the node.

        :param data: form fields and their values
        :param files: form fields whose values are read from files in the
                      container, so that a password stays out of the
                      command
        :returns: the status code and the decoded body
        :raises TroveError: for a status code that is not expected
        """
        command = ['curl', '-s', '-X', method, '-w', '\n%{http_code}']
        if local:
            command += ['-u', '%s:%s' % (LOCAL_TOKEN_USER, self.local_token())]
        else:
            command += ['--netrc-file',
                        f'{CONTAINER_CONF_DIR}/{NETRC_FILE_NAME}']
        for name, value in (data or {}).items():
            command += ['--data-urlencode', '%s=%s' % (name, value)]
        for name, file_path in (files or {}).items():
            command += ['--data-urlencode', '%s@%s' % (name, file_path)]
        command.append('http://127.0.0.1:%d%s' % (PORT, path))

        text, _sep, code = self.app.execute(command).rpartition('\n')
        code = int(code)
        try:
            result = json.loads(text) if text.strip() else None
        except ValueError:
            result = text.strip()
        if expected is not None and code not in expected:
            raise exception.TroveError(
                _("%(method)s %(path)s returned %(code)s: %(result)s")
                % {'method': method, 'path': path, 'code': code,
                   'result': result})
        return code, result

    def local_token(self):
        """The token of the node, from the volume: the guest agent has the
        volume, the container has the shell.
        """
        return operating_system.read_file(
            self.app.local_token_path, as_root=True).strip()

    @staticmethod
    def _quote(name):
        return urllib.parse.quote(str(name), safe='')

    def _bucket_path(self, name):
        return '/pools/default/buckets/' + self._quote(name)

    def _user_path(self, name):
        return '/settings/rbac/users/local/' + self._quote(name)

    ##########
    # Server
    ##########

    def is_initialized(self):
        """A node that is not part of a cluster has no default pool."""
        code, _result = self.request('GET', '/pools/default', local=True,
                                     expected=(200, 404))
        return code == 200

    def init_cluster(self, memory_quota, index_memory_quota):
        """Make the node a cluster of one, with the services and the
        quotas, and the Trove admin as its administrator.
        """
        self.request('POST', '/node/controller/setupServices',
                     data={'services': SERVICES}, local=True)
        quotas = {}
        if memory_quota is not None:
            quotas['memoryQuota'] = memory_quota
        if index_memory_quota is not None:
            quotas['indexMemoryQuota'] = index_memory_quota
        if quotas:
            self.request('POST', '/pools/default', data=quotas, local=True)
        # The index storage the edition has: the Community Edition has one.
        self.request('POST', '/settings/indexes',
                     data={'storageMode': self.index_storage_mode()},
                     local=True)
        self.request('POST', '/settings/web',
                     data={'username': ADMIN_USER, 'port': 'SAME'},
                     files={'password': CONTAINER_PASSWORD_FILE}, local=True)

    def index_storage_mode(self):
        """The storage of indexes the edition has."""
        _code, pools = self.request('GET', '/pools', local=True)
        return 'plasma' if pools.get('isEnterprise') else 'forestdb'

    def reset_admin_password(self):
        """Give the administrator the password of this instance, with the
        token of the node: the password the server has is not known.
        """
        self.request('POST', '/controller/resetAdminPassword',
                     files={'password': CONTAINER_PASSWORD_FILE}, local=True)

    #############
    # Databases
    #############

    def _bucket_names(self):
        ignored = cfg.get_ignored_dbs()
        return [bucket['name']
                for bucket in self.request('GET', '/pools/default/buckets')[1]
                if bucket['name'] not in ignored]

    def create_database(self, databases):
        settings = self.app.bucket_settings()
        for item in databases:
            database = models.CouchbaseSchema.deserialize(item)
            database.check_create()
            LOG.debug("Creating bucket '%s'.", database.name)
            self.request('POST', '/pools/default/buckets', data={
                'name': database.name,
                'bucketType': 'couchbase',
                'ramQuota': settings['bucket_ramsize'],
                'replicaNumber': settings['bucket_replicas'],
                'evictionPolicy': settings['bucket_eviction_policy'],
                'flushEnabled': 1,
            })

    def delete_database(self, database):
        """The server drops the roles of the users on a deleted bucket."""
        database = models.CouchbaseSchema.deserialize(database)
        database.check_delete()
        LOG.debug("Deleting bucket '%s'.", database.name)
        self.request('DELETE', self._bucket_path(database.name))

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [models.CouchbaseSchema(name) for name in self._bucket_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    #########
    # Users
    #########

    def _check_modifiable(self, username):
        if username in cfg.get_ignored_users():
            raise exception.BadRequest(
                _("User %s is reserved.") % username)

    def _user_records(self):
        return [user for user in self.request('GET', '/settings/rbac/users')[1]
                if user.get('domain') == 'local']

    def _user_record(self, username):
        """The user as the server has it, or None."""
        code, record = self.request('GET', self._user_path(username),
                                    expected=(200, 404))
        return record if code == 200 else None

    @staticmethod
    def _buckets_of(record):
        """The buckets a user has its role on."""
        return [role['bucket_name'] for role in record.get('roles', [])
                if role['role'] == BUCKET_ROLE and role.get('bucket_name')]

    @staticmethod
    def _roles(buckets):
        return ','.join('%s[%s]' % (BUCKET_ROLE, bucket)
                        for bucket in buckets)

    def _put_user(self, username, buckets, password=None):
        """Write a user with its role on each of the buckets. The server
        takes the roles as a whole and keeps the password if none is
        given.
        """
        data = {'roles': self._roles(buckets)}
        if password is not None:
            data['password'] = password
        self.request('PUT', self._user_path(username), data=data)

    def _build_user(self, record):
        user = models.CouchbaseUser(record['id'])
        for bucket in self._buckets_of(record):
            user.databases = bucket
        return user

    def create_user(self, users):
        for item in users:
            user = models.CouchbaseUser.deserialize(item)
            self._check_modifiable(user.name)
            LOG.debug("Creating user '%s'.", user.name)
            self._put_user(
                user.name,
                [models.CouchbaseSchema.deserialize(database).name
                 for database in user.databases],
                password=user.password)

    def delete_user(self, user):
        user = models.CouchbaseUser.deserialize(user)
        self._check_modifiable(user.name)
        LOG.debug("Deleting user '%s'.", user.name)
        self.request('DELETE', self._user_path(user.name),
                     expected=(200, 404))

    def list_users(self, limit=None, marker=None, include_marker=False):
        ignored = cfg.get_ignored_users()
        users = [self._build_user(record) for record in self._user_records()
                 if record['id'] not in ignored]
        return guestagent_utils.serialize_list(
            users, limit=limit, marker=marker, include_marker=include_marker)

    def get_user(self, username, hostname=None):
        if username in cfg.get_ignored_users():
            return None
        record = self._user_record(username)
        if record is None:
            return None
        return self._build_user(record).serialize()

    def _existing_user(self, username):
        self._check_modifiable(username)
        record = self._user_record(username)
        if record is None:
            raise exception.UserNotFound(uuid=username)
        return record

    def grant_access(self, username, hostname, databases):
        buckets = self._buckets_of(self._existing_user(username))
        for name in databases:
            models.CouchbaseSchema(name).check_reserved()
            if name not in buckets:
                buckets.append(name)
        self._put_user(username, buckets)

    def revoke_access(self, username, hostname, database):
        buckets = self._buckets_of(self._existing_user(username))
        models.CouchbaseSchema(database).check_reserved()
        self._put_user(username,
                       [name for name in buckets if name != database])

    def list_access(self, username, hostname=None):
        return self._build_user(self._existing_user(username)).databases

    def _set_password(self, username, password):
        """The server takes the roles with a new password, or drops them."""
        record = self._existing_user(username)
        self._put_user(username, self._buckets_of(record), password=password)

    def change_passwords(self, users):
        for item in users:
            user = models.CouchbaseUser.deserialize(item)
            LOG.debug("Changing password of user '%s'.", user.name)
            self._set_password(user.name, user.password)

    def update_attributes(self, username, hostname, user_attrs):
        """The name of a user is its identifier and cannot change."""
        new_name = user_attrs.get('name')
        if new_name is not None and new_name != username:
            raise exception.UnprocessableEntity(
                _("The name of a Couchbase user cannot be changed."))
        if user_attrs.get('password') is not None:
            self._set_password(username, user_attrs['password'])

    ########
    # Root
    ########

    def enable_root(self, root_password=None):
        """Root is a second full administrator: a local user with the
        admin role.
        """
        root = models.CouchbaseUser.root(password=root_password)
        self.request('PUT', self._user_path(root.name), data={
            'password': root.password, 'roles': ROOT_ROLE})
        return root.serialize()

    def is_root_enabled(self):
        return self._user_record(models.CouchbaseUser.root_username) \
            is not None

    def disable_root(self):
        self.request('DELETE',
                     self._user_path(models.CouchbaseUser.root_username),
                     expected=(200, 404))
