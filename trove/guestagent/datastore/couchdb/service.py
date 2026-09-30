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
from trove.common.db.couchdb import models
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

# The directory of the guest with the files Trove writes, mounted as the
# directory of local settings of the server. The server reads the files in
# it in the order of their names, the later ones over the earlier ones, and
# writes what it is told at runtime into the last.
HOST_CONF_DIR = '/etc/couchdb'
CONTAINER_CONF_DIR = '/opt/couchdb/etc/local.d'
# First: what the configuration template renders. The overrides of the
# configuration manager sort after it and before the server's own file.
CONFIG_FILE = f'{HOST_CONF_DIR}/00-trove.ini'
# Last, and the name the entrypoint of the image creates: what the server
# itself writes. The admin goes here in clear text and the server replaces
# the password by its hash, in place.
SERVER_FILE = f'{HOST_CONF_DIR}/docker.ini'
# Not a file the server reads: the credentials for the commands the guest
# agent runs in the container.
NETRC_FILE_NAME = 'trove.netrc'
NETRC_FILE = f'{HOST_CONF_DIR}/{NETRC_FILE_NAME}'

ADMIN_USER = 'os_admin'
PORT = 5984
USER_PREFIX = 'org.couchdb.user:'
# The role of a server admin. It stays among the members of every database
# Trove touches: a database with no member names and no member roles is
# readable by everybody, without authentication.
ADMIN_ROLE = '_admin'


class CouchDBApp(service.BaseDbApp):
    _configuration_manager = None

    # Answers without authentication, which is all a health check has.
    HEALTHCHECK = {
        "test": ["CMD-SHELL", "curl -sf http://127.0.0.1:%d/_up" % PORT],
        "start_period": 10 * 1000000000,
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
            stream_codecs.IniCodec(),
            requires_root=True,
            override_strategy=configuration.ImportOverrideStrategy(
                HOST_CONF_DIR, 'ini')
        )
        return self._configuration_manager

    def __init__(self, status, docker_client):
        super(CouchDBApp, self).__init__(status, docker_client)
        self.mount_point = cfg.get_configuration_property('mount_point')
        self.datadir = f'{self.mount_point}/data'
        self.adm = CouchDBAdmin(self)

    ################
    # Configuration
    ################

    def apply_initial_guestagent_configuration(self):
        """Settings the guest agent decides.

        A single node creates the databases of the server itself, the one
        with the users among them, on its first start.
        """
        self.configuration_manager.apply_system_override({
            'couchdb': {
                'single_node': 'true',
                'database_dir': self.datadir,
                'view_index_dir': self.datadir,
            },
            'chttpd': {
                'bind_address': '0.0.0.0',
                'port': PORT,
            },
        })

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    ##############
    # Admin user
    ##############

    def has_admin(self):
        return operating_system.exists(SERVER_FILE, as_root=True)

    def secure(self, password=None):
        """Give the server its admin before it starts: since 3.0 it does
        not start without one.
        """
        password = password or utils.generate_random_password()
        LOG.info('Configuring the Trove admin.')
        self._write_owned(
            SERVER_FILE, f'[admins]\n{ADMIN_USER} = {password}\n')
        self._write_owned(
            NETRC_FILE,
            f'machine 127.0.0.1 login {ADMIN_USER} password {password}\n')
        self.save_password(ADMIN_USER, password)

    def _write_owned(self, path, content):
        """A file of the database user alone: the server rewrites the one,
        the other holds a password.
        """
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
        for folder in (HOST_CONF_DIR, self.mount_point, self.datadir):
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
        command = command or '/opt/couchdb/bin/couchdb'

        self._ensure_directories()

        volumes = {
            HOST_CONF_DIR: {'bind': CONTAINER_CONF_DIR, 'mode': 'rw'},
            self.mount_point: {'bind': self.mount_point, 'mode': 'rw'},
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
        return {self.datadir: {'bind': self.datadir, 'mode': 'rw'}}

    def create_backup(self, context, backup_info):
        """A backup is a copy of the database files. They are only ever
        appended to, so a copy taken while the server runs is a database
        as of some moment during the copy.
        """
        super(CouchDBApp, self).create_backup(
            context, backup_info,
            volumes_mapping=self._backup_volumes(),
            need_dbuser=False,
            extra_params=f'--db-datadir={self.datadir}')

    def restore_backup(self, context, backup_info, restore_location):
        """Unpack a backup into the empty data directory.

        The admin of a server is in its configuration, not in its data: a
        restored instance has the users of the backup and its own admin.
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
            f'--db-datadir={self.datadir}'
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


class CouchDBAdmin(object):
    """Administrative operations, over the HTTP API of the server.

    The API is called from inside the database container: with the user's
    network interface handed to the container the guest agent cannot reach
    the address of the server, and the server has no socket to share.
    """

    def __init__(self, app):
        self.app = app

    #######
    # API
    #######

    def request(self, method, path, body=None, expected=(200, 201, 202)):
        """Call the API as the admin.

        :returns: the status code and the decoded body
        :raises TroveError: for a status code that is not expected
        """
        command = [
            'curl', '-s', '-X', method,
            '--netrc-file', f'{CONTAINER_CONF_DIR}/{NETRC_FILE_NAME}',
            '-H', 'Content-Type: application/json',
            '-w', '\n%{http_code}',
        ]
        if body is not None:
            command += ['-d', json.dumps(body)]
        command.append('http://127.0.0.1:%d%s' % (PORT, path))

        text, _sep, code = self.app.execute(command).rpartition('\n')
        code = int(code)
        result = json.loads(text) if text.strip() else None
        if expected is not None and code not in expected:
            raise exception.TroveError(
                _("%(method)s %(path)s returned %(code)s: %(result)s")
                % {'method': method, 'path': path, 'code': code,
                   'result': result})
        return code, result

    @staticmethod
    def _quote(name):
        """A name as one segment of a path. Database names may contain a
        slash.
        """
        return urllib.parse.quote(str(name), safe='')

    def _database_path(self, name):
        return '/' + self._quote(name)

    def _user_path(self, name):
        return '/_users/' + self._quote(USER_PREFIX + name)

    #############
    # Databases
    #############

    def _database_names(self):
        ignored = cfg.get_ignored_dbs()
        return [name for name in self.request('GET', '/_all_dbs')[1]
                if name not in ignored and not name.startswith('_')]

    def create_database(self, databases):
        for item in databases:
            database = models.CouchDBSchema.deserialize(item)
            database.check_create()
            LOG.debug("Creating database '%s'.", database.name)
            self.request('PUT', self._database_path(database.name))

    def delete_database(self, database):
        database = models.CouchDBSchema.deserialize(database)
        database.check_delete()
        LOG.debug("Deleting database '%s'.", database.name)
        self.request('DELETE', self._database_path(database.name))

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [models.CouchDBSchema(name) for name in self._database_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    ##############
    # Membership
    ##############

    def _members(self, database):
        """The names that are members of a database."""
        security = self.request(
            'GET', self._database_path(database) + '/_security')[1] or {}
        return list(security.get('members', {}).get('names', []))

    def _set_members(self, database, change):
        """Apply a change to the member names of a database, leaving the
        rest of its security object as it is.
        """
        path = self._database_path(database) + '/_security'
        security = self.request('GET', path)[1] or {}
        members = security.setdefault('members', {})
        names = list(members.get('names', []))
        changed = change(names)
        if changed == names:
            return
        members['names'] = changed
        roles = list(members.get('roles', []))
        if ADMIN_ROLE not in roles:
            roles.append(ADMIN_ROLE)
        members['roles'] = roles
        self.request('PUT', path, body=security)

    def _grant(self, username, database):
        self._set_members(
            database,
            lambda names: names if username in names else names + [username])

    def _revoke(self, username, database):
        self._set_members(
            database,
            lambda names: [name for name in names if name != username])

    def _databases_of(self, username):
        return [name for name in self._database_names()
                if username in self._members(name)]

    #########
    # Users
    #########

    def _check_modifiable(self, username):
        if username in cfg.get_ignored_users():
            raise exception.BadRequest(
                _("User %s is reserved.") % username)

    def _user_names(self):
        ignored = cfg.get_ignored_users()
        names = []
        for row in self.request('GET', '/_users/_all_docs')[1]['rows']:
            if not row['id'].startswith(USER_PREFIX):
                continue
            name = row['id'][len(USER_PREFIX):]
            if name not in ignored:
                names.append(name)
        return names

    def _user_doc(self, username):
        """The document of a user, or None."""
        code, doc = self.request('GET', self._user_path(username),
                                 expected=(200, 404))
        return doc if code == 200 else None

    def _build_user(self, username):
        user = models.CouchDBUser(username)
        for database in self._databases_of(username):
            user.databases = database
        return user

    def create_user(self, users):
        for item in users:
            user = models.CouchDBUser.deserialize(item)
            self._check_modifiable(user.name)
            LOG.debug("Creating user '%s'.", user.name)
            self.request('PUT', self._user_path(user.name), body={
                'name': user.name, 'password': user.password,
                'roles': [], 'type': 'user'})
            for database in user.databases:
                self._grant(
                    user.name,
                    models.CouchDBSchema.deserialize(database).name)

    def delete_user(self, user):
        user = models.CouchDBUser.deserialize(user)
        self._check_modifiable(user.name)
        for database in self._databases_of(user.name):
            self._revoke(user.name, database)
        doc = self._user_doc(user.name)
        if doc is None:
            return
        LOG.debug("Deleting user '%s'.", user.name)
        self.request(
            'DELETE',
            self._user_path(user.name) + '?rev=' + self._quote(doc['_rev']))

    def list_users(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [self._build_user(name) for name in self._user_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    def get_user(self, username, hostname=None):
        if username in cfg.get_ignored_users() or \
                self._user_doc(username) is None:
            return None
        return self._build_user(username).serialize()

    def _existing_user(self, username):
        self._check_modifiable(username)
        doc = self._user_doc(username)
        if doc is None:
            raise exception.UserNotFound(uuid=username)
        return doc

    def grant_access(self, username, hostname, databases):
        self._existing_user(username)
        for name in databases:
            models.CouchDBSchema(name).check_reserved()
            self._grant(username, name)

    def revoke_access(self, username, hostname, database):
        self._existing_user(username)
        models.CouchDBSchema(database).check_reserved()
        self._revoke(username, database)

    def list_access(self, username, hostname=None):
        self._existing_user(username)
        return self._build_user(username).databases

    def _set_password(self, username, password):
        doc = self._existing_user(username)
        # The server derives the key from the password and drops it.
        for derived in ('derived_key', 'salt', 'iterations',
                        'password_scheme', 'pbkdf2_prf'):
            doc.pop(derived, None)
        doc['password'] = password
        self.request('PUT', self._user_path(username), body=doc)

    def change_passwords(self, users):
        for item in users:
            user = models.CouchDBUser.deserialize(item)
            LOG.debug("Changing password of user '%s'.", user.name)
            self._set_password(user.name, user.password)

    def update_attributes(self, username, hostname, user_attrs):
        """The name of a user is the identifier of its document and cannot
        change.
        """
        new_name = user_attrs.get('name')
        if new_name is not None and new_name != username:
            raise exception.UnprocessableEntity(
                _("The name of a CouchDB user cannot be changed."))
        if user_attrs.get('password') is not None:
            self._set_password(username, user_attrs['password'])

    ########
    # Root
    ########

    def _admins(self):
        return self.request('GET', '/_node/_local/_config/admins')[1]

    def enable_root(self, root_password=None):
        """Root is a second server admin. The server hashes its password
        and keeps it in the file it writes its own settings to.
        """
        root = models.CouchDBUser.root(password=root_password)
        self.request(
            'PUT', '/_node/_local/_config/admins/' + self._quote(root.name),
            body=root.password)
        # The password works once the server has replaced it by its hash,
        # a moment later.
        utils.poll_until(
            lambda: str(self._admins().get(root.name, '')).startswith('-'),
            sleep_time=1, time_out=CONF.state_change_wait_time)
        return root.serialize()

    def is_root_enabled(self):
        return models.CouchDBUser.root_username in self._admins()

    def disable_root(self):
        if self.is_root_enabled():
            self.request(
                'DELETE', '/_node/_local/_config/admins/' +
                self._quote(models.CouchDBUser.root_username))
