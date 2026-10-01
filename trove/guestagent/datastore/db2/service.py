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

import os
import re
import uuid

import docker
from oslo_log import log as logging

from trove.common import cfg
from trove.common import constants
from trove.common.db.db2 import models
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

# The directory of the guest with the files Trove writes that are not the
# database's: the settings, and the password of the instance owner.
HOST_CONF_DIR = '/etc/db2-trove'
CONFIG_FILE = f'{HOST_CONF_DIR}/db2.conf'
ADMIN_SECRET = 'admin.secret'
# Mounted as /var/custom, whose scripts the entrypoint of the image runs
# every time it has set the instance up.
HOST_CUSTOM_DIR = f'{HOST_CONF_DIR}/custom'
CONTAINER_CUSTOM_DIR = '/var/custom'
USERS_SCRIPT = '10-trove-users.sh'
# The volume, as the container sees it. The image keeps the instance, its
# configuration, its license and the databases under it.
CONTAINER_DATA_DIR = '/database'
# Trove's directory on the volume: the users, the statements the guest
# agent runs, the backups being taken or restored.
TROVE_DIR = 'trove'
USERS_FILE = 'users'
RUN_DIR = 'run'
BACKUP_DIR = 'backup'
RESTORE_DIR = 'restore'
ARCHIVE_DIR = 'archive'
LICENSE_FILE = 'license.lic'
SETUP_COMPLETE = (f'{CONTAINER_DATA_DIR}/config/.shared-data/'
                  'setup_complete')

INSTANCE_OWNER = 'db2inst1'
INSTANCE_GROUP = 'db2iadm1'
PORT = 50000
# The server needs these rather than a privileged container: the shared
# memory of the instance, its resource limits and its priorities.
CAPABILITIES = ['IPC_OWNER', 'SYS_RESOURCE', 'SYS_NICE']
OPEN_FILES = 65536
# What a user has on each of its databases.
AUTHORITIES = 'DBADM, CREATETAB, BINDADD, CONNECT, DATAACCESS'
# Marks the rows of a query among what the command line processor prints.
ROW = 'TROVE_ROW'
# What the command line processor exits with when it did what it was told:
# 1 is a query without rows, 2 a warning.
CLP_OK = (0, 1, 2)
# The first start sets the instance up, and the image is large: more than
# the usual time to wait.
START_TIMEOUT = 900
# A database that cannot be connected to for now: in exclusive use while
# it is created or backed up offline, pending a backup or a roll forward.
UNAVAILABLE = ('SQL1035N', 'SQL1116N', 'SQL1117N')
# A backup image: <database>.0.<owner>.DBPART000.<timestamp>.001
BACKUP_IMAGE = re.compile(
    r'^([A-Z@#$][A-Z0-9@#$]{0,7})\.0\.%s\.DBPART000\.(\d{14})\.001$'
    % 'db2inst1')

# Run by the entrypoint of the image after every setup: the container is
# new after a restore or an upgrade, and its users are those of the
# operating system.
USERS_SCRIPT_CONTENT = """#!/bin/bash
# Written by the Trove guest agent. Gives the container the users Trove
# made, with their passwords: the users of Db2 are those of the system.
USERS=%(users)s
[ -f "$USERS" ] || exit 0
while IFS=: read -r name hash admin; do
    [ -n "$name" ] || continue
    if ! id "$name" >/dev/null 2>&1; then
        useradd -M -s /sbin/nologin "$name"
    fi
    usermod -p "$hash" "$name"
    if [ "$admin" = "1" ]; then
        usermod -a -G %(group)s "$name"
    fi
done < "$USERS"
"""


def quote_identifier(name):
    return '"%s"' % str(name).replace('"', '""')


class DB2App(service.BaseDbApp):
    _configuration_manager = None

    HEALTHCHECK = {
        "test": ["CMD-SHELL",
                 "test -f %s && su - %s -c 'db2gcf -s -p 0 -i %s' "
                 ">/dev/null" % (SETUP_COMPLETE, INSTANCE_OWNER,
                                 INSTANCE_OWNER)],
        "start_period": 120 * 1000000000,
        "interval": 15 * 1000000000,
        "timeout": 15 * 1000000000,
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
        super(DB2App, self).__init__(status, docker_client)
        self.mount_point = cfg.get_configuration_property('mount_point')
        self.adm = DB2Admin(self)

    def trove_path(self, *names):
        """A path of Trove's directory on the volume, as the guest sees
        it.
        """
        return os.path.join(self.mount_point, TROVE_DIR, *names)

    @staticmethod
    def container_path(*names):
        """The same path, as the container sees it."""
        return os.path.join(CONTAINER_DATA_DIR, TROVE_DIR, *names)

    ################
    # Configuration
    ################

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def remove_overrides(self):
        self.configuration_manager.remove_user_override()

    def apply_overrides(self, overrides):
        for name, value in overrides.items():
            self.adm.set_parameter(name, value)

    def apply_settings(self):
        """Tell the instance the parameters of the configuration file. It
        keeps them in its configuration on the volume.
        """
        for name, value in \
                self.configuration_manager.parse_configuration().items():
            self.adm.set_parameter(name, value)

    ##############
    # Admin user
    ##############

    def _secret_path(self):
        return f'{HOST_CONF_DIR}/{ADMIN_SECRET}'

    def has_admin(self):
        return operating_system.exists(self._secret_path(), as_root=True)

    def secure(self, password=None):
        """Decide the password of the instance owner. The image gives it
        to the owner when it first sets the instance up.
        """
        password = password or utils.generate_random_password()
        LOG.info('Configuring the Trove admin.')
        operating_system.ensure_directory(HOST_CONF_DIR, as_root=True)
        operating_system.write_file(self._secret_path(), password,
                                    as_root=True)
        operating_system.chmod(self._secret_path(), FileMode.SET_USR_RW,
                               as_root=True)

    @property
    def admin_password(self):
        return operating_system.read_file(self._secret_path(),
                                          as_root=True)

    ############
    # Commands
    ############

    def execute(self, command, environment=None, ok_codes=(0,)):
        """Run a command in the database container, as root, and return
        what it wrote to its standard output.
        """
        container = self.docker_client.containers.get('database')
        code, output = container.exec_run(
            command, environment=environment, demux=True)
        stdout, stderr = (stream.decode() if stream else ''
                          for stream in output)
        if code not in ok_codes:
            raise exception.TroveError(
                _("Command %(command)s failed with %(code)s: %(error)s")
                % {'command': command[0], 'code': code,
                   'error': (stderr or stdout).strip()[-800:]})
        return stdout

    def as_owner(self, command):
        """Run a fixed command line as the instance owner, in its
        environment. The command line processor exits with 1 when a query
        finds no row and with 2 on a warning; 4 and 8 are errors.
        """
        return self.execute(['su', '-', INSTANCE_OWNER, '-c', command],
                            ok_codes=CLP_OK)

    def write_run_file(self, content):
        """Write a file only the instance owner reads into Trove's
        directory on the volume, and return its path in the container.
        """
        name = uuid.uuid4().hex
        path = self.trove_path(RUN_DIR, name)
        operating_system.write_file(path, content, as_root=True)
        operating_system.chown(path, self.database_service_uid,
                               self.database_service_gid, as_root=True)
        operating_system.chmod(path, FileMode.SET_USR_RW, as_root=True)
        return path, self.container_path(RUN_DIR, name)

    ############
    # Lifecycle
    ############

    def _ensure_directories(self):
        operating_system.ensure_directory(HOST_CUSTOM_DIR, force=True,
                                          as_root=True)
        operating_system.ensure_directory(self.mount_point, force=True,
                                          as_root=True)
        for name in (RUN_DIR, BACKUP_DIR, RESTORE_DIR, ARCHIVE_DIR):
            operating_system.ensure_directory(
                self.trove_path(name), user=self.database_service_uid,
                group=self.database_service_gid, force=True, as_root=True)

    def write_users_script(self):
        path = f'{HOST_CUSTOM_DIR}/{USERS_SCRIPT}'
        operating_system.write_file(path, USERS_SCRIPT_CONTENT % {
            'users': self.container_path(USERS_FILE),
            'group': INSTANCE_GROUP}, as_root=True)
        operating_system.chmod(path, FileMode.SET_FULL, as_root=True)

    def start_db(self, update_db=False, ds_version=None, command=None,
                 extra_volumes=None):
        """Start the container and wait until the instance is available.

        The entrypoint of the image creates the users and the instance on
        a new volume, and on every start takes up what the volume has.
        """
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        ds_version = ds_version or CONF.datastore_version
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')

        self._ensure_directories()
        self.write_users_script()

        volumes = {
            self.mount_point: {'bind': CONTAINER_DATA_DIR, 'mode': 'rw'},
            HOST_CUSTOM_DIR: {'bind': CONTAINER_CUSTOM_DIR, 'mode': 'ro'},
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

        # The image accepts its license only when told so; the password
        # is the owner's when the image creates it.
        environment = {
            'LICENSE': 'accept',
            'DB2INSTANCE': INSTANCE_OWNER,
            'DB2INST1_PASSWORD': self.admin_password,
            'PERSISTENT_HOME': 'true',
            'ARCHIVE_LOGS': 'true',
        }
        try:
            docker_util.start_container(
                self.docker_client,
                image,
                volumes=volumes,
                network_mode=network_mode,
                ports=ports,
                environment=environment,
                healthcheck=self.HEALTHCHECK,
                command=command or '',
                cap_add=CAPABILITIES,
                ulimits=[docker.types.Ulimit(name='nofile', soft=OPEN_FILES,
                                             hard=OPEN_FILES)]
            )
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        self._wait_until_healthy(update_db)

    def _wait_until_healthy(self, update_db=True):
        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            max(CONF.state_change_wait_time, START_TIMEOUT), update_db
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
        self._wait_until_healthy()
        LOG.info("Finished restarting database")

    ##########
    # Backup
    ##########

    def _backup_volumes(self, name):
        path = self.trove_path(name)
        return {path: {'bind': path, 'mode': 'rw'}}

    def create_backup(self, context, backup_info):
        """The instance writes an online backup image of every database,
        with the logs that make it consistent, into Trove's directory on
        the volume, and the backup container packs that directory with
        the users. The databases stay available.
        """
        backup_dir = self.trove_path(BACKUP_DIR)
        operating_system.remove_dir_contents(backup_dir)
        try:
            self.adm.backup_databases(self.container_path(BACKUP_DIR))
            operating_system.copy(self.trove_path(USERS_FILE),
                                  f'{backup_dir}/{USERS_FILE}',
                                  preserve=True, as_root=True)
            super(DB2App, self).create_backup(
                context, backup_info,
                volumes_mapping=self._backup_volumes(BACKUP_DIR),
                need_dbuser=False,
                extra_params=f'--db-datadir={backup_dir}')
        finally:
            operating_system.remove_dir_contents(backup_dir)

    def restore_backup(self, context, backup_info, restore_location):
        """Unpack a backup into Trove's directory on the volume. The
        databases are restored from it once the instance is up.
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
        restore_dir = self.trove_path(RESTORE_DIR)

        command = (
            f'python3 main.py --nobackup '
            f'--storage-driver={storage_driver} --driver={backup_driver} '
            f'--os-token={user_token} --swift-url={swift_url} '
            f'--restore-from={backup_info["location"]} '
            f'--restore-checksum={backup_info["checksum"]} '
            f'--db-datadir={restore_dir}'
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
            volumes=self._backup_volumes(RESTORE_DIR), command=command)
        result = output[-1]
        if not ret:
            msg = f'Failed to run restore container, error: {result}'
            LOG.error(msg)
            raise Exception(msg)

    def restore_databases(self):
        """Restore the databases of an unpacked backup on the running
        instance, then give the container the users of the backup.
        """
        restore_dir = self.trove_path(RESTORE_DIR)
        operating_system.chown(restore_dir, self.database_service_uid,
                               self.database_service_gid, force=True,
                               as_root=True)
        images = [name for name in self.execute(
            ['ls', '-1', self.container_path(RESTORE_DIR)]).split()
            if BACKUP_IMAGE.match(name)]
        # An instance without databases has a backup of its users alone.
        for image in sorted(images):
            self.adm.restore_database(image,
                                      self.container_path(RESTORE_DIR))
        users = f'{restore_dir}/{USERS_FILE}'
        if operating_system.exists(users, as_root=True):
            operating_system.copy(users, self.trove_path(USERS_FILE),
                                  preserve=True, as_root=True)
            self.adm.apply_users()
        operating_system.remove_dir_contents(restore_dir)

    ###########
    # License
    ###########

    def write_license(self, content):
        """Keep a license file where the instance reads it."""
        # A license file is text, and write_file writes text.
        if isinstance(content, bytes):
            content = content.decode()
        operating_system.ensure_directory(
            self.trove_path(), force=True, as_root=True)
        path = self.trove_path(LICENSE_FILE)
        operating_system.write_file(path, content, as_root=True)
        operating_system.chmod(path, FileMode.SET_USR_RW, as_root=True)

    def install_license(self):
        """Install the license the guest keeps and have the image keep it
        on the volume, from where it takes it up when the container is
        new. Returns what the instance then says of its license.
        """
        path = self.container_path(LICENSE_FILE)
        self.execute(['/opt/ibm/db2/V12.1/adm/db2licm', '-a', path])
        self.execute(['/var/db2_setup/lib/backup_cfg.sh'])
        return self.as_owner('db2licm -l')


class DB2Admin(object):
    """Administrative operations as the instance owner, with the command
    line processor in the database container.

    The statements go into a file only the owner reads, so that no name
    or password passes through a shell. The users of Db2 are users of the
    container's operating system; Trove keeps the ones it made, with
    their password hashes, on the volume, and the container gets them
    again whenever it is new.
    """

    def __init__(self, app):
        self.app = app

    #######
    # SQL
    #######

    def run(self, statements, database=None):
        """Run statements, connected to a database if one is given, and
        return the rows marked as such in what they print.
        """
        lines = []
        if database:
            lines.append('CONNECT TO %s;' % database)
        lines += ['%s;' % statement for statement in statements]
        if database:
            lines.append('CONNECT RESET;')
        host_path, path = self.app.write_run_file('\n'.join(lines) + '\n')
        try:
            output = self.app.as_owner('db2 -txf %s' % path)
        finally:
            operating_system.remove(host_path, force=True, as_root=True)
        rows = []
        for line in output.splitlines():
            if line.startswith(ROW + '\t'):
                rows.append([field.strip()
                             for field in line.split('\t')[1:]])
        return rows

    @staticmethod
    def _query(*columns):
        """A select list that marks its rows."""
        return "'%s' || CHR(9) || %s" % (
            ROW, " || CHR(9) || ".join(columns))

    ##############
    # Parameters
    ##############

    def set_parameter(self, name, value):
        if isinstance(value, bool):
            value = 'YES' if value else 'NO'
        if not re.match(r'^[A-Za-z0-9_.-]+$', str(value)):
            raise exception.BadRequest(
                _("Value %(value)s of %(name)s is not valid.")
                % {'name': name, 'value': value})
        self.app.as_owner('db2 UPDATE DBM CFG USING %s %s IMMEDIATE'
                          % (name, value))

    #############
    # Databases
    #############

    def _database_names(self):
        """The databases of the instance, as their names are."""
        try:
            output = self.app.as_owner('db2 list database directory')
        except exception.TroveError as e:
            # Until the first database there is no directory.
            if 'SQL1031N' in str(e):
                return []
            raise
        names, name = [], None
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Database name'):
                name = line.split('=', 1)[1].strip()
            elif line.startswith('Directory entry type') and name:
                if line.split('=', 1)[1].strip() == 'Indirect':
                    names.append(name)
                name = None
        ignored = [db.upper() for db in cfg.get_ignored_dbs()]
        return sorted(n for n in names if n.upper() not in ignored)

    def create_database(self, databases):
        """Create databases ready for online backups: with their logs
        archived, after the full backup the server then asks for.
        """
        archive = self.app.container_path(ARCHIVE_DIR)
        for item in databases:
            database = models.DB2Schema.deserialize(item)
            database.check_create()
            name = database.name.upper()
            LOG.info("Creating database '%s'.", name)
            self.app.as_owner('db2 CREATE DATABASE %s' % name)
            self.app.as_owner('db2 UPDATE DB CFG FOR %s USING LOGARCHMETH1 '
                              'DISK:%s' % (name, archive))
            self.app.as_owner('db2 BACKUP DATABASE %s TO /dev/null' % name)

    def delete_database(self, database):
        database = models.DB2Schema.deserialize(database)
        database.check_delete()
        name = database.name.upper()
        LOG.info("Dropping database '%s'.", name)
        self.app.as_owner('db2 DEACTIVATE DATABASE %s; db2 DROP DATABASE %s'
                          % (name, name))

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [models.DB2Schema(name) for name in self._database_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    #########
    # Users
    #########

    def _users_path(self):
        return self.app.trove_path(USERS_FILE)

    def _read_users(self):
        """The users Trove made: name to (hash, admin)."""
        path = self._users_path()
        if not operating_system.exists(path, as_root=True):
            return {}
        users = {}
        for line in operating_system.read_file(
                path, as_root=True).splitlines():
            if line.strip():
                name, hash_, admin = line.split(':')
                users[name] = (hash_, admin == '1')
        return users

    def _write_users(self, users):
        content = ''.join('%s:%s:%s\n' % (name, hash_, '1' if admin else '0')
                          for name, (hash_, admin) in sorted(users.items()))
        path = self._users_path()
        operating_system.write_file(path, content, as_root=True)
        operating_system.chmod(path, FileMode.SET_USR_RW, as_root=True)

    def _password_hash(self, username):
        line = self.app.execute(['getent', 'shadow', username]).strip()
        return line.split(':')[1]

    def apply_users(self):
        """Give the container the users of the file."""
        self.app.execute(['/bin/bash', os.path.join(CONTAINER_CUSTOM_DIR,
                                                    USERS_SCRIPT)])

    def _set_system_user(self, username, password, admin=False):
        """Create or update a user of the container, and keep it."""
        users = self._read_users()
        if username not in users:
            try:
                self.app.execute(['useradd', '-M', '-s', '/sbin/nologin',
                                  username])
            except exception.TroveError as e:
                if 'already exists' not in str(e):
                    raise
        host_path, path = self.app.write_run_file(
            '%s:%s\n' % (username, password))
        try:
            self.app.execute(['sh', '-c', 'chpasswd < %s' % path])
        finally:
            operating_system.remove(host_path, force=True, as_root=True)
        if admin:
            self.app.execute(['usermod', '-a', '-G', INSTANCE_GROUP,
                              username])
        users[username] = (self._password_hash(username), admin)
        self._write_users(users)

    def _remove_system_user(self, username):
        users = self._read_users()
        users.pop(username, None)
        self._write_users(users)
        try:
            self.app.execute(['userdel', username])
        except exception.TroveError as e:
            if 'does not exist' not in str(e):
                raise

    def _check_modifiable(self, username):
        if username in cfg.get_ignored_users() or \
                username == models.DB2User.root_username:
            raise exception.BadRequest(
                _("User %s is reserved.") % username)

    def _user_names(self):
        ignored = cfg.get_ignored_users() + [models.DB2User.root_username]
        return sorted(name for name in self._read_users()
                      if name not in ignored)

    def _databases_of(self, username):
        """The databases the user can connect to. A database that cannot
        be connected to for now, one being created among them, is left
        out instead of failing the whole answer.
        """
        databases = []
        for database in self._database_names():
            try:
                rows = self.run(["SELECT %s FROM SYSIBM.SYSDBAUTH WHERE "
                                 "GRANTEE = '%s' AND CONNECTAUTH = 'Y'"
                                 % (self._query("GRANTEE"),
                                    username.upper())],
                                database=database)
            except exception.TroveError as e:
                if any(code in str(e) for code in UNAVAILABLE):
                    LOG.warning("Database %s is not available for now, "
                                "left out: %s", database, e)
                    continue
                raise
            if rows:
                databases.append(database)
        return databases

    def _build_user(self, username):
        user = models.DB2User(username)
        for database in self._databases_of(username):
            user.databases = database
        return user

    def _grant(self, username, database):
        self.run(['GRANT %s ON DATABASE TO USER %s'
                  % (AUTHORITIES, quote_identifier(username.upper()))],
                 database=database.upper())

    def _revoke(self, username, database):
        self.run(['REVOKE %s ON DATABASE FROM USER %s'
                  % (AUTHORITIES, quote_identifier(username.upper()))],
                 database=database.upper())

    def create_user(self, users):
        for item in users:
            user = models.DB2User.deserialize(item)
            self._check_modifiable(user.name)
            LOG.info("Creating user '%s'.", user.name)
            self._set_system_user(user.name, user.password)
            for database in user.databases:
                self._grant(user.name,
                            models.DB2Schema.deserialize(database).name)

    def delete_user(self, user):
        """Revoke the user's authorities everywhere first: a later user
        of the same name would have them.
        """
        user = models.DB2User.deserialize(user)
        self._check_modifiable(user.name)
        if user.name not in self._read_users():
            return
        for database in self._databases_of(user.name):
            self._revoke(user.name, database)
        LOG.info("Deleting user '%s'.", user.name)
        self._remove_system_user(user.name)

    def list_users(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [self._build_user(name) for name in self._user_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    def get_user(self, username, hostname=None):
        if username not in self._user_names():
            return None
        return self._build_user(username).serialize()

    def _existing_user(self, username):
        self._check_modifiable(username)
        if username not in self._read_users():
            raise exception.UserNotFound(uuid=username)

    def grant_access(self, username, hostname, databases):
        self._existing_user(username)
        for name in databases:
            models.DB2Schema(name).check_reserved()
            self._grant(username, name)

    def revoke_access(self, username, hostname, database):
        self._existing_user(username)
        models.DB2Schema(database).check_reserved()
        self._revoke(username, database)

    def list_access(self, username, hostname=None):
        self._existing_user(username)
        return self._build_user(username).databases

    def change_passwords(self, users):
        for item in users:
            user = models.DB2User.deserialize(item)
            self._existing_user(user.name)
            LOG.info("Changing password of user '%s'.", user.name)
            self._set_system_user(user.name, user.password)

    def update_attributes(self, username, hostname, user_attrs):
        """A user is a user of the system, with authorities granted to
        its name: it cannot be renamed.
        """
        new_name = user_attrs.get('name')
        if new_name is not None and new_name != username:
            raise exception.UnprocessableEntity(
                _("The name of a Db2 user cannot be changed."))
        if user_attrs.get('password') is not None:
            self._existing_user(username)
            self._set_system_user(username, user_attrs['password'])

    ########
    # Root
    ########

    def enable_root(self, root_password=None):
        """Root is a user in the group of the instance owner, which has
        the system administration authority over the instance.
        """
        root = models.DB2User.root(password=root_password)
        self._set_system_user(root.name, root.password, admin=True)
        return root.serialize()

    def is_root_enabled(self):
        return models.DB2User.root_username in self._read_users()

    def disable_root(self):
        if self.is_root_enabled():
            self._remove_system_user(models.DB2User.root_username)

    ##########
    # Backup
    ##########

    def backup_databases(self, directory):
        for database in self._database_names():
            LOG.info("Backing up database '%s'.", database)
            self.app.as_owner('db2 BACKUP DATABASE %s ONLINE TO %s COMPRESS '
                              'INCLUDE LOGS' % (database, directory))

    def restore_database(self, image, directory):
        """Restore a database from its image, as of the end of the
        backup.
        """
        match = BACKUP_IMAGE.match(image)
        if not match:
            raise exception.TroveError(
                _("%s is not a backup image of a database.") % image)
        database, timestamp = match.groups()
        logs = '%s/logs_%s' % (directory, database)
        self.app.execute(['mkdir', '-p', logs])
        self.app.execute(['chown', '%s:%s' % (INSTANCE_OWNER,
                                              INSTANCE_GROUP), logs])
        LOG.info("Restoring database '%s'.", database)
        self.app.as_owner('db2 RESTORE DATABASE %s FROM %s TAKEN AT %s '
                          'LOGTARGET %s WITHOUT PROMPTING'
                          % (database, directory, timestamp, logs))
        self.app.as_owner("db2 \"ROLLFORWARD DATABASE %s TO END OF BACKUP "
                          "AND COMPLETE OVERFLOW LOG PATH (%s)\""
                          % (database, logs))
        self.app.as_owner('db2 UPDATE DB CFG FOR %s USING LOGARCHMETH1 '
                          'DISK:%s' % (database, self.app.container_path(
                              ARCHIVE_DIR)))
