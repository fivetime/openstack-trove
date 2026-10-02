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

import bcrypt
from oslo_log import log as logging
from oslo_utils import netutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.cassandra import models
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

# The directory of the guest with the files Trove writes. It is not mounted
# over the configuration directory of the image, which holds a dozen files
# the server needs and Trove does not manage; the server is pointed at the
# two files instead.
HOST_CONF_DIR = '/etc/cassandra'
CONTAINER_CONF_DIR = '/etc/cassandra-trove'
CONFIG_FILE_NAME = 'cassandra.yaml'
TOPOLOGY_FILE_NAME = 'cassandra-rackdc.properties'
CREDENTIALS_FILE_NAME = 'credentials'

CONFIG_FILE = f'{HOST_CONF_DIR}/{CONFIG_FILE_NAME}'
TOPOLOGY_FILE = f'{HOST_CONF_DIR}/{TOPOLOGY_FILE_NAME}'
CREDENTIALS_FILE = f'{HOST_CONF_DIR}/{CREDENTIALS_FILE_NAME}'

ADMIN_USER = 'os_admin'
# What a server creates on its first start.
DEFAULT_SUPERUSER = 'cassandra'
DEFAULT_SUPERUSER_PASSWORD = 'cassandra'

PASSWORD_AUTHENTICATOR = 'org.apache.cassandra.auth.PasswordAuthenticator'
CASSANDRA_AUTHORIZER = 'org.apache.cassandra.auth.CassandraAuthorizer'
ALLOW_ALL_AUTHENTICATOR = 'org.apache.cassandra.auth.AllowAllAuthenticator'
ALLOW_ALL_AUTHORIZER = 'org.apache.cassandra.auth.AllowAllAuthorizer'
SEED_PROVIDER = 'org.apache.cassandra.locator.SimpleSeedProvider'

DEFAULT_DATA_CENTER = 'dc1'
DEFAULT_RACK = 'rack1'

# Change set of the settings a restored data directory is started with
# once, before it becomes this instance.
CNF_RESTORE = 'restore'

# The replicas of the keyspace with the roles and of a keyspace created
# through Trove on a cluster: all members, up to this many.
MAX_REPLICATION_FACTOR = 3


class CassandraApp(service.BaseDbApp):
    _configuration_manager = None

    # The native transport is the last thing a server starts.
    HEALTHCHECK = {
        "test": ["CMD-SHELL", "nodetool statusbinary | grep -q '^running'"],
        "start_period": 60 * 1000000000,
        "interval": 10 * 1000000000,
        "timeout": 10 * 1000000000,
        "retries": 3
    }

    _TOPOLOGY_CODEC = stream_codecs.PropertiesCodec(
        delimiter='=', unpack_singletons=True, string_mappings={
            'true': True, 'false': False})

    @property
    def configuration_manager(self):
        if self._configuration_manager:
            return self._configuration_manager

        revision_dir = guestagent_utils.build_file_path(
            HOST_CONF_DIR,
            configuration.ConfigurationManager.
            DEFAULT_STRATEGY_OVERRIDES_SUB_DIR)
        self._configuration_manager = configuration.ConfigurationManager(
            CONFIG_FILE,
            self.database_service_uid,
            self.database_service_gid,
            stream_codecs.SafeYamlCodec(default_flow_style=False),
            requires_root=True,
            override_strategy=configuration.OneFileOverrideStrategy(
                revision_dir)
        )
        return self._configuration_manager

    def __init__(self, status, docker_client):
        super(CassandraApp, self).__init__(status, docker_client)
        self.mount_point = cfg.get_configuration_property('mount_point')
        self.datadir = f'{self.mount_point}/data'
        self.adm = CassandraAdmin(self)

    ############
    # Commands
    ############

    def execute(self, command, check=True):
        """Run a command in the database container.

        :returns: what the command wrote to its standard output
        """
        container = self.docker_client.containers.get('database')
        # The tools keep a history in the home directory, which the
        # database user of the image has none it can write to.
        code, output = container.exec_run(
            command, environment={'HOME': '/tmp'}, demux=True)
        stdout, stderr = (stream.decode() if stream else ''
                          for stream in output)
        if check and code != 0:
            raise exception.TroveError(
                _("Command %(command)s failed with %(code)s: %(error)s")
                % {'command': command[0], 'code': code,
                   'error': (stderr or stdout).strip()[-500:]})
        return stdout

    def nodetool(self, *args):
        return self.execute(['nodetool'] + list(args))

    ##########
    # Address
    ##########

    @property
    def address(self):
        """The address the clients and the other nodes reach this one at.

        With the user's network interface handed to the container the
        guest agent does not have it; it is on record for the container.
        """
        if CONF.network_isolation and \
                os.path.exists(constants.ETH1_CONFIG_PATH):
            with open(constants.ETH1_CONFIG_PATH) as fd:
                address = json.load(fd).get('ipv4_address')
            if address:
                return address
        return netutils.get_my_ipv4()

    ################
    # Configuration
    ################

    def get_config_value(self, name, default=None):
        config = self.configuration_manager.parse_configuration() or {}
        return config.get(name, default)

    @property
    def is_cluster_member(self):
        return self.get_config_value('endpoint_snitch') == \
            'GossipingPropertyFileSnitch'

    def apply_initial_guestagent_configuration(self, cluster_name=None):
        """Settings the guest agent decides.

        A single instance is a cluster of its own, named after the
        instance, and may use the SimpleSnitch. The members of a cluster
        share the name of the cluster, which is what keeps the nodes of
        one cluster from talking to another, and need a snitch that knows
        about data centers and racks.
        """
        mount_point = self.mount_point
        self.configuration_manager.apply_system_override({
            'cluster_name': cluster_name or CONF.guest_id,
            'endpoint_snitch': ('GossipingPropertyFileSnitch' if cluster_name
                                else 'SimpleSnitch'),
            'data_file_directories': [self.datadir],
            'commitlog_directory': f'{mount_point}/commitlog',
            'saved_caches_directory': f'{mount_point}/saved_caches',
            'hints_directory': f'{mount_point}/hints',
            'cdc_raw_directory': f'{mount_point}/cdc_raw',
        })
        self._enable_authentication()
        self._enable_remote_access()

    def _enable_authentication(self):
        self.configuration_manager.apply_system_override({
            'authenticator': PASSWORD_AUTHENTICATOR,
            'authorizer': CASSANDRA_AUTHORIZER,
        })

    def _enable_remote_access(self):
        """rpc_address is where clients connect, all interfaces;
        broadcast_rpc_address is what the node tells them to connect to and
        listen_address where the other nodes reach it, neither of which
        can be all interfaces.
        """
        address = self.address
        self.configuration_manager.apply_system_override({
            'rpc_address': '0.0.0.0',
            'broadcast_rpc_address': address,
            'listen_address': address,
            'seed_provider': self._seed_provider([address]),
        })

    @staticmethod
    def _seed_provider(seeds):
        return [{'class_name': SEED_PROVIDER,
                 'parameters': [{'seeds': ','.join(seeds)}]}]

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def apply_overrides(self, overrides):
        # Every parameter takes effect with a restart.
        pass

    def write_cluster_topology(self, data_center, rack, prefer_local=True):
        LOG.info('Saving the cluster topology: %s, %s', data_center, rack)
        operating_system.write_file(
            TOPOLOGY_FILE,
            {'dc': data_center, 'rack': rack, 'prefer_local': prefer_local},
            codec=self._TOPOLOGY_CODEC, as_root=True)
        operating_system.chown(
            TOPOLOGY_FILE, self.database_service_uid,
            self.database_service_gid, as_root=True)
        operating_system.chmod(
            TOPOLOGY_FILE, FileMode.ADD_READ_ALL, as_root=True)

    def _read_topology(self):
        return operating_system.read_file(
            TOPOLOGY_FILE, codec=self._TOPOLOGY_CODEC, as_root=True)

    def get_data_center(self):
        return self._read_topology()['dc']

    def get_rack(self):
        return self._read_topology()['rack']

    def set_seeds(self, seeds):
        LOG.debug("Setting seed nodes: %s", seeds)
        self.configuration_manager.apply_system_override(
            {'seed_provider': self._seed_provider(sorted(seeds))})

    def get_seeds(self):
        for provider in self.get_config_value('seed_provider', []):
            for parameter in provider.get('parameters', []):
                if parameter.get('seeds'):
                    return parameter['seeds'].split(',')
        return []

    def set_auto_bootstrap(self, enabled):
        """A node that bootstraps takes over a share of the data from the
        nodes it joins. The nodes a cluster starts from have no data to
        take over and are told not to; a node that joins later has to.
        """
        LOG.debug("Setting auto-bootstrapping: %s", enabled)
        self.configuration_manager.apply_system_override(
            {'auto_bootstrap': enabled})

    ############
    # Lifecycle
    ############

    def _ensure_directories(self):
        for folder in (HOST_CONF_DIR, self.mount_point, self.datadir):
            operating_system.ensure_directory(
                folder, user=self.database_service_uid,
                group=self.database_service_gid, force=True,
                as_root=True)

    def _wait_until_healthy(self, update_db):
        """A node that joins a cluster is healthy once it has taken over
        its share of the data, which takes as long as there is data.
        """
        if self.status.wait_for_status(
                service_status.ServiceStatuses.HEALTHY,
                CONF.state_change_wait_time, update_db):
            return True
        if docker_util.get_container_status(self.docker_client) != 'running':
            return False
        LOG.info("The database service is still starting, waiting on.")
        return self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.restore_usage_timeout, update_db)

    def start_db(self, update_db=False, ds_version=None, command=None,
                 extra_volumes=None):
        """Start and wait for the database service."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        ds_version = ds_version or CONF.datastore_version
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')
        if not command:
            # The entrypoint of the image rewrites the seeds and the
            # addresses in the configuration every time it starts the
            # command 'cassandra'. Given a path it starts it untouched.
            command = (
                '/opt/cassandra/bin/cassandra -f '
                f'-Dcassandra.config=file://{CONTAINER_CONF_DIR}/'
                f'{CONFIG_FILE_NAME} '
                '-Dcassandra-rackdc.properties='
                f'file://{CONTAINER_CONF_DIR}/{TOPOLOGY_FILE_NAME}')

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
                environment={'HOME': '/tmp'},
                healthcheck=self.HEALTHCHECK,
                command=command
            )
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        if not self._wait_until_healthy(update_db):
            raise exception.TroveError("Failed to start database service")

    def restart(self):
        """Restart the service, or start it: the members of a cluster are
        configured by prepare and first started by the taskmanager, with
        this call.
        """
        LOG.info("Restarting database")
        self._ensure_directories()
        try:
            self.docker_client.containers.get('database')
        except Exception:
            self.start_db(update_db=True)
            return

        try:
            docker_util.restart_container(self.docker_client)
        except Exception:
            LOG.exception("Failed to restart database")
            raise exception.TroveError("Failed to restart database")

        if not self._wait_until_healthy(update_db=True):
            raise exception.TroveError("Failed to start database")

        LOG.info("Finished restarting database")

    ##############
    # Admin user
    ##############

    def _write_credentials(self, username, password):
        """Keep the credentials for the guest agent and, in the form the
        shell reads them, for the container. The shell refuses a file that
        is not its user's own or that others can read.
        """
        self.save_password(ADMIN_USER, password)
        operating_system.write_file(
            CREDENTIALS_FILE,
            '[PlainTextAuthProvider]\n'
            f'username = {username}\npassword = {password}\n',
            as_root=True)
        operating_system.chown(
            CREDENTIALS_FILE, self.database_service_uid,
            self.database_service_gid, as_root=True)
        operating_system.chmod(
            CREDENTIALS_FILE, FileMode.SET_USR_RW, as_root=True)

    @property
    def admin_password(self):
        return self.get_auth_password()

    def secure(self, password=None):
        """Replace the superuser every server starts with by Trove's own.

        The built-in superuser is created a moment after the server
        accepts connections, so its first use is retried.
        """
        password = password or utils.generate_random_password()
        LOG.info('Configuring the Trove superuser.')
        self._write_credentials(DEFAULT_SUPERUSER, DEFAULT_SUPERUSER_PASSWORD)
        utils.poll_until(self.adm.is_available, sleep_time=5,
                         time_out=CONF.state_change_wait_time)
        self.adm.create_superuser(ADMIN_USER, password)
        self._write_credentials(ADMIN_USER, password)
        self.adm.drop_role(DEFAULT_SUPERUSER)
        return password

    def get_admin_credentials(self):
        return models.CassandraUser(
            ADMIN_USER, self.admin_password).serialize()

    def store_admin_credentials(self, admin_credentials):
        user = models.CassandraUser.deserialize(admin_credentials)
        self._write_credentials(user.name, user.password)

    def cluster_secure(self, password):
        """Give the cluster its superuser, from its first node, and keep
        the roles on more than one node.

        The keyspace with the roles starts with one replica. A cluster
        with any of its nodes down then cannot look up some role or
        permission, and nobody can log in on the nodes that are up.
        """
        self.secure(password)
        replicas = min(MAX_REPLICATION_FACTOR, self.count_nodes())
        self.adm.set_replication(
            'system_auth', self.get_data_center(), replicas)
        self.repair_auth()
        return self.get_admin_credentials()

    def count_nodes(self):
        """The nodes of the ring, from the lines of the status listing that
        start with the state of a node.
        """
        return len([line for line in self.nodetool('status').splitlines()
                    if line[:2] in ('UN', 'UL', 'UJ', 'UM',
                                    'DN', 'DL', 'DJ', 'DM')])

    def repair_auth(self):
        """Bring the replicas of the roles up to date. Every node does it
        for the ranges it holds when the cluster is complete.
        """
        try:
            self.nodetool('repair', '--full', 'system_auth')
        except exception.TroveError:
            LOG.exception("Could not repair the roles keyspace.")

    #################
    # Cluster member
    #################

    def node_cleanup_begin(self):
        """Mark the node busy for the cleanup that follows."""
        self.status.set_status(service_status.ServiceStatuses.BLOCKED)

    def node_cleanup(self):
        """Remove the data that went to a node which has joined since.

        A failed cleanup is not fatal: the node keeps serving, with data
        it no longer owns still on disk.
        """
        LOG.debug("Running node cleanup.")
        try:
            self.nodetool('cleanup')
        except Exception:
            LOG.exception("The node failed to complete its cleanup.")
        self.status.set_status(self.status.get_actual_db_status())

    def node_decommission(self):
        """Hand the data of this node to the rest of the ring and stop.

        The taskmanager waits for the service to be shut down.
        """
        LOG.debug("Decommissioning the node.")
        try:
            self.nodetool('decommission')
        except Exception:
            LOG.exception("The node failed to decommission itself.")
            self.status.set_status(service_status.ServiceStatuses.FAILED)
            return

        self.stop_db(update_db=True)

    ##########
    # Backup
    ##########

    def create_backup(self, context, backup_info):
        """A backup is a snapshot of every table, which the server takes
        and the backup container packs.
        """
        snapshot = backup_info['id']
        with self.backup_preparation(context, backup_info):
            self.nodetool('clearsnapshot', '-t', snapshot)
            self.nodetool('snapshot', '-t', snapshot)
        try:
            super(CassandraApp, self).create_backup(
                context, backup_info,
                volumes_mapping={
                    self.datadir: {'bind': self.datadir, 'mode': 'rw'}},
                need_dbuser=False,
                extra_params=f'--db-datadir={self.datadir}')
        finally:
            self.nodetool('clearsnapshot', '-t', snapshot)

    def restore_backup(self, context, backup_info, restore_location):
        """Unpack a backup into the empty data directory."""
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
            volumes={self.datadir: {'bind': self.datadir, 'mode': 'rw'}},
            command=command)
        result = output[-1]
        if not ret:
            msg = f'Failed to run restore container, error: {result}'
            LOG.error(msg)
            raise Exception(msg)

        operating_system.chown(
            self.mount_point, self.database_service_uid,
            self.database_service_gid, force=True, as_root=True)

    def apply_post_restore_updates(self, backup_info, ds_version=None):
        """Make the restored data this instance's own.

        The data carries the name of the cluster it was taken from, the
        instance the backup was made on, and a server refuses to start
        under another name. It also carries that instance's superuser,
        whose password this one does not have.

        The server is started once under the old name, without
        authentication and reachable from the instance only, to write the
        new name and the hash of a new password into its system tables.
        """
        LOG.info("Applying post-restore updates to the database.")
        password = utils.generate_random_password()
        salted_hash = bcrypt.hashpw(
            password.encode(), bcrypt.gensalt(10, prefix=b'2a')).decode()

        self.configuration_manager.apply_system_override({
            'cluster_name': backup_info['instance_id'],
            'authenticator': ALLOW_ALL_AUTHENTICATOR,
            'authorizer': ALLOW_ALL_AUTHORIZER,
            'rpc_address': '127.0.0.1',
            'broadcast_rpc_address': '127.0.0.1',
            'listen_address': '127.0.0.1',
            'seed_provider': self._seed_provider(['127.0.0.1']),
        }, CNF_RESTORE)
        try:
            self.start_db(ds_version=ds_version)
            utils.poll_until(
                lambda: self.adm.is_available(authenticate=False),
                sleep_time=5, time_out=CONF.state_change_wait_time)
            self.adm.reset_superuser(ADMIN_USER, salted_hash)
            self.adm.set_cluster_name(CONF.guest_id)
            self.nodetool('flush', 'system')
            self.nodetool('flush', 'system_auth')
        finally:
            self.stop_db()
            self.configuration_manager.remove_system_override(CNF_RESTORE)

        self._write_credentials(ADMIN_USER, password)


class CassandraAdmin(object):
    """Administrative operations, with the shell of the database container.

    The native transport has no socket the container could share with the
    guest agent, and with the user's network interface handed to the
    container the guest agent cannot reach its address either.

    Only a superuser can create roles and grant permissions. The roles
    created here are ordinary ones and the permissions granted are the
    ones on a keyspace that do not let their holder grant them on, so
    that nobody becomes a superuser through the Trove API.
    """

    # Everything on a keyspace but AUTHORIZE.
    KEYSPACE_PERMISSIONS = ('ALTER', 'CREATE', 'DROP', 'MODIFY', 'SELECT')

    def __init__(self, app):
        self.app = app

    #########
    # Shell
    #########

    def execute(self, statement, authenticate=True):
        command = ['cqlsh', '--no-color', '--request-timeout=60']
        if authenticate:
            command.append(
                f'--credentials={CONTAINER_CONF_DIR}/{CREDENTIALS_FILE_NAME}')
        command += ['-e', statement]
        return self.app.execute(command)

    def query(self, statement, authenticate=True):
        """Run a SELECT JSON statement and return its rows."""
        rows = []
        for line in self.execute(statement, authenticate).splitlines():
            line = line.strip()
            if line.startswith('{'):
                rows.append(json.loads(line))
        return rows

    def is_available(self, authenticate=True):
        try:
            self.execute('SELECT key FROM system.local', authenticate)
            return True
        except exception.TroveError as e:
            LOG.debug("The database does not take statements yet: %s", e)
            return False

    @staticmethod
    def _quote(value):
        """A string literal: role names and passwords."""
        return "'%s'" % str(value).replace("'", "''")

    @staticmethod
    def _identifier(value):
        """A case-sensitive identifier: keyspace names."""
        return '"%s"' % str(value).replace('"', '""')

    #########
    # Roles
    #########

    def create_superuser(self, name, password):
        self.execute(
            'CREATE ROLE %s WITH PASSWORD = %s AND SUPERUSER = true '
            'AND LOGIN = true' % (self._quote(name), self._quote(password)))

    def drop_role(self, name):
        self.execute('DROP ROLE %s' % self._quote(name))

    def reset_superuser(self, name, salted_hash):
        """On a server without authentication, where roles cannot be
        altered: the password of a role is a hash in a table.
        """
        self.execute(
            "INSERT INTO system_auth.roles (role, can_login, is_superuser, "
            "salted_hash) VALUES (%s, true, true, %s)"
            % (self._quote(name), self._quote(salted_hash)),
            authenticate=False)

    def set_cluster_name(self, name):
        self.execute(
            "UPDATE system.local SET cluster_name = %s WHERE key = 'local'"
            % self._quote(name), authenticate=False)

    def _roles(self):
        return self.query(
            'SELECT JSON role, is_superuser, can_login '
            'FROM system_auth.roles')

    def _acl(self):
        """The keyspaces each role holds a permission on.

        A resource is 'data/<keyspace>' for a keyspace, 'data' for all of
        them and 'data/<keyspace>/<table>' for a table, which is not a
        permission on the keyspace.
        """
        all_keyspaces = None
        acl = {}
        for row in self.query(
                'SELECT JSON role, resource, permissions '
                'FROM system_auth.role_permissions'):
            if not row.get('permissions'):
                continue
            parts = row['resource'].split('/')
            if parts[0] != 'data' or len(parts) > 2:
                continue
            if len(parts) == 1:
                if all_keyspaces is None:
                    all_keyspaces = [ks.name for ks in self._keyspaces()]
                keyspaces = all_keyspaces
            else:
                keyspaces = [parts[1]]
            acl.setdefault(row['role'], set()).update(keyspaces)
        return acl

    def _build_user(self, name, acl):
        user = models.CassandraUser(name)
        ignored = cfg.get_ignored_dbs()
        for keyspace in sorted(acl.get(name, ())):
            if keyspace not in ignored:
                user.databases.append(
                    models.CassandraSchema(keyspace).serialize())
        return user

    def _users(self, matcher):
        acl = self._acl()
        return [self._build_user(role['role'], acl)
                for role in self._roles() if matcher(role)]

    def _listed_users(self):
        ignored = cfg.get_ignored_users()
        return self._users(
            lambda role: role['can_login'] and not role['is_superuser'] and
            role['role'] not in ignored)

    def list_superusers(self):
        return self._users(lambda role: role['is_superuser'])

    def _deserialize_user(self, item):
        user = models.CassandraUser.deserialize(item)
        user.check_reserved()
        return user

    def _deserialize_keyspace(self, item):
        keyspace = models.CassandraSchema.deserialize(item)
        keyspace.check_reserved()
        return keyspace

    def _create_user_and_grant(self, user):
        LOG.debug("Creating a new user '%s'.", user.name)
        self.execute(
            'CREATE ROLE %s WITH PASSWORD = %s AND SUPERUSER = false '
            'AND LOGIN = true'
            % (self._quote(user.name), self._quote(user.password)))
        for item in user.databases:
            self._grant(user.name, self._deserialize_keyspace(item).name)

    def _grant(self, username, keyspace):
        for permission in self.KEYSPACE_PERMISSIONS:
            self.execute('GRANT %s ON KEYSPACE %s TO %s' % (
                permission, self._identifier(keyspace),
                self._quote(username)))

    def create_user(self, users):
        for item in users:
            self._create_user_and_grant(self._deserialize_user(item))

    def delete_user(self, user):
        self.drop_role(self._deserialize_user(user).name)

    def _find_user(self, username):
        return next((user for user in self._listed_users()
                     if user.name == username), None)

    def get_user(self, username, hostname=None):
        user = self._find_user(username)
        return user.serialize() if user is not None else None

    def list_users(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            self._listed_users(),
            limit=limit, marker=marker, include_marker=include_marker)

    def list_access(self, username, hostname=None):
        user = self._find_user(username)
        if user:
            return user.databases
        raise exception.UserNotFound(uuid=username)

    def grant_access(self, username, hostname, databases):
        models.CassandraUser(username).check_reserved()
        for name in databases:
            keyspace = models.CassandraSchema(name)
            keyspace.check_reserved()
            self._grant(username, keyspace.name)

    def revoke_access(self, username, hostname, database):
        models.CassandraUser(username).check_reserved()
        keyspace = models.CassandraSchema(database)
        keyspace.check_reserved()
        self.execute('REVOKE ALL PERMISSIONS ON KEYSPACE %s FROM %s' % (
            self._identifier(keyspace.name), self._quote(username)))

    def alter_password(self, name, password):
        LOG.debug("Changing password of user '%s'.", name)
        self.execute('ALTER ROLE %s WITH PASSWORD = %s' % (
            self._quote(name), self._quote(password)))

    def change_passwords(self, users):
        for item in users:
            user = self._deserialize_user(item)
            self.alter_password(user.name, user.password)

    def update_attributes(self, username, hostname, user_attrs):
        """A new name needs a new role: the permissions of the old one go
        to it and the old one is dropped, which takes a password.
        """
        models.CassandraUser(username).check_reserved()
        user = self._find_user(username)
        if user is None:
            raise exception.UserNotFound(uuid=username)
        new_name = user_attrs.get('name')
        new_password = user_attrs.get('password')
        if new_name is not None and new_name != user.name:
            if new_password is None:
                raise exception.UnprocessableEntity(
                    _("Updating username requires specifying a password "
                      "as well."))
            new_user = models.CassandraUser(new_name, new_password)
            new_user.check_reserved()
            new_user.databases.extend(user.databases)
            self._create_user_and_grant(new_user)
            self.drop_role(user.name)
        elif new_password is not None:
            self.alter_password(user.name, new_password)

    ########
    # Root
    ########

    def enable_root(self, root_password=None):
        """The root user of Cassandra is called 'cassandra'."""
        root = models.CassandraUser.root(password=root_password)
        if any(user.name == root.name for user in self.list_superusers()):
            self.alter_password(root.name, root.password)
        else:
            self.create_superuser(root.name, root.password)
        return root.serialize()

    def is_root_enabled(self):
        """The Trove superuser is normally the only one."""
        return any(user.name != ADMIN_USER
                   for user in self.list_superusers())

    def disable_root(self):
        if self.is_root_enabled():
            self.drop_role(models.CassandraUser.root_username)

    #############
    # Keyspaces
    #############

    def _keyspaces(self):
        ignored = cfg.get_ignored_dbs()
        return [models.CassandraSchema(row['keyspace_name'])
                for row in self.query(
                    'SELECT JSON keyspace_name FROM system_schema.keyspaces')
                if row['keyspace_name'] not in ignored]

    def _replication(self):
        """One replica on a single instance. On a cluster a keyspace with
        one replica loses the rows of any node that is down, so it gets a
        replica on every member, up to three.
        """
        if not self.app.is_cluster_member:
            return "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        return "{'class': 'NetworkTopologyStrategy', %s: %d}" % (
            self._quote(self.app.get_data_center()),
            min(MAX_REPLICATION_FACTOR, self.app.count_nodes()))

    def set_replication(self, keyspace, data_center, replicas):
        self.execute(
            "ALTER KEYSPACE %s WITH replication = "
            "{'class': 'NetworkTopologyStrategy', %s: %d}"
            % (self._identifier(keyspace), self._quote(data_center),
               replicas))

    def create_database(self, databases):
        replication = self._replication()
        for item in databases:
            keyspace = self._deserialize_keyspace(item)
            LOG.debug("Creating keyspace '%s'.", keyspace.name)
            self.execute('CREATE KEYSPACE %s WITH REPLICATION = %s' % (
                self._identifier(keyspace.name), replication))

    def delete_database(self, database):
        keyspace = self._deserialize_keyspace(database)
        LOG.debug("Dropping keyspace '%s'.", keyspace.name)
        self.execute('DROP KEYSPACE %s' % self._identifier(keyspace.name))

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            self._keyspaces(),
            limit=limit, marker=marker, include_marker=include_marker)
