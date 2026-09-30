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
import urllib.parse

from oslo_log import log as logging
from oslo_utils import netutils
import pymongo
from pymongo import errors as pymongo_errors

from trove.common import cfg
from trove.common import constants
from trove.common.db.mongodb import models
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

CONFIG_DIR = '/etc/mongodb'
CONFIG_FILE = f'{CONFIG_DIR}/mongod.conf'
KEY_FILE = f'{CONFIG_DIR}/keyfile'
# Mounted into every container. The guest agent, the health check and the
# backup container all talk to the server over the socket in here: the
# server itself only accepts connections from the network.
SOCKET_DIR = '/var/run/mongodb'

ADMIN_USER = 'os_admin'
ADMIN_ROLES = [{'role': 'root', 'db': 'admin'}]
# The name of the config server replica set. Every query router of a
# cluster needs it and the taskmanager only sends the addresses, so the
# guests agree on it here.
CONFIGSVR_REPLICA_SET = 'configsvr'
# The collection that gives a database an existence: MongoDB creates a
# database on its first write and drops it with its last collection.
PLACEHOLDER_COLLECTION = 'trove_placeholder'

# Configuration group for clustering-related settings.
CNF_CLUSTER = 'clustering'

INSTANCE_TYPE_MEMBER = 'member'
INSTANCE_TYPE_CONFIG_SERVER = 'config_server'
INSTANCE_TYPE_QUERY_ROUTER = 'query_router'

# Replica set member states that count as healthy: PRIMARY and SECONDARY.
RS_HEALTHY_STATES = (1, 2)


def socket_path(port):
    return f'{SOCKET_DIR}/mongodb-{port}.sock'


def socket_uri(port):
    """The socket as a connection string, for the shell and the tools."""
    return 'mongodb://%s' % urllib.parse.quote(socket_path(port), safe='')


class MongoDBApp(service.BaseDbApp):
    _configuration_manager = None

    @property
    def configuration_manager(self):
        if self._configuration_manager:
            return self._configuration_manager

        revision_dir = guestagent_utils.build_file_path(
            CONFIG_DIR,
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
        super(MongoDBApp, self).__init__(status, docker_client)
        mount_point = cfg.get_configuration_property('mount_point')
        self.datadir = f'{mount_point}/data'
        self.adm = MongoDBAdmin(self)

    ##################
    # Roles and ports
    ##################

    def get_config_value(self, name, default=None):
        """Return a value of the configuration file by its dotted name."""
        value = self.configuration_manager.parse_configuration() or {}
        for key in name.split('.'):
            if not isinstance(value, dict) or key not in value:
                return default
            value = value[key]
        return value

    @property
    def is_query_router(self):
        return self.get_config_value('sharding.configDB') is not None

    @property
    def is_config_server(self):
        return self.get_config_value('sharding.clusterRole') == 'configsvr'

    @property
    def is_cluster_member(self):
        return (not self.is_config_server and
                self.get_config_value('replication.replSetName') is not None)

    @property
    def replica_set_name(self):
        return self.get_config_value('replication.replSetName')

    @property
    def port(self):
        default = (CONF.mongodb.configsvr_port if self.is_config_server
                   else CONF.mongodb.mongodb_port)
        return int(self.get_config_value('net.port', default))

    @property
    def instance_ip(self):
        return netutils.get_my_ipv4()

    #################
    # Configuration
    #################

    def apply_initial_guestagent_configuration(self, cluster_config=None):
        """Settings the guest agent decides for every instance.

        The server listens on the network for its clients and on a socket
        for the guest agent. The socket is what makes the localhost
        exception usable: the first user can be created over it and over
        nothing else.
        """
        instance_type = (cluster_config['instance_type'] if cluster_config
                         else None)
        overrides = {
            'net': {
                'bindIp': '0.0.0.0',
                'port': CONF.mongodb.mongodb_port,
                'unixDomainSocket': {
                    'enabled': True,
                    'pathPrefix': SOCKET_DIR,
                    # The guest agent is not the database user; the
                    # socket is only reachable from the instance, like
                    # the MySQL socket that has the same mode.
                    'filePermissions': 0o777,
                },
            },
        }
        if instance_type == INSTANCE_TYPE_QUERY_ROUTER:
            # The configuration template is written for mongod. mongos has
            # no storage and no authorization setting of its own: it
            # authenticates its users against the config servers.
            self.configuration_manager.reset_configuration(
                {}, remove_overrides=True)
        else:
            overrides['storage'] = {'dbPath': self.datadir}
            overrides['security'] = {'authorization': 'enabled'}
        self.configuration_manager.apply_system_override(overrides)

        if cluster_config is not None:
            self._configure_as_cluster_instance(cluster_config)

    def _configure_as_cluster_instance(self, cluster_config):
        instance_type = cluster_config['instance_type']
        if instance_type == INSTANCE_TYPE_QUERY_ROUTER:
            self._configure_as_query_router()
        elif instance_type == INSTANCE_TYPE_CONFIG_SERVER:
            self._configure_as_config_server()
        elif instance_type == INSTANCE_TYPE_MEMBER:
            self._configure_as_cluster_member(
                cluster_config['replica_set_name'])
        else:
            raise exception.TroveError(
                _("Bad cluster configuration; instance type given as %s.")
                % instance_type)

        if 'key' in cluster_config:
            self._configure_cluster_security(cluster_config['key'])

    def _configure_as_query_router(self):
        """The config servers are not known yet. The taskmanager sends them
        with add_config_servers, which also starts the router.
        """
        LOG.info("Configuring instance as a cluster query router.")
        self.configuration_manager.apply_system_override({
            'net': {'port': CONF.mongodb.mongodb_port},
            'sharding': {'configDB': ''},
        }, CNF_CLUSTER)

    def _configure_as_config_server(self):
        LOG.info("Configuring instance as a cluster config server.")
        self.configuration_manager.apply_system_override({
            'net': {'port': CONF.mongodb.configsvr_port},
            'sharding': {'clusterRole': 'configsvr'},
            'replication': {'replSetName': CONFIGSVR_REPLICA_SET},
        }, CNF_CLUSTER)

    def _configure_as_cluster_member(self, replica_set_name):
        LOG.info("Configuring instance as a cluster member.")
        self.configuration_manager.apply_system_override({
            'net': {'port': CONF.mongodb.mongodb_port},
            'sharding': {'clusterRole': 'shardsvr'},
            'replication': {'replSetName': replica_set_name},
        }, CNF_CLUSTER)

    def _configure_cluster_security(self, key):
        """Members of a cluster authenticate each other with a shared key.

        The key file implies authorization on mongod and is the only
        security setting mongos accepts.
        """
        self.store_key(key)
        self.configuration_manager.apply_system_override({
            'security': {
                'clusterAuthMode': 'keyFile',
                'keyFile': KEY_FILE,
            },
        }, CNF_CLUSTER)

    def store_key(self, key):
        LOG.debug('Storing the cluster key.')
        operating_system.write_file(KEY_FILE, key, as_root=True)
        operating_system.chmod(KEY_FILE, FileMode.SET_USR_RO, as_root=True)
        operating_system.chown(KEY_FILE, self.database_service_uid,
                               self.database_service_gid, as_root=True)

    def get_key(self):
        return operating_system.read_file(KEY_FILE, as_root=True).rstrip()

    def set_config_servers(self, config_server_hosts):
        """Point a query router at the config server replica set."""
        config_db = '%s/%s' % (
            CONFIGSVR_REPLICA_SET,
            ','.join('%s:%s' % (host, CONF.mongodb.configsvr_port)
                     for host in config_server_hosts))
        LOG.info("Setting config servers: %s", config_db)
        self.configuration_manager.apply_system_override(
            {'sharding': {'configDB': config_db}}, CNF_CLUSTER)

    def update_overrides(self, overrides):
        """User overrides come with dotted names, the file is nested."""
        if overrides:
            self.configuration_manager.apply_user_override(
                guestagent_utils.expand_dict(overrides))

    def apply_overrides(self, overrides):
        # Every parameter in the validation rules needs a restart.
        pass

    ############
    # Lifecycle
    ############

    def _healthcheck(self, port):
        """Ping over the socket.

        The socket only exists once the server that owns it is up, so this
        never passes for anything but the server being checked.
        """
        return {
            "test": ["CMD-SHELL",
                     "mongosh --quiet %s --eval "
                     "'db.adminCommand(\"ping\").ok' | grep -q '^1$'"
                     % socket_uri(port)],
            "start_period": 10 * 1000000000,
            "interval": 10 * 1000000000,
            "timeout": 5 * 1000000000,
            "retries": 3
        }

    def _ensure_directories(self):
        for folder in (CONFIG_DIR, SOCKET_DIR, self.datadir):
            operating_system.ensure_directory(
                folder, user=self.database_service_uid,
                group=self.database_service_gid, force=True,
                as_root=True)

    def start_db(self, update_db=False, ds_version=None, command=None,
                 extra_volumes=None):
        """Start and wait for the database service."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')
        if not command:
            binary = 'mongos' if self.is_query_router else 'mongod'
            command = f'{binary} --config {CONFIG_FILE}'

        self._ensure_directories()

        volumes = {
            CONFIG_DIR: {'bind': CONFIG_DIR, 'mode': 'rw'},
            SOCKET_DIR: {'bind': SOCKET_DIR, 'mode': 'rw'},
            self.datadir: {'bind': self.datadir, 'mode': 'rw'},
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
                healthcheck=self._healthcheck(self.port),
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

    def start_temporary_db(self, ds_version=None):
        """Start the server without access control, reachable over the
        socket and the container's loopback interface only.

        This is how a restored data directory, whose admin user belongs to
        the instance the backup was taken from, gets this instance's admin
        user; it is the procedure the MongoDB documentation gives for a
        lost admin password. stop_temporary_db has to follow.
        """
        LOG.info("Starting a temporary database service without access "
                 "control.")
        command = (f'mongod --dbpath {self.datadir} --bind_ip 127.0.0.1 '
                   f'--port {self.port} --unixSocketPrefix {SOCKET_DIR}')
        self.start_db(ds_version=ds_version, command=command)

    def stop_temporary_db(self):
        """Stop the temporary server and forget its container, so that the
        next start creates the real one.
        """
        self.stop_db()
        docker_util.remove_container(self.docker_client)

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

    ################
    # Admin user
    ################

    def secure(self, password=None):
        """Create the Trove admin user over the socket.

        The localhost exception lets the first user of a server be created
        without authenticating, and the socket is the only local way in.
        On a replica set member this needs the set to be initiated and the
        member to be its primary.
        """
        password = password or utils.generate_random_password()
        LOG.info("Creating the %s user.", ADMIN_USER)
        self.adm.create_admin_user(password)
        self.save_password(ADMIN_USER, password)

    def store_admin_password(self, password):
        """Record the admin password another instance created.

        A replica set member takes its users from the primary with the
        rest of the data, so the password the taskmanager collected from
        the primary is the one that works here. A query router has no
        users of its own and authenticates against the config servers.
        """
        self.save_password(ADMIN_USER, password)

    def reset_admin_password(self, password=None):
        """Give the admin user this instance's password on a server that
        runs without access control, creating the user if the data came
        from a server that never had it.
        """
        password = password or utils.generate_random_password()
        self.adm.set_admin_password(password)
        self.save_password(ADMIN_USER, password)

    @property
    def admin_password(self):
        return self.get_auth_password()

    ##################
    # Cluster actions
    ##################

    def prep_primary(self):
        """Turn this member into the primary of a replica set of one and
        give it the admin user.

        The replica set name is in the configuration file; initiating the
        set is allowed under the localhost exception.
        """
        LOG.info("Initiating replica set %s.", self.replica_set_name)
        self.adm.rs_initiate(self.replica_set_name,
                             '%s:%s' % (self.instance_ip, self.port))
        utils.poll_until(self.adm.is_primary, sleep_time=3,
                         time_out=CONF.mongodb.add_members_timeout)
        self.secure()

    def add_members(self, members):
        """Add the given hosts to the replica set this primary leads and
        wait until they have caught up.
        """
        member_hosts = ['%s:%s' % (member, self.port) for member in members]
        LOG.info("Adding members %s to replica set %s.", member_hosts,
                 self.replica_set_name)
        self.adm.rs_add_members(member_hosts)
        expected = len(members) + 1

        def _all_members_healthy():
            return self.adm.count_healthy_members() == expected

        utils.poll_until(_all_members_healthy, sleep_time=10,
                         time_out=CONF.mongodb.add_members_timeout)

    def add_config_servers(self, config_server_hosts):
        """Configure the config servers on a query router and start it."""
        self.set_config_servers(config_server_hosts)
        self.start_db(update_db=True)

    def add_shard(self, replica_set_name, replica_set_member):
        """Add a replica set as a shard, from a query router."""
        url = "%(rs)s/%(host)s:%(port)s" % {
            'rs': replica_set_name,
            'host': replica_set_member,
            'port': CONF.mongodb.mongodb_port}
        self.adm.add_shard(url)

    def is_shard_active(self, replica_set_name):
        return replica_set_name in self.adm.list_active_shard_names()

    ##########
    # Backup
    ##########

    def _backup_volumes(self):
        return {SOCKET_DIR: {'bind': SOCKET_DIR, 'mode': 'rw'}}

    def _backup_params(self, authenticate=True):
        """The backup container reaches the server over the socket."""
        params = f'--db-host={socket_path(self.port)}'
        if authenticate:
            params += (f' --db-user={ADMIN_USER} '
                       f'--db-password={self.admin_password}')
        return params

    def create_backup(self, context, backup_info):
        super(MongoDBApp, self).create_backup(
            context, backup_info,
            volumes_mapping=self._backup_volumes(),
            need_dbuser=False,
            extra_params=self._backup_params())

    def restore_backup(self, context, backup_info, restore_location):
        """Restore into the running temporary server."""
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
            f'{self._backup_params(authenticate=False)}'
        )
        if CONF.swift_api_insecure:
            command = f"{command} --swift-api-insecure"
        if CONF.backup_aes_cbc_key:
            command = (f"{command} "
                       f"--backup-encryption-key={CONF.backup_aes_cbc_key}")

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


class MongoDBAdmin(object):
    """Administrative operations, over the socket of the local server."""

    def __init__(self, app):
        self.app = app

    def _client(self, authenticate=True):
        # pymongo takes a socket only in its URI form.
        kwargs = {
            'host': socket_uri(self.app.port),
            'directConnection': True,
            'serverSelectionTimeoutMS': 10000,
        }
        if authenticate:
            kwargs.update({
                'username': ADMIN_USER,
                'password': self.app.admin_password,
                'authSource': 'admin',
            })
        return pymongo.MongoClient(**kwargs)

    def _run(self, command, value=1, database='admin', authenticate=True,
             **kwargs):
        with self._client(authenticate) as client:
            return client[database].command(command, value, **kwargs)

    ##############
    # Admin user
    ##############

    def create_admin_user(self, password):
        self._run('createUser', ADMIN_USER, authenticate=False,
                  pwd=password, roles=ADMIN_ROLES)

    def set_admin_password(self, password):
        """On a server without access control."""
        with self._client(authenticate=False) as client:
            if client.admin.system.users.find_one(
                    {'user': ADMIN_USER, 'db': 'admin'}):
                client.admin.command('updateUser', ADMIN_USER, pwd=password,
                                     roles=ADMIN_ROLES)
            else:
                client.admin.command('createUser', ADMIN_USER, pwd=password,
                                     roles=ADMIN_ROLES)

    #########
    # Users
    #########

    def _user_from_record(self, record):
        user = models.MongoDBUser(name=record['_id'])
        user.roles = record['roles']
        return user

    def _get_user_record(self, name, client=None):
        user = models.MongoDBUser(name)
        if user.is_ignored:
            LOG.warning('Skipping retrieval of user with reserved '
                        'name %(user)s', {'user': user.name})
            return None
        if client is None:
            with self._client() as client:
                return self._get_user_record(name, client=client)
        record = client.admin.system.users.find_one(
            {'user': user.username, 'db': user.database.name})
        if not record:
            return None
        return self._user_from_record(record)

    def get_existing_user(self, name):
        user = self._get_user_record(name)
        if not user:
            raise ValueError(_('User with name %(user)s does not '
                               'exist.') % {'user': name})
        return user

    def get_user(self, name, hostname=None):
        LOG.debug('Getting user %s.', name)
        user = self._get_user_record(name)
        if not user:
            return None
        return user.serialize()

    def _create_user_with_client(self, user, client):
        client[user.database.name].command(
            'createUser', user.username, pwd=user.password, roles=user.roles)

    def _update_user_with_client(self, user, client, password=None):
        kwargs = {'roles': user.roles}
        if password:
            kwargs['pwd'] = password
        client[user.database.name].command('updateUser', user.username,
                                           **kwargs)

    def create_users(self, users):
        with self._client() as client:
            for item in users:
                user = models.MongoDBUser.deserialize(item)
                # this could be called to create multiple users at once;
                # catch exceptions, log the message, and continue
                try:
                    user.check_create()
                    if self._get_user_record(user.name, client=client):
                        raise ValueError(_('User with name %(user)s already '
                                           'exists.') % {'user': user.name})
                    LOG.debug('Creating user %(user)s on database %(db)s '
                              'with roles %(role)s.',
                              {'user': user.username,
                               'db': user.database.name,
                               'role': str(user.roles)})
                    self._create_user_with_client(user, client)
                except (ValueError, pymongo_errors.PyMongoError) as e:
                    LOG.error(e)
                    LOG.warning('Skipping creation of user with name '
                                '%(user)s', {'user': user.name})

    def delete_user(self, user):
        user = models.MongoDBUser.deserialize(user)
        user.check_delete()
        LOG.debug('Deleting user %(user)s from database %(db)s.',
                  {'user': user.username, 'db': user.database.name})
        with self._client() as client:
            client[user.database.name].command('dropUser', user.username)

    def list_users(self, limit=None, marker=None, include_marker=False):
        users = []
        with self._client() as client:
            for record in client.admin.system.users.find():
                user = self._user_from_record(record)
                if not user.is_ignored:
                    users.append(user)
        return guestagent_utils.serialize_list(
            users, limit=limit, marker=marker, include_marker=include_marker)

    def change_passwords(self, users):
        with self._client() as client:
            for item in users:
                user = models.MongoDBUser.deserialize(item)
                try:
                    user.check_create()
                    existing = self._get_user_record(user.name, client=client)
                    if not existing:
                        raise ValueError(_('User with name %(user)s does '
                                           'not exist.') % {'user': user.name})
                    LOG.debug('Changing password for user %(user)s',
                              {'user': user.name})
                    self._update_user_with_client(existing, client,
                                                  password=user.password)
                except (ValueError, pymongo_errors.PyMongoError) as e:
                    LOG.error(e)
                    LOG.warning('Skipping password change for user with '
                                'name %(user)s', {'user': user.name})

    def update_attributes(self, name, user_attrs):
        user = self.get_existing_user(name)
        password = user_attrs.get('password')
        if password:
            user.password = password
            self.change_passwords([user.serialize()])
        if user_attrs.get('name'):
            LOG.warning('Changing user name is not supported.')
        if user_attrs.get('host'):
            LOG.warning('Changing user host is not supported.')

    def enable_root(self, password=None):
        """Create the user 'admin.root' with the role 'root'."""
        if not password:
            password = utils.generate_random_password()
        root_user = models.MongoDBUser.root(password=password)
        root_user.roles = {'db': 'admin', 'role': 'root'}
        root_user.check_create()
        with self._client() as client:
            if self._get_user_record(root_user.name, client=client):
                self._update_user_with_client(root_user, client,
                                              password=password)
            else:
                self._create_user_with_client(root_user, client)
        return root_user.serialize()

    def is_root_enabled(self):
        with self._client() as client:
            return bool(client.admin.system.users.find_one(
                {'user': 'root', 'db': 'admin'}))

    def grant_access(self, username, databases):
        """Add the readWrite role on each database to the user."""
        user = self.get_existing_user(username)
        for db_name in databases:
            models.MongoDBSchema(db_name)
            role = {'db': db_name, 'role': 'readWrite'}
            if role not in user.roles:
                user.roles = role
        with self._client() as client:
            self._update_user_with_client(user, client)

    def revoke_access(self, username, database):
        user = self.get_existing_user(username)
        models.MongoDBSchema(database)
        user.revoke_role({'db': database, 'role': 'readWrite'})
        with self._client() as client:
            self._update_user_with_client(user, client)

    def list_access(self, username):
        return self.get_existing_user(username).databases

    #############
    # Databases
    #############

    def create_database(self, databases):
        """A database exists once it holds a collection.
    """
        with self._client() as client:
            for item in databases:
                schema = models.MongoDBSchema.deserialize(item)
                schema.check_create()
                LOG.debug('Creating MongoDB database %s', schema.name)
                db = client[schema.name]
                if PLACEHOLDER_COLLECTION not in db.list_collection_names():
                    db.create_collection(PLACEHOLDER_COLLECTION)

    def delete_database(self, database):
        with self._client() as client:
            schema = models.MongoDBSchema.deserialize(database)
            schema.check_delete()
            client.drop_database(schema.name)

    def list_databases(self, limit=None, marker=None, include_marker=False):
        databases = []
        with self._client() as client:
            for db_name in client.list_database_names():
                schema = models.MongoDBSchema(name=db_name)
                if not schema.is_ignored():
                    databases.append(schema)
        return guestagent_utils.serialize_list(
            databases, limit=limit, marker=marker,
            include_marker=include_marker)

    ###########
    # Cluster
    ###########

    def rs_initiate(self, replica_set_name, host):
        self._run('replSetInitiate',
                  {'_id': replica_set_name,
                   'members': [{'_id': 0, 'host': host}]},
                  authenticate=False)

    def get_repl_status(self):
        return self._run('replSetGetStatus')

    def is_primary(self):
        try:
            return self.get_repl_status().get('myState') == 1
        except pymongo_errors.PyMongoError as e:
            LOG.debug("Replica set status not available yet: %s", e)
            return False

    def count_healthy_members(self):
        status = self.get_repl_status()
        primaries = [m for m in status['members'] if m['state'] == 1]
        if len(primaries) != 1:
            return 0
        return len([m for m in status['members']
                    if m['health'] == 1 and m['state'] in RS_HEALTHY_STATES])

    def rs_add_members(self, hosts):
        config = self._run('replSetGetConfig')['config']
        config['version'] += 1
        next_id = max(m['_id'] for m in config['members']) + 1
        for host in hosts:
            config['members'].append({'_id': next_id, 'host': host})
            next_id += 1
        self._run('replSetReconfig', config)

    def add_shard(self, url):
        self._run('addShard', url)

    def list_active_shard_names(self):
        with self._client() as client:
            return [shard['_id'] for shard in client.config.shards.find()]
