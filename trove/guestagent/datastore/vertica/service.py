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

import datetime
import ipaddress
import json
import os

import docker

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.x509.oid import NameOID
from oslo_log import log as logging
from oslo_utils import netutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.vertica import models
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
# database container at the same path.
HOST_CONF_DIR = '/etc/vertica-trove'
CONTAINER_CONF_DIR = HOST_CONF_DIR
# The settings, as the configuration template renders them and the
# configuration manager overrides them. The server reads no file: they
# are database parameters, set over SQL.
CONFIG_FILE = f'{HOST_CONF_DIR}/vertica.conf'
# The password of the admin, for the commands that need it: the creation
# of the database and the start of the database by the container.
ADMIN_SECRET = 'admin.secret'
# The address the node has on the user's network.
HOST_FILE = 'host'
# What the container runs: the node management agent, and the database on
# it.
START_SCRIPT = 'start.sh'
# A license a module installed, for the creation of a database that needs
# one.
LICENSE_FILE = 'license.key'
# The certificates of the node management agent and of the HTTPS service
# of the server, where the image looks for them.
CERT_DIR = f'{HOST_CONF_DIR}/certs'
CONTAINER_CERT_DIR = '/opt/vertica/config/https_certs'
# The volume, as the container sees it.
CONTAINER_DATA_DIR = '/data'
CLUSTER_CONFIG = f'{CONTAINER_DATA_DIR}/vertica_cluster.yaml'

DB_NAME = 'db_srvr'
ADMIN_USER = 'dbadmin'
PORT = 5433
NMA_PORT = 5554
# The admin logs in without a password over the socket of the server,
# which only the processes of the container reach. The guest agent needs
# nothing else, and a restored database takes the password of the
# instance it is restored on without the one of the instance it came
# from.
LOCAL_AUTHENTICATION = 'trove_local'
ROOT_ROLE = 'PSEUDOSUPERUSER'
# The server refuses to create or start a database with fewer open files
# than it requires, and docker gives a container 1024 unless told.
OPEN_FILES = 65536
# The parameters of the database the guest agent sets itself.
SYSTEM_PARAMETERS = {
    # The objects of a dropped user go to the admin instead of with it.
    'GlobalHeirUsername': ADMIN_USER,
}

TLS_ARGS = [
    '--cert-file', f'{CONTAINER_CERT_DIR}/{ADMIN_USER}.pem',
    '--key-file', f'{CONTAINER_CERT_DIR}/{ADMIN_USER}.key',
    '--ca-cert-file', f'{CONTAINER_CERT_DIR}/rootca.pem',
]

START_SCRIPT_CONTENT = """#!/bin/bash
# Written by the Trove guest agent. Runs the node management agent, starts
# the database on it if there is one, and stays with the agent.
set -u
CONF=%(conf)s
CFG=%(cluster_config)s
TLS="%(tls)s"
export USER=%(admin)s HOME=/tmp
/opt/vertica/bin/node_management_agent &
NMA=$!
stop() {
    vsql -U %(admin)s -Atc "SELECT SHUTDOWN()"
    kill $NMA
    wait $NMA
}
trap stop TERM INT
for i in $(seq 1 60); do
    curl -sk -o /dev/null https://127.0.0.1:%(nma_port)d/v1/health && break
    sleep 1
done
if [ -f "$CFG" ]; then
    HOST=$(cat $CONF/%(host_file)s)
    OLD=$(sed -n 's/^ *address: *//p' "$CFG" | head -1)
    if [ -n "$OLD" ] && [ "$OLD" != "$HOST" ]; then
        # The data comes from a node with another address.
        printf '[{"from_address": "%%s", "to_address": "%%s"}]' \\
            "$OLD" "$HOST" > /tmp/re_ip.json
        vcluster re_ip --db-name %(db)s --hosts "$HOST" \\
            --catalog-path %(data)s --re-ip-file /tmp/re_ip.json $TLS \\
            --config "$CFG"
    fi
    # Restored from a backup, the database has the password of the
    # instance it came from: the start cannot confirm it and stops
    # waiting, the database runs anyway and the guest agent sets the
    # password.
    vcluster start_db --db-name %(db)s --password-file $CONF/%(secret)s \\
        --timeout 120 $TLS --config "$CFG" &
fi
wait $NMA
"""


def quote_identifier(name):
    return '"%s"' % str(name).replace('"', '""')


def quote_literal(value):
    return "'%s'" % str(value).replace("'", "''")


class VerticaApp(service.BaseDbApp):
    _configuration_manager = None

    # Over the socket of the server, as the admin, without a password.
    HEALTHCHECK = {
        "test": ["CMD-SHELL",
                 "vsql -U %s -Atc 'SELECT 1' >/dev/null" % ADMIN_USER],
        "start_period": 60 * 1000000000,
        "interval": 10 * 1000000000,
        "timeout": 10 * 1000000000,
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
        super(VerticaApp, self).__init__(status, docker_client)
        self.mount_point = cfg.get_configuration_property('mount_point')
        self.adm = VerticaAdmin(self)

    ###########
    # Address
    ###########

    @property
    def address(self):
        """The address of the node on the user's network."""
        if operating_system.exists(constants.ETH1_CONFIG_PATH):
            with open(constants.ETH1_CONFIG_PATH) as f:
                address = json.load(f).get('ipv4_address')
            if address:
                return address
        return netutils.get_my_ipv4()

    ################
    # Configuration
    ################

    def update_overrides(self, overrides):
        if overrides:
            self.configuration_manager.apply_user_override(overrides)

    def remove_overrides(self):
        """Forget the overrides, and tell the server to forget them."""
        names = list(self._user_overrides())
        self.configuration_manager.remove_user_override()
        for name in names:
            self.adm.clear_parameter(name)

    def _user_overrides(self):
        overrides = self.configuration_manager.get_user_override()
        return overrides or {}

    def apply_overrides(self, overrides):
        for name, value in overrides.items():
            self.adm.set_parameter(name, value)

    def apply_settings(self):
        """Tell the server the parameters Trove decides and those of the
        configuration file. The server keeps them in its catalog.
        """
        settings = dict(SYSTEM_PARAMETERS)
        settings.update(self.configuration_manager.parse_configuration())
        for name, value in settings.items():
            self.adm.set_parameter(name, value)

    ##############
    # Admin user
    ##############

    def _conf_path(self, name):
        return f'{HOST_CONF_DIR}/{name}'

    def has_admin(self):
        return operating_system.exists(self._conf_path(ADMIN_SECRET),
                                       as_root=True)

    def secure(self, password=None):
        """Decide the password of the admin and write what the container
        needs: the password, the address of the node, the script it runs
        and the certificates of its agent.
        """
        password = password or utils.generate_random_password()
        LOG.info('Configuring the Trove admin.')
        operating_system.ensure_directory(
            CERT_DIR, user=self.database_service_uid,
            group=self.database_service_gid, force=True, as_root=True)
        self._write_owned(self._conf_path(ADMIN_SECRET), password)
        self.write_node_files()
        for name, content in generate_certificates().items():
            self._write_owned(f'{CERT_DIR}/{name}', content)
        self.save_password(ADMIN_USER, password)

    def write_node_files(self):
        """The address and the start script, which a node gets anew."""
        self._write_owned(self._conf_path(HOST_FILE), self.address,
                          mode=FileMode.ADD_READ_ALL)
        self._write_owned(
            self._conf_path(START_SCRIPT), START_SCRIPT_CONTENT % {
                'conf': CONTAINER_CONF_DIR,
                'cluster_config': CLUSTER_CONFIG,
                'tls': ' '.join(TLS_ARGS),
                'admin': ADMIN_USER,
                'nma_port': NMA_PORT,
                'host_file': HOST_FILE,
                'db': DB_NAME,
                'data': CONTAINER_DATA_DIR,
                'secret': ADMIN_SECRET,
            }, mode=FileMode.ADD_READ_ALL)

    def _write_owned(self, path, content, mode=None):
        """A file of the database user, readable by it alone unless told
        otherwise.
        """
        operating_system.write_file(path, content, as_root=True)
        operating_system.chown(
            path, self.database_service_uid, self.database_service_gid,
            as_root=True)
        operating_system.chmod(path, FileMode.SET_USR_RW, as_root=True)
        if mode:
            operating_system.chmod(path, mode, as_root=True)

    @property
    def admin_password(self):
        # save_password keeps it under the name of the user.
        return self.get_auth_password(file=f'{ADMIN_USER}.cnf')

    def write_license(self, content):
        """Keep a license where the server reads it. A license file is
        text.
        """
        if isinstance(content, bytes):
            content = content.decode()
        operating_system.ensure_directory(
            HOST_CONF_DIR, user=self.database_service_uid,
            group=self.database_service_gid, force=True, as_root=True)
        self._write_owned(self._conf_path(LICENSE_FILE), content)

    def has_license(self):
        return operating_system.exists(self._conf_path(LICENSE_FILE),
                                       as_root=True)

    ############
    # Commands
    ############

    def execute(self, command, environment=None):
        """Run a command in the database container and return what it
        wrote to its standard output.
        """
        container = self.docker_client.containers.get('database')
        code, output = container.exec_run(
            command, environment=environment, demux=True)
        stdout, stderr = (stream.decode() if stream else ''
                          for stream in output)
        if code != 0:
            raise exception.TroveError(
                _("Command %(command)s failed with %(code)s: %(error)s")
                % {'command': command[0], 'code': code,
                   'error': (stderr or stdout).strip()[-500:]})
        return stdout

    def vcluster(self, *args):
        command = ['vcluster'] + list(args) + TLS_ARGS + [
            '--config', CLUSTER_CONFIG]
        return self.execute(command, environment={'USER': ADMIN_USER,
                                                  'HOME': '/tmp'})

    def agent_is_up(self):
        try:
            self.execute(['curl', '-skf', '-o', '/dev/null',
                          'https://127.0.0.1:%d/v1/health' % NMA_PORT])
            return True
        except exception.TroveError:
            return False

    def database_is_running(self):
        """Whether a server process runs in the container."""
        try:
            self.execute(['sh', '-c', 'ps -eo args | '
                          'grep -q "^/opt/vertica/bin/vertica "'])
            return True
        except exception.TroveError:
            return False

    ############
    # Lifecycle
    ############

    def _ensure_directories(self):
        for folder in (HOST_CONF_DIR, CERT_DIR, self.mount_point):
            operating_system.ensure_directory(
                folder, user=self.database_service_uid,
                group=self.database_service_gid, force=True,
                as_root=True)

    def database_exists(self):
        return operating_system.exists(
            f'{self.mount_point}/vertica_cluster.yaml', as_root=True)

    def start_db(self, update_db=False, ds_version=None, command=None,
                 extra_volumes=None):
        """Start the container and, if there is a database, wait for it."""
        docker_image = CONF.get(CONF.datastore_manager).docker_image
        ds_version = ds_version or CONF.datastore_version
        image = (f'{docker_image}:latest' if not ds_version else
                 f'{docker_image}:{ds_version}')
        command = command or ['/bin/bash',
                              f'{CONTAINER_CONF_DIR}/{START_SCRIPT}']

        self._ensure_directories()
        # The address of the node may have changed since the last start.
        self.write_node_files()

        volumes = {
            HOST_CONF_DIR: {'bind': CONTAINER_CONF_DIR, 'mode': 'rw'},
            CERT_DIR: {'bind': CONTAINER_CERT_DIR, 'mode': 'rw'},
            self.mount_point: {'bind': CONTAINER_DATA_DIR, 'mode': 'rw'},
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
                command=command,
                ulimits=[docker.types.Ulimit(name='nofile', soft=OPEN_FILES,
                                             hard=OPEN_FILES)]
            )
        except Exception:
            LOG.exception("Failed to start database service")
            raise exception.TroveError("Failed to start database service")

        if self.database_exists():
            self._wait_until_healthy(update_db)

    def _wait_until_healthy(self, update_db=True):
        if not self.status.wait_for_status(
            service_status.ServiceStatuses.HEALTHY,
            CONF.state_change_wait_time, update_db
        ):
            raise exception.TroveError("Failed to start database service")

    def create_database(self):
        """Create the database on the node, as the one node of it.

        From 26.1 the server refuses the license of the Community Edition
        the image has: the database needs one installed with the instance.
        """
        utils.poll_until(self.agent_is_up, sleep_time=2,
                         time_out=CONF.state_change_wait_time)
        args = ['create_db', '--db-name', DB_NAME,
                '--hosts', self.address,
                '--catalog-path', CONTAINER_DATA_DIR,
                '--data-path', CONTAINER_DATA_DIR,
                '--password-file', f'{CONTAINER_CONF_DIR}/{ADMIN_SECRET}']
        if self.has_license():
            args += ['--license', f'{CONTAINER_CONF_DIR}/{LICENSE_FILE}']
        LOG.info('Creating the database.')
        try:
            self.vcluster(*args)
        except exception.TroveError as e:
            if 'license' in str(e).lower() and not self.has_license():
                raise exception.TroveError(
                    _("This version of Vertica needs a license: create the "
                      "instance with a module of type vertica_license. %s")
                    % e)
            raise
        # The only time the guest agent logs in with the password.
        self.adm.run([
            'CREATE AUTHENTICATION %s METHOD %s LOCAL' % (
                quote_identifier(LOCAL_AUTHENTICATION),
                quote_literal('trust')),
            'GRANT AUTHENTICATION %s TO %s' % (
                quote_identifier(LOCAL_AUTHENTICATION),
                quote_identifier(ADMIN_USER)),
        ], password=self.admin_password)
        self._wait_until_healthy(update_db=False)

    def reset_admin_password(self):
        """Give the admin of a restored database the password of this
        instance.
        """
        self.adm.run(['ALTER USER %s IDENTIFIED BY %s' % (
            quote_identifier(ADMIN_USER),
            quote_literal(self.admin_password))])

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

    def shutdown_database(self):
        """Stop the server, leaving the container and its agent."""
        LOG.info('Shutting the database down.')
        self.adm.run(['SELECT SHUTDOWN()'])
        utils.poll_until(lambda: not self.database_is_running(),
                         sleep_time=2, time_out=CONF.state_change_wait_time)

    def start_database(self):
        """Start the server on the running agent."""
        LOG.info('Starting the database.')
        self.vcluster('start_db', '--db-name', DB_NAME,
                      '--password-file',
                      f'{CONTAINER_CONF_DIR}/{ADMIN_SECRET}')
        self._wait_until_healthy()

    ##########
    # Backup
    ##########

    def _backup_volumes(self):
        return {self.mount_point: {'bind': self.mount_point, 'mode': 'rw'}}

    def create_backup(self, context, backup_info):
        """A backup is a copy of the catalog and the data, taken with the
        server stopped: the image has no tool that copies them from a
        running server consistently. The database is down for the time of
        the copy.
        """
        self.shutdown_database()
        try:
            super(VerticaApp, self).create_backup(
                context, backup_info,
                volumes_mapping=self._backup_volumes(),
                need_dbuser=False,
                extra_params=f'--db-datadir={self.mount_point}')
        finally:
            self.start_database()

    def restore_backup(self, context, backup_info, restore_location):
        """Unpack a backup onto the empty volume.

        The container moves the database to the address of this node when
        it starts it; the guest agent then gives the admin this instance's
        password.
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


def generate_certificates(days=3650):
    """A certificate authority of the node and the certificates signed by
    it that the node management agent, the HTTPS service of the server
    and vcluster use, with the configuration of the HTTPS service.
    """
    def pem(key):
        return key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()).decode()

    now = datetime.datetime.now(datetime.timezone.utc)
    not_before = now - datetime.timedelta(days=1)
    not_after = now + datetime.timedelta(days=days)

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,
                                            'trove-vertica-rootca')])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name).issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before).not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None),
                       critical=True)
        .add_extension(x509.KeyUsage(
            digital_signature=False, content_commitment=False,
            key_encipherment=False, data_encipherment=False,
            key_agreement=False, key_cert_sign=True, crl_sign=True,
            encipher_only=False, decipher_only=False), critical=True)
        .sign(ca_key, hashes.SHA256()))
    ca_pem = ca_cert.public_bytes(serialization.Encoding.PEM).decode()

    files = {'rootca.pem': ca_pem, 'rootca.key': pem(ca_key)}
    for name in ('vertica_https', 'nma_client', ADMIN_USER):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([
                x509.NameAttribute(NameOID.COMMON_NAME, ADMIN_USER)]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(not_before).not_valid_after(not_after)
            .add_extension(x509.SubjectAlternativeName([
                x509.DNSName('localhost'),
                x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),
                critical=False)
            .add_extension(x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.SERVER_AUTH,
                ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
        files[f'{name}.pem'] = cert.public_bytes(
            serialization.Encoding.PEM).decode()
        files[f'{name}.key'] = pem(key)

    files['httpstls.json'] = json.dumps({
        'name': 'server', 'cipher_suites': '', 'mode': 2,
        'key': files['vertica_https.key'],
        'certificate': files['vertica_https.pem'],
        'chain_certs': [], 'ca_certificates': [ca_pem]})
    return files


class VerticaAdmin(object):
    """Administrative operations, in SQL over the socket of the server.

    vsql runs in the database container, where the admin logs in without
    a password. A Trove database is a schema of the one database of the
    instance; a user has all privileges on each of its schemas, and on
    whatever is created in them later.
    """

    FIELD_SEPARATOR = '\x1f'

    def __init__(self, app):
        self.app = app

    #######
    # SQL
    #######

    def run(self, statements, password=None):
        """Run statements in one session and return the rows of their
        results, each a list of fields.

        :param password: log in with it over TCP instead of over the
                         socket, before the socket admits the admin.
        """
        command = ['vsql', '-U', ADMIN_USER, '-At',
                   '-F', self.FIELD_SEPARATOR, '-v', 'ON_ERROR_STOP=1']
        environment = None
        if password is not None:
            command += ['-h', '127.0.0.1']
            environment = {'VSQL_PASSWORD': password}
        command += ['-c', '; '.join(statements)]
        output = self.app.execute(command, environment=environment)
        return [line.split(self.FIELD_SEPARATOR)
                for line in output.splitlines() if line]

    def query(self, statement):
        return self.run([statement])

    ##############
    # Parameters
    ##############

    @staticmethod
    def _parameter_value(value):
        if isinstance(value, bool):
            return '1' if value else '0'
        if isinstance(value, int):
            return str(value)
        text = str(value)
        try:
            int(text)
            return text
        except ValueError:
            return quote_literal(text)

    def set_parameter(self, name, value):
        self.run(['ALTER DATABASE DEFAULT SET PARAMETER %s = %s' % (
            quote_identifier(name), self._parameter_value(value))])

    def clear_parameter(self, name):
        self.run(['ALTER DATABASE DEFAULT CLEAR PARAMETER %s'
                  % quote_identifier(name)])

    #############
    # Databases
    #############

    def _schema_names(self):
        ignored = [name.lower() for name in cfg.get_ignored_dbs()]
        return [row[0] for row in self.query(
                'SELECT schema_name FROM v_catalog.schemata '
                'WHERE NOT is_system_schema ORDER BY schema_name')
                if row[0].lower() not in ignored]

    def create_database(self, databases):
        for item in databases:
            schema = models.VerticaSchema.deserialize(item)
            schema.check_create()
            LOG.debug("Creating schema '%s'.", schema.name)
            # Tables created in it later take its privileges.
            self.run(['CREATE SCHEMA %s DEFAULT INCLUDE SCHEMA PRIVILEGES'
                      % quote_identifier(schema.name)])

    def delete_database(self, database):
        schema = models.VerticaSchema.deserialize(database)
        schema.check_delete()
        LOG.debug("Dropping schema '%s'.", schema.name)
        self.run(['DROP SCHEMA %s CASCADE' % quote_identifier(schema.name)])

    def list_databases(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [models.VerticaSchema(name) for name in self._schema_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    #########
    # Users
    #########

    def _check_modifiable(self, username):
        if username.lower() in [name.lower()
                                for name in cfg.get_ignored_users()]:
            raise exception.BadRequest(
                _("User %s is reserved.") % username)

    def _user_names(self):
        ignored = [name.lower() for name in cfg.get_ignored_users()]
        return [row[0] for row in self.query(
                'SELECT user_name FROM v_catalog.users ORDER BY user_name')
                if row[0].lower() not in ignored]

    def _find_user(self, username):
        """The name of the user as the server has it, or None."""
        for row in self.query(
                'SELECT user_name FROM v_catalog.users '
                'WHERE lower(user_name) = lower(%s)'
                % quote_literal(username)):
            return row[0]
        return None

    def _schemas_of(self, username):
        ignored = [name.lower() for name in cfg.get_ignored_dbs()]
        return [row[0] for row in self.query(
                "SELECT object_name FROM v_catalog.grants "
                "WHERE object_type = 'SCHEMA' "
                "AND lower(grantee) = lower(%s) "
                "AND privileges_description LIKE '%%USAGE%%' "
                "ORDER BY object_name" % quote_literal(username))
                if row[0].lower() not in ignored]

    def _build_user(self, username):
        user = models.VerticaUser(username)
        for schema in self._schemas_of(username):
            user.databases = schema
        return user

    def _grant(self, username, schema):
        self.run(['GRANT ALL PRIVILEGES EXTEND ON SCHEMA %s TO %s' % (
            quote_identifier(schema), quote_identifier(username))])

    def _revoke(self, username, schema):
        self.run(['REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s' % (
            quote_identifier(schema), quote_identifier(username))])

    def create_user(self, users):
        for item in users:
            user = models.VerticaUser.deserialize(item)
            self._check_modifiable(user.name)
            LOG.debug("Creating user '%s'.", user.name)
            self.run(['CREATE USER %s IDENTIFIED BY %s' % (
                quote_identifier(user.name), quote_literal(user.password))])
            for database in user.databases:
                self._grant(user.name,
                            models.VerticaSchema.deserialize(database).name)

    def delete_user(self, user):
        """The objects of the user go to the admin, not with the user."""
        user = models.VerticaUser.deserialize(user)
        self._check_modifiable(user.name)
        if self._find_user(user.name) is None:
            return
        LOG.debug("Dropping user '%s'.", user.name)
        self.run(['DROP USER %s CASCADE' % quote_identifier(user.name)])

    def list_users(self, limit=None, marker=None, include_marker=False):
        return guestagent_utils.serialize_list(
            [self._build_user(name) for name in self._user_names()],
            limit=limit, marker=marker, include_marker=include_marker)

    def get_user(self, username, hostname=None):
        if username.lower() in [name.lower()
                                for name in cfg.get_ignored_users()]:
            return None
        name = self._find_user(username)
        if name is None:
            return None
        return self._build_user(name).serialize()

    def _existing_user(self, username):
        self._check_modifiable(username)
        name = self._find_user(username)
        if name is None:
            raise exception.UserNotFound(uuid=username)
        return name

    def grant_access(self, username, hostname, databases):
        name = self._existing_user(username)
        for database in databases:
            models.VerticaSchema(database).check_reserved()
            self._grant(name, database)

    def revoke_access(self, username, hostname, database):
        name = self._existing_user(username)
        models.VerticaSchema(database).check_reserved()
        self._revoke(name, database)

    def list_access(self, username, hostname=None):
        return self._build_user(self._existing_user(username)).databases

    def _set_password(self, username, password):
        name = self._existing_user(username)
        self.run(['ALTER USER %s IDENTIFIED BY %s' % (
            quote_identifier(name), quote_literal(password))])

    def change_passwords(self, users):
        for item in users:
            user = models.VerticaUser.deserialize(item)
            LOG.debug("Changing password of user '%s'.", user.name)
            self._set_password(user.name, user.password)

    def update_attributes(self, username, hostname, user_attrs):
        name = self._existing_user(username)
        if user_attrs.get('password') is not None:
            self._set_password(name, user_attrs['password'])
        new_name = user_attrs.get('name')
        if new_name is not None and new_name.lower() != name.lower():
            new_user = models.VerticaUser(new_name)
            self._check_modifiable(new_user.name)
            self.run(['ALTER USER %s RENAME TO %s' % (
                quote_identifier(name), quote_identifier(new_user.name))])

    ###########
    # License
    ###########

    def install_license(self):
        """Install the license the guest keeps and return what the
        server says of the license it then has.
        """
        self.run(['SELECT INSTALL_LICENSE(%s)' % quote_literal(
            f'{CONTAINER_CONF_DIR}/{LICENSE_FILE}')])
        rows = self.query('SELECT DISPLAY_LICENSE()')
        return ' '.join(' '.join(row).strip() for row in rows).strip()

    ########
    # Root
    ########

    def enable_root(self, root_password=None):
        """Root is a user with the pseudosuperuser role, active when it
        logs in.
        """
        root = models.VerticaUser.root(password=root_password)
        name = quote_identifier(root.name)
        if self._find_user(root.name) is None:
            self.run([
                'CREATE USER %s IDENTIFIED BY %s' % (
                    name, quote_literal(root.password)),
                'GRANT %s TO %s' % (ROOT_ROLE, name),
                'ALTER USER %s DEFAULT ROLE %s' % (name, ROOT_ROLE),
            ])
        else:
            self.run(['ALTER USER %s IDENTIFIED BY %s' % (
                name, quote_literal(root.password))])
        return root.serialize()

    def is_root_enabled(self):
        return self._find_user(models.VerticaUser.root_username) is not None

    def disable_root(self):
        if self.is_root_enabled():
            self.run(['DROP USER %s CASCADE' % quote_identifier(
                models.VerticaUser.root_username)])
