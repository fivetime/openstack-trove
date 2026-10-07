#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""Galera clustering for the datastores built on the MySQL guest agent.

A cluster member starts life as a single instance: ``prepare`` brings the
database up on its own. The task manager then calls ``install_cluster`` on
each member in turn, the first with ``bootstrap=True``, and
``cluster_complete`` on all of them once every member has joined.
"""

import time

import docker
from oslo_log import log as logging
from sqlalchemy import exc
from sqlalchemy.sql.expression import text

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.guestagent.common import cluster_probe
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.common import sql_query
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.guestagent.utils import docker as docker_util
from trove.guestagent.utils import mysql as mysql_util
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

CNF_CLUSTER = 'cluster'
# Written when every member has joined.
CNF_COMPLETE = 'cluster-complete'
# The writer mode the tenant chose, kept from the creation of the instance
# until the task manager renders the cluster configuration, which carries
# it from then on.
CNF_MODE = 'cluster-mode'
# The section of the configuration that is Trove's, not the server's: the
# server reads the groups it knows and leaves the others alone.
TROVE_SECTION = 'trove'
WRITER_MODE_OPTION = 'cluster_writer_mode'
SINGLE_WRITER = 'single'
MULTI_WRITER = 'multi'
WRITER_MODES = (SINGLE_WRITER, MULTI_WRITER)
# Makes the server form a new cluster instead of joining the one named in
# wsrep_cluster_address.
BOOTSTRAP_OPTION = '--wsrep-new-cluster'
HEALTHCHECK_FILE = 'cluster-healthcheck'
CONTAINER_NAME = 'database'
# Where a member stands, as the server tells it.
WSREP_STATUS = (
    "SHOW GLOBAL STATUS WHERE Variable_name IN ("
    "'wsrep_ready', 'wsrep_cluster_status', 'wsrep_local_state_comment', "
    "'wsrep_cluster_conf_id', 'wsrep_incoming_addresses', "
    "'wsrep_last_committed', 'wsrep_cluster_state_uuid', "
    "'wsrep_local_state_uuid', 'wsrep_cluster_size')")
SYNCED = 'Synced'
PRIMARY = 'Primary'


def _parse_status(output):
    """The status rows of batch output, name and value a tab apart; any
    other line (a warning of the client, say) is left out.
    """
    status = {}
    for line in output.splitlines():
        name, sep, value = line.partition('\t')
        if sep:
            status[name.strip()] = value.strip()
    return status


def _address(address):
    """(ip, port) of an address as wsrep lists them: ip:port, with the
    ip in brackets when it is IPv6; port '' when there is none.
    """
    address = address.strip()
    host, sep, port = address.rpartition(':')
    if not sep or ']' in port:
        host, port = address, ''
    return host.strip('[]'), port


class GaleraAppMixin(object):
    """Galera clustering for an app built on ``BaseMySqlApp``.

    List it before the app class it extends, so that ``start_db`` here runs
    first.
    """

    # The section of the configuration file that holds the wsrep options.
    CLUSTER_CONF_SECTION = 'mysqld'

    @property
    def cluster_configuration(self):
        return self.configuration_manager.get_value(
            self.CLUSTER_CONF_SECTION) or {}

    def is_cluster_member(self):
        return self.configuration_manager.has_system_override(CNF_CLUSTER)

    def is_cluster_complete(self):
        """Whether every member has joined, so that a member out of the
        cluster is one to bring back.
        """
        return self.configuration_manager.has_system_override(CNF_COMPLETE)

    def keep_writer_mode(self, mode):
        """The mode the tenant chose, for the task manager to render the
        cluster configuration with.
        """
        if mode not in WRITER_MODES:
            raise exception.BadRequest(
                _("The writer mode must be one of %s.") %
                ', '.join(WRITER_MODES))
        self.configuration_manager.apply_system_override(
            {TROVE_SECTION: {WRITER_MODE_OPTION: mode}}, CNF_MODE)

    @property
    def writer_mode(self):
        """Single: one member takes writes, the lowest address of those
        that are synced. Multi: every synced member does.
        """
        section = self.configuration_manager.get_value(TROVE_SECTION) or {}
        mode = str(section.get(WRITER_MODE_OPTION) or '').strip('"')
        return mode if mode in WRITER_MODES else SINGLE_WRITER

    @property
    def cluster_healthcheck_file(self):
        # In the configuration directory on the data volume, which the
        # container sees at the same path.
        return guestagent_utils.build_file_path(
            guestagent_utils.get_conf_dir(), HEALTHCHECK_FILE,
            mysql_service.CNF_EXT)

    @property
    def cluster_healthcheck(self):
        """The container health check of a cluster member.

        A member is healthy when it is synced with the cluster, not when it
        merely answers.

        The check of a single instance cannot be used. It logs in with an
        account whose password the image generates when it initializes the
        data directory and keeps in a file beside the data. A member that
        joins receives the accounts of the cluster with its state transfer
        but keeps its own file, so the two no longer match and the member
        would be unhealthy for ever although it is part of the cluster.
        """
        raise NotImplementedError()

    def start_db(self, *args, **kwargs):
        if self.is_cluster_member():
            # On the instance, so that it replaces the check of the class
            # for every container this app creates from here on.
            self.HEALTHCHECK = self.cluster_healthcheck
        return super(GaleraAppMixin, self).start_db(*args, **kwargs)

    def _create_cluster_replication_user(self, replication_user):
        LOG.info("Creating the cluster replication user.")
        name = replication_user['name']
        password = replication_user['password']
        with mysql_util.SqlClient(
                self.get_engine(), use_flush=True) as client:
            try:
                create = sql_query.CreateUser(name, clear=password)
                client.execute(text(str(create)), **create.keyArgs)
            except (exc.OperationalError, exc.InternalError) as err:
                # It is there already; make sure of its password.
                LOG.debug(err)
                update = sql_query.SetPassword(
                    name, new_password=password,
                    ds=CONF.datastore_manager,
                    ds_version=CONF.datastore_version)
                client.execute(text(str(update)))

            grant = sql_query.Grant(
                permissions=['REPLICATION CLIENT', 'RELOAD', 'LOCK TABLES'],
                user=name)
            client.execute(text(str(grant)))

    def _write_cluster_healthcheck_file(self, replication_user):
        path = self.cluster_healthcheck_file
        operating_system.write_file(
            path,
            # Over TCP: a server started only to initialize or upgrade the
            # data directory listens on the socket alone.
            {'client': {'user': replication_user['name'],
                        'password': replication_user['password'],
                        'host': '127.0.0.1',
                        'protocol': 'tcp'}},
            codec=self.CFG_CODEC, as_root=True)
        # The health check runs in the container as the database user.
        operating_system.chown(
            path, self.database_service_uid, self.database_service_gid,
            as_root=True)
        operating_system.chmod(
            path, operating_system.FileMode.SET_USR_RW, as_root=True)

    def write_cluster_configuration_overrides(self, cluster_configuration):
        self.configuration_manager.apply_system_override(
            cluster_configuration, CNF_CLUSTER)

    def reset_admin_password(self, admin_password):
        """Give the admin user the password the whole cluster uses.

        A member that joins receives the accounts of the cluster, so the
        password stored on it has to be the cluster's before it joins.
        """
        with mysql_util.SqlClient(
                self.get_engine(), use_flush=True) as client:
            self._create_admin_user(client, admin_password)
        self.save_password(mysql_service.ADMIN_USER_NAME, admin_password)
        # The cached engine holds the old password.
        mysql_service.ENGINE = None

    def started_with_bootstrap(self):
        try:
            container = self.docker_client.containers.get(CONTAINER_NAME)
        except docker.errors.NotFound:
            return False
        return BOOTSTRAP_OPTION in (container.attrs['Config'].get('Cmd') or
                                    [])

    def start_cluster_node(self, command, bootstrap=False):
        """Create the database container as a cluster member and start it.

        A container keeps the command it was created with, so an existing
        one is removed first.
        """
        docker_util.remove_container(self.docker_client)
        if bootstrap:
            command = ('%s %s' % (command or '', BOOTSTRAP_OPTION)).strip()

        try:
            self.start_db(ds_version=CONF.datastore_version, command=command)
        except exception.TroveError:
            status = docker_util.get_container_status(self.docker_client)
            if bootstrap or status != 'running':
                raise
            # A joining member is not healthy until it has received the
            # state of the cluster, which takes as long as the data is big.
            LOG.info("The database is still starting, it may be receiving "
                     "the state of the cluster.")
            if not self.status.wait_for_status(
                    service_status.ServiceStatuses.HEALTHY,
                    CONF.restore_usage_timeout):
                raise

    def install_cluster(self, replication_user, cluster_configuration,
                        command, bootstrap=False):
        LOG.info("Installing the cluster configuration, bootstrap: %s.",
                 bootstrap)
        self._create_cluster_replication_user(replication_user)
        self.stop_db()
        self.write_cluster_configuration_overrides(cluster_configuration)
        self._write_cluster_healthcheck_file(replication_user)
        self.wipe_ib_logfiles()
        self.start_cluster_node(command, bootstrap=bootstrap)

    def leave_bootstrap(self, command):
        """Start the member that formed the cluster as an ordinary member.

        It was started with the option that forms a new cluster, and its
        container would be started with it again after every reboot. Galera
        refuses to form a cluster from a member that was not the last to
        leave it, so the database would not come back.
        """
        if not self.started_with_bootstrap():
            return
        LOG.info("Starting the member that formed the cluster again, to "
                 "join the cluster like the others.")
        self.stop_db()
        self.start_cluster_node(command)

    def complete_cluster(self, command=None):
        """Every member has joined. The one that formed the cluster can
        leave and come back without the cluster going down.
        """
        self.leave_bootstrap(command)
        self.configuration_manager.apply_system_override(
            {TROVE_SECTION: {'cluster_complete': 'yes'}}, CNF_COMPLETE)

    def leave_group(self):
        """The member is about to be deleted. A Galera member leaves the
        cluster when its database stops; nothing to do before.
        """
        pass

    # What the cluster probe asks of the app: where the member stands.

    def _wsrep_status(self):
        """The wsrep status of the member, None when the server does not
        answer.
        """
        try:
            return {name: value for name, value in
                    self.execute_sql(WSREP_STATUS)}
        except Exception as err:
            LOG.debug("No wsrep status: %s", err)
            return None

    def _self_ip(self):
        host, _port = _address(str(self.cluster_configuration.get(
            'wsrep_node_address') or '').strip('"'))
        return host

    def _seed_ips(self):
        """The members the configuration names, this one included."""
        address = str(self.cluster_configuration.get(
            'wsrep_cluster_address') or '').strip('"')
        _scheme, _sep, members = address.partition('://')
        return [_address(member)[0] for member in members.split(',')
                if member.strip()]

    def _peer_status(self, ip, user, password, timeout):
        """The wsrep status of a peer, asked over its database port with
        the cluster account from inside the database container; None
        when it does not answer.
        """
        command = [self.PEER_CLIENT, '--connect-timeout=%d' % timeout,
                   '--host=%s' % ip, '--port=%d' % self.DATABASE_PORT,
                   '--user=%s' % user, '--batch', '--skip-column-names',
                   '--execute=%s' % WSREP_STATUS]
        try:
            output = mysql_service.exec_client_in_container(
                self.docker_client, command, {'MYSQL_PWD': password},
                3 * timeout + 5)
            return _parse_status(output)
        except Exception as err:
            LOG.debug("Peer %s did not answer: %s", ip, err)
            return None

    def _query_peer(self, ip, user, password, timeout):
        """A peer as the recovery sees it: in the cluster when it is in a
        primary component, and its position in the cluster's history.
        """
        status = self._peer_status(ip, user, password, timeout)
        if status is None:
            return cluster_probe.PeerView(ip, False, False, False, None)
        in_group = status.get('wsrep_cluster_status') == PRIMARY
        return cluster_probe.PeerView(
            ip, True, in_group, in_group,
            (status.get('wsrep_cluster_state_uuid'),
             status.get('wsrep_last_committed')))

    def _incoming(self, status):
        """The members of the member's component, (ip, port) each; the
        port is 0 for a member that does not take connections yet (a
        joiner receiving a state transfer).
        """
        addresses = status.get('wsrep_incoming_addresses') or ''
        return [_address(member) for member in addresses.split(',')
                if member.strip()]

    _writer = None

    def _elect_writer(self, status):
        """The member that takes writes in single writer mode: the one
        with the lowest address among the synced members of the primary
        component. Every member works it out the same way from the same
        view, asking only the members below it whether they are synced;
        the answer is kept until the view changes or for a while, except
        when a member below is not synced yet (it may be about to): then
        it is asked again next time.
        """
        conf = CONF.get(CONF.datastore_manager or 'mysql')
        keep_for = (conf.cluster_ready_port_reconcile *
                    conf.cluster_probe_interval)
        conf_id = status.get('wsrep_cluster_conf_id')
        now = time.monotonic()
        cached = self._writer
        if (cached and cached['conf_id'] == conf_id and cached['stable'] and
                now - cached['when'] < keep_for):
            return cached['writer']
        self_ip = self._self_ip()
        ip_key = cluster_probe.ip_key
        below = sorted({ip for ip, port in self._incoming(status)
                        if port != '0' and ip_key(ip) < ip_key(self_ip)},
                       key=ip_key)
        writer, stable = self_ip, True
        if below:
            user, password = self._recovery_credentials()
            for ip in below:
                peer = self._peer_status(ip, user, password,
                                         conf.cluster_peer_timeout)
                if (peer and peer.get('wsrep_local_state_comment') == SYNCED
                        and peer.get('wsrep_cluster_status') == PRIMARY):
                    writer = ip
                    break
                stable = False
        if not cached or cached['writer'] != writer:
            LOG.info("The writer of the cluster is %s.", writer)
        self._writer = {'conf_id': conf_id, 'writer': writer,
                        'stable': stable, 'when': now}
        return writer

    def member_view(self):
        """The member's wsrep state; it takes writes when it is synced in
        a primary component and, in single writer mode, it is the writer.
        The role is PRIMARY for a member that takes writes and SECONDARY
        for a synced member that does not; a member in any other state
        has none.
        """
        status = self._wsrep_status()
        if status is None:
            return cluster_probe.UNKNOWN_MEMBER
        state = status.get('wsrep_local_state_comment')
        primary = status.get('wsrep_cluster_status') == PRIMARY
        synced = (primary and state == SYNCED and
                  status.get('wsrep_ready') == 'ON')
        role = None
        if synced:
            if self.writer_mode == MULTI_WRITER:
                role = 'PRIMARY'
            elif self._elect_writer(status) == self._self_ip():
                role = 'PRIMARY'
            else:
                role = 'SECONDARY'
        # A member out of the primary component is not brought back yet:
        # the next step.
        return cluster_probe.MemberView(state, role, role == 'PRIMARY',
                                        False)

    def get_member_role(self):
        """The member's state and role in the cluster, and whether it
        takes writes, as the cluster's view shows them.
        """
        view = self.member_view()
        return {'state': view.state, 'role': view.role,
                'writable': bool(view.writable)}

    def is_writable_member(self):
        return bool(self.member_view().writable)

    def _recovery_credentials(self):
        """The account the members ask each other with: the health check's,
        which every member has.
        """
        credentials = operating_system.read_file(
            self.cluster_healthcheck_file, codec=self.CFG_CODEC,
            as_root=True)['client']
        return credentials['user'], credentials['password']

    def get_cluster_context(self):
        configuration = self.cluster_configuration
        name, _sep, password = configuration.get(
            'wsrep_sst_auth', '').replace('"', '').partition(':')
        return {
            'replication_user': {
                'name': name,
                'password': password,
            },
            'cluster_name': configuration.get('wsrep_cluster_name'),
            'admin_password': self.get_auth_password(),
            'writer_mode': self.writer_mode,
        }
