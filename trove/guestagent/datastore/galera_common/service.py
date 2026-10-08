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

import datetime
import os
import re
import threading
import time

import docker
from oslo_log import log as logging
from oslo_utils import encodeutils
from sqlalchemy import exc
from sqlalchemy.sql.expression import text

from trove.common import cfg
from trove.common import constants
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
# Where the server keeps where it stands between two runs.
GRASTATE_FILE = 'grastate.dat'
SAFE_TO_BOOTSTRAP = re.compile(r'^safe_to_bootstrap:.*$', re.MULTILINE)
# What a server that starts logs once it has worked out its position: on
# Percona XtraDB Cluster "Setting GCS initial position to <uuid>:<seqno>",
# on MariaDB "Setting initial position to <uuid>:<seqno>"; a recovery run
# prints "Recovered position: <uuid>:<seqno>".
LOGGED_POSITION = re.compile(
    r'(?:initial position to|Recovered position:)\s*'
    r'([0-9a-f-]{36}):(-?\d+)')
LOG_TAIL = 400


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


def _epoch(timestamp):
    """Seconds since the epoch of a timestamp as Docker writes them
    (RFC 3339, nanoseconds), None when there is none or it does not
    parse.
    """
    if not timestamp:
        return None
    try:
        return int(datetime.datetime.strptime(
            re.sub(r'\.\d+', '', timestamp), '%Y-%m-%dT%H:%M:%SZ').replace(
                tzinfo=datetime.timezone.utc).timestamp())
    except ValueError:
        return None


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
            try:
                user, password = self._recovery_credentials()
            except Exception as err:
                # No cluster account yet: the cluster is being built
                # (install_cluster writes it), and no member takes writes
                # before it is complete.
                if self.is_cluster_complete():
                    LOG.warning("Cannot ask the members below which takes "
                                "writes: %s", err)
                else:
                    LOG.debug("No cluster account yet to ask the members "
                              "below: %s", err)
                self._writer = {'conf_id': conf_id, 'writer': None,
                                'stable': False, 'when': now}
                return None
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
            else:
                writer = self._elect_writer(status)
                if writer == self._self_ip():
                    role = 'PRIMARY'
                elif writer:
                    role = 'SECONDARY'
                # Else not known yet: no role.
        # A member out of the primary component is brought back by the
        # task manager, not by the probe: a member that waits for a
        # primary view does not open its database port, so the members
        # cannot ask each other where they stand (see recovery_view).
        return cluster_probe.MemberView(state, role, role == 'PRIMARY',
                                        False)

    # Recovery, after every member went down. A member that comes up and
    # finds no primary component waits for one, with its database port
    # closed: where it stands is in grastate.dat (after a clean stop) or in
    # what the server logged when it recovered its position (after a
    # crash). The task manager asks every member, decides and tells one
    # member to form the cluster again.

    def _grastate(self):
        """What grastate.dat holds: uuid, seqno, safe_to_bootstrap; {}
        when there is none.
        """
        path = os.path.join(self.get_data_dir(), GRASTATE_FILE)
        try:
            content = operating_system.read_file(path, as_root=True)
        except Exception as err:
            LOG.debug("No grastate.dat: %s", err)
            return {}
        state = {}
        for line in content.splitlines():
            name, sep, value = line.partition(':')
            if sep and not name.startswith('#'):
                state[name.strip()] = value.strip()
        return state

    def _logged_position(self):
        """The position the server logged when it started this time: a
        server that crashed has -1 in grastate.dat and recovers its
        position from the storage engine. Only the log of the container's
        current run counts: the log holds every run, and what an earlier
        run logged is where the member stood then.
        """
        try:
            container = self.docker_client.containers.get(CONTAINER_NAME)
            since = _epoch(container.attrs.get('State', {}).get('StartedAt'))
            output = encodeutils.safe_decode(
                (container.logs(tail=LOG_TAIL, since=since) if since
                 else container.logs(tail=LOG_TAIL)) or b'')
        except Exception as err:
            LOG.debug("No container log: %s", err)
            return None
        found = None
        for line in output.splitlines():
            match = LOGGED_POSITION.search(line)
            # -1: the server does not know either.
            if match and int(match.group(2)) >= 0:
                found = (match.group(1), int(match.group(2)))
        return found

    # The server binary of the image, for a recovery run.
    SERVER_BINARY = 'mysqld'
    _recovered = None

    def _recover_position(self):
        """The position a recovery run of the server finds in the storage
        engine, with the database stopped: what MariaDB does on every
        start through galera_recovery, which a container has not. The
        container is started again after; the answer is kept until the
        member is in a primary component again (recovery_view forgets
        it then), as nothing changes on a member that waits.
        """
        state = self._grastate()
        key = (state.get('uuid'), state.get('seqno'))
        if self._recovered and self._recovered[0] == key:
            return self._recovered[1]
        # One run at a time: the task manager asks again while one runs.
        with self._recovering:
            if self._recovered and self._recovered[0] == key:
                return self._recovered[1]
            return self._run_recovery(key)

    _recovering = threading.Lock()

    def _run_recovery(self, key):
        command = self._container_command().split()
        if not command:
            return None
        LOG.info("Running the server's recovery to find the position of "
                 "the member.")
        position = None
        try:
            self.stop_db()
            output = encodeutils.safe_decode(
                self.docker_client.containers.run(
                    self._image(), command + ['--wsrep-recover'],
                    entrypoint=self.SERVER_BINARY, remove=True,
                    user='%s:%s' % (self.database_service_uid,
                                    self.database_service_gid),
                    volumes=self._volumes(), stdout=True, stderr=True)
                or b'')
            for line in output.splitlines():
                match = LOGGED_POSITION.search(line)
                if match and int(match.group(2)) >= 0:
                    position = (match.group(1), int(match.group(2)))
            if position:
                LOG.info("The recovery run found position %s:%s.",
                         *position)
            else:
                LOG.warning("The recovery run found no position; it ended "
                            "with: %s", ' | '.join(output.splitlines()[-5:]))
        except Exception as err:
            LOG.warning("The recovery run did not give a position: %s", err)
        finally:
            try:
                self.docker_client.containers.get(CONTAINER_NAME).start()
            except Exception as err:
                LOG.warning("The database container did not start again: "
                            "%s", err)
        self._recovered = (key, position)
        return position

    def _image(self):
        return '%s:%s' % (CONF.get(CONF.datastore_manager).docker_image,
                          CONF.datastore_version)

    def _volumes(self):
        return {
            "/etc/mysql": {"bind": "/etc/mysql", "mode": "rw"},
            constants.MYSQL_HOST_SOCKET_PATH: {"bind": "/var/run/mysqld",
                                               "mode": "rw"},
            "/var/lib/mysql": {"bind": "/var/lib/mysql", "mode": "rw"},
        }

    def _position(self):
        """Where the member stands: (state uuid, seqno), None when not
        known. As the server last logged it (right after a crash,
        grastate.dat says -1 and Percona XtraDB Cluster logs what it
        recovered), else from grastate.dat (after a clean stop), else from
        a recovery run (MariaDB, which logs nothing and leaves -1 in
        grastate.dat even after a clean stop).
        """
        logged = self._logged_position()
        if logged is not None:
            return logged
        state = self._grastate()
        try:
            seqno = int(state.get('seqno', -1))
        except ValueError:
            seqno = -1
        if seqno >= 0 and state.get('uuid'):
            return (state['uuid'], seqno)
        if not state:
            return None
        return self._recover_position()

    def _not_ahead(self, position, other):
        """Whether the first position holds nothing the second lacks: the
        same history (uuid) and a seqno no higher. A member without a
        known position holds nothing; different histories are never
        compared, so no member forms the cluster again from them.
        """
        if position is None:
            return True
        if other is None:
            return False
        return position[0] == other[0] and position[1] <= other[1]

    def _waiting_for_primary(self):
        """Whether the server is up but waiting for a primary component:
        its container runs (or is being started again: MariaDB gives up
        waiting after a while, Percona XtraDB Cluster too when it cannot
        restore the view it saved) while its database port, which asked
        first, does not answer. What the server logs meanwhile is not
        looked at: the log of a container started again and again holds
        every run, and the last line of it says nothing dependable.
        """
        try:
            container = self.docker_client.containers.get(CONTAINER_NAME)
        except Exception:
            return False
        return container.status in ('running', 'restarting')

    def recovery_view(self):
        """Where the member stands, for the task manager: in a primary
        component, waiting for one (database port closed, or non-primary),
        or neither (the server is down or starting); and its position.
        """
        status = self._wsrep_status()
        if status is not None:
            in_group = (status.get('wsrep_cluster_status') == PRIMARY and
                        status.get('wsrep_local_state_comment') !=
                        'Initialized')
            waiting = not in_group
            position = (status.get('wsrep_cluster_state_uuid'),
                        int(status.get('wsrep_last_committed') or 0))
            if in_group:
                # The next wait starts from a new position.
                self._recovered = None
        else:
            in_group = False
            waiting = self._waiting_for_primary()
            position = self._position()
        # The last member to leave the cluster holds everything it
        # committed, and Galera marks it so.
        safe = (not in_group and
                self._grastate().get('safe_to_bootstrap') == '1')
        return {'ip': self._self_ip(), 'in_group': in_group,
                'waiting': waiting, 'position': position,
                'safe_to_bootstrap': safe,
                'bootstrapped': self.started_with_bootstrap()}

    def rejoin_group(self, state):
        # A member that waits for a primary component joins one as soon
        # as it appears, and a member cut off joins again when the
        # network is back: Galera does both by itself.
        LOG.info("Out of the primary component (%s); the member joins by "
                 "itself once one is there.", state)

    def bootstrap_group(self):
        """Form the cluster again on this member: mark it safe to
        bootstrap and start it again as the first member of a new
        cluster, then as an ordinary member once the others have joined,
        so that a restart does not form yet another cluster.
        """
        if not self._forming.acquire(blocking=False):
            LOG.info("The cluster is being formed again already; not "
                     "starting over.")
            return
        try:
            self._form_group()
        finally:
            self._forming.release()

    _forming = threading.Lock()

    def _form_group(self):
        if self._primary_component_meanwhile():
            return
        LOG.info("Forming the cluster again.")
        # The server waiting for a primary component holds no
        # transaction in flight and does not answer a polite stop: killed
        # before grastate.dat is marked, as a server that stops rewrites
        # the file.
        self._kill_db()
        path = os.path.join(self.get_data_dir(), GRASTATE_FILE)
        state = self._grastate()
        if state:
            content = operating_system.read_file(path, as_root=True)
            if 'safe_to_bootstrap:' in content:
                content = SAFE_TO_BOOTSTRAP.sub('safe_to_bootstrap: 1',
                                                content)
            else:
                content = content.rstrip('\n') + '\nsafe_to_bootstrap: 1\n'
            operating_system.write_file(path, content, as_root=True)
            operating_system.chown(path, self.database_service_uid,
                                   self.database_service_gid, as_root=True)
        command = self._container_command()
        self.start_cluster_node(command, bootstrap=True)
        deadline = time.time() + CONF.cluster_usage_timeout
        while time.time() < deadline:
            if self._others_synced(self._wsrep_status() or {}):
                LOG.info("The others have joined the cluster formed again.")
                break
            time.sleep(5)
        else:
            LOG.warning("No member joined the cluster formed again within "
                        "%s seconds; going on alone.",
                        CONF.cluster_usage_timeout)
        self.leave_bootstrap(command)

    def _primary_component_meanwhile(self):
        """Whether a primary component appeared since the task manager
        decided: on this member, or on a member of the cluster (then this
        one joins it by itself, or is receiving its state from it).
        Forming another cluster next to it would split the cluster.
        """
        status = self._wsrep_status() or {}
        if status.get('wsrep_cluster_status') == PRIMARY:
            LOG.info("In a primary component meanwhile; not forming the "
                     "cluster again.")
            return True
        conf = CONF.get(CONF.datastore_manager or 'mysql')
        self_ip = self._self_ip()
        try:
            user, password = self._recovery_credentials()
        except Exception as err:
            LOG.warning("Cannot ask the other members before forming the "
                        "cluster again: %s", err)
            return False
        for ip in self._seed_ips():
            if ip == self_ip:
                continue
            peer = self._peer_status(ip, user, password,
                                     conf.cluster_peer_timeout)
            if peer and peer.get('wsrep_cluster_status') == PRIMARY:
                LOG.info("Member %s is in a primary component meanwhile; "
                         "not forming the cluster again.", ip)
                return True
        return False

    def _kill_db(self):
        """Kill the database container's server: for a server that
        waits for a primary component, which does not answer a stop
        until state_change_wait_time runs out and is killed then anyway.
        """
        try:
            self.docker_client.containers.get(CONTAINER_NAME).kill()
        except docker.errors.NotFound:
            return
        except Exception as err:
            # Not running: nothing to kill.
            LOG.debug("The database container was not killed: %s", err)

    def _others_synced(self, status):
        """Whether the member that formed the cluster may leave it for a
        moment: it is synced in a primary component, and so is every
        other member of the component, over its own database port. A
        joiner still receiving the state dies with the transfer when its
        donor stops, and the member would come back to no component at
        all.
        """
        if (status.get('wsrep_cluster_status') != PRIMARY or
                status.get('wsrep_local_state_comment') != SYNCED):
            return False
        self_ip = self._self_ip()
        others = [(ip, port) for ip, port in self._incoming(status)
                  if ip != self_ip]
        if not others or any(port == '0' for _ip, port in others):
            return False
        conf = CONF.get(CONF.datastore_manager or 'mysql')
        user, password = self._recovery_credentials()
        for ip, _port in others:
            peer = self._peer_status(ip, user, password,
                                     conf.cluster_peer_timeout)
            if not peer or peer.get('wsrep_local_state_comment') != SYNCED:
                return False
        return True

    def _container_command(self):
        """The command the database container was created with, the
        bootstrap option taken out.
        """
        try:
            container = self.docker_client.containers.get(CONTAINER_NAME)
            cmd = container.attrs['Config'].get('Cmd') or []
        except Exception:
            cmd = []
        return ' '.join(arg for arg in cmd if arg != BOOTSTRAP_OPTION)

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
