# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""MySQL Group Replication for the MySQL and Percona Server apps.

A cluster member starts life as a single instance: ``prepare`` brings the
database up on its own. The task manager then calls ``install_cluster`` on
each member in turn, the first with ``bootstrap=True``: the member gets the
cluster configuration, its container is created again with it, and the
member forms the group or joins it. ``cluster_complete`` follows on all of
them once every member has joined, and from then on a member that starts
joins the group by itself.

A member that joins a group whose binary logs no longer hold what it lacks
is provisioned with a clone of another member. The clone replaces its data
and then needs the server restarted, which the server cannot do in the
container: it stops, the container's restart policy starts it again, and
the guest agent starts Group Replication on it once more.
"""

import time

from oslo_log import log as logging
from sqlalchemy.sql.expression import text

from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _
from trove.guestagent.common import cluster_probe
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.guestagent.utils import docker as docker_util
from trove.guestagent.utils import mysql as mysql_util

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# Written when every member has joined: from then on a member that starts
# joins the group. Before, it must not: a member joins once the guest agent
# has prepared it, and the first member forms the group instead.
CNF_STARTED = 'cluster-started'
# The mode the tenant chose, kept from the creation of the instance until
# the task manager renders the cluster configuration. With the loose-
# prefix the server takes the options once Group Replication is loaded and
# ignores them before.
CNF_MODE = 'cluster-mode'
SINGLE_PRIMARY = 'single-primary'
MULTI_PRIMARY = 'multi-primary'
MODES = (SINGLE_PRIMARY, MULTI_PRIMARY)
RECOVERY_CHANNEL = 'group_replication_recovery'
# A member in these states is in the group.
IN_GROUP = ('ONLINE', 'RECOVERING')
# A member in these states is out of it, and one to bring back.
OUT_OF_GROUP = ('OFFLINE', 'ERROR')
MEMBERS_QUERY = ("SELECT MEMBER_ID, MEMBER_STATE FROM "
                 "performance_schema.replication_group_members")
MEMBER_STATE = ("SELECT MEMBER_STATE, MEMBER_ROLE FROM "
                "performance_schema.replication_group_members "
                "WHERE MEMBER_ID = @@server_uuid")


def mode_options(mode, loose=False):
    """The options of a mode, as the [mysqld] section takes them."""
    if mode not in MODES:
        raise exception.BadRequest(
            _("The Group Replication mode must be one of %s.") %
            ', '.join(MODES))
    single = mode == SINGLE_PRIMARY
    prefix = 'loose-' if loose else ''
    return {
        prefix + 'group_replication_single_primary_mode':
            'ON' if single else 'OFF',
        prefix + 'group_replication_enforce_update_everywhere_checks':
            'OFF' if single else 'ON',
    }


class GroupReplicationAppMixin(galera_service.GaleraAppMixin):
    """Group Replication for an app built on ``MySqlApp``.

    It shares with Galera how a member keeps the cluster configuration,
    the admin password of the cluster and its health check account. List
    it before the app class it extends.
    """

    @property
    def cluster_healthcheck(self):
        """Healthy once the server answers and the member is in the group,
        or recovering into it. Until the cluster is complete a member that
        answers is healthy: it is restarted with the cluster configuration
        before it joins.
        """
        healthcheck = dict(self.HEALTHCHECK)
        healthcheck["test"] = [
            "CMD-SHELL",
            "mysql --defaults-file=%s -N -B -e \""
            "SELECT @@group_replication_start_on_boot = 0 OR EXISTS ("
            "SELECT 1 FROM performance_schema.replication_group_members "
            "WHERE MEMBER_ID = @@server_uuid AND "
            "MEMBER_STATE IN ('ONLINE', 'RECOVERING'))\" | grep -qx 1"
            % self.cluster_healthcheck_file]
        return healthcheck

    def keep_group_mode(self, mode):
        self.configuration_manager.apply_system_override(
            {'mysqld': mode_options(mode, loose=True)}, CNF_MODE)

    def _group_mode(self):
        configuration = self.cluster_configuration
        single = (configuration.get('group_replication_single_primary_mode')
                  or configuration.get(
                      'loose-group_replication_single_primary_mode')
                  or 'ON')
        return (SINGLE_PRIMARY if str(single).upper() in ('ON', '1')
                else MULTI_PRIMARY)

    def _create_cluster_replication_user(self, replication_user):
        """The account a member recovers with, and the health check logs in
        with. Every member creates it itself, outside the binary log: it
        has to exist before the member joins the group.
        """
        LOG.info("Creating the cluster replication user.")
        account = {'name': replication_user['name'],
                   'password': replication_user['password']}
        with mysql_util.SqlClient(self.get_engine()) as client:
            client.execute(text("SET SESSION sql_log_bin = 0"))
            client.execute(text("CREATE USER IF NOT EXISTS :name@'%' "
                                "IDENTIFIED BY :password"), **account)
            client.execute(text("ALTER USER :name@'%' "
                                "IDENTIFIED BY :password"), **account)
            client.execute(text("GRANT REPLICATION SLAVE, CONNECTION_ADMIN, "
                                "BACKUP_ADMIN, GROUP_REPLICATION_STREAM "
                                "ON *.* TO :name@'%'"),
                           name=account['name'])
            client.execute(text("GRANT SELECT ON performance_schema.* "
                                "TO :name@'%'"), name=account['name'])
            client.execute(text("SET SESSION sql_log_bin = 1"))

    def _set_recovery_credentials(self, replication_user):
        # Kept by the server, so that a member that starts again rejoins.
        with mysql_util.SqlClient(self.get_engine()) as client:
            client.execute(
                text("CHANGE REPLICATION SOURCE TO SOURCE_USER = :name, "
                     "SOURCE_PASSWORD = :password FOR CHANNEL '%s'"
                     % RECOVERY_CHANNEL),
                name=replication_user['name'],
                password=replication_user['password'])

    def _member_state(self):
        try:
            rows = list(self.execute_sql(MEMBER_STATE))
        except Exception as err:
            # Down, or being restarted after a clone.
            LOG.debug("No member state: %s", err)
            return None, None
        if not rows:
            return 'OFFLINE', None
        return rows[0][0], rows[0][1]

    def _restart_count(self):
        try:
            container = self.docker_client.containers.get(
                galera_service.CONTAINER_NAME)
            return container.attrs.get('RestartCount', 0)
        except Exception:
            return None

    def _start_group_replication(self, bootstrap=False):
        with mysql_util.SqlClient(self.get_engine()) as client:
            if bootstrap:
                client.execute(text(
                    "SET GLOBAL group_replication_bootstrap_group = ON"))
            try:
                client.execute(text("START GROUP_REPLICATION"))
            finally:
                if bootstrap:
                    client.execute(text(
                        "SET GLOBAL group_replication_bootstrap_group = OFF"))

    def _wait_for_group(self):
        """Wait until this member is ONLINE in the group. A member that is
        cloned stops once the clone is in place and its container starts it
        again: start Group Replication on it once more.
        """
        restarts = self._restart_count()
        started_again = False
        deadline = time.time() + CONF.restore_usage_timeout
        state = None
        while time.time() < deadline:
            state, role = self._member_state()
            if state == 'ONLINE':
                LOG.info("The member is ONLINE in the group, as %s.", role)
                return
            if state in ('OFFLINE', 'ERROR') and not started_again:
                current = self._restart_count()
                if (restarts is not None and current is not None and
                        current > restarts):
                    LOG.info("The database was restarted after a clone, "
                             "starting Group Replication again.")
                    mysql_service.ENGINE = None
                    started_again = True
                    self._start_group_replication()
            time.sleep(5)
        raise exception.TroveError(
            _("The member did not get ONLINE in the group: %s.") % state)

    def install_cluster(self, replication_user, cluster_configuration,
                        command, bootstrap=False):
        LOG.info("Installing the cluster configuration, bootstrap: %s.",
                 bootstrap)
        self._create_cluster_replication_user(replication_user)
        self.stop_db()
        self.write_cluster_configuration_overrides(cluster_configuration)
        self._write_cluster_healthcheck_file(replication_user)
        # The container keeps the health check it was created with.
        docker_util.remove_container(self.docker_client)
        self.start_db(ds_version=CONF.datastore_version, command=command)

        if not bootstrap:
            # Its own transactions, the admin user's for one, would make
            # the group refuse it: the group's are what it gets.
            self.execute_sql("RESET BINARY LOGS AND GTIDS"
                             if self._is_mysql84() else "RESET MASTER")
        self._set_recovery_credentials(replication_user)
        LOG.info("%s the group.", "Forming" if bootstrap else "Joining")
        self._start_group_replication(bootstrap=bootstrap)
        self._wait_for_group()

    def complete_cluster(self, command=None):
        """From now on the member joins the group when it starts.

        Not Galera's: Group Replication forms the group with a setting it
        turns off right after, so nothing stays to undo on the first
        member.
        """
        self.configuration_manager.apply_system_override(
            {'mysqld': {'group_replication_start_on_boot': 'ON'}},
            CNF_STARTED)

    def is_cluster_complete(self):
        return self.configuration_manager.has_system_override(CNF_STARTED)

    def write_cluster_configuration_overrides(self, cluster_configuration):
        """Keep the configuration, and give a running member the members
        it lets in and looks for: a member to come is let in before it
        tries to join.
        """
        super(GroupReplicationAppMixin,
              self).write_cluster_configuration_overrides(
            cluster_configuration)
        state, _role = self._member_state()
        if state not in ('ONLINE', 'RECOVERING'):
            return
        configuration = self.cluster_configuration
        with mysql_util.SqlClient(self.get_engine()) as client:
            for option in ('group_replication_ip_allowlist',
                           'group_replication_group_seeds'):
                value = str(configuration.get(option, '')).strip('"')
                if value:
                    client.execute(text("SET GLOBAL %s = :value" % option),
                                   value=value)

    # What the cluster probe asks of the app: where the member stands, and
    # what it takes to bring it back into the group, or to form the group
    # again after every member went down.

    def member_view(self):
        """The member's state and role in the group; it takes writes as
        the primary of a single-primary group, or as any member of a
        multi-primary one.
        """
        state, role = self._member_state()
        return cluster_probe.MemberView(
            state, role, state == 'ONLINE' and role == 'PRIMARY',
            state in OUT_OF_GROUP)

    def _self_ip(self):
        configuration = self.cluster_configuration
        host = str(configuration.get('report_host') or '').strip('"')
        if host:
            return host
        local = str(configuration.get(
            'group_replication_local_address') or '').strip('"')
        return local.rsplit(':', 1)[0].strip('[]')

    def _seed_ips(self):
        """The members the configuration names, this one included."""
        seeds = str(self.cluster_configuration.get(
            'group_replication_group_seeds') or '').strip('"')
        return [seed.strip().rsplit(':', 1)[0].strip('[]')
                for seed in seeds.split(',') if seed.strip()]

    def _query_peer(self, ip, user, password, timeout):
        """Ask a peer, over its database port with the recovery account,
        where it stands: whether it is in the group (ONLINE or RECOVERING),
        whether it sees members that are, and its executed transactions. A
        peer that does not answer is unreachable.
        """
        # One call, three result sets: the member rows have two columns,
        # then the server's uuid, then its gtid set (empty for a fresh
        # server; batch mode escapes the newlines inside it as "\n").
        command = [self.PEER_CLIENT, '--connect-timeout=%d' % timeout,
                   '--host=%s' % ip, '--port=%d' % self.DATABASE_PORT,
                   '--user=%s' % user, '--batch', '--skip-column-names',
                   '--execute=%s; SELECT @@server_uuid; '
                   'SELECT @@global.gtid_executed' % MEMBERS_QUERY]
        try:
            # The connect timeout is the client's; the whole exchange
            # gets a few times that before it is given up on.
            output = mysql_service.exec_client_in_container(
                self.docker_client, command, {'MYSQL_PWD': password},
                3 * timeout + 5)
            lines = output.rstrip('\n').split('\n')
            members = [line.split('\t', 1) for line in lines if '\t' in line]
            scalars = [line for line in lines if '\t' not in line]
            uuid = scalars[0]
            gtid = scalars[1].replace('\\n', '') if len(scalars) > 1 else ''
        except Exception as err:
            LOG.debug("Peer %s did not answer: %s", ip, err)
            return cluster_probe.PeerView(ip, False, False, False, None)
        in_group = False
        sees_group = False
        for member_id, state in members:
            if state in IN_GROUP:
                sees_group = True
                if member_id == uuid:
                    in_group = True
        return cluster_probe.PeerView(ip, True, in_group, sees_group, gtid)

    def _position(self):
        """Where the member stands: its executed transactions."""
        rows = list(self.execute_sql("SELECT @@global.gtid_executed"))
        return (rows[0][0] or '') if rows else ''

    def _not_ahead(self, position, other):
        """Whether every transaction of the first gtid set is in the
        second, as the server works it out.
        """
        with mysql_util.SqlClient(self.get_engine()) as client:
            rows = list(client.execute(
                text("SELECT GTID_SUBSET(:subset, :superset)"),
                subset=position or '', superset=other or ''))
        return bool(rows and rows[0][0])

    def rejoin_group(self, state):
        """Join the group that is up. A member in ERROR has to stop
        first: the server refuses to start it again before.
        """
        LOG.info("Rejoining the group from %s.", state)
        if state == 'ERROR':
            self.execute_sql("STOP GROUP_REPLICATION")
        self._start_group_replication()

    def bootstrap_group(self):
        """Form the group again, after every member went down."""
        LOG.info("Forming the group again.")
        self._start_group_replication(bootstrap=True)

    def leave_group(self):
        LOG.info("Leaving the group.")
        try:
            self.execute_sql("STOP GROUP_REPLICATION")
        except Exception:
            LOG.exception("Could not leave the group; the group will "
                          "expel the member once it is gone.")

    def get_cluster_context(self):
        credentials = operating_system.read_file(
            self.cluster_healthcheck_file, codec=self.CFG_CODEC,
            as_root=True)['client'] if self.is_cluster_member() else {}
        return {
            'replication_user': {
                'name': credentials.get('user'),
                'password': credentials.get('password'),
            },
            'cluster_name': str(self.cluster_configuration.get(
                'group_replication_group_name', '')).strip('"'),
            'admin_password': self.get_auth_password(),
            'mode': self._group_mode(),
        }
