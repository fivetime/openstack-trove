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

"""A PostgreSQL cluster member: the primary, or a standby streaming from
it through a replication slot of its own.

What the member knows of its cluster is in ``trove-cluster.json`` beside
the configuration (the members by id and address, the mode, which member
it is, the replication user); the server settings of a member are system
overrides in ``conf.d``, never ``ALTER SYSTEM``, which a base backup or
pg_rewind would carry from one member to another. The mode is the
tenant's: a commit on the primary waits for one standby (``sync``, back
to none when no standby streams for a grace period; ``strict``, always)
or for none (``async``); the primary keeps ``synchronous_standby_names``
right every probe tick. The primary answers on its ready port; a standby
that streams and keeps up answers on its read port.

The primary takes writes only while something says it is the primary the
cluster follows: a standby streaming from it, or a lease the task manager
renews for the primary it judges healthy (for ``cluster_fence_grace``
seconds after the last of either); a primary cut off from both closes
its write port by itself, and opens it again when either is back. A
member whose data is a primary's but whose cluster file names another
primary (the cluster failed over while it was away) stays closed until
the task manager has it rejoin. Failing over is the task manager's: it
fences the old primary (a checkpoint, a fast stop), promotes the standby
that holds the most, points the other standbys at it, and has the old
primary rejoin as a standby: ``pg_rewind`` against the new primary, or
a base backup when that fails.
"""

from concurrent import futures
import json
import os
import re
import time

from oslo_log import log as logging
from oslo_utils import encodeutils
from oslo_utils import netutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.postgresql import models
from trove.common import exception
from trove.common.i18n import _
from trove.common import ssl
from trove.common import stream_codecs
from trove.common import utils
from trove.guestagent.common import cluster_member
from trove.guestagent.common import cluster_probe
from trove.guestagent.common import operating_system
from trove.guestagent.common import readyport
from trove.guestagent.datastore.postgres import service as pg_service
from trove.guestagent.utils import docker as docker_util
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

MODE_KEY = 'replication_mode'
ASYNC, SYNC, STRICT = 'async', 'sync', 'strict'
MODES = (ASYNC, SYNC, STRICT)

CLUSTER_FILE = '/etc/postgresql/trove-cluster.json'
# The system overrides in conf.d: the server settings of every member,
# what a standby streams from, what the primary's commits wait for.
CNF_SETTINGS = 'cluster-settings'
CNF_REPLICATION = 'cluster-replication'
CNF_SYNC = 'cluster-sync'
# A commit waits for any one standby, whichever answers first.
ANY_ONE = 'ANY 1 (*)'

PRIMARY, STANDBY, REBUILDING, STOPPED = ('primary', 'standby',
                                         'rebuilding', 'stopped')
# What the primary reports a standby's sync_state as when a commit waits
# for it.
SYNCHRONOUS_STATES = ('sync', 'quorum')
STREAMING = 'streaming'
# Seconds a standby waits for another member on the tenant network;
# the task manager waits cluster_peer_timeout for the whole answer.
PEER_PROBE_TIMEOUT = 1

SLOT_PATTERN = re.compile(r'^[a-z0-9_]{1,63}$')

STANDBYS_QUERY = (
    "SELECT application_name, state, sync_state, "
    "pg_wal_lsn_diff(sent_lsn, replay_lsn)::bigint "
    "FROM pg_stat_replication")
RECEIVER_QUERY = (
    "SELECT status, sender_host, "
    "extract(epoch FROM now() - last_msg_receipt_time)::float, "
    "pg_wal_lsn_diff(pg_last_wal_receive_lsn(), "
    "pg_last_wal_replay_lsn())::bigint "
    "FROM pg_stat_wal_receiver")
# Where the member stands in the cluster's history: its timeline, and
# its position as a number of bytes.
TIMELINE_QUERY = "SELECT timeline_id FROM pg_control_checkpoint()"
PRIMARY_LSN_QUERY = (
    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint")
RECEIVE_LSN_QUERY = (
    "SELECT pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0')::bigint")
REPLAY_LSN_QUERY = (
    "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '0/0')::bigint")
# Seconds pg_promote waits for the promotion.
PROMOTE_TIMEOUT = 60


def slot_name(member_id):
    """The replication slot a member streams through: its id, as a slot
    name can be spelled.
    """
    name = 'trove_' + str(member_id).lower().replace('-', '_')
    if not SLOT_PATTERN.match(name):
        raise exception.BadRequest(_("Not a slot name: %s") % name)
    return name


class PostgresqlClusterAppMixin(object):
    """Clustering for an app built on ``PgSqlApp``: list it before the app
    class it extends.
    """

    DATABASE_PORT = 5432

    _sync_names = None
    _no_standby_since = None
    _standbys = None
    rebuilding = False
    # The primary's self-fencing: until when the task manager's lease
    # runs, when a standby or the lease last said this is the primary,
    # and whether the task manager fenced the member.
    _lease_until = 0
    _lease_ttl = 0
    _confirmed_at = None
    _stale_warned = False
    fenced = False
    # A standby: since when it has not been streaming, and which
    # members it last reached on the tenant network meanwhile.
    _detached_since = None
    _reached = None

    # What the member knows of its cluster

    def _read_cluster(self):
        try:
            return operating_system.read_file(
                CLUSTER_FILE, codec=stream_codecs.JsonCodec(),
                as_root=True) or {}
        except Exception:
            return {}

    def _write_cluster(self, cluster):
        operating_system.write_file(
            CLUSTER_FILE, cluster, codec=stream_codecs.JsonCodec(),
            as_root=True)
        operating_system.chmod(CLUSTER_FILE,
                               operating_system.FileMode.SET_USR_RW,
                               as_root=True)

    def _update_cluster(self, **changes):
        cluster = self._read_cluster()
        cluster.update(changes)
        self._write_cluster(cluster)
        return cluster

    def is_cluster_member(self):
        return bool(self._read_cluster().get('members'))

    def is_cluster_complete(self):
        return bool(self._read_cluster().get('complete'))

    def complete_cluster(self):
        """Every member has joined."""
        self._update_cluster(complete=True)

    def keep_cluster_mode(self, mode):
        """The mode the tenant chose, for the task manager to set the
        cluster up with.
        """
        if mode not in MODES:
            raise exception.BadRequest(
                _("The replication mode must be one of %s.") %
                ', '.join(MODES))
        self._update_cluster(mode=mode)

    @property
    def cluster_mode(self):
        mode = self._read_cluster().get('mode')
        return mode if mode in MODES else SYNC

    @property
    def cluster_members(self):
        return self._read_cluster().get('members') or {}

    @property
    def self_id(self):
        return self._read_cluster().get('self_id') or CONF.guest_id

    def update_cluster_members(self, members, mode, primary_id=None):
        """The members and the mode, as the task manager has them after
        a grow or a shrink, and the primary after a failover; the primary
        applies the mode at its next tick.
        """
        changes = {'members': members, 'mode': mode}
        if primary_id:
            changes['primary_id'] = primary_id
        self._update_cluster(**changes)

    @property
    def primary_id(self):
        return self._read_cluster().get('primary_id')

    def remember_sync_set(self, primary_id, members, lsn=None, alone=False):
        """The standbys the primary last streamed to, whether it has
        gone on without any since, and in async mode where it was, kept
        here for the task manager: when the primary is gone, they say
        which standbys may hold its last acknowledged commits, whether
        it may have acknowledged some alone, and how far behind the
        best standby is, even to a task manager that has restarted
        meanwhile. ``members`` None keeps the set kept for the same
        primary. Returns whether the primary, the standbys or whether
        it is alone changed.
        """
        kept = self._read_cluster().get('sync_set') or {}
        if members is None and kept.get('primary_id') == primary_id:
            members = kept.get('members')
        record = {'primary_id': primary_id,
                  'members': sorted(members) if members is not None
                  else None,
                  'lsn': lsn, 'alone': bool(alone)}
        self._update_cluster(sync_set=record)
        return (kept.get('primary_id'), kept.get('members'),
                bool(kept.get('alone'))) != (
            primary_id, record['members'], record['alone'])

    def get_cluster_context(self):
        cluster = self._read_cluster()
        return {'replication_user': cluster.get('replication_user'),
                'mode': self.cluster_mode,
                'members': cluster.get('members') or {},
                'primary_id': cluster.get('primary_id'),
                'self_id': cluster.get('self_id')}

    # The lease: the task manager's word that this is the primary

    def renew_lease(self, ttl):
        """The task manager judged this member the healthy primary: it
        takes writes for ``ttl`` seconds more, standbys or not.
        """
        now = time.monotonic()
        self._lease_until = now + ttl
        self._lease_ttl = ttl
        self._confirmed_at = now

    def lease_fresh(self):
        return time.monotonic() < self._lease_until

    # Installing

    def _cluster_conf(self):
        return CONF.get(CONF.datastore_manager)

    def _cluster_settings(self):
        """The server settings of every member; they need a restart, once,
        when the member is installed.
        """
        conf = self._cluster_conf()
        timeout = '%ss' % conf.cluster_wal_timeout
        return {'wal_level': 'replica', 'hot_standby': 'on',
                'wal_log_hints': 'on',
                'max_wal_senders': 20, 'max_replication_slots': 20,
                'wal_sender_timeout': timeout,
                'wal_receiver_timeout': timeout,
                'max_slot_wal_keep_size': conf.cluster_max_slot_wal_keep_size,
                'synchronous_commit': 'on',
                'password_encryption': 'scram-sha-256'}

    def _apply_cluster_settings(self):
        self.configuration_manager.apply_system_override(
            self._cluster_settings(), CNF_SETTINGS)

    def _apply_cluster_access_rules(self, replication_user):
        # The SSL mode is registered on the guest alone.
        mode = getattr(CONF, 'ssl_mode', None)
        self.apply_access_rules(
            ssl=mode in (ssl.MODE_ENFORCED, ssl.MODE_MTLS),
            mtls=mode == ssl.MODE_MTLS,
            replication_user=replication_user, scram=True)

    def _ensure_replication_user(self, replication_user):
        """The account the standbys stream with, with the password of the
        cluster; a superuser, as pg_rewind needs one.
        """
        name, password = replication_user['name'], replication_user['password']
        options = ('REPLICATION', 'SUPERUSER', 'LOGIN')
        user = models.PostgreSQLUser(name=name, password=password)
        if self.adm.user_exists(name):
            self.adm.alter_user(user, None, *options)
        else:
            self.adm.create_user(user, None, *options)

    def _write_sync_names(self, names):
        self.configuration_manager.apply_system_override(
            {'synchronous_standby_names': names}, CNF_SYNC)
        self._sync_names = names

    def _write_replication(self, primary_host, replication_user):
        """What a standby streams from: through its own slot, as itself
        by name, so that the primary tells the standbys apart.
        """
        conninfo = ('host=%s port=%s user=%s password=%s '
                    'application_name=%s dbname=postgres' % (
                        primary_host, self.DATABASE_PORT,
                        replication_user['name'],
                        replication_user['password'], self.self_id))
        self.configuration_manager.apply_system_override(
            {'primary_conninfo': conninfo,
             'primary_slot_name': slot_name(self.self_id)}, CNF_REPLICATION)

    def _write_standby_signal(self):
        signal_file = os.path.join(self.datadir, 'standby.signal')
        operating_system.execute_shell_cmd(
            'touch %s' % signal_file, [], shell=True, as_root=True)
        operating_system.chown(signal_file, self.database_service_uid,
                               self.database_service_gid, force=True,
                               as_root=True)

    def _clear_data(self):
        """An empty data directory and WAL archive, for a base backup.

        Not ``rm -rf dir/*``: the glob is expanded by the agent's shell
        before sudo, and the agent cannot list the database user's data
        directory (mode 700), so nothing would be removed. find runs as
        root and sees everything, dot files included.
        """
        for path in (self.datadir, pg_service.WAL_ARCHIVE_DIR):
            operating_system.ensure_directory(
                path, user=self.database_service_uid,
                group=self.database_service_gid, as_root=True)
            utils.execute_with_timeout(
                'find', path, '-mindepth', '1', '-delete',
                run_as_root=True, root_helper='sudo', timeout=600)

    def _run_in_parked_container(self, command, environment=None):
        """Run a tool against the data directory while the server is
        down, with the tenant NIC: the database container's own network,
        which only one container has at a time. The database container
        is replaced by one that does nothing, the tool runs in it, and
        the container is removed; the server is started anew after.
        Returns what the tool printed, raises when it failed.
        """
        docker_util.remove_container(self.docker_client)
        spec = self.container_spec(ds_version=CONF.datastore_version)
        docker_util.start_container(
            self.docker_client, spec['image'], name=readyport.CONTAINER_NAME,
            restart_policy='no', volumes=spec['volumes'],
            ports=spec['ports'], user=spec['user'],
            network_mode=spec['network_mode'],
            environment=spec['environment'], command='sleep infinity')
        container = self.docker_client.containers.get(readyport.CONTAINER_NAME)
        try:
            LOG.info("Running %s in the parked container.", command[0])
            code, output = container.exec_run(
                command, environment=environment or {}, user=spec['user'])
        finally:
            container.remove(force=True)
        output = encodeutils.safe_decode(output or b'')
        if code != 0:
            raise exception.TroveError(
                _("%(tool)s failed: %(output)s") % {
                    'tool': command[0], 'output': output.strip()[-2000:]})
        return output

    def _server_command(self):
        return 'postgres -c config_file=%s' % pg_service.CONFIG_FILE

    def _wait_streaming(self, timeout=180):
        deadline = time.time() + timeout
        status = None
        while time.time() < deadline:
            try:
                rows = self.adm.query(
                    "SELECT status FROM pg_stat_wal_receiver")
                status = rows[0][0] if rows else None
            except Exception as err:
                LOG.debug("No receiver status yet: %s", err)
            if status == STREAMING:
                LOG.info("The standby is streaming.")
                return
            time.sleep(3)
        raise exception.TroveError(
            _("The standby is not streaming from the primary: %s") % status)

    def install_cluster(self, cluster_config):
        """Install the member: as the primary when the configuration names
        none, as a standby of it otherwise.
        """
        members = cluster_config['members']
        mode = cluster_config.get('mode') or SYNC
        replication_user = cluster_config['replication_user']
        primary = cluster_config.get('primary')
        self_id = cluster_config.get('self_id') or CONF.guest_id
        self._update_cluster(
            members=members, mode=mode, self_id=self_id,
            replication_user=replication_user,
            primary_id=self_id if primary is None else primary['id'],
            complete=False)
        self._apply_cluster_settings()
        if primary is None:
            self._install_primary(mode, replication_user)
        else:
            self._install_standby(primary, replication_user)

    def _install_primary(self, mode, replication_user):
        LOG.info("Installing the member as the primary, mode %s.", mode)
        self._ensure_replication_user(replication_user)
        self._apply_cluster_access_rules(replication_user['name'])
        # Until a standby streams, a commit waits for none, unless the
        # mode is strict.
        self._write_sync_names(ANY_ONE if mode == STRICT else '')
        self._no_standby_since = None
        self.restart()

    def _install_standby(self, primary, replication_user):
        LOG.info("Installing the member as a standby of %s.",
                 primary['host'])
        self._apply_cluster_access_rules(replication_user['name'])
        self._write_replication(primary['host'], replication_user)
        self.rebuilding = True
        try:
            self.stop_db()
            self._base_backup(primary, replication_user)
            self._write_standby_signal()
            self.start_db(ds_version=CONF.datastore_version,
                          command=self._server_command())
            self._wait_streaming()
        finally:
            self.rebuilding = False

    def _base_backup(self, primary, replication_user):
        """The data directory emptied and taken from the primary, through
        this member's slot.
        """
        self._clear_data()
        self._run_in_parked_container(
            ['pg_basebackup', '-h', primary['host'],
             '-p', str(self.DATABASE_PORT),
             '-U', replication_user['name'], '-D', self.datadir,
             '-X', 'stream', '-S', slot_name(self.self_id),
             '--checkpoint=fast', '--no-password'],
            {'PGPASSWORD': replication_user['password']})

    # Slots

    def _slot_exists(self, name):
        rows = self.adm.query(
            "SELECT active_pid FROM pg_replication_slots "
            "WHERE slot_name = '%s'" % name)
        return rows[0] if rows else None

    def create_member_slot(self, member_id):
        name = slot_name(member_id)
        if self._slot_exists(name) is None:
            LOG.info("Creating replication slot %s.", name)
            self.adm.query(
                "SELECT pg_create_physical_replication_slot('%s')" % name)

    def drop_member_slot(self, member_id):
        name = slot_name(member_id)
        row = self._slot_exists(name)
        if row is None:
            return
        if row[0] is not None:
            # The member still streams: its WAL sender goes first.
            self.adm.query("SELECT pg_terminate_backend(%s)" % row[0])
            time.sleep(1)
        LOG.info("Dropping replication slot %s.", name)
        self.adm.query("SELECT pg_drop_replication_slot('%s')" % name)

    # Where the member stands

    def _self_ip(self):
        if (CONF.network_isolation and
                os.path.exists(constants.ETH1_CONFIG_PATH)):
            with open(constants.ETH1_CONFIG_PATH) as fd:
                eth1 = json.load(fd)
            ip = eth1.get('ipv4_address') or eth1.get('ipv6_address')
            if ip:
                return ip
        return netutils.get_my_ipv4()

    def _standbys_of_primary(self):
        """The standbys as the primary sees them, by name."""
        return {row[0]: {'state': row[1], 'sync_state': row[2],
                         'lag_bytes': int(row[3] or 0)}
                for row in self.adm.query(STANDBYS_QUERY)}

    def _reach_peers(self, members, timeout=PEER_PROBE_TIMEOUT):
        """Which of the other members this one reaches on the tenant
        network, from the database container (the tenant NIC is its):
        ``pg_isready`` to each one at once; a server that answers at
        all, accepting connections or not, is reached; a probe not back
        in time says nothing (None). Asked while the member does not
        stream, so that the task manager can tell a primary cut off from
        the cluster from standbys cut off themselves; logged when what
        is reached changes.
        """
        self_id = self.self_id
        others = {i: ip for i, ip in members.items()
                  if i != self_id and ip}
        if not others:
            return {}
        container = self.docker_client.containers.get(readyport.CONTAINER_NAME)
        user = (self._read_cluster().get('replication_user') or {}).get(
            'name') or 'postgres'
        port = cfg.get_configuration_property('postgresql_port')

        def probe(ip):
            code, _output = container.exec_run(
                ['pg_isready', '-h', ip, '-p', str(port),
                 '-t', str(timeout), '-U', user])
            return code in (0, 1)
        pool = futures.ThreadPoolExecutor(max_workers=len(others))
        try:
            asked = {i: pool.submit(probe, ip) for i, ip in others.items()}
            # A probe that fails takes the timeout, and the exec some
            # more.
            futures.wait(asked.values(), timeout=timeout + 1)
        finally:
            pool.shutdown(wait=False)
        reached = {}
        for i, future in asked.items():
            try:
                reached[i] = future.result(timeout=0)
            except futures.TimeoutError:
                LOG.warning("The probe of %s is not back in time.",
                            others[i])
                reached[i] = None
            except Exception as err:
                LOG.warning("No probe of %s: %s", others[i], err)
                reached[i] = None
        if reached != self._reached:
            def which(wanted):
                return ', '.join(others[i] for i, r in reached.items()
                                 if r is wanted) or 'nobody'
            LOG.info("On the tenant network, this member reaches %s, not "
                     "%s; no answer about %s.", which(True), which(False),
                     which(None))
            self._reached = reached
        return reached

    def _receiver(self):
        rows = self.adm.query(RECEIVER_QUERY)
        if not rows:
            return None
        status, sender, age, lag = rows[0]
        return {'status': status, 'sender_host': sender,
                'last_msg_age': float(age) if age is not None else None,
                'replay_lag_bytes': int(lag or 0)}

    def member_view(self):
        """The member now: the primary takes writes (in strict mode, only
        with a synchronous standby there); a standby serves reads while
        it streams and keeps up.
        """
        if self.rebuilding:
            return cluster_probe.MemberView(REBUILDING, None, False, False)
        try:
            in_recovery = self.adm.query("SELECT pg_is_in_recovery()")[0][0]
        except Exception as err:
            LOG.debug("The server does not answer: %s", err)
            return cluster_probe.MemberView(STOPPED, None, False, False)
        if not in_recovery:
            standbys = self._standbys_of_primary()
            self._standbys = standbys
            self._detached_since = None
            return cluster_probe.MemberView(
                PRIMARY, 'PRIMARY', self._primary_writable(standbys), False)
        receiver = self._receiver()
        conf = self._cluster_conf()
        streaming = bool(receiver and receiver['status'] == STREAMING)
        if streaming:
            self._detached_since = None
        elif self._detached_since is None:
            self._detached_since = time.monotonic()
        readable = (streaming and
                    receiver['last_msg_age'] is not None and
                    receiver['last_msg_age'] < 2 * conf.cluster_wal_timeout
                    and (receiver['replay_lag_bytes'] <
                         conf.cluster_read_max_lag_bytes))
        return cluster_probe.MemberView(
            STANDBY, 'STREAMING' if streaming else 'DETACHED', False,
            False, readable)

    def _primary_writable(self, standbys):
        """Whether the primary takes writes now.

        Never when the task manager fenced it, nor when the cluster file
        names another member as the primary: this one was the primary
        before it was away, and the cluster went on without it. In
        strict mode, only with a synchronous standby streaming. Else,
        with a standby streaming from it, or with the task manager's
        lease, or for ``cluster_fence_grace`` seconds after the last of
        either; a primary that has had neither since it came up waits
        for one. A cluster of one member takes writes regardless.
        """
        if self.fenced:
            return False
        cluster = self._read_cluster()
        primary_id = cluster.get('primary_id')
        self_id = cluster.get('self_id') or CONF.guest_id
        if primary_id and primary_id != self_id:
            if not self._stale_warned:
                LOG.warning("This member's data is a primary's, but the "
                            "cluster's primary is %s: no writes until it "
                            "rejoins.", primary_id)
                self._stale_warned = True
            return False
        self._stale_warned = False
        streaming = any(s['state'] == STREAMING for s in standbys.values())
        if self.cluster_mode == STRICT:
            return any(s['state'] == STREAMING and
                       s['sync_state'] in SYNCHRONOUS_STATES
                       for s in standbys.values())
        if len(cluster.get('members') or {}) <= 1:
            return True
        now = time.monotonic()
        if streaming or self.lease_fresh():
            self._confirmed_at = now
            return True
        return (self._confirmed_at is not None and
                now - self._confirmed_at <
                self._cluster_conf().cluster_fence_grace)

    def _timeline(self):
        return int(self.adm.query(TIMELINE_QUERY)[0][0])

    def _lsn(self, query):
        value = self.adm.query(query)[0][0]
        return int(value) if value is not None else None

    def _sync_names_wanted(self, mode, streaming):
        """What the primary's commits wait for, by the mode and whether a
        standby streams; None to leave it as it is for now.
        """
        if mode == ASYNC:
            return ''
        if mode == STRICT:
            return ANY_ONE
        if streaming:
            return ANY_ONE
        if (time.monotonic() - self._no_standby_since >=
                self._cluster_conf().cluster_sync_fallback_grace):
            return ''
        return None

    def reconcile(self, view):
        """Every probe tick, on the primary: keep synchronous_standby_names
        as the mode and the standbys that stream warrant.
        """
        if view.state != PRIMARY or self.fenced:
            self._no_standby_since = None
            return
        if self._sync_names is None:
            self._sync_names = self.get_config_param(
                'synchronous_standby_names')
        streaming = any(s['state'] == STREAMING
                        for s in (self._standbys or {}).values())
        # Since when no standby streams, in every mode: the task manager
        # reads it to tell a primary cut off from its standbys.
        if streaming:
            self._no_standby_since = None
        elif self._no_standby_since is None:
            self._no_standby_since = time.monotonic()
        wanted = self._sync_names_wanted(self.cluster_mode, streaming)
        if wanted is None or wanted == self._sync_names:
            return
        LOG.info("Commits wait for %s now (%s mode, %s standby streaming).",
                 wanted or 'no standby', self.cluster_mode,
                 'a' if streaming else 'no')
        self._write_sync_names(wanted)
        self.reload()

    def get_member_role(self):
        view = self.member_view()
        answer = {'state': view.state, 'role': view.role,
                  'writable': bool(view.writable),
                  'readable': bool(view.readable),
                  'mode': self.cluster_mode}
        if view.state == PRIMARY:
            answer['standbys'] = self._standbys or {}
        return answer

    def is_writable_member(self):
        return bool(self.member_view().writable)

    def recovery_view(self):
        """Where the member stands, for the task manager: its state and
        whom it takes for the primary; on the primary its standbys, its
        position and for how long no standby has streamed; on a standby
        its receiver, its received and replayed positions, for how long
        it has not been streaming and, while it does not, which members
        it reaches on the tenant network; the timeline on both.
        """
        receiver = None
        view = self.member_view()
        cluster = self._read_cluster()
        now = time.monotonic()
        answer = {'ip': self._self_ip(), 'state': view.state,
                  'role': view.role, 'mode': self.cluster_mode,
                  'self_id': self.self_id,
                  'primary_id': cluster.get('primary_id'),
                  'sync_set': cluster.get('sync_set'),
                  'writable': bool(view.writable),
                  'fenced': self.fenced, 'lease_fresh': self.lease_fresh()}
        try:
            if view.state == PRIMARY:
                answer['standbys'] = self._standbys or {}
                answer['lsn'] = self._lsn(PRIMARY_LSN_QUERY)
                answer['timeline'] = self._timeline()
                answer['no_standby_for'] = (
                    now - self._no_standby_since
                    if self._no_standby_since is not None else None)
            elif view.state == STANDBY:
                receiver = self._receiver()
                answer['receiver'] = receiver
                answer['lsn'] = self._lsn(RECEIVE_LSN_QUERY)
                answer['replay_lsn'] = self._lsn(REPLAY_LSN_QUERY)
                answer['timeline'] = self._timeline()
                answer['disconnected_for'] = (
                    now - self._detached_since
                    if self._detached_since is not None else None)
        except Exception as err:
            LOG.debug("No position: %s", err)
        # Which members this one reaches on the tenant network: a
        # standby that does not stream, and a primary without a standby
        # (the task manager tells a primary cut off from the cluster
        # from standbys cut off themselves by it).
        if (view.state == STANDBY and not (
                receiver and receiver['status'] == STREAMING)) or (
                view.state == PRIMARY and self._no_standby_since is not None):
            try:
                answer['peers'] = self._reach_peers(
                    cluster.get('members') or {})
            except Exception as err:
                LOG.debug("No probe of the members: %s", err)
        return answer

    def cluster_signal(self, view):
        """What the probe tells the control plane: abnormal while
        rebuilding, stopped, fenced, a primary the cluster left, a
        primary in strict mode without a synchronous standby, or a
        standby that does not stream; a primary alone in sync or async
        mode asks for its lease instead, again when a third of it is
        left (every few seconds before it is granted).
        """
        signal = cluster_probe.ClusterSignal
        now = time.monotonic()
        if self.rebuilding or view.state == REBUILDING:
            return signal(True, cluster_probe.ABNORMAL, 'rebuilding')
        if view.state == STOPPED:
            return signal(True, cluster_probe.ABNORMAL,
                          'the server does not answer')
        if self.fenced:
            return signal(True, cluster_probe.ABNORMAL, 'fenced')
        if view.state == STANDBY:
            if view.role != 'DETACHED':
                return cluster_probe.NORMAL
            away = now - (self._detached_since or now)
            return signal(True, cluster_probe.ABNORMAL,
                          'not streaming for %d seconds' % away,
                          extra={'disconnected_for': away,
                                 'primary_id': self.primary_id})
        if view.state != PRIMARY:
            return None
        cluster = self._read_cluster()
        self_id = cluster.get('self_id') or CONF.guest_id
        if cluster.get('primary_id') not in (None, self_id):
            return signal(True, cluster_probe.ABNORMAL,
                          "a primary's data, but the cluster's primary is "
                          "%s" % cluster.get('primary_id'))
        if (len(cluster.get('members') or {}) <= 1 or
                self._no_standby_since is None):
            return cluster_probe.NORMAL
        alone_for = now - self._no_standby_since
        if self.cluster_mode == STRICT:
            return signal(True, cluster_probe.ABNORMAL,
                          'strict mode: no synchronous standby for %d '
                          'seconds' % alone_for,
                          extra={'no_standby_for': alone_for})
        left = max(self._lease_until - now, 0)
        grace = self._cluster_conf().cluster_fence_grace
        # Before a grant, ask again well within the fence grace; after
        # one, when a third of the lease is left.
        resend = grace / 3.0
        if self._lease_ttl:
            resend = max(left - self._lease_ttl / 3.0, grace / 3.0)
        return signal(True, cluster_probe.LEASE_REQUEST,
                      'no standby streams for %d seconds; the lease runs %d '
                      'more seconds' % (alone_for, left),
                      extra={'no_standby_for': alone_for, 'lease_left': left,
                             'lease_ttl': self._lease_ttl,
                             'writable': bool(view.writable)},
                      resend_after=resend)

    # Failing over: what the task manager has the members do

    def fence_member(self):
        """The task manager is moving the primary elsewhere: no more
        writes here. The write port closes with the next tick, the
        lease is gone, and the server stops: a checkpoint when it
        answers (a rewind is quick then), then the container is
        stopped, which is a fast stop (the image's stop signal is
        SIGINT). Not pg_ctl stop inside the container: the container
        restarts on its own then (its restart policy), as a primary.
        """
        LOG.info("Fencing the member: the primary is moving elsewhere.")
        self.fenced = True
        self._lease_until = 0
        self._confirmed_at = None
        # CHECKPOINT returns no rows: psql, not query.
        try:
            self.adm.psql("CHECKPOINT")
        except Exception as err:
            LOG.info("No checkpoint (%s).", err)
        self.stop_db()

    def promote_member(self, members, mode):
        """A standby becomes the primary: as far as its received WAL goes.
        It stops streaming, commits wait as the mode says, every other
        member gets a slot, and a checkpoint marks the new timeline for
        the rewinds to come.
        """
        LOG.info("Promoting the member to primary, mode %s.", mode)
        self.fenced = False
        self.configuration_manager.remove_system_override(CNF_REPLICATION)
        self._write_sync_names(ANY_ONE if mode == STRICT else '')
        self._no_standby_since = None
        self._update_cluster(members=members, mode=mode,
                             primary_id=self.self_id)
        if self.adm.query("SELECT pg_is_in_recovery()")[0][0]:
            promoted = self.adm.query(
                "SELECT pg_promote(true, %s)" % PROMOTE_TIMEOUT)[0][0]
            if not promoted:
                raise exception.TroveError(
                    _("The member was not promoted within %s seconds.") %
                    PROMOTE_TIMEOUT)
        for member_id in members:
            if member_id != self.self_id:
                self.create_member_slot(member_id)
        self.reload()
        # For the rewinds of the members that rejoin; CHECKPOINT returns
        # no rows. A member that rejoins without it takes a base backup.
        try:
            self.adm.psql("CHECKPOINT")
        except Exception as err:
            LOG.warning("No checkpoint after the promotion: %s", err)
        self._confirmed_at = time.monotonic()
        return {'lsn': self._lsn(PRIMARY_LSN_QUERY),
                'timeline': self._timeline()}

    def follow_primary(self, primary):
        """A standby streams from another primary from now on: its
        connection settings change and the server reloads them, which
        restarts the WAL receiver; the member is not restarted.
        """
        if not self.adm.query("SELECT pg_is_in_recovery()")[0][0]:
            raise exception.TroveError(
                _("The member is a primary: it has to rejoin instead."))
        cluster = self._read_cluster()
        receiver = self._receiver()
        if (cluster.get('primary_id') == primary['id'] and receiver and
                receiver['status'] == STREAMING and
                receiver['sender_host'] == primary['host']):
            # Told twice (two task managers watch the cluster).
            LOG.info("Already following primary %s (%s).", primary['id'],
                     primary['host'])
            return
        LOG.info("Following primary %s (%s).", primary['id'],
                 primary['host'])
        self._write_replication(primary['host'], cluster['replication_user'])
        self._update_cluster(primary_id=primary['id'])
        self.reload()
        self._detached_since = None

    def _remove_stale_pid(self):
        """A server that crashed leaves postmaster.pid behind, naming a
        process that lives on in the tool's container (PID 1): pg_rewind
        would take the server for running.
        """
        operating_system.remove(os.path.join(self.datadir, 'postmaster.pid'),
                                force=True, as_root=True)

    def _rewind(self, primary, replication_user):
        """The data directory made a standby's of the primary again from
        the point where the two parted: what this member wrote on its
        own after that is gone.
        """
        self._remove_stale_pid()
        self._run_in_parked_container(
            ['pg_rewind', '--target-pgdata=%s' % self.datadir,
             '--source-server=host=%s port=%s user=%s dbname=postgres' % (
                 primary['host'], self.DATABASE_PORT,
                 replication_user['name']),
             '--config-file=%s' % pg_service.CONFIG_FILE, '--progress'],
            {'PGPASSWORD': replication_user['password']})

    def _wait_reachable(self, host, replication_user, timeout=120):
        """Until the primary accepts connections on the tenant network,
        from the parked container (the tenant NIC is the database
        container's, not the guest agent's): a member told to rejoin
        while its own link is still coming back would fail its rewind
        and its base backup alike. The user is named: the container's
        numeric user has no name for libpq to default to.
        """
        self._run_in_parked_container(
            ['sh', '-c',
             'for i in $(seq 1 %d); do pg_isready -h %s -p %s -U %s '
             '-d postgres && exit 0; sleep 5; done; '
             'echo "the primary %s is not reachable"; exit 1' % (
                 max(timeout // 5, 1), host, self.DATABASE_PORT,
                 replication_user['name'], host)])

    def rejoin_cluster(self, primary):
        """A member that was the primary, or a standby that fell too far
        behind, becomes a standby of the primary: by a rewind when the
        primary still holds the WAL from where they parted, by a base
        backup otherwise. The cluster file names the primary from the
        start, so that a rejoin that fails leaves the member stopped and
        closed; the task manager has it rejoin again. A rejoin under way
        is not started twice.
        """
        if self.rebuilding:
            LOG.info("Already rejoining; not again.")
            return
        LOG.info("Rejoining the cluster as a standby of %s (%s).",
                 primary['id'], primary['host'])
        cluster = self._read_cluster()
        replication_user = cluster['replication_user']
        self.fenced = False
        self.rebuilding = True
        try:
            self._update_cluster(primary_id=primary['id'])
            self.configuration_manager.remove_system_override(CNF_SYNC)
            self._sync_names = None
            self.stop_db()
            self._wait_reachable(primary['host'], replication_user)
            try:
                self._rewind(primary, replication_user)
                LOG.info("pg_rewind done.")
            except Exception as err:
                LOG.warning("pg_rewind failed (%s); taking a base backup "
                            "instead.", err)
                self._base_backup(primary, replication_user)
            self._write_replication(primary['host'], replication_user)
            self._write_standby_signal()
            self.start_db(ds_version=CONF.datastore_version,
                          command=self._server_command())
            self._wait_streaming()
        finally:
            self.rebuilding = False
        self._detached_since = None
        LOG.info("The member streams from %s.", primary['host'])

    def leave_group(self):
        """The member is being removed: its ports are closed by the
        probe; the primary drops its slot.
        """
        LOG.info("Leaving the cluster.")


class PgSqlClusterApp(PostgresqlClusterAppMixin, pg_service.PgSqlApp):
    """The PostgreSQL app, a cluster member when told."""


class PostgresqlMemberMixin(cluster_member.ClusterMemberMixin):
    """The cluster calls of the PostgreSQL manager: list it before the
    manager class it extends.
    """

    CLUSTER_MODE_KEY = MODE_KEY

    def cluster_complete(self, context):
        self.app.complete_cluster()
        super(PostgresqlMemberMixin, self).cluster_complete(context)

    def install_cluster(self, context, cluster_config):
        try:
            self.app.install_cluster(cluster_config)
            LOG.info("The cluster configuration is installed.")
        except Exception:
            LOG.exception("Failed to install the cluster configuration.")
            self.status.set_status(service_status.ServiceStatuses.FAILED)
            raise
        if self.cluster_probe:
            self.cluster_probe.enable_member()

    def create_member_slot(self, context, member_id):
        self.app.create_member_slot(member_id)

    def drop_member_slot(self, context, member_id):
        self.app.drop_member_slot(member_id)

    def update_cluster_members(self, context, members, mode,
                               primary_id=None):
        LOG.info("The cluster has members %s, mode %s%s.",
                 sorted(members.values()), mode,
                 ', primary %s' % primary_id if primary_id else '')
        self.app.update_cluster_members(members, mode, primary_id=primary_id)

    def renew_lease(self, context, ttl):
        self.app.renew_lease(ttl)

    def remember_sync_set(self, context, primary_id, members, lsn=None,
                          alone=False):
        if self.app.remember_sync_set(primary_id, members, lsn=lsn,
                                      alone=alone):
            if alone:
                LOG.info("The primary %s goes on without a standby (it "
                         "last streamed to %s).", primary_id,
                         ', '.join(sorted(members)) if members
                         else 'nobody known')
            else:
                LOG.info("The primary %s streams to %s.", primary_id,
                         ', '.join(sorted(members or [])) or 'no standby')

    def fence_member(self, context):
        self.app.fence_member()

    def promote_member(self, context, members, mode):
        return self.app.promote_member(members, mode)

    def follow_primary(self, context, primary):
        self.app.follow_primary(primary)

    def rejoin_cluster(self, context, primary):
        try:
            self.app.rejoin_cluster(primary)
        except Exception:
            LOG.exception("The member could not rejoin the cluster; the "
                          "task manager tries again.")
            raise
