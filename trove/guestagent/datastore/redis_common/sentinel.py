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

"""A member of a Redis family cluster of the sentinel topology: a primary
with replicas, a sentinel beside each server, the sentinels electing the
primary.

The sentinel runs in the database container with the server (one
script, ``sidecar``), so that it has the container's tenant NIC: the
guest agent cannot reach the other members with network isolation, the
sentinels can. Its configuration, which it rewrites as it learns, and
its unix socket, which the guest agent asks it through, are on the data
volume.

Users, all in the server's ACL file, which loads before the server takes
connections (a sentinel that fails to log in to a server just started
stays connected as the default user and never logs in again):

* the cluster admin (``set_cluster_admin``): the replicas sync with
  their primary as it, as the members of a Redis Cluster do;
* ``sentinel``: what a sentinel needs on the servers, and no more.

The sentinel has users of its own: ``default`` asks where the primary is
and subscribes (what a client that speaks sentinel does), with no
password, and cannot reconfigure anything; ``trove-sentinel`` can do it
all, the sentinels log in to each other as it, and so does the guest
agent.

``replicaof`` is not kept in the configuration: a member started again
starts as a primary and the sentinels make it a replica of the primary
they elected. Meanwhile the member takes no writes from the cluster's
endpoint: its ready port opens only while its own sentinel takes it for
the primary too.
"""

import os

from oslo_log import log as logging
from valkey import Valkey

from trove.common import cluster_health
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import cluster_probe
from trove.guestagent.common import operating_system
from trove.guestagent.common import sidecar
from trove.guestagent.utils import docker as docker_util

LOG = logging.getLogger(__name__)

MASTER_NAME = 'trove'
SENTINEL_PORT = 26379
SENTINEL_USER = 'sentinel'
SENTINEL_ADMIN = 'trove-sentinel'
CHANGE_ID = 'sentinel'
SYNC, ASYNC = 'sync', 'async'
DOWN_AFTER_MS = 5000
FAILOVER_TIMEOUT_MS = 60000
# A replica serves reads while its link is up and it heard from its
# primary within this many seconds.
READ_MAX_SILENCE = 10
# What a sentinel needs on a server (the sentinel documentation's list).
SENTINEL_COMMANDS = ['+multi', '+slaveof', '+ping', '+exec', '+subscribe',
                     '+config|rewrite', '+role', '+publish', '+info',
                     '+client|setname', '+client|kill', '+script|kill']
# What a client that speaks sentinel asks, without a password.
CLIENT_COMMANDS = ('-@all +ping +sentinel|get-master-addr-by-name '
                   '+sentinel|master +sentinel|masters +sentinel|replicas '
                   '+sentinel|slaves +sentinel|sentinels +subscribe '
                   '+psubscribe +unsubscribe +punsubscribe &*')
# Commands a sentinel sends a server: a connection that sends them as the
# default user, which may only ping (for the container's health check),
# is a sentinel that failed to log in.
SENTINEL_LINK_COMMANDS = ('publish', 'subscribe', 'info', 'client|setname')


def sentinel_config(self_ip, primary_ip, quorum, admin_password,
                    sentinel_password, socket_path):
    """The sentinel's configuration as the guest agent writes it; the
    sentinel adds what it learns.
    """
    return '\n'.join([
        'port %d' % SENTINEL_PORT,
        'unixsocket %s' % socket_path,
        'unixsocketperm 777',
        'user default on nopass %s' % CLIENT_COMMANDS,
        'user %s on >%s ~* &* +@all' % (SENTINEL_ADMIN, admin_password),
        'sentinel sentinel-user %s' % SENTINEL_ADMIN,
        'sentinel sentinel-pass %s' % admin_password,
        'sentinel announce-ip %s' % self_ip,
        'sentinel resolve-hostnames no',
        'sentinel monitor %s %s 6379 %d' % (MASTER_NAME, primary_ip, quorum),
        'sentinel auth-user %s %s' % (MASTER_NAME, SENTINEL_USER),
        'sentinel auth-pass %s %s' % (MASTER_NAME, sentinel_password),
        'sentinel down-after-milliseconds %s %d' % (
            MASTER_NAME, DOWN_AFTER_MS),
        'sentinel failover-timeout %s %d' % (
            MASTER_NAME, FAILOVER_TIMEOUT_MS),
        'sentinel parallel-syncs %s 1' % MASTER_NAME,
    ]) + '\n'


def quorum(members):
    """The sentinels that must agree a primary is down: a majority."""
    return len(members) // 2 + 1


class SentinelAppMixin(object):
    """The sentinel topology on the app of a Redis family datastore. List
    it before the app class it extends.
    """

    @property
    def sentinel_dir(self):
        return os.path.join(self.datadir, 'sentinel')

    @property
    def sentinel_config_file(self):
        return os.path.join(self.sentinel_dir, 'sentinel.conf')

    @property
    def sentinel_socket(self):
        return os.path.join(self.sentinel_dir, 'sentinel.sock')

    @property
    def start_script(self):
        return '/etc/%s/trove-start.sh' % self.DATASTORE_NAME

    @property
    def sentinel_binary(self):
        return self.SERVER_BINARY.replace('-server', '-sentinel')

    def is_sentinel_member(self):
        return self.configuration_manager.has_system_override(CHANGE_ID)

    def sentinel_mode(self):
        value = self.get_configuration_property('min-replicas-to-write', 0)
        try:
            return SYNC if int(value or 0) > 0 else ASYNC
        except (TypeError, ValueError):
            return ASYNC

    def sentinel_command(self):
        """The database container's command for a sentinel member: the
        server and its sentinel side by side.
        """
        return ['/bin/sh', self.start_script]

    def _write_start_script(self):
        text = sidecar.script(
            [[self.SERVER_BINARY, self.CONFIG_FILE],
             [self.sentinel_binary, self.sentinel_config_file]],
            purpose='The server and its sentinel (Trove guest agent).')
        operating_system.write_file(self.start_script, text, as_root=True)
        operating_system.chmod(self.start_script,
                               operating_system.FileMode.ADD_READ_ALL,
                               as_root=True)

    def install_sentinel(self, config):
        """Make this member one of a sentinel cluster: its replication
        and sentinel users, its server settings, its sentinel; the
        database is started again with the sentinel beside it.

        :param config: {'primary', 'members', 'mode', 'admin_password',
                        'sentinel_password', 'sentinel_admin_password'}
        """
        self_ip = self.get_node_ip()[0]
        primary = config['primary']
        LOG.info("Joining the sentinel cluster as %s (the primary is "
                 "%s).", 'the primary' if primary == self_ip else
                 'a replica', primary)
        self.set_cluster_admin(config['admin_password'])
        self.adm.connection.acl_setuser(
            username=SENTINEL_USER, enabled=True, reset_passwords=True,
            passwords=['+%s' % config['sentinel_password']],
            commands=SENTINEL_COMMANDS, channels=['*'])
        self.adm.connection.acl_save()
        self.save_password(SENTINEL_ADMIN, config['sentinel_admin_password'])
        # What a member that joins later is given (sentinel_config).
        self.save_password(SENTINEL_USER, config['sentinel_password'])
        sync = config.get('mode', SYNC) == SYNC
        self.configuration_manager.apply_system_override({
            'replica-announce-ip': self_ip,
            'min-replicas-to-write': 1 if sync else 0,
            'min-replicas-max-lag': 10,
        }, CHANGE_ID)
        operating_system.ensure_directory(
            self.sentinel_dir, user=self.database_service_uid,
            group=self.database_service_gid, force=True, as_root=True)
        operating_system.write_file(
            self.sentinel_config_file,
            sentinel_config(self_ip, primary, quorum(config['members']),
                            config['sentinel_admin_password'],
                            config['sentinel_password'],
                            self.sentinel_socket),
            as_root=True)
        operating_system.chown(self.sentinel_config_file,
                               self.database_service_uid,
                               self.database_service_gid, force=True,
                               as_root=True)
        self._write_start_script()
        self.restart_with_sentinel()
        if primary != self_ip:
            self.adm.connection.replicaof(primary, 6379)
            self.wait_for_primary_link()

    def restart_with_sentinel(self):
        """The container again, with the sentinel beside the server: a
        container keeps the command it was created with. Of the image it
        was created from: without one, start_db takes the image's latest
        tag, another version than the datastore version (an 8.1 member
        ran 9.1.2, 2026-10-20).
        """
        image = docker_util.get_container_image(self.docker_client)
        self.stop_db()
        docker_util.remove_container(self.docker_client)
        self.start_db(update_db=False, command=self.sentinel_command(),
                      image=image)

    def wait_for_primary_link(self, timeout=300):
        def linked():
            info = self.adm.connection.info('replication')
            return info.get('master_link_status') == 'up'
        try:
            utils.poll_until(linked, sleep_time=2, time_out=timeout)
        except exception.PollTimeOut:
            raise exception.TroveError(
                "The replica did not sync with its primary within %s "
                "seconds." % timeout)

    # What the probe asks

    def _sentinel(self):
        return Valkey(unix_socket_path=self.sentinel_socket,
                      username=SENTINEL_ADMIN,
                      password=self.get_auth_password(
                          file='%s.cnf' % SENTINEL_ADMIN),
                      socket_timeout=5, decode_responses=True)

    def sentinel_state(self):
        """What this member's sentinel knows: the primary, the replicas,
        the other sentinels, as valkey-py parses them.
        """
        client = self._sentinel()
        try:
            return {'master': client.sentinel_master(MASTER_NAME),
                    'replicas': client.sentinel_slaves(MASTER_NAME),
                    'sentinels': client.sentinel_sentinels(MASTER_NAME)}
        finally:
            client.close()

    def sentinel_primary(self):
        """The primary this member's sentinel knows of, its address."""
        client = self._sentinel()
        try:
            addr = client.sentinel_get_master_addr_by_name(MASTER_NAME)
        finally:
            client.close()
        return addr[0] if addr else None

    def sentinel_member_view(self):
        """The member as its server and its sentinel see it: a primary
        takes writes while its sentinel takes it for the primary too and,
        in sync mode, a replica follows it; a replica serves reads while
        it keeps up.
        """
        info = self.adm.connection.info('replication')
        role = 'master' if info.get('role') == 'master' else 'replica'
        writable = readable = False
        self.replaced_primary = False
        if role == 'master':
            try:
                elected = self.sentinel_primary()
            except Exception as err:
                LOG.debug("The sentinel does not answer: %s", err)
                elected = None
            me = self.get_node_ip()[0]
            replicas = int(info.get('connected_slaves') or 0)
            writable = (elected == me and
                        (self.sentinel_mode() == ASYNC or replicas > 0))
            # Started again a primary, the sentinels having elected
            # another meanwhile: a replica soon, when they reconfigure it.
            self.replaced_primary = elected is not None and elected != me
        else:
            silence = info.get('master_last_io_seconds_ago')
            readable = (info.get('master_link_status') == 'up' and
                        silence is not None and
                        int(silence) <= READ_MAX_SILENCE)
        return cluster_probe.MemberView(role, role, writable, False,
                                        readable)

    def cluster_signal(self, view):
        """A primary the sentinels have replaced is not where it should
        be until it is a replica: told then, its cluster is looked at
        when it is whole again (a member that starts normal says so only
        once). Otherwise the probe's own word.
        """
        if getattr(self, 'replaced_primary', False) and \
                self.is_sentinel_member():
            return cluster_probe.ClusterSignal(
                True, cluster_probe.ABNORMAL,
                'a primary the sentinels replaced')
        return None

    def sentinel_health(self):
        """What this member's sentinel sees of the cluster: the members
        it holds down are out; down when it knows of no primary or holds
        the primary down.
        """
        me = self.get_node_ip()[0]
        state = self.sentinel_state()
        master = state['master']
        out = {r['ip'] for r in state['replicas']
               if r.get('is_sdown') or r.get('is_odown') or
               r.get('is_disconnected')}
        out |= {s['ip'] for s in state['sentinels'] if s.get('is_sdown')}
        if not master.get('ip') or master.get('is_sdown') or \
                master.get('is_odown'):
            # A replica the sentinels promoted takes the writes before
            # they switch to it: told, the cluster is not down.
            port = getattr(self, 'ready_port', None)
            return cluster_health.view(
                cluster_health.DOWN, sorted(out | {master.get('ip')} - {None}),
                'the sentinels hold the primary down', me,
                writable=port is not None and port.held())
        if out:
            return cluster_health.view(
                cluster_health.DEGRADED, sorted(out),
                'down as the sentinel sees it', me)
        return cluster_health.view(cluster_health.HEALTHY, me=me)

    def drop_failed_sentinel_links(self):
        """The connections of sentinels that failed to log in to this
        server (it had just started, say): they stay as the default user,
        which may only ping, and never log in again. Dropped, the
        sentinels connect anew and log in.
        """
        dropped = 0
        for client in self.adm.connection.client_list(_type='normal'):
            if (client.get('user') == 'default' and
                    client.get('cmd') in SENTINEL_LINK_COMMANDS):
                self.adm.connection.client_kill_filter(
                    _id=client.get('id'))
                dropped += 1
        if dropped:
            LOG.info("Dropped %d sentinel connections that had failed to "
                     "log in.", dropped)
        return dropped

    # What the task manager has the members do after the build

    def joining_config(self):
        """The cluster's passwords and mode, for a member that joins."""
        return {
            'admin_password': self.get_cluster_admin_password(),
            'sentinel_password': self.get_auth_password(
                file='%s.cnf' % SENTINEL_USER),
            'sentinel_admin_password': self.get_auth_password(
                file='%s.cnf' % SENTINEL_ADMIN),
            'mode': self.sentinel_mode(),
        }

    def _with_sentinel(self, act):
        client = self._sentinel()
        try:
            return act(client)
        finally:
            client.close()

    def _replicas(self):
        """The primary's replication offset and its replicas, as INFO
        REPLICATION gives them.
        """
        info = self.adm.connection.info('replication')
        replicas = [value for key, value in info.items()
                    if key.startswith('slave') and isinstance(value, dict)]
        return int(info.get('master_repl_offset', 0)), replicas

    def _replicas_online(self):
        """Whether every replica is in sync (``online``): not one in a
        full resynchronization, as a replica started again is.
        """
        _offset, replicas = self._replicas()
        return bool(replicas) and all(
            replica.get('state') == 'online' for replica in replicas)

    def _replicas_caught_up(self):
        """Whether every replica is in sync and has all the primary
        wrote.
        """
        offset, replicas = self._replicas()
        return bool(replicas) and all(
            replica.get('state') == 'online' and
            int(replica.get('offset', -1)) >= offset
            for replica in replicas)

    def sentinel_failover(self, timeout=30, catch_up=15, in_sync=600):
        """On the primary, before it is restarted, upgraded or removed:
        hand over to a replica without losing a write it acknowledged.
        Returns the new primary's address.

        SENTINEL FAILOVER alone promotes a replica at once while the old
        primary takes writes until it is told to follow, and those are
        lost (12 of about 10000 on production, 2026-10-20). So writes are
        paused here first (CLIENT PAUSE WRITE: held, not refused), the
        replicas catch up, the sentinels elect one, and this member
        follows it before writes go on: those that were held then get
        READONLY and the client tries again on the new primary. The member
        follows by itself: the sentinels tell it over a link the pause may
        hold up (their PUBLISH waits as a write would).

        Before the pause, every replica must be in sync (``in_sync``
        seconds, writes going on meanwhile): the sentinels may pick a
        replica still in a full resynchronization (one just started again
        in a rolling restart), whose resynchronization then makes it a
        replica of this member again, and the promotion never comes (Redis
        8.10, 2026-10-20). With writes paused, the sentinels take this
        member for down after a few seconds: ``timeout`` for the promotion
        is short, and on any failure writes go on here as before.
        """
        conn = self.adm.connection
        old = self.sentinel_primary()
        if old != self.get_node_ip()[0]:
            raise exception.TroveError(
                "%s is not the primary (%s is): nothing to hand over." % (
                    self.get_node_ip()[0], old))
        try:
            utils.poll_until(self._replicas_online, sleep_time=1,
                             time_out=in_sync)
        except exception.PollTimeOut:
            raise exception.TroveError(
                "The replicas were not all in sync within %s seconds: no "
                "failover." % in_sync)
        LOG.info("Handing over: writes paused until another primary is "
                 "elected.")
        # Lifted below; the time is a bound should the agent stop here.
        conn.client_pause((timeout + catch_up + 30) * 1000, all=False)
        try:
            try:
                utils.poll_until(self._replicas_caught_up, sleep_time=0.2,
                                 time_out=catch_up)
            except exception.PollTimeOut:
                raise exception.TroveError(
                    "The replicas did not catch up within %s seconds: no "
                    "failover." % catch_up)
            self._with_sentinel(lambda c: c.sentinel_failover(MASTER_NAME))
            try:
                new = utils.poll_until(lambda: self._promoted(old),
                                       sleep_time=0.2, time_out=timeout)
            except exception.PollTimeOut:
                raise exception.TroveError(
                    "The sentinels did not promote a replica within %s "
                    "seconds." % timeout)
            conn.replicaof(new, self.DATABASE_PORT)
        finally:
            conn.client_unpause()
        LOG.info("%s promoted; this member follows it.", new)
        # The sentinels tell the new primary once they reconfigured the
        # members; a client asking them before then is sent here still.
        try:
            utils.poll_until(lambda: self.sentinel_primary() == new,
                             sleep_time=0.5, time_out=timeout)
        except exception.PollTimeOut:
            LOG.warning("The sentinels did not tell %s as the primary "
                        "within %s seconds.", new, timeout)
        return new

    def _promoted(self, old):
        """The replica the sentinels promoted, once it says it is a
        primary; None before.

        Not the primary they tell (SENTINEL GET-MASTER-ADDR-BY-NAME): they
        tell the new one once every member follows it, and the old primary
        follows it only after its writes go on again (the sentinels' link to
        it waits behind a PUBLISH the pause holds): waiting for that, the
        old primary would wait for them, and they for it, until their
        failover timeout (89 s of 90 in a container, 2026-10-20).
        """
        replicas = self._with_sentinel(
            lambda c: c.sentinel_slaves(MASTER_NAME))
        for replica in replicas:
            if (replica.get('role-reported') == 'master' and
                    replica.get('ip') != old):
                return replica['ip']
        return None

    def sentinel_leave(self):
        """Never to be elected: the member is about to be removed."""
        self.adm.connection.config_set('replica-priority', 0)

    def sentinel_set_quorum(self, quorum):
        self._with_sentinel(
            lambda c: c.sentinel_set(MASTER_NAME, 'quorum', quorum))

    def sentinel_reset(self, members, timeout=120):
        """Forget the members the sentinel knew, and learn those there are
        again: done once members are removed, one member at a time.
        """
        self._with_sentinel(lambda c: c.sentinel_reset(MASTER_NAME))

        def relearnt():
            master = self.sentinel_state()['master']
            return (int(master.get('num-other-sentinels', 0)) ==
                    members - 1 and
                    int(master.get('num-slaves', 0)) == members - 1)
        try:
            utils.poll_until(relearnt, sleep_time=2, time_out=timeout)
        except exception.PollTimeOut:
            raise exception.TroveError(
                "The sentinel did not learn the %s members again within "
                "%s seconds." % (members, timeout))
