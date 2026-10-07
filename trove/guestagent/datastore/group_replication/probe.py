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

"""The role of a Group Replication member, watched by the guest agent.

A member that takes writes (the primary of a single-primary group, every
member of a multi-primary one) answers on a ready port, redirected to the
database port inside the database container's network namespace: the
tenant NIC lives there, moved in by the docker-hostnic plugin. A load
balancer in front of the cluster checks that port, so that it sends
writes to a member that takes them and to no other.

The probe runs every few seconds, apart from the guest agent's periodic
tasks, which run every report interval: a failover has to show on the
ready port within seconds.
"""

import time

import eventlet
from oslo_log import log as logging
from oslo_service import loopingcall

from trove.common import cfg
from trove.common import exception
from trove.common import utils
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.group_replication import service as gr_service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The port the database listens on in the container.
DATABASE_PORT = 3306


class ReadyPort(object):
    """An iptables REDIRECT of the ready port to the database port in the
    database container's network namespace, present on a member that takes
    writes and absent otherwise.

    The rule is on PREROUTING only: the traffic comes in through the tenant
    NIC in the container. Connections already established through the port
    go on after the rule is removed (conntrack keeps their translation); a
    member that stopped taking writes refuses their writes itself, as it is
    super_read_only, and new connections are refused: nothing listens on
    the port, which is what the load balancer's check sees.
    """

    def __init__(self, docker_client, port, target_port=DATABASE_PORT):
        self.docker_client = docker_client
        self.port = port
        self.target_port = target_port
        self.rule = ['PREROUTING', '-p', 'tcp', '--dport', str(port),
                     '-j', 'REDIRECT', '--to-ports', str(target_port)]
        # The container the rule was last applied in, and whether the rule
        # is there; None when not known.
        self.pid = None
        self.applied = None

    def container_pid(self):
        """The PID of the database container, None when it is not running:
        its network namespace, and any rule in it, are gone with it.
        """
        try:
            container = self.docker_client.containers.get(
                galera_service.CONTAINER_NAME)
            return container.attrs.get('State', {}).get('Pid') or None
        except Exception as err:
            LOG.debug("No database container: %s", err)
            return None

    def _iptables(self, pid, action):
        # -w waits for the xtables lock instead of failing on it.
        utils.execute_with_timeout(
            'nsenter', '-t', str(pid), '-n', 'iptables', '-w', '5',
            '-t', 'nat', action, *self.rule,
            run_as_root=True, root_helper='sudo', timeout=15)

    def _present(self, pid):
        try:
            self._iptables(pid, '-C')
            return True
        except exception.ProcessExecutionError as err:
            # 1 is "no such rule"; anything else is a failure.
            if err.exit_code == 1:
                return False
            raise

    def ensure(self, wanted, pid=None):
        """Make the rule present or absent. Returns what it is, or None
        when there is no container to hold it.
        """
        pid = pid or self.container_pid()
        self.pid = pid
        if pid is None:
            self.applied = None
            return None
        try:
            present = self._present(pid)
            if present != wanted:
                self._iptables(pid, '-A' if wanted else '-D')
                LOG.info("Ready port %s %s.", self.port,
                         'opened' if wanted else 'closed')
            self.applied = wanted
        except Exception:
            # Not known any more; the next check puts it right.
            self.applied = None
            raise
        return wanted


class RoleProbe(object):
    """Watches the member's state in the group and keeps the ready port
    open on a member that takes writes.

    Started with the manager and quiet until the member is in a cluster;
    the manager's cluster calls tell it when the member joins, when the
    cluster is complete and when the member is leaving.
    """

    def __init__(self, app, docker_client, conf=None):
        conf = conf or CONF.get(CONF.datastore_manager or 'mysql')
        self.app = app
        self.interval = conf.group_replication_probe_interval
        self.query_timeout = conf.group_replication_probe_timeout
        self.reconcile_every = conf.group_replication_ready_port_reconcile
        self.port = ReadyPort(docker_client, conf.group_replication_ready_port)
        self.state = None
        self.role = None
        self.since = time.monotonic()
        # Whether the member is in a cluster, whether the cluster is
        # complete (every member has joined) and whether the member is on
        # its way out; read from the configuration once, then told.
        self.member = False
        self.complete = False
        self.leaving = False
        self._flags_loaded = False
        self.ticks = 0
        self._loop = None

    def start(self):
        if self.interval <= 0 or self._loop is not None:
            return
        try:
            self._loop = loopingcall.FixedIntervalLoopingCall(self._tick)
            self._loop.start(interval=self.interval,
                             initial_delay=self.interval,
                             stop_on_exception=False)
        except Exception:
            LOG.exception("Could not start the role probe.")
            self._loop = None

    def stop(self):
        if self._loop is not None:
            self._loop.stop()
            self._loop = None

    def enable_member(self):
        """The member has joined the group."""
        self.member = True
        self.leaving = False
        self._flags_loaded = True

    def enable_complete(self):
        """Every member has joined; the cluster is complete."""
        self.member = True
        self.complete = True
        self.leaving = False
        self._flags_loaded = True

    def disable(self):
        """The member is leaving the group: close the port, do nothing to
        bring it back.
        """
        self.leaving = True

    def _load_flags(self):
        self.member = self.app.is_cluster_member()
        self.complete = (self.member and
                         self.app.configuration_manager.has_system_override(
                             gr_service.CNF_STARTED))
        self._flags_loaded = True

    def _probe_state(self):
        """The member's state and role, (None, None) when the database does
        not answer in time.
        """
        result = (None, None)
        with eventlet.Timeout(self.query_timeout, False):
            result = self.app._member_state()
        return result

    def writable(self):
        return (not self.leaving and self.state == 'ONLINE' and
                self.role == 'PRIMARY')

    def _tick(self):
        try:
            self._run()
        except Exception:
            LOG.exception("The role probe failed; trying again.")

    def _run(self):
        if not self._flags_loaded:
            self._load_flags()
        if not self.member:
            return
        state, role = self._probe_state()
        if (state, role) != (self.state, self.role):
            LOG.info("Member state %s/%s -> %s/%s.", self.state, self.role,
                     state, role)
            self.state, self.role = state, role
            self.since = time.monotonic()

        wanted = self.writable()
        pid = self.port.container_pid()
        # The check costs a sudo and the xtables lock: only when something
        # changed, or now and then in case the rule went away.
        if (wanted != self.port.applied or pid != self.port.pid or
                self.ticks % self.reconcile_every == 0):
            try:
                self.port.ensure(wanted, pid=pid)
            except Exception:
                LOG.exception("Could not set the ready port.")
        self.ticks += 1

    def snapshot(self):
        return {'state': self.state, 'role': self.role,
                'writable': self.writable(), 'ready_port': self.port.applied}
