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

"""The probe of a cluster member, run by the guest agent.

It watches where the member stands in its cluster, keeps the ready port
open on a member that takes writes, and brings a member that fell out of
the cluster back: into the cluster if it is up, or by forming it again with
the other members when every member went down.

What a member's state is, how its peers are asked and how the cluster is
joined or formed again is the datastore's; the app gives it through a
small set of methods:

* ``is_cluster_member()``, ``is_cluster_complete()``: read once, until
  the cluster calls tell the probe.
* ``member_view()``: a ``MemberView`` of the member now.
* ``_recovery_credentials()``, ``_self_ip()``, ``_seed_ips()``,
  ``_query_peer(ip, user, password, timeout)``: to ask the peers, each
  answering with a ``PeerView``.
* ``_position()``, ``_not_ahead(a, b)``: where the member stands in the
  cluster's history, and whether position ``a`` holds nothing that ``b``
  lacks.
* ``rejoin_group(state)``, ``bootstrap_group()``: what recovery does.

The probe runs every few seconds, apart from the guest agent's periodic
tasks, which run every report interval: a failover has to show on the
ready port within seconds.
"""

import collections
import ipaddress
import random
import time

import eventlet
from oslo_log import log as logging
from oslo_service import loopingcall

from trove.common import cfg
from trove.guestagent.common import readyport

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# What the recovery decides.
REJOIN, BOOTSTRAP, WAIT = 'rejoin', 'bootstrap', 'wait'

# What a peer tells of itself: whether it answered, whether it is in a
# cluster that is up, whether it sees one (other members in it), and its
# position in the cluster's history, as the datastore counts it.
PeerView = collections.namedtuple(
    'PeerView', 'ip reachable in_group sees_group position')
# What the member is now: its state and role as the datastore names them,
# whether it takes writes, and whether it is out of the cluster.
MemberView = collections.namedtuple('MemberView', 'state role writable down')
UNKNOWN_MEMBER = MemberView(None, None, False, False)


def ip_key(ip):
    try:
        return (0, ipaddress.ip_address(ip))
    except ValueError:
        return (1, ip)


def decide(self_ip, self_position, peers, n_members, not_ahead,
           needs_all=False):
    """What a member that is out of the cluster should do, given what its
    peers told: join a cluster that is up, form the cluster again, or wait.

    Forming the cluster again loses whatever only the members that are not
    here hold, so it takes a majority that is here (every member, with
    needs_all), all of it out of the cluster, and this member holding every
    transaction any of them holds; when more than one member qualifies,
    the one with the lowest address goes. Transactions the cluster committed
    were certified by a majority, so a majority that is here holds them.

    :param peers: PeerView of every other member
    :param not_ahead: not_ahead(a, b) -> whether position a holds nothing
                      that position b lacks
    """
    reachable = [p for p in peers if p.reachable]
    if any(p.sees_group or p.in_group for p in reachable):
        return REJOIN, 'a peer is in the group'
    quorum = n_members if needs_all else n_members // 2 + 1
    if 1 + len(reachable) < quorum:
        return WAIT, '%s of %s members here, %s needed' % (
            1 + len(reachable), n_members, quorum)
    behind = [p.ip for p in reachable
              if not not_ahead(p.position, self_position)]
    if behind:
        return WAIT, 'missing transactions that %s hold' % ', '.join(behind)
    for peer in sorted(reachable, key=lambda p: ip_key(p.ip)):
        if ip_key(peer.ip) >= ip_key(self_ip):
            break
        if not_ahead(self_position, peer.position) and all(
                not_ahead(other.position, peer.position)
                for other in reachable):
            return WAIT, 'deferring to %s' % peer.ip
    return BOOTSTRAP, 'majority here, out of the group, nothing missing'


class ClusterProbe(object):
    """Watches the member's state in the cluster, keeps the ready port
    open on a member that takes writes, and brings a member that is out
    of the cluster back.

    Started with the manager and quiet until the member is in a cluster;
    the manager's cluster calls tell it when the member joins, when the
    cluster is complete and when the member is leaving.
    """

    def __init__(self, app, docker_client, conf=None):
        conf = conf or CONF.get(CONF.datastore_manager or 'mysql')
        self.app = app
        self.interval = conf.cluster_probe_interval
        self.query_timeout = conf.cluster_probe_timeout
        self.reconcile_every = conf.cluster_ready_port_reconcile
        self.recovery_grace = conf.cluster_recovery_grace
        self.recovery_interval = conf.cluster_recovery_interval
        self.peer_timeout = conf.cluster_peer_timeout
        self.auto_bootstrap = conf.cluster_auto_bootstrap
        self.needs_all = conf.cluster_bootstrap_needs_all_members
        self.jitter = conf.cluster_bootstrap_jitter
        self.last_recovery = 0
        self.port = readyport.ReadyPort(
            docker_client, conf.cluster_ready_port, app.DATABASE_PORT)
        self.view = UNKNOWN_MEMBER
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

    @property
    def state(self):
        return self.view.state

    @property
    def role(self):
        return self.view.role

    def start(self):
        if self.interval <= 0 or self._loop is not None:
            return
        try:
            self._loop = loopingcall.FixedIntervalLoopingCall(self._tick)
            self._loop.start(interval=self.interval,
                             initial_delay=self.interval,
                             stop_on_exception=False)
        except Exception:
            LOG.exception("Could not start the cluster probe.")
            self._loop = None

    def stop(self):
        if self._loop is not None:
            self._loop.stop()
            self._loop = None

    def enable_member(self):
        """The member has joined the cluster."""
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
        """The member is leaving the cluster: close the port, do nothing to
        bring it back.
        """
        self.leaving = True

    def _load_flags(self):
        self.member = self.app.is_cluster_member()
        self.complete = self.member and self.app.is_cluster_complete()
        self._flags_loaded = True

    def _probe_view(self):
        """The member as it is now; unknown when the database does not
        answer in time.
        """
        view = UNKNOWN_MEMBER
        with eventlet.Timeout(self.query_timeout, False):
            view = self.app.member_view()
        return view

    def writable(self):
        return not self.leaving and bool(self.view.writable)

    def _tick(self):
        try:
            self._run()
        except Exception:
            LOG.exception("The cluster probe failed; trying again.")

    def _run(self):
        if not self._flags_loaded:
            self._load_flags()
        if not self.member:
            return
        view = self._probe_view()
        if (view.state, view.role) != (self.view.state, self.view.role):
            LOG.info("Member state %s/%s -> %s/%s.", self.view.state,
                     self.view.role, view.state, view.role)
            self.since = time.monotonic()
        self.view = view

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

        now = time.monotonic()
        if (self.complete and not self.leaving and view.down and
                now - self.since >= self.recovery_grace and
                now - self.last_recovery >= self.recovery_interval):
            self.last_recovery = now
            self.recover(view.state)

    def _scan_peers(self):
        user, password = self.app._recovery_credentials()
        self_ip = self.app._self_ip()
        seeds = self.app._seed_ips()
        peers = [self.app._query_peer(ip, user, password, self.peer_timeout)
                 for ip in seeds if ip != self_ip]
        return self_ip, len(seeds), peers

    def recover(self, state):
        """Out of the cluster past the grace period: rejoin the cluster if
        it is up, form it again if it is safe, else wait and say why.
        """
        self_ip, n_members, peers = self._scan_peers()
        position = self.app._position()
        action, reason = decide(self_ip, position, peers, n_members,
                                self.app._not_ahead, self.needs_all)
        if action == WAIT and self._last_standing(position, peers):
            # The last member in the cluster holds everything it
            # committed: it forms the cluster again without the others,
            # unless one of them holds more, which would mean the mark is
            # stale.
            action, reason = BOOTSTRAP, 'the last one standing'
        if action == BOOTSTRAP and not self.auto_bootstrap:
            action, reason = WAIT, 'auto bootstrap is off; an operator ' \
                                   'forms the group again'
        LOG.info("Recovery from %s: %s (%s). Peers: %s", state, action,
                 reason, [(p.ip, p.reachable, p.in_group, p.sees_group)
                          for p in peers])
        if action == REJOIN:
            self.app.rejoin_group(state)
        elif action == BOOTSTRAP:
            # Another member may be deciding the same: look again after a
            # moment, and only go when nothing changed.
            time.sleep(random.uniform(0, self.jitter))
            _ip, _n, again = self._scan_peers()
            if (sorted(p for p in again if p.reachable) ==
                    sorted(p for p in peers if p.reachable)):
                self.app.bootstrap_group()
            else:
                LOG.info("The peers changed meanwhile; not forming the "
                         "group now.")

    def _last_standing(self, position, peers):
        was = getattr(self.app, 'was_last_standing', None)
        if not was or not was():
            return False
        reachable = [p for p in peers if p.reachable]
        if any(p.in_group or p.sees_group for p in reachable):
            return False
        return all(self.app._not_ahead(p.position, position)
                   for p in reachable)

    def snapshot(self):
        return {'state': self.view.state, 'role': self.view.role,
                'writable': self.writable(), 'ready_port': self.port.applied}
