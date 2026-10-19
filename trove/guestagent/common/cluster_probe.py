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
* ``cluster_signal(view)``, optional: a ``ClusterSignal`` saying whether
  the member is where it should be in the cluster, for the control
  plane; without it, a member that does not answer or is out of the
  cluster is abnormal.
* ``cluster_event_view(view)``, optional: what to attach to an event;
  without it, ``recovery_view()`` when the app has one.

The probe runs every few seconds, apart from the guest agent's periodic
tasks, which run every report interval: a failover has to show on the
ready port within seconds.

A datastore whose clusters heal themselves (Redis, MongoDB, Cassandra,
Vertica) runs only the part that watches and tells, ``ClusterWatch``: its
app gives ``is_cluster_member()``, ``member_view()`` and
``cluster_health()``, what the member sees of its cluster from its own
database (``trove.common.cluster_health``). The cluster is complete once
the task manager says so (``enable_complete``), which the watch keeps on
the data volume.

The probe also tells the control plane, through the conductor, when the
member's cluster state changes (``cluster_event``), and again every
``cluster_event_interval`` seconds while the member is not where it
should be, so that a lost message heals itself: the task manager looks
at a cluster when a member says so, not by polling every member of
every cluster. A PostgreSQL primary without a standby asks for its write
lease the same way.
"""

import collections
import ipaddress
import random
import time

import eventlet
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import timeutils

from trove.common import cfg
from trove.common import cluster_health
from trove.common import context as trove_context
from trove.conductor import api as conductor_api
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
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
MemberView = collections.namedtuple(
    'MemberView', 'state role writable down readable', defaults=(False,))
UNKNOWN_MEMBER = MemberView(None, None, False, False)

# What a member says of its cluster state: whether it is abnormal (not
# where it should be), the kind the control plane acts on, a reason for
# the log, extra signals for the control plane, and how soon to say it
# again while it lasts (None: the configured event interval).
ClusterSignal = collections.namedtuple(
    'ClusterSignal', 'abnormal kind reason extra resend_after',
    defaults=(None, None, None))
# The kinds: the control plane dispatches on these three; a datastore's
# finer word goes in the reason.
NORMAL_KIND, ABNORMAL, LEASE_REQUEST = 'normal', 'abnormal', 'lease_request'
NORMAL = ClusterSignal(False, NORMAL_KIND, None)


class ClusterEventReporter(object):
    """Decides when a member's cluster state is worth telling, and tells
    it: on every change between normal and abnormal or of the kind,
    again every ``interval`` seconds while abnormal (a lost message heals
    itself, and a datastore may ask for sooner), never twice within
    ``min_gap`` seconds (a member whose state flaps tells its latest
    state once the gap has passed). A member that starts abnormal tells
    it once; one that starts normal says nothing.

    ``send(kind, signal, view, since)`` does the telling and may raise.
    """

    def __init__(self, interval, min_gap, send, now=None):
        self.interval = interval
        self.min_gap = min_gap
        self.send = send
        self._now = now or time.monotonic
        # The (abnormal, kind) last seen and since when; when it was last
        # told; whether a change waits for the gap to pass.
        self.key = None
        self.since = None
        self.last_sent = None
        self.pending = False
        self.sent = 0

    def observe(self, signal, view):
        """One look at the member's signal. Returns the kind sent, or
        None.
        """
        now = self._now()
        key = (bool(signal.abnormal), signal.kind)
        if key != self.key:
            was_abnormal = self.key is not None and self.key[0]
            self.since = now
            due = bool(signal.abnormal) or was_abnormal
        elif signal.abnormal:
            period = self.interval
            if signal.resend_after is not None:
                period = min(period, max(signal.resend_after, 0))
            due = self.last_sent is None or now - self.last_sent >= period
        else:
            due = False
        self.key = key
        gap_over = (self.last_sent is None or
                    now - self.last_sent >= self.min_gap)
        if due and not gap_over:
            self.pending = True
            return None
        if self.pending and gap_over:
            due = True
        if not due:
            return None
        if signal.kind == LEASE_REQUEST:
            kind = LEASE_REQUEST
        elif signal.abnormal:
            kind = ABNORMAL
        else:
            kind = NORMAL_KIND
        self.send(kind, signal, view, now - self.since)
        self.last_sent, self.pending, self.sent = now, False, self.sent + 1
        return kind


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


class ClusterWatch(object):
    """Watches the member's state in its cluster and tells the control
    plane when it is not where it should be, and what it sees of the
    cluster.

    Started with the manager and quiet until the member is in a cluster;
    the manager's cluster calls tell it when the member joins, when the
    cluster is complete and when the member is leaving.
    """

    # Where the watch keeps that the cluster is complete, for an app
    # without is_cluster_complete(): in the data volume's conf.d.
    COMPLETE_MARK = 'cluster_complete'

    def __init__(self, app, conf=None):
        conf = conf or CONF.get(CONF.datastore_manager or 'mysql')
        self.app = app
        self.interval = conf.cluster_probe_interval
        self.query_timeout = conf.cluster_probe_timeout
        # Telling the control plane: whether to, and the reporter that
        # decides when; a cast may block while the broker is away, so it
        # runs apart from the tick, one at a time.
        self.events = getattr(conf, 'cluster_events', True)
        self.reporter = ClusterEventReporter(
            getattr(conf, 'cluster_event_interval', 60),
            getattr(conf, 'cluster_event_min_interval', 5),
            send=self._send_event)
        self._spawn = eventlet.spawn_n
        self._sending = False
        self.view = UNKNOWN_MEMBER
        # What the member sees of its cluster (cluster_health()), for an
        # app that tells it; None otherwise or when it did not answer.
        self.health = None
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
        if not hasattr(self.app, 'is_cluster_complete'):
            try:
                operating_system.write_file(
                    self._complete_mark(), 'complete', as_root=True)
            except Exception:
                LOG.exception("Could not keep that the cluster is complete.")

    def disable(self):
        """The member is leaving the cluster: close the port, do nothing to
        bring it back.
        """
        self.leaving = True

    def _complete_mark(self):
        return guestagent_utils.build_file_path(
            guestagent_utils.get_conf_dir(), self.COMPLETE_MARK)

    def _load_flags(self):
        # A datastore may watch more than its cluster members (the config
        # servers of a MongoDB cluster).
        watched = getattr(self.app, 'is_watched_member', None)
        self.member = (watched or self.app.is_cluster_member)()
        if not self.member:
            self.complete = False
        elif hasattr(self.app, 'is_cluster_complete'):
            self.complete = self.app.is_cluster_complete()
        else:
            self.complete = operating_system.exists(
                self._complete_mark(), as_root=True)
        self._flags_loaded = True

    def _probe_view(self):
        """The member as it is now; unknown when the database does not
        answer in time.
        """
        view = UNKNOWN_MEMBER
        with eventlet.Timeout(self.query_timeout, False):
            view = self.app.member_view()
        return view

    def _probe_health(self):
        """What the member sees of its cluster; None when the app does not
        tell it or the database does not answer in time.
        """
        tell = getattr(self.app, 'cluster_health', None)
        if tell is None:
            return None
        health = None
        try:
            with eventlet.Timeout(self.query_timeout, False):
                health = tell()
        except Exception as err:
            LOG.debug("No health of the cluster: %s", err)
        return health

    def writable(self):
        return not self.leaving and bool(self.view.writable)

    def readable(self):
        return not self.leaving and bool(getattr(self.view, 'readable',
                                                 False))

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
        if self.complete and not self.leaving:
            self.health = self._probe_health()
        self._serve(view)

        # Tell the control plane, once the cluster is complete (a cluster
        # being built is full of members not where they should be yet)
        # and what the member serves is set: that never waits on it.
        if self.events and self.complete and not self.leaving:
            try:
                self.reporter.observe(self._signal(view), view)
            except Exception:
                LOG.exception("The cluster event was not told; next tick.")
        self._after(view)

    def _serve(self, view):
        """What the member does every tick before telling, if anything."""

    def _after(self, view):
        """What the member does every tick after telling, if anything."""

    # Telling the control plane

    def _signal(self, view):
        """What the member says of its cluster state: the datastore's
        word, or else abnormal when the member does not answer or is out
        of the cluster.
        """
        hook = getattr(self.app, 'cluster_signal', None)
        if hook is not None:
            signal = hook(view)
            if signal is not None:
                return signal
        if view.state is None:
            return ClusterSignal(True, ABNORMAL, 'the member does not answer')
        if view.down:
            return ClusterSignal(True, ABNORMAL,
                                 'out of the cluster: %s' % view.state)
        health = self.health or {}
        if health.get('status') not in (None, cluster_health.HEALTHY):
            return ClusterSignal(True, ABNORMAL, 'the cluster is %s: %s' % (
                health['status'], health.get('reason')))
        return NORMAL

    def _event(self, kind, signal, view, since):
        """What the event carries: the member as the probe sees it, the
        datastore's extra signals, and its view for the task manager
        (the one it would ask for), when the app gives one.
        """
        event = {'kind': kind, 'reason': signal.reason, 'since': since,
                 'datastore_manager': CONF.datastore_manager,
                 'member': self.snapshot(), 'extra': signal.extra or {},
                 'view': None, 'health': self.health}
        give = getattr(self.app, 'cluster_event_view', None)
        if give is None:
            give = getattr(self.app, 'recovery_view', None)
            if give is not None:
                give = lambda _view, _give=give: _give()  # noqa: E731
        if give is not None:
            try:
                event['view'] = give(view)
            except Exception as err:
                LOG.debug("No view for the event: %s", err)
        return event

    def _send_event(self, kind, signal, view, since):
        if self._sending:
            LOG.warning("A cluster event is still being sent (the broker?); "
                        "not sending %s now.", kind)
            return
        event = self._event(kind, signal, view, since)
        LOG.info("Cluster event %s (%s): %s/%s, writable %s, readable %s, "
                 "%ds in this state.", kind, signal.reason, view.state,
                 view.role, self.writable(), self.readable(), int(since))
        self._sending = True
        self._spawn(self._cast_event, event)

    def _cast_event(self, event):
        try:
            context = trove_context.TroveContext()
            conductor_api.API(context).cluster_event(
                CONF.guest_id, event['kind'],
                sent=timeutils.utcnow_ts(microsecond=True), payload=event)
        except Exception as err:
            LOG.warning("The cluster event %s was not sent: %s",
                        event['kind'], err)
        finally:
            self._sending = False

    def snapshot(self):
        return {'state': self.view.state, 'role': self.view.role,
                'writable': self.writable()}


class ClusterProbe(ClusterWatch):
    """Watches the member's state in the cluster, keeps the ready port
    open on a member that takes writes, and brings a member that is out
    of the cluster back.
    """

    def __init__(self, app, docker_client, conf=None):
        conf = conf or CONF.get(CONF.datastore_manager or 'mysql')
        super(ClusterProbe, self).__init__(app, conf)
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
        # A second port, open on a member that serves reads (a standby of
        # a primary), for a datastore whose conf has one.
        read_port = getattr(conf, 'cluster_read_port', 0)
        self.read_port = readyport.ReadyPort(
            docker_client, read_port, app.DATABASE_PORT) if read_port else None

    def _serve(self, view):
        # What the member does for the cluster every tick, if anything
        # (a primary keeping its standbys' settings right).
        reconcile = getattr(self.app, 'reconcile', None)
        if reconcile is not None:
            try:
                reconcile(view)
            except Exception:
                LOG.exception("The member's reconcile failed.")

        pid = self.port.container_pid()
        ports = [(self.port, self.writable())]
        if self.read_port is not None:
            ports.append((self.read_port, self.readable()))
        for port, wanted in ports:
            # The check costs a sudo and the xtables lock: only when
            # something changed, or now and then in case the rule went
            # away.
            if (wanted != port.applied or pid != port.pid or
                    self.ticks % self.reconcile_every == 0):
                try:
                    port.ensure(wanted, pid=pid)
                except Exception:
                    LOG.exception("Could not set the ready port %s.",
                                  port.port)
        self.ticks += 1

    def _after(self, view):
        now = time.monotonic()
        if (self.complete and not self.leaving and view.down and
                now - self.since >= self.recovery_grace and
                now - self.last_recovery >= self.recovery_interval):
            self.last_recovery = now
            self.recover(view.state)

    def snapshot(self):
        snapshot = super(ClusterProbe, self).snapshot()
        snapshot['ready_port'] = self.port.applied
        if self.read_port is not None:
            snapshot['readable'] = self.readable()
            snapshot['read_port'] = self.read_port.applied
        return snapshot

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
