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

"""The task manager's repair of clusters: what a datastore does not bring
back by itself, done for every datastore alike.

The datastores fail over and catch up on their own, and the recovery
forms a cluster again after every member went down; neither starts a
member whose server is off. Nova shuts a server off by itself when it
finds its domain gone (powered off, killed: ``_sync_instance_power_state``
records a ``stop`` without a user), and nothing starts it again: the
cluster goes on short of the member. The repair starts it.

It starts only a member that is quiet to the control plane (its heartbeat
is stale), whose server Nova shut off by itself (its latest ``stop`` has
no user: one stopped through the API, on purpose, stays stopped), on a
host whose compute service is up (Nova's word on a server of a host it
does not hear from is old), and while neither the cluster nor any of its
members has a task. A start that does not bring the server up is tried
again after ``cluster_repair_start_interval`` seconds, at most
``cluster_repair_start_attempts`` times since the stop; the starts are
counted from the server's actions in Nova, so every task manager counts
the same and a task manager that starts again forgets nothing.

A member that is lost for good (its server gone, in ERROR, or off and
given up on) is replaced, when the operator lets it
(``cluster_repair_replace``) and its datastore has a plugin:
``common.replace``.

The recovery hub runs the repair on every look at a cluster, after the
datastore's recovery; a look that started a member, or a replacement
under way, asks for another one, for it to be followed.
"""

import datetime
import time

from novaclient import exceptions as nova_exceptions
from oslo_log import log as logging
from oslo_utils import timeutils

from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import clients
from trove.common import cluster_health as ch
from trove.common import context as trove_context
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import replace
from trove import rpc

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The extended property of a cluster that keeps the repair off it, or
# lets only some of it on: no more than the operator lets on.
AUTO_REPAIR = 'auto_repair'
OFF, START, REPLACE = 'off', 'start', 'replace'
LEVELS = (OFF, START, REPLACE)

EVENT_START = 'trove.cluster.repair.start'
EVENT_END = 'trove.cluster.repair.end'
EVENT_ERROR = 'trove.cluster.repair.error'


def _wanted(db_cluster):
    try:
        return db_cluster.get_properties().get(AUTO_REPAIR)
    except Exception:
        return None


def allows_start(db_cluster):
    """Whether the operator and the cluster let a member be started."""
    return bool(CONF.cluster_repair_start) and _wanted(db_cluster) != OFF


def allows_replace(db_cluster):
    """Whether the operator and the cluster let a lost member be
    replaced (``common.replace``).
    """
    return (bool(CONF.cluster_repair_replace) and
            _wanted(db_cluster) not in (OFF, START))


def notify(event_type, db_cluster, member, action, reason):
    try:
        payload = {
            'cluster_id': db_cluster.id,
            'cluster_name': db_cluster.name,
            'tenant_id': db_cluster.tenant_id,
            'instance_id': member.id,
            'instance_name': member.name,
            'action': action,
            'reason': reason,
            'region': CONF.region,
        }
        rpc.get_notifier(service='taskmanager').info(
            trove_context.TroveContext(), event_type, payload)
    except Exception:
        LOG.exception("The repair of cluster %s was not told.",
                      db_cluster.id)


def _when(value):
    try:
        return timeutils.normalize_time(timeutils.parse_isotime(value))
    except Exception:
        return None


class StartDecision(object):
    """Whether to start a member's server, and why (or why not)."""

    def __init__(self, start, reason, again_in=None):
        self.start = start
        self.reason = reason
        # Seconds until the member is worth another look, if it is.
        self.again_in = again_in


class ClusterRepair(object):
    """The repair of the clusters of every datastore."""

    def __init__(self, nova_factory=None, now=None):
        self.nova_factory = nova_factory or clients.create_nova_client
        self._now = now or timeutils.utcnow
        # The members this task manager started, and when: the end of the
        # repair is told when one is heard from again.
        self.started = {}
        self.replacement = replace.MemberReplacement(notify, now=self._now)

    def check_now(self, context, db_cluster, members=None, manager=None):
        """One look at a cluster. Returns the seconds until the cluster
        is worth another look for the repair, or None.
        """
        if members is None:
            from trove.instance.models import DBInstance
            members = DBInstance.find_all(cluster_id=db_cluster.id,
                                          deleted=False).all()
        state = db_cluster.get_properties().get(replace.STATE)
        if state:
            # A replacement under way is followed to its end, whatever
            # the switches say now.
            return self.replacement.follow(context, db_cluster, manager,
                                           members, state)
        if not allows_start(db_cluster) and not allows_replace(db_cluster):
            return None
        if db_cluster.task_status != ClusterTasks.NONE:
            return None
        if not members or any(m.task_status not in health.SETTLED_TASKS
                              for m in members):
            return None
        stale = health.stale_members(m.id for m in members)
        self._ended(db_cluster, [m for m in members if m.id not in stale])
        quiet = [m for m in members
                 if m.id in stale and m.compute_instance_id]
        if not quiet:
            return self._until_stale(db_cluster, members, stale)
        nova = self.nova_factory(context)
        computes = {}

        def compute_up(host):
            if host not in computes:
                computes[host] = self._compute_up(nova, host)
            return computes[host]

        again = None
        lost = []
        for member in quiet:
            decision = None
            try:
                decision = self.start_decision(nova, member, computes)
            except nova_exceptions.NotFound:
                pass
            except Exception as err:
                LOG.warning("Cluster %s: member %s not looked at for a "
                            "start: %s", db_cluster.id, member.id, err)
                continue
            if decision is not None and decision.start and \
                    allows_start(db_cluster):
                self._start(nova, db_cluster, member, decision.reason)
                decision.again_in = CONF.cluster_repair_start_interval
            elif decision is not None:
                LOG.debug("Cluster %s: member %s not started: %s",
                          db_cluster.id, member.id, decision.reason)
            if decision is not None and decision.again_in is not None:
                again = (decision.again_in if again is None
                         else min(again, decision.again_in))
            if allows_replace(db_cluster):
                try:
                    verdict = self.replacement.lost_decision(
                        nova, member, decision, compute_up)
                except Exception as err:
                    LOG.warning("Cluster %s: member %s not looked at for a "
                                "replacement: %s", db_cluster.id,
                                member.id, err)
                    continue
                if verdict.lost:
                    lost.append((member, verdict.reason))
        if lost and manager:
            # One at a time: the cluster is looked at again after it.
            member, reason = lost[0]
            later = self.replacement.begin(context, db_cluster, manager,
                                           members, member, reason)
            if later is not None:
                again = later if again is None else min(again, later)
        return again

    @staticmethod
    def _until_stale(db_cluster, members, stale):
        """A cluster that is not healthy while none of its members is
        quiet yet: a member that just went down still has a fresh
        heartbeat, and nothing else would look at the cluster before the
        next scan. Looked at again when the oldest heartbeat goes stale.
        """
        if health.status_of(db_cluster.id) in (None, ch.HEALTHY):
            return None
        return health.seconds_to_expiry(
            m.id for m in members
            if m.id not in stale and m.compute_instance_id)

    def start_decision(self, nova, member, computes=None):
        """Whether to start the server of a member that is quiet."""
        return decide_start(nova, member, computes, now=self._now)

    @staticmethod
    def _compute_up(nova, host):
        return compute_up(nova, host)

    def _start(self, nova, db_cluster, member, reason):
        LOG.info("Cluster %s: starting member %s (%s): %s.",
                 db_cluster.id, member.name, member.id, reason)
        try:
            started = start_server(nova, member.compute_instance_id)
        except Exception as err:
            LOG.warning("Cluster %s: member %s was not started: %s",
                        db_cluster.id, member.id, err)
            notify(EVENT_ERROR, db_cluster, member, 'start',
                   '%s; the start failed: %s' % (reason, err))
            return
        if not started:
            # The other task manager that watches the cluster started it
            # a moment before (both look on the same report): no error.
            LOG.info("Cluster %s: member %s is being started already.",
                     db_cluster.id, member.id)
            return
        self.started[member.id] = time.monotonic()
        notify(EVENT_START, db_cluster, member, 'start', reason)

    def _ended(self, db_cluster, heard):
        for member in heard:
            since = self.started.pop(member.id, None)
            if since is None:
                continue
            LOG.info("Cluster %s: member %s is back, %d s after its start.",
                     db_cluster.id, member.name, time.monotonic() - since)
            notify(EVENT_END, db_cluster, member, 'start',
                   'heard from again %d s after its start' %
                   (time.monotonic() - since))


def decide_start(nova, member, computes=None, now=None):
    """Whether to start the server of an instance that is quiet to the
    control plane (a cluster member or a standalone instance alike):
    only one Nova shut off by itself, on a host whose compute service is
    up, not yet started ``cluster_repair_start_attempts`` times since,
    and not within ``cluster_repair_start_interval`` of the last start.
    """
    computes = {} if computes is None else computes
    now = now or timeutils.utcnow
    server = nova.servers.get(member.compute_instance_id)
    if server.status != 'SHUTOFF':
        return StartDecision(False, 'the server is %s' % server.status)
    host = getattr(server, 'OS-EXT-SRV-ATTR:host', None)
    if host not in computes:
        computes[host] = compute_up(nova, host)
    if not computes[host]:
        return StartDecision(False, 'the compute service of host %s is '
                                    'not up' % host)
    actions = sorted(
        nova.instance_action.list(server.id),
        key=lambda a: _when(getattr(a, 'start_time', None)) or
        datetime.datetime.min,
        reverse=True)
    stops = [a for a in actions if a.action == 'stop']
    if not stops:
        return StartDecision(False, 'off without a stop')
    stop = stops[0]
    if getattr(stop, 'user_id', None):
        return StartDecision(False, 'stopped by user %s' % stop.user_id)
    stopped_at = _when(stop.start_time)
    starts = [a for a in actions if a.action == 'start' and
              stopped_at is not None and
              (_when(a.start_time) or stopped_at) > stopped_at]
    if len(starts) >= CONF.cluster_repair_start_attempts:
        return StartDecision(False, 'started %d times since Nova shut '
                                    'it off; given up' % len(starts))
    if starts:
        last = _when(starts[0].start_time)
        waited = (now() - last).total_seconds() if last else 0
        wait = CONF.cluster_repair_start_interval - waited
        if wait > 0:
            return StartDecision(False, 'started %d s ago' % waited,
                                 again_in=int(wait) + 1)
    return StartDecision(True, 'Nova shut it off by itself at %s%s' % (
        stop.start_time,
        ', start %d' % (len(starts) + 1) if starts else ''))


def compute_up(nova, host):
    """Whether the compute service of the host is up, by Nova."""
    if not host:
        return False
    try:
        services = nova.services.list(host=host, binary='nova-compute')
    except Exception as err:
        LOG.warning("The compute service of host %s could not be "
                    "looked up: %s", host, err)
        return False
    return any(getattr(s, 'state', None) == 'up' for s in services)


def start_server(nova, server_id):
    """Start a server. False when it is being started already (another
    task manager asked a moment before: Nova answers 409).
    """
    try:
        nova.servers.start(server_id)
    except nova_exceptions.Conflict:
        return False
    return True
