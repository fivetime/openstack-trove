# Copyright 2026 Simon Zhou
#
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

"""Which cluster events the conductor forwards to the task managers.

Every member of a cluster reports its cluster state, and repeats it while
it is abnormal; the conductor hands each report to every task manager
(a fanout). A task manager looks at the whole cluster, all its members, on
any one report, so three members saying the same thing within a few
seconds make one look's worth of information, and the receiving alone
costs each task manager: in a test of 3000 Galera clusters losing power
at once, the reports of the waiting members were three in four of the
messages each task manager took, and they grew the longer recovery took.

The gate forwards the first report of a kind for a cluster and drops the
same kind for that cluster until cluster_event_forward_interval seconds
have passed. A different kind (abnormal after normal, normal after
abnormal) always passes, and so does a write lease request, which is
answered, not looked at.
"""

import threading
import time

from trove.common import cfg

CONF = cfg.CONF

ALWAYS = ('lease_request',)


class ClusterEventGate(object):

    def __init__(self, clock=time.monotonic):
        self._clock = clock
        self._last = {}     # cluster id -> (kind, when forwarded)
        self._lock = threading.Lock()
        self._pruned = clock()

    def admit(self, cluster_id, kind):
        """Whether to forward this report; remembers it when so."""
        window = CONF.cluster_event_forward_interval
        if not window or kind in ALWAYS:
            return True
        now = self._clock()
        with self._lock:
            last = self._last.get(cluster_id)
            if last is not None and last[0] == kind and \
                    now - last[1] < window:
                return False
            self._last[cluster_id] = (kind, now)
            if now - self._pruned >= window:
                self._prune(now, window)
            return True

    def _prune(self, now, window):
        self._pruned = now
        for cluster_id in [c for c, (_, at) in self._last.items()
                           if now - at >= window]:
            del self._last[cluster_id]


class ClusterEventRoute(object):
    """Which task managers get a cluster event.

    With cluster_recovery_slots, the task manager replicas are members of
    the recovery group under their slots' names, which are also their RPC
    server names: an event goes to the cluster_recovery_coverage replicas
    that watch its cluster (the rendezvous hash every replica computes for
    itself), not to every replica. Every task manager still gets:

    * a write lease request: it is answered, and a stale membership must
      not send it to a replica that is gone;
    * everything, while slots are off, no coordination backend is
      configured, the membership cannot be read or is empty, or a member is
      not a slot (an older task manager during an upgrade).

    An event sent to a slot just given up waits in the slot's queue for the
    next holder; the replicas that took over learn of the cluster from the
    next event, their own scan or the members' repeated reports, as when an
    event is lost today.
    """

    def __init__(self, coordinator=None):
        self._coordinator = coordinator
        self._started = False

    @property
    def coordinator(self):
        if self._coordinator is None:
            from trove.common import coordination
            self._coordinator = coordination.Coordinator(observer=True)
        if not self._started:
            self._started = True
            self._coordinator.start()
        return self._coordinator

    def servers(self, cluster_id, kind):
        """The task manager servers to send to; None for every one."""
        if not CONF.cluster_recovery_slots or kind in ALWAYS:
            return None
        if not CONF.coordination.backend_url:
            return None
        from trove.common import coordination
        try:
            members = self.coordinator.members()
        except Exception:
            return None
        if not members or not all(coordination.is_slot(m) for m in members):
            return None
        return [m.decode() if isinstance(m, bytes) else m
                for m in coordination.coverers(
                    cluster_id, members, CONF.cluster_recovery_coverage)]
