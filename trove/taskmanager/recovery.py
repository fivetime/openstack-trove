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

"""The hub of the task manager's cluster recovery: which clusters this
replica looks at, and when.

A cluster is looked at when one of its members says so (a cluster event,
through the conductor, to every task manager replica), when the scan of
the database finds a member whose heartbeat is stale or a recovery under
way, and, while a cluster waits or recovers, again when its wait ends or
after the recovery interval; a sweep of every cluster (the old poll)
only when configured. Each replica looks only at the clusters it covers
(``trove.common.coordination``: ``cluster_recovery_coverage`` replicas
per cluster, all acting, deduplicated by the cluster's recovery task),
so the replicas share the work and a healthy cluster costs nothing.

The looks of one cluster are coalesced: however many members report
within ``cluster_event_coalesce`` seconds, the cluster is looked at
once; a report that arrives while a look is under way makes one more
look after it. The looks run on a bounded pool
(``cluster_recovery_workers``).
"""

from concurrent import futures
import threading
import time

from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_service import loopingcall
from oslo_utils import timeutils
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy.sql import expression

from trove.cluster.models import DBCluster
from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import coordination
from trove.common import exception
from trove.common.strategies.cluster import strategy
from trove.datastore import models as datastore_models
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove import rpc

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
# How long the RPC server of a slot may take to start before the slot is
# given up (its queue may be one the broker cannot declare any more).
SLOT_SERVE_TIMEOUT = 60
# How often a replica without a slot in service looks at the membership
# (and so tries for a slot).
SLOT_POKE = 10

_hub = None
_hub_lock = threading.Lock()


def hub(context=None):
    """The one hub of this process, started on first use."""
    global _hub
    with _hub_lock:
        if _hub is None:
            _hub = RecoveryHub()
        if context is not None and not _hub.started:
            _hub.start(context)
        return _hub


def suspect_clusters(stale_before=None):
    """The clusters that may need the task manager, from the database
    alone: (cluster id, task id, datastore version id, the oldest
    heartbeat of their members) of every cluster that is not deleted
    and has no task but a recovery, with a member whose heartbeat is
    older than ``stale_before`` or a recovery under way; every such
    cluster when ``stale_before`` is None.
    """
    oldest = func.min(InstanceServiceStatus.updated_at)
    with DBCluster.query() as query:
        rows = query.session.query(
            DBCluster.id, DBCluster.task_id, DBCluster.datastore_version_id,
            oldest
        ).select_from(DBCluster).join(
            DBInstance, DBInstance.cluster_id == DBCluster.id
        ).join(
            InstanceServiceStatus,
            InstanceServiceStatus.instance_id == DBInstance.id
        ).filter(
            DBCluster.deleted == expression.false(),
            DBInstance.deleted == expression.false(),
            DBCluster.task_id.in_([ClusterTasks.NONE.code,
                                   ClusterTasks.RECOVERING_CLUSTER.code])
        ).group_by(DBCluster.id, DBCluster.task_id,
                   DBCluster.datastore_version_id)
        if stale_before is not None:
            rows = rows.having(or_(
                oldest < stale_before,
                DBCluster.task_id == ClusterTasks.RECOVERING_CLUSTER.code))
        return rows.all()


class RecoveryHub(object):

    def __init__(self, coordinator=None):
        self.coordinator = coordinator or coordination.Coordinator()
        self.context = None
        self.started = False
        # The recovery of each datastore manager (None for a manager
        # whose clusters recover by themselves), the manager of each
        # datastore version and of each cluster.
        self.recoveries = {}
        self.managers = {}
        self.cluster_manager = {}
        # The looks asked for: cluster -> (due, reason); the clusters
        # being looked at; when each was last looked at.
        self.pending = {}
        self.inflight = set()
        self.last_look = {}
        self._lock = threading.Lock()
        self._pool = None
        self._ticker = None
        # The RPC server of the slot held (cluster_recovery_slots), the
        # slot whose server is starting and since when, and a counter that
        # tells a server started for a slot given up meanwhile.
        self._slot_server = None
        self._slot_serving = None
        self._slot_generation = 0
        self._slot_poked = 0

    def start(self, context):
        self.context = context
        self.coordinator.on_slot_held = self._serve_slot
        self.coordinator.on_slot_lost = self._stop_slot
        self.coordinator.start()
        self._pool = futures.ThreadPoolExecutor(
            max_workers=CONF.cluster_recovery_workers)
        self._ticker = loopingcall.FixedIntervalLoopingCall(self._tick)
        self._ticker.start(interval=1, initial_delay=1,
                           stop_on_exception=False)
        self.started = True
        LOG.info("Cluster recovery: looking at the clusters the members "
                 "report, scanning every %s seconds, %s at a time.",
                 CONF.cluster_recovery_scan_interval or 'no',
                 CONF.cluster_recovery_workers)

    def stop(self):
        if self._ticker is not None:
            self._ticker.stop()
            self._ticker = None
        if self._pool is not None:
            self._pool.shutdown(wait=False)
            self._pool = None
        self._stop_slot()
        self.coordinator.stop()
        self.started = False

    # The slot (cluster_recovery_slots): trove-conductor sends this replica
    # the events of the clusters it covers through the slot's server. The
    # server starts in the background (declaring its queue can hang on a
    # broker that cannot declare that queue any more); the replica joins
    # the group once it is up, and gives the slot up when it is not up in
    # SLOT_SERVE_TIMEOUT seconds.

    def _serve_slot(self, slot):
        self._stop_slot()
        with self._lock:
            self._slot_generation += 1
            generation = self._slot_generation
            self._slot_serving = (slot, time.monotonic())
        threading.Thread(target=self._start_slot_server,
                         args=(slot, generation), daemon=True).start()

    def _start_slot_server(self, slot, generation):
        from trove.taskmanager import api as task_api
        key = (CONF.taskmanager_rpc_encr_key
               if CONF.enable_secure_rpc_messaging else None)
        target = messaging.Target(topic=CONF.taskmanager_queue, server=slot,
                                  version=task_api.API.API_LATEST_VERSION)
        try:
            server = rpc.get_server(target, [SlotEndpoint(self)], key=key)
            server.start()
        except Exception:
            LOG.exception("Cannot serve slot %s.", slot)
            return
        with self._lock:
            current = generation == self._slot_generation
            if current:
                self._slot_server = server
                self._slot_serving = None
        if not current:
            # Given up meanwhile: another replica may hold the slot now.
            self._stop_server(server)
            return
        LOG.info("Taking the cluster events of slot %s.", slot)
        self.coordinator.slot_served()

    def _check_slot(self, now):
        with self._lock:
            serving = self._slot_serving
            late = (serving is not None and
                    now - serving[1] > SLOT_SERVE_TIMEOUT)
            if late:
                self._slot_generation += 1
                self._slot_serving = None
        if late:
            LOG.error("The RPC server of slot %s did not start in %d "
                      "seconds.", serving[0], SLOT_SERVE_TIMEOUT)
            self.coordinator.give_up_slot()

    def _stop_slot(self):
        with self._lock:
            self._slot_generation += 1
            self._slot_serving = None
            server, self._slot_server = self._slot_server, None
        if server is not None:
            self._stop_server(server)

    @staticmethod
    def _stop_server(server):
        try:
            server.stop()
            server.wait()
        except Exception:
            LOG.exception("Failed to stop the server of the slot.")

    # What asks for a look

    def on_event(self, cluster_id, instance_id, kind, payload=None):
        """A member's report: a look at its cluster, when this replica
        covers it.
        """
        if not self.coordinator.covers(cluster_id):
            LOG.debug("Cluster %s: event %s from member %s; not watched "
                      "here.", cluster_id, kind, instance_id)
            return
        reason = 'event %s from member %s (%s)' % (
            kind, instance_id, (payload or {}).get('reason') or 'no reason')
        self.request_look(cluster_id, reason)

    def scan(self):
        """The clusters the database says may need a look: a member
        whose heartbeat is stale, or a recovery under way.
        """
        stale_before = timeutils.utcnow() - timeutils.datetime.timedelta(
            seconds=CONF.agent_heartbeat_expiry)
        self._look_at(suspect_clusters(stale_before), 'scan')

    def sweep(self):
        """Every cluster this replica covers."""
        self._look_at(suspect_clusters(None), 'sweep')

    def _look_at(self, rows, what):
        by_id = {row[0]: row for row in rows}
        for cluster_id, row in by_id.items():
            self.managers.setdefault(row[2], None)
        covered = self.coordinator.covered(by_id)
        LOG.debug("The %s finds %s clusters, %s watched here.", what,
                  len(by_id), len(covered))
        for cluster_id in covered:
            row = by_id[cluster_id]
            if row[1] == ClusterTasks.RECOVERING_CLUSTER.code:
                reason = '%s: a recovery under way' % what
            elif row[3] is not None:
                reason = '%s: the oldest heartbeat %s' % (what, row[3])
            else:
                reason = what
            self.request_look(cluster_id, reason)

    def request_look(self, cluster_id, reason, delay=0):
        """A look at the cluster, at the earliest after ``delay``
        seconds and ``cluster_event_coalesce`` seconds after the last
        look; the earliest of the looks asked for meanwhile.
        """
        now = time.monotonic()
        due = max(now + delay,
                  self.last_look.get(cluster_id, 0) +
                  CONF.cluster_event_coalesce)
        with self._lock:
            current = self.pending.get(cluster_id)
            if current is None or due < current[0]:
                self.pending[cluster_id] = (due, reason)

    # The looks

    def _tick(self):
        try:
            now = time.monotonic()
            self._check_slot(now)
            # Without a slot in service, ask for the membership now and
            # then: that is where a slot is tried for (no event comes to a
            # replica that watches nothing).
            if (not self.coordinator.watching and
                    now - self._slot_poked >= SLOT_POKE):
                self._slot_poked = now
                self.coordinator.members()
            with self._lock:
                ready = [(cluster_id, reason)
                         for cluster_id, (due, reason) in self.pending.items()
                         if due <= now and cluster_id not in self.inflight]
                for cluster_id, _reason in ready:
                    del self.pending[cluster_id]
                    self.inflight.add(cluster_id)
            for cluster_id, reason in ready:
                self._pool.submit(self._look_safely, cluster_id, reason)
        except Exception:
            LOG.exception("The recovery hub's tick failed.")

    def _look_safely(self, cluster_id, reason):
        try:
            self._look(cluster_id, reason)
        except Exception:
            LOG.exception("The look at cluster %s failed.", cluster_id)
        finally:
            with self._lock:
                self.inflight.discard(cluster_id)
                self.last_look[cluster_id] = time.monotonic()

    def _look(self, cluster_id, reason):
        try:
            db_cluster = DBCluster.find_by(id=cluster_id, deleted=False)
        except exception.NotFound:
            LOG.debug("Cluster %s is gone; forgotten.", cluster_id)
            self._forget(cluster_id)
            return
        recovery = self._recovery_of(db_cluster)
        if recovery is None:
            return
        recovery.check_now(self.context, db_cluster, reason)
        if recovery.still_watching(cluster_id):
            self.request_look(cluster_id, 'still watching',
                              delay=recovery.look_again_in(cluster_id))

    def _forget(self, cluster_id):
        with self._lock:
            self.pending.pop(cluster_id, None)
            self.last_look.pop(cluster_id, None)
        self.cluster_manager.pop(cluster_id, None)

    def _recovery_of(self, db_cluster):
        """The recovery of the cluster's datastore manager; None when
        its clusters recover by themselves.
        """
        manager = self.cluster_manager.get(db_cluster.id)
        if manager is None:
            version_id = db_cluster.datastore_version_id
            manager = self.managers.get(version_id)
            if manager is None:
                manager = datastore_models.DatastoreVersion.load_by_uuid(
                    version_id).manager
                self.managers[version_id] = manager
            self.cluster_manager[db_cluster.id] = manager
        if manager not in self.recoveries:
            strat = strategy.load_taskmanager_strategy(manager)
            make = getattr(strat, 'cluster_recovery', None)
            self.recoveries[manager] = make(manager) if make else None
        return self.recoveries[manager]


class SlotEndpoint(object):
    """What trove-conductor sends a slot: the events of the clusters the
    replica holding it covers.
    """

    def __init__(self, hub):
        self.hub = hub

    @property
    def target(self):
        from trove.taskmanager import api as task_api
        return messaging.Target(version=task_api.API.API_LATEST_VERSION)

    def cluster_event(self, context, cluster_id, instance_id, kind,
                      payload=None, sent=None):
        self.hub.on_event(cluster_id, instance_id, kind, payload)
