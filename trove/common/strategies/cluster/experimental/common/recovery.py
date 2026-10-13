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

"""The task manager's periodic look at the clusters of a datastore, and
what it does when a cluster needs it: the skeleton every datastore's
recovery shares.

Every ``cluster_recovery_check_interval`` seconds the task manager asks
each member of each cluster where it stands (``get_recovery_view``) and
hands the answers to the datastore's recovery, which decides. The
skeleton keeps the decision off a cluster that is being built, grown,
shrunk or whose members are busy; waits a grace period before the first
decision and an interval between decisions; takes the cluster's recovery
task as a lock (compare-and-set, so that two task managers cannot both
take it) while something is under way, and frees it once the cluster is
settled or the recovery has had its time.

A look waits on the members (a member that is gone, for its timeout),
so the members of a cluster are asked at once, and the clusters are
looked at ``cluster_recovery_workers`` at a time: a look at every
cluster has to end well within the interval, or the leases the looks
renew run out.
"""

from concurrent import futures
import random
import time

from oslo_log import log as logging

from trove.cluster import models as cluster_models
from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import clients
from trove.instance.models import DBInstance
from trove.instance import tasks as inst_tasks

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class ClusterRecovery(object):
    """The recovery of the clusters of one datastore manager.

    A datastore's recovery gives:

    * ``_needs_look(answers)``: whether the members' answers show a
      cluster that may need the recovery; the grace period counts from
      the first look that says so.
    * ``_decide(context, db_cluster, members, views)``: look at a cluster
      whose grace period and interval have passed, and act. ``_take``
      takes the cluster's recovery task first; ``_follow`` frees it.
    * ``_settled(views)``: whether a cluster under recovery is back; the
      task is freed then, or after ``cluster_recovery_timeout``.
    """

    def __init__(self, manager, guest_factory=None):
        self.manager = manager
        self.conf = CONF.get(manager)
        # The clusters seen needing a look: when first, and when last
        # decided on; and the clusters under recovery: since when.
        self.waiting_since = {}
        self.last_try = {}
        self.forming_since = {}
        self.guest_factory = guest_factory or self._guest

    def _guest(self, context, member):
        return clients.create_guest_client(context, member.id, self.manager)

    def _members(self, context, cluster_id):
        """The cluster's members as the database holds them: the task
        manager's context owns no tenant, so the instances are not loaded
        through it, and the guest client needs only the id.
        """
        return DBInstance.find_all(cluster_id=cluster_id, deleted=False).all()

    def _views(self, context, members):
        """What every member says, asked all at once; a member that
        does not answer is unreachable (None).
        """
        def ask(member):
            try:
                answer = self.guest_factory(context, member).get_recovery_view(
                    timeout=self.conf.cluster_peer_timeout)
            except Exception as err:
                LOG.debug("Member %s did not answer: %s", member.id, err)
                answer = None
            return member, answer
        if len(members) < 2:
            return [ask(member) for member in members]
        with futures.ThreadPoolExecutor(max_workers=len(members)) as pool:
            return list(pool.map(ask, members))

    @staticmethod
    def _member_busy(member):
        """A member with a task of its own (being built, resized, restarted,
        upgraded...) is left alone, and so is its cluster.
        """
        try:
            return member.task_status != inst_tasks.InstanceTasks.NONE
        except Exception:
            return False

    def _needs_look(self, answers):
        raise NotImplementedError()

    def _decide(self, context, db_cluster, members, views):
        raise NotImplementedError()

    def _settled(self, views):
        raise NotImplementedError()

    def check(self, context, db_cluster):
        """One look at a cluster."""
        cluster_id = db_cluster.id
        if db_cluster.task_status == ClusterTasks.RECOVERING_CLUSTER:
            self._follow(context, db_cluster)
            return
        if db_cluster.task_status != ClusterTasks.NONE:
            return
        members = self._members(context, cluster_id)
        if len(members) < 2:
            return
        if any(self._member_busy(member) for member in members):
            return
        views = self._views(context, members)
        answers = [answer for _m, answer in views if answer]
        if not self._needs_look(answers):
            self.waiting_since.pop(cluster_id, None)
            return
        now = time.monotonic()
        since = self.waiting_since.setdefault(cluster_id, now)
        if now - since < self.conf.cluster_recovery_grace:
            return
        if now - self.last_try.get(cluster_id, 0) < \
                self.conf.cluster_recovery_interval:
            return
        self.last_try[cluster_id] = now
        self._decide(context, db_cluster, members, views)

    def _follow(self, context, db_cluster):
        """A cluster under recovery: free it once it is settled, or once
        the recovery has had its time; the next look decides anew then.
        """
        cluster_id = db_cluster.id
        now = time.monotonic()
        since = self.forming_since.setdefault(cluster_id, now)
        views = self._views(context, self._members(context, cluster_id))
        if self._settled(views):
            LOG.info("Cluster %s is back.", cluster_id)
        elif now - since < self.conf.cluster_recovery_timeout:
            return
        else:
            LOG.error("Cluster %s was not back within %s seconds; looking "
                      "at it anew.", cluster_id,
                      self.conf.cluster_recovery_timeout)
        self._release(cluster_id, db_cluster)

    def _release(self, cluster_id, db_cluster=None):
        self.forming_since.pop(cluster_id, None)
        self.waiting_since.pop(cluster_id, None)
        db_cluster = db_cluster or cluster_models.DBCluster.find_by(
            id=cluster_id)
        db_cluster.update(task_status=ClusterTasks.NONE)

    def _take(self, cluster_id):
        """Take the cluster's recovery task, which keeps another task
        manager, and a grow or a shrink, off it meanwhile: only when no
        task is on it, checked and set in one statement.
        """
        with cluster_models.DBCluster.query() as query:
            taken = query.filter_by(
                id=cluster_id, task_id=ClusterTasks.NONE.code).update(
                    {'task_id': ClusterTasks.RECOVERING_CLUSTER.code},
                    synchronize_session=False)
        if taken:
            self.forming_since[cluster_id] = time.monotonic()
        return bool(taken)

    def _unchanged_after_jitter(self, context, members, views, key):
        """Another look after a moment, against two task managers or a
        member deciding the same: whether nothing changed meanwhile.
        ``key(answer)`` picks what counts as a change.
        """
        time.sleep(random.uniform(0, self.conf.cluster_bootstrap_jitter))
        again = self._views(context, members)
        return ([(m.id, key(a)) for m, a in again] ==
                [(m.id, key(a)) for m, a in views])

    def run(self, context, db_clusters):
        """One look at every cluster, several at a time."""
        def look(db_cluster):
            try:
                self.check(context, db_cluster)
            except Exception:
                LOG.exception("The recovery check of cluster %s failed.",
                              db_cluster.id)
        started = time.monotonic()
        workers = max(1, min(CONF.cluster_recovery_workers, len(db_clusters)))
        if workers < 2:
            for db_cluster in db_clusters:
                look(db_cluster)
        else:
            with futures.ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(look, db_clusters))
        took = time.monotonic() - started
        interval = CONF.cluster_recovery_check_interval
        if took > interval:
            LOG.warning("The look at the %s %s clusters took %d seconds, "
                        "more than the %d between two looks: the primaries' "
                        "leases may run out. More cluster_recovery_workers "
                        "(%s now), or another task manager.",
                        len(db_clusters), self.manager, took, interval,
                        CONF.cluster_recovery_workers)
        else:
            LOG.debug("Looked at the %s %s clusters in %.1f seconds.",
                      len(db_clusters), self.manager, took)
