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

"""Bringing a Galera cluster back after every member went down.

A Galera member that comes up and finds no primary component waits for
one, with its database port closed, so the members cannot ask each other
where they stand as Group Replication members do; the task manager asks
them instead, on its periodic task, and decides as the cluster probe
would: a member waiting past the grace period, no member in a primary
component, a majority of the members answering, and the member that holds
every transaction any of them holds (the lowest address when several do)
forms the cluster again; the others join it by themselves. The member is
told, not waited for: the cluster keeps its recovery task until a member
is in a primary component again, or the member has had its time.

Galera brings the cluster back by itself when every member of the last
primary component returns (pc.recovery); then nothing is waiting here.
"""

import random
import time

from oslo_log import log as logging

from trove.cluster import models as cluster_models
from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import clients
from trove.guestagent.common import cluster_probe
from trove.instance.models import DBInstance

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class GaleraClusterRecovery(object):
    """The recovery of the Galera clusters of one datastore manager, run
    by the task manager every cluster_recovery_check_interval seconds.
    """

    def __init__(self, manager, guest_factory=None):
        self.manager = manager
        self.conf = CONF.get(manager)
        # The clusters seen waiting: when first, and when last tried; and
        # the clusters being formed again: since when.
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
        """What every member says, PeerView each; a member that does not
        answer is unreachable.
        """
        views = []
        for member in members:
            try:
                answer = self.guest_factory(context, member).get_recovery_view(
                    timeout=self.conf.cluster_peer_timeout)
            except Exception as err:
                LOG.debug("Member %s did not answer: %s", member.id, err)
                answer = None
            views.append((member, answer))
        return views

    @staticmethod
    def _position(answer):
        position = answer.get('position') if answer else None
        return tuple(position) if position else None

    @staticmethod
    def _not_ahead(position, other):
        if position is None:
            return True
        if other is None:
            return False
        return position[0] == other[0] and position[1] <= other[1]

    def _peers_of(self, views, member):
        return [cluster_probe.PeerView(
            answer.get('ip') if answer else None, answer is not None,
            bool(answer and answer.get('in_group')),
            bool(answer and answer.get('in_group')),
            self._position(answer))
            for other, answer in views if other is not member]

    def check(self, context, db_cluster):
        """One look at a cluster: nothing, or a member told to form the
        cluster again.
        """
        cluster_id = db_cluster.id
        if db_cluster.task_status == ClusterTasks.RECOVERING_CLUSTER:
            self._follow(context, db_cluster)
            return
        if db_cluster.task_status != ClusterTasks.NONE:
            return
        members = self._members(context, cluster_id)
        if len(members) < 2:
            return
        views = self._views(context, members)
        answers = [answer for _m, answer in views if answer]
        if any(answer.get('in_group') for answer in answers):
            # A primary component is up; whoever waits joins it.
            self.waiting_since.pop(cluster_id, None)
            return
        if not any(answer.get('waiting') for answer in answers):
            # Down, starting, or being built: not for the recovery.
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
        """A cluster being formed again: free it once a member is in a
        primary component (the others join by themselves), or once the
        member told to form it has had its time; the next look decides
        anew then.
        """
        cluster_id = db_cluster.id
        now = time.monotonic()
        since = self.forming_since.setdefault(cluster_id, now)
        views = self._views(context, self._members(context, cluster_id))
        if any(answer and answer.get('in_group') for _m, answer in views):
            LOG.info("Cluster %s is formed again.", cluster_id)
        elif now - since < self.conf.cluster_recovery_timeout:
            return
        else:
            LOG.error("Cluster %s was not formed again within %s seconds; "
                      "looking at it anew.", cluster_id,
                      self.conf.cluster_recovery_timeout)
        self.forming_since.pop(cluster_id, None)
        self.waiting_since.pop(cluster_id, None)
        db_cluster.update(task_status=ClusterTasks.NONE)

    def _decide(self, context, db_cluster, members, views):
        cluster_id = db_cluster.id
        # Every waiting member decides for itself from the same views, as
        # the probe would; the one the decision falls on goes.
        chosen = None
        for member, answer in views:
            if not answer or not answer.get('waiting'):
                continue
            position = self._position(answer)
            if position is None:
                # Not knowing what it holds, it never forms the cluster.
                action, reason = cluster_probe.WAIT, 'position unknown'
            else:
                action, reason = cluster_probe.decide(
                    answer.get('ip'), position,
                    self._peers_of(views, member), len(members),
                    self._not_ahead,
                    self.conf.cluster_bootstrap_needs_all_members)
            LOG.info("Cluster %s, member %s (%s) at %s: %s (%s).",
                     cluster_id, member.id, answer.get('ip'),
                     '%s:%s' % position if position else 'unknown',
                     action, reason)
            if action == cluster_probe.BOOTSTRAP:
                chosen = member
                break
        if chosen is None:
            return
        if not self.conf.cluster_auto_bootstrap:
            LOG.info("Cluster %s would be formed again on member %s; auto "
                     "bootstrap is off, an operator does it.", cluster_id,
                     chosen.id)
            return
        # Another look after a moment: only when nothing changed.
        time.sleep(random.uniform(0, self.conf.cluster_bootstrap_jitter))
        again = self._views(context, members)
        if ([(m.id, a and a.get('in_group'), a and a.get('waiting'),
              self._position(a)) for m, a in again] !=
                [(m.id, a and a.get('in_group'), a and a.get('waiting'),
                  self._position(a)) for m, a in views]):
            LOG.info("Cluster %s changed meanwhile; not forming it now.",
                     cluster_id)
            return
        # The cluster's task keeps another task manager, and a grow or a
        # shrink, off it meanwhile.
        db_cluster = cluster_models.DBCluster.find_by(id=cluster_id)
        if db_cluster.task_status != ClusterTasks.NONE:
            return
        db_cluster.update(task_status=ClusterTasks.RECOVERING_CLUSTER)
        self.forming_since[cluster_id] = time.monotonic()
        try:
            LOG.info("Forming cluster %s again on member %s.", cluster_id,
                     chosen.id)
            self.guest_factory(context, chosen).bootstrap_cluster()
        except Exception:
            LOG.exception("Member %s of cluster %s could not be told to "
                          "form it again.", chosen.id, cluster_id)
            self.forming_since.pop(cluster_id, None)
            db_cluster.update(task_status=ClusterTasks.NONE)

    def run(self, context, db_clusters):
        for db_cluster in db_clusters:
            try:
                self.check(context, db_cluster)
            except Exception:
                LOG.exception("The recovery check of cluster %s failed.",
                              db_cluster.id)
