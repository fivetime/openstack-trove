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

from oslo_log import log as logging

from trove.common.strategies.cluster.experimental.common import (
    recovery as common_recovery)
from trove.guestagent.common import cluster_probe

LOG = logging.getLogger(__name__)


class GaleraClusterRecovery(common_recovery.ClusterRecovery):
    """The recovery of the Galera clusters of one datastore manager, run
    by the task manager every cluster_recovery_check_interval seconds.
    """

    @staticmethod
    def _position(answer):
        position = answer.get('position') if answer else None
        return tuple(position) if position else None

    def _position_text(self, answer):
        position = self._position(answer)
        return '%s:%s' % position if position else 'unknown'

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

    def _needs_look(self, answers):
        """A primary component up: whoever waits joins it. Nobody waiting:
        down, starting, or being built. Neither is for the recovery.
        """
        if any(answer.get('in_group') for answer in answers):
            return False
        return any(answer.get('waiting') for answer in answers)

    def _settled(self, views):
        """Formed again: a member is in a primary component, the others
        join by themselves.
        """
        return any(answer and answer.get('in_group') for _m, answer in views)

    def _decide(self, context, db_cluster, members, views):
        cluster_id = db_cluster.id
        # A member here whose position is not known yet (its recovery
        # run comes after the grace period) may hold the most: nothing is
        # decided until every member here answers its position.
        unknown = [answer.get('ip') for _m, answer in views
                   if answer and answer.get('waiting') and
                   self._position(answer) is None]
        if unknown:
            for member, answer in views:
                if answer and answer.get('waiting'):
                    LOG.info("Cluster %s, member %s (%s) at %s: wait (the "
                             "position of %s is not known yet).",
                             cluster_id, member.id, answer.get('ip'),
                             self._position_text(answer),
                             ', '.join(unknown))
            return
        # The last member to leave the cluster holds everything it
        # committed, and Galera marked it so: it forms the cluster again
        # without the others, unless one of them holds more, which would
        # mean the mark is stale.
        chosen = None
        for member, answer in views:
            if not (answer and answer.get('waiting') and
                    answer.get('safe_to_bootstrap')):
                continue
            position = self._position(answer)
            ahead = [a.get('ip') for m, a in views
                     if a and m is not member and
                     not self._not_ahead(self._position(a), position)]
            if ahead:
                LOG.warning("Cluster %s, member %s (%s) at %s:%s was the "
                            "last to leave, yet %s hold more; not trusted.",
                            cluster_id, member.id, answer.get('ip'),
                            position[0], position[1], ', '.join(ahead))
                continue
            LOG.info("Cluster %s, member %s (%s) at %s:%s: bootstrap (the "
                     "last to leave the cluster).", cluster_id, member.id,
                     answer.get('ip'), position[0], position[1])
            chosen = member
            break
        # Otherwise every waiting member decides for itself from the same
        # views, as the probe would, and every decision is logged; the
        # first member the decision falls on goes.
        for member, answer in views if chosen is None else []:
            if not answer or not answer.get('waiting'):
                continue
            position = self._position(answer)
            action, reason = cluster_probe.decide(
                answer.get('ip'), position,
                self._peers_of(views, member), len(members),
                self._not_ahead,
                self.conf.cluster_bootstrap_needs_all_members)
            LOG.info("Cluster %s, member %s (%s) at %s: %s (%s).",
                     cluster_id, member.id, answer.get('ip'),
                     self._position_text(answer), action, reason)
            if action == cluster_probe.BOOTSTRAP and chosen is None:
                chosen = member
        if chosen is None:
            return
        if not self.conf.cluster_auto_bootstrap:
            LOG.info("Cluster %s would be formed again on member %s; auto "
                     "bootstrap is off, an operator does it.", cluster_id,
                     chosen.id)
            return
        # Another look after a moment: only when nothing changed.
        if not self._unchanged_after_jitter(
                context, members, views,
                lambda a: (a and a.get('in_group'), a and a.get('waiting'),
                           self._position(a))):
            LOG.info("Cluster %s changed meanwhile; not forming it now.",
                     cluster_id)
            return
        if not self._take(cluster_id):
            return
        try:
            LOG.info("Forming cluster %s again on member %s.", cluster_id,
                     chosen.id)
            self.guest_factory(context, chosen).bootstrap_cluster()
        except Exception:
            LOG.exception("Member %s of cluster %s could not be told to "
                          "form it again.", chosen.id, cluster_id)
            self._release(cluster_id)
