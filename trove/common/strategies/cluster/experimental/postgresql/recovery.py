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

"""Keeping a PostgreSQL cluster on one primary: the task manager's look
at every cluster, every ``cluster_recovery_check_interval`` seconds.

PostgreSQL's streaming replication has no notion of the cluster: the
primary knows a WAL sender timed out, a standby knows it receives no
WAL, and neither draws a conclusion. The task manager does, from what
every member answers (``get_recovery_view``), and acts as a reconciler:
each look derives from the answers what the cluster should be and takes
the next idempotent step towards it, so that a look interrupted halfway,
two primaries, a primary back after days or a standby rebuilt halfway
all converge under the same rules.

With a primary answering: it gets the lease that keeps its write port
open while no standby streams from it; a standby pointed elsewhere is
pointed at it; a former primary (a lower timeline) and a member whose
server is down rejoin it as standbys; a standby without it for too long
is rebuilt from it.

Without one: the primary is given up only when every standby here has
been without its WAL for the grace period and the primary itself does
not answer, or answers that it has no standby either; never when a
standby still streams from it (it lives on the tenant network,
whatever the management network says), never when no member answers at
all (the task manager's network), never with two members, and never
while the promotion could lose an acknowledged commit (the quorum rule)
or, in async mode, more than ``cluster_failover_max_lag_bytes``. The
old primary is fenced if it answers, the standby that holds the most is
promoted once the old primary's lease has run out, the other standbys
follow it, and the old primary rejoins it as a standby.
"""

import time

from oslo_log import log as logging

from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common.strategies.cluster.experimental.common import (
    recovery as common_recovery)
from trove.common.strategies.cluster.experimental.postgresql import (
    topology)

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# What a call to a member returns when it failed.
FAILED = object()


class PostgresqlClusterRecovery(common_recovery.ClusterRecovery):
    """The reconciler of the PostgreSQL clusters of one datastore
    manager.
    """

    def __init__(self, manager, guest_factory=None):
        super(PostgresqlClusterRecovery, self).__init__(manager,
                                                        guest_factory)
        # What the task manager remembers of each cluster's primary: its
        # id and address, where it was, which standbys streamed from it
        # (the synchronous set), and when its lease was last renewed.
        self.memory = {}
        # When this task manager first looked at each cluster: a lease
        # it does not know of (granted by the task manager before a
        # restart) is taken as renewed then.
        self.first_seen = {}
        # When a member was last told to follow, rejoin or be rebuilt.
        self.last_action = {}

    @property
    def lease_ttl(self):
        """A lease outlives two looks: one missed look does not close
        the primary's write port.
        """
        return 2 * CONF.cluster_recovery_check_interval

    def _tell(self, context, member_id, what, *args, **kwargs):
        """One call to a member, logged when it fails."""
        try:
            return getattr(self.guest_factory(context, self._member(
                member_id)), what)(*args, **kwargs)
        except Exception as err:
            LOG.warning("Member %s: %s failed: %s", member_id, what, err)
            return FAILED

    def _member(self, member_id):
        for member in self._current_members:
            if member.id == member_id:
                return member
        raise LookupError(member_id)

    def _renew_lease(self, context, cluster_id, primary_id):
        if self._tell(context, primary_id, 'renew_lease',
                      self.lease_ttl) is not FAILED:
            self.memory.setdefault(cluster_id, {})['lease_at'] = \
                time.monotonic()

    def _slot_then(self, context, leader, member_id, what, primary):
        """A member follows or rejoins the primary through its slot: the
        slot first (kept when it is there), then the member.
        """
        self._tell(context, leader, 'create_member_slot', member_id)
        self._tell(context, member_id, what, primary)

    def _due(self, cluster_id, member_id, now):
        """Whether a member may be told again: once an interval."""
        key = (cluster_id, member_id)
        last = self.last_action.get(key)
        if last is not None and \
                now - last < self.conf.cluster_recovery_interval:
            return False
        self.last_action[key] = now
        return True

    # The skeleton's hooks

    def _needs_look(self, answers):
        return not any(a.get('state') == topology.PRIMARY for a in answers)

    def _decide(self, context, db_cluster, members, views):
        self._current_members = members
        top = topology.Topology(members, views)
        primary_id, host = self._expected_primary(context, db_cluster.id,
                                                  top)
        self._failover(context, db_cluster, top, primary_id, host,
                       'decided on')

    def _settled(self, views):
        """Back: one primary, and every standby here that is not being
        rebuilt streams from it.
        """
        top = topology.Topology([m for m, _a in views], views)
        leader, ambiguous = top.leader()
        if leader is None or ambiguous:
            return False
        host = top.ip(leader)
        standbys = [i for i in top.standbys
                    if not top.answer(i).get('rebuilding')]
        return bool(standbys) and all(top.streaming_from(i, host)
                                      for i in standbys)

    # The look

    def check(self, context, db_cluster):
        cluster_id = db_cluster.id
        if db_cluster.task_status == ClusterTasks.RECOVERING_CLUSTER:
            self._follow(context, db_cluster)
            return
        if db_cluster.task_status != ClusterTasks.NONE:
            return
        members = self._members(context, cluster_id)
        self._current_members = members
        if len(members) < 2:
            return
        if any(self._member_busy(member) for member in members):
            return
        self.first_seen.setdefault(cluster_id, time.monotonic())
        views = self._views(context, members)
        top = topology.Topology(members, views)
        if not top.reachable:
            LOG.warning("Cluster %s: no member answers; nothing is decided "
                        "(the task manager's network?).", cluster_id)
            return
        leader, ambiguous = top.leader()
        if ambiguous:
            LOG.error("Cluster %s: members %s answer as primaries on the "
                      "same timeline; an operator decides.", cluster_id,
                      ', '.join(top.primaries))
            return
        now = time.monotonic()
        if leader is not None:
            # A primary answers; still, when every standby here has been
            # without its WAL for the grace period and it has had no
            # standby as long, it is cut off from the standbys (the
            # tenant network), not healthy: no lease for it.
            primary_id, host = leader, top.ip(leader)
            dead, why = topology.primary_dead(
                top, primary_id, host, self.conf.cluster_recovery_grace)
            if not dead:
                self.waiting_since.pop(cluster_id, None)
                self._healthy(context, cluster_id, top, leader, now)
                return
        else:
            primary_id, host = self._expected_primary(context, cluster_id,
                                                      top)
            dead, why = topology.primary_dead(
                top, primary_id, host, self.conf.cluster_recovery_grace)
        since = self.waiting_since.setdefault(cluster_id, now)
        if not dead:
            LOG.info("Cluster %s: the primary %s does not answer as one; "
                     "not given up: %s.", cluster_id, primary_id, why)
            if why.startswith('standby') and 'streams' in why:
                self.waiting_since.pop(cluster_id, None)
            return
        if now - since < self.conf.cluster_recovery_grace:
            LOG.info("Cluster %s: the primary %s seems gone (%s); %s more "
                     "seconds before deciding.", cluster_id, primary_id,
                     why, int(self.conf.cluster_recovery_grace -
                              (now - since)))
            return
        if now - self.last_try.get(cluster_id, 0) < \
                self.conf.cluster_recovery_interval:
            return
        self.last_try[cluster_id] = now
        self.memory.setdefault(cluster_id, {})['failing_since'] = since
        self._failover(context, db_cluster, top, primary_id, host, why)

    def _expected_primary(self, context, cluster_id, top):
        """The primary the members follow and its address: as the task
        manager remembers them, or as the members say; the address, when
        no standby streams any more and the task manager has restarted,
        from the members' list of the cluster.
        """
        mem = self.memory.get(cluster_id) or {}
        voted_id, voted_host = top.expected_primary()
        primary_id = mem.get('primary_id') or voted_id
        host = mem.get('primary_ip') or voted_host
        if primary_id and not host:
            host = self._address_of(context, top, primary_id)
        return primary_id, host

    def _address_of(self, context, top, member_id):
        for other in top.reachable:
            known = self._tell(context, other, 'get_cluster_context')
            if known is not FAILED and known:
                address = (known.get('members') or {}).get(member_id)
                if address:
                    return address
        return None

    def _healthy(self, context, cluster_id, top, leader, now):
        """A primary answers: it keeps its lease, the task manager
        remembers it, and the other members are reconciled with it.
        """
        answer = top.answer(leader)
        mem = self.memory.setdefault(cluster_id, {})
        mem.pop('failing_since', None)
        sync_set = sorted(i for i, s in
                          (answer.get('standbys') or {}).items()
                          if s.get('state') == topology.STREAMING and
                          i in top.ids)
        mem.update(primary_id=leader, primary_ip=answer.get('ip'),
                   lsn=answer.get('lsn'), timeline=answer.get('timeline'),
                   sync_set=sync_set)
        self._renew_lease(context, cluster_id, leader)
        self._share_sync_set(context, top, leader, sync_set,
                             answer.get('lsn')
                             if top.mode == topology.ASYNC else None)
        host = answer.get('ip')
        primary = {'id': leader, 'host': host}
        for member_id in top.reachable:
            if member_id == leader:
                continue
            other = top.answer(member_id)
            state = other.get('state')
            if other.get('rebuilding') or state == topology.REBUILDING:
                continue
            if state == topology.PRIMARY:
                # A former primary, on a lower timeline.
                if self._due(cluster_id, member_id, now):
                    LOG.info("Cluster %s: member %s is a former primary "
                             "(timeline %s, the primary's %s): rejoin.",
                             cluster_id, member_id, other.get('timeline'),
                             answer.get('timeline'))
                    self._slot_then(context, leader, member_id,
                                    'rejoin_cluster', primary)
            elif state == topology.STOPPED:
                # A member whose server is down: the fenced former
                # primary, or a member whose rejoin failed (the primary
                # not reachable yet); told again every interval.
                if self._due(cluster_id, member_id, now):
                    LOG.info("Cluster %s: member %s is down (follows %s): "
                             "rejoin.", cluster_id, member_id,
                             other.get('primary_id'))
                    self._slot_then(context, leader, member_id,
                                    'rejoin_cluster', primary)
            elif state == topology.STANDBY:
                receiver = other.get('receiver') or {}
                elsewhere = (receiver.get('sender_host') not in (None, host)
                             or other.get('primary_id') != leader)
                away = other.get('disconnected_for') or 0
                if elsewhere and self._due(cluster_id, member_id, now):
                    LOG.info("Cluster %s: standby %s follows %s, not the "
                             "primary %s: follow.", cluster_id, member_id,
                             other.get('primary_id'), leader)
                    self._slot_then(context, leader, member_id,
                                    'follow_primary', primary)
                elif (away >= self.conf.cluster_standby_rebuild_after and
                      self._due(cluster_id, member_id, now)):
                    LOG.info("Cluster %s: standby %s has not streamed for "
                             "%s seconds: rejoin (rewind or base backup).",
                             cluster_id, member_id, int(away))
                    self._slot_then(context, leader, member_id,
                                    'rejoin_cluster', primary)

    def _share_sync_set(self, context, top, leader, sync_set, lsn=None):
        """Every other member keeps the standbys the primary streams to
        and, in async mode, where the primary is: they outlive both the
        primary and the task manager's memory. A member is told when what
        it keeps differs, or when the primary has moved on by more than
        half the lag a failover may lose since it was last told.
        """
        step = (self.conf.cluster_failover_max_lag_bytes or 0) // 2
        for member_id in top.reachable:
            if member_id == leader:
                continue
            kept = top.answer(member_id).get('sync_set') or {}
            stale = (kept.get('primary_id') != leader or
                     kept.get('members') != sync_set)
            if lsn is not None:
                stale = stale or kept.get('lsn') is None or \
                    lsn - kept['lsn'] > step
            if stale:
                self._tell(context, member_id, 'remember_sync_set', leader,
                           sync_set, lsn=lsn)

    @staticmethod
    def _kept(top, primary_id):
        """What the members keep of the given primary."""
        kept = [top.answer(i).get('sync_set') for i in top.reachable]
        return [k for k in kept
                if k and k.get('primary_id') == primary_id]

    @classmethod
    def _known_lsn(cls, mem, top, primary_id):
        """Where the gone primary last was: as the task manager
        remembers it, or else the furthest the members keep.
        """
        if mem.get('primary_id') == primary_id and \
                mem.get('lsn') is not None:
            return mem['lsn']
        kept = [k['lsn'] for k in cls._kept(top, primary_id)
                if k.get('lsn') is not None]
        return max(kept) if kept else None

    @classmethod
    def _known_sync_set(cls, mem, top, primary_id):
        """The standbys the gone primary last reported streaming: as
        the task manager remembers it, or else as the members keep it
        (all of the sets they keep for that primary together, the safe
        side when one missed an update); None when nobody knows.
        """
        if mem.get('primary_id') == primary_id and \
                mem.get('sync_set') is not None:
            return mem['sync_set']
        kept = [k for k in cls._kept(top, primary_id)
                if k.get('members') is not None]
        if not kept:
            return None
        return sorted(set().union(*(k['members'] for k in kept)))

    def _failover(self, context, db_cluster, top, primary_id=None,
                  host=None, why=''):
        """The primary is gone: the standby that holds the most takes
        over, when it safely can.
        """
        cluster_id = db_cluster.id
        now = time.monotonic()
        mem = self.memory.setdefault(cluster_id, {})
        if top.n < 3:
            LOG.warning("Cluster %s: the primary %s is gone (%s), but a "
                        "cluster of two does not fail over by itself.",
                        cluster_id, primary_id, why)
            return
        mode = top.mode
        timelines = [top.timeline(i) for i in top.standbys
                     if top.timeline(i) is not None]
        if mem.get('timeline') is not None:
            timelines.append(mem['timeline'])
        timeline = max(timelines) if timelines else None
        candidate_ids = topology.candidates(top, timeline)
        if not candidate_ids:
            LOG.error("Cluster %s: the primary %s is gone (%s), and no "
                      "standby on timeline %s can take over.", cluster_id,
                      primary_id, why, timeline)
            return
        failing_for = now - mem.get('failing_since', now)
        ok, quorum_why = topology.quorum(
            top, candidate_ids, mode,
            self._known_sync_set(mem, top, primary_id), primary_id,
            failing_for, self.conf.cluster_failover_force_after)
        if not ok:
            LOG.warning("Cluster %s: the primary %s is gone (%s); waiting: "
                        "%s.", cluster_id, primary_id, why, quorum_why)
            return
        chosen = topology.choose(top, candidate_ids)
        last_lsn = self._known_lsn(mem, top, primary_id)
        if mode == topology.ASYNC and not topology.lag_ok(
                top, chosen, last_lsn,
                self.conf.cluster_failover_max_lag_bytes):
            LOG.error("Cluster %s: the primary %s is gone (%s), but the best "
                      "standby %s is at %s, more than %s bytes behind where "
                      "the primary was (%s); an operator decides.",
                      cluster_id, primary_id, why, chosen,
                      top.answer(chosen).get('lsn'),
                      self.conf.cluster_failover_max_lag_bytes, last_lsn)
            return
        # A lease this task manager did not grant may have been granted
        # by the one before it, until this one first looked.
        lease_at = mem.get('lease_at')
        if lease_at is None:
            lease_at = self.first_seen.get(cluster_id)
        lease_left = (lease_at + self.lease_ttl - now
                      if lease_at is not None else 0)
        if lease_left > 0:
            LOG.info("Cluster %s: the primary %s is gone (%s); its lease "
                     "runs %s more seconds.", cluster_id, primary_id, why,
                     int(lease_left) + 1)
            return
        if not self._unchanged_after_jitter(
                context, top.members, top.views,
                lambda a: (a and a.get('state'),
                           a and (a.get('receiver') or {}).get('status'),
                           a and a.get('lsn'))):
            LOG.info("Cluster %s changed meanwhile; not failing over now.",
                     cluster_id)
            return
        if not self._take(cluster_id):
            return
        chosen_answer = top.answer(chosen)
        LOG.info("Cluster %s: failover: promote %s (%s, timeline %s, lsn "
                 "%s); the primary %s (%s) is gone: %s; %s.", cluster_id,
                 chosen, chosen_answer.get('ip'), timeline,
                 chosen_answer.get('lsn'), primary_id, host, why,
                 quorum_why)
        try:
            self._do_failover(context, cluster_id, top, primary_id, chosen,
                              mode, now)
        except Exception:
            LOG.exception("Cluster %s: the failover to %s failed; looking "
                          "anew.", cluster_id, chosen)
            self._release(cluster_id)

    def _do_failover(self, context, cluster_id, top, old_id, chosen, mode,
                     now):
        mem = self.memory[cluster_id]
        if old_id in top.reachable:
            LOG.info("Cluster %s: fencing the former primary %s.",
                     cluster_id, old_id)
            self._tell(context, old_id, 'fence_member')
        members = self.guest_factory(
            context, self._member(chosen)).get_cluster_context()['members']
        result = self.guest_factory(context, self._member(chosen)
                                    ).promote_member(members, mode) or {}
        host = top.ip(chosen)
        mem.update(primary_id=chosen, primary_ip=host,
                   lsn=result.get('lsn'), timeline=result.get('timeline'),
                   sync_set=None, lease_at=now)
        mem.pop('failing_since', None)
        LOG.info("Cluster %s: %s is the primary now (timeline %s).",
                 cluster_id, chosen, mem.get('timeline'))
        primary = {'id': chosen, 'host': host}
        for member_id in top.standbys:
            if member_id != chosen and not top.answer(member_id).get(
                    'rebuilding'):
                self._slot_then(context, chosen, member_id, 'follow_primary',
                                primary)
        for member_id in top.reachable:
            if member_id != old_id:
                self._tell(context, member_id, 'update_cluster_members',
                           members, mode, primary_id=chosen)
        if old_id in top.reachable:
            self._slot_then(context, chosen, old_id, 'rejoin_cluster',
                            primary)
        self._renew_lease(context, cluster_id, chosen)

    def _follow(self, context, db_cluster):
        """A cluster under recovery: the new primary keeps its lease
        meanwhile; the cluster is freed once it is settled, or once the
        recovery has had its time.
        """
        cluster_id = db_cluster.id
        now = time.monotonic()
        since = self.forming_since.setdefault(cluster_id, now)
        members = self._members(context, cluster_id)
        self._current_members = members
        views = self._views(context, members)
        top = topology.Topology(members, views)
        leader, ambiguous = top.leader()
        if leader is not None and not ambiguous:
            self._renew_lease(context, cluster_id, leader)
        if self._settled(views):
            LOG.info("Cluster %s is back on primary %s.", cluster_id, leader)
        elif now - since < self.conf.cluster_recovery_timeout:
            return
        else:
            LOG.error("Cluster %s was not back within %s seconds; looking "
                      "at it anew.", cluster_id,
                      self.conf.cluster_recovery_timeout)
        self._release(cluster_id, db_cluster)
