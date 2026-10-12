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

"""What the members' answers say about a PostgreSQL cluster, and what
follows from them: which member is the primary, whether the primary is
gone, which standby takes over and whether it safely can. Functions of
the answers alone, so that every rule can be tested on a table.

An answer is what ``get_recovery_view`` returns, None for a member that
did not answer. The rules follow CloudNativePG's, as far as a cluster
whose control plane and data plane are on different networks allows:
the task manager's view (the management network) and the standbys'
view (the tenant network) have to agree before the primary is given up.
"""

import collections

from trove.guestagent.common import cluster_probe

PRIMARY, STANDBY, REBUILDING, STOPPED = ('primary', 'standby',
                                         'rebuilding', 'stopped')
STREAMING = 'streaming'
ASYNC, SYNC, STRICT = 'async', 'sync', 'strict'


class Topology(object):
    """The members and their answers."""

    def __init__(self, members, views):
        self.members = list(members)
        self.ids = [member.id for member in self.members]
        self.views = list(views)
        self.answers = {member.id: answer for member, answer in views}
        self.n = len(self.members)

    @property
    def reachable(self):
        return [i for i in self.ids if self.answers.get(i)]

    def _with_state(self, state):
        return [i for i in self.reachable
                if self.answers[i].get('state') == state]

    @property
    def primaries(self):
        return self._with_state(PRIMARY)

    @property
    def standbys(self):
        return self._with_state(STANDBY)

    def answer(self, member_id):
        return self.answers.get(member_id) or {}

    def ip(self, member_id):
        return self.answer(member_id).get('ip')

    def timeline(self, member_id):
        return self.answer(member_id).get('timeline')

    @property
    def mode(self):
        for i in self.reachable:
            if self.answers[i].get('mode'):
                return self.answers[i]['mode']
        return SYNC

    def streaming_from(self, member_id, host):
        """Whether a standby streams from the given host now."""
        receiver = self.answer(member_id).get('receiver') or {}
        return (receiver.get('status') == STREAMING and
                (host is None or receiver.get('sender_host') == host))

    def leader(self):
        """The primary the cluster follows: the one member that answers
        as a primary, or the one on the highest timeline when a former
        primary is back and still takes itself for one. Two primaries
        on the same timeline: nobody is, and the cluster is left alone.
        Returns (id, ambiguous).
        """
        primaries = self.primaries
        if not primaries:
            return None, False
        top = max(self.timeline(i) or 0 for i in primaries)
        leaders = [i for i in primaries if (self.timeline(i) or 0) == top]
        if len(leaders) > 1:
            return None, True
        return leaders[0], False

    def expected_primary(self):
        """Whom the members take for the primary (most of them), and its
        address as the standbys stream from it; when no member says.
        """
        votes = collections.Counter(
            self.answers[i].get('primary_id') for i in self.reachable
            if self.answers[i].get('primary_id'))
        primary_id = votes.most_common(1)[0][0] if votes else None
        hosts = collections.Counter(
            (self.answers[i].get('receiver') or {}).get('sender_host')
            for i in self.standbys
            if (self.answers[i].get('receiver') or {}).get('sender_host'))
        return primary_id, hosts.most_common(1)[0][0] if hosts else None


def primary_dead(topology, primary_id, primary_host, grace):
    """Whether the primary is gone, by both views: every standby here
    has been without its WAL for the grace period, and the primary
    itself does not answer, or answers that it has had no standby for
    the grace period, or that its server is down. A standby that still
    streams from it says the primary lives, whatever the task manager
    sees. A standby that follows another primary (not yet pointed at
    this one after a failover) says nothing about it. Returns
    (dead, why).
    """
    standbys = [i for i in topology.standbys
                if not topology.answer(i).get('rebuilding')]
    streaming = [i for i in standbys
                 if topology.streaming_from(i, primary_host)]
    if streaming:
        return False, 'standby %s streams from it' % ', '.join(streaming)
    standbys = [i for i in standbys
                if topology.answer(i).get('primary_id') in (None, primary_id)]
    if not standbys:
        return False, 'no standby that follows it answers'
    waiting = [i for i in standbys
               if (topology.answer(i).get('disconnected_for') or 0) < grace]
    if waiting:
        return False, 'standby %s has not been without it for %s seconds' % (
            ', '.join(waiting), grace)
    answer = topology.answers.get(primary_id)
    if not answer:
        return True, ('it does not answer and every standby has been '
                      'without its WAL for %s seconds' % grace)
    if answer.get('state') == STOPPED:
        return True, 'its server is down'
    if answer.get('state') == PRIMARY:
        if (answer.get('no_standby_for') or 0) >= grace:
            return True, ('it answers but has had no standby for %s '
                          'seconds, and every standby has been without its '
                          'WAL as long' % grace)
        return False, 'it answers and reports a standby'
    return False, 'it answers as %s' % answer.get('state')


def candidates(topology, timeline):
    """The standbys that may take over: here, not being rebuilt, on the
    timeline, with a position.
    """
    return [i for i in topology.standbys
            if not topology.answer(i).get('rebuilding') and
            topology.timeline(i) == timeline and
            topology.answer(i).get('lsn') is not None]


def choose(topology, candidate_ids):
    """The candidate that holds the most: the highest received position,
    then the highest replayed, then the lowest address.
    """
    def key(member_id):
        answer = topology.answer(member_id)
        return (-answer['lsn'], -(answer.get('replay_lsn') or 0),
                cluster_probe.ip_key(answer.get('ip') or ''))
    return min(candidate_ids, key=key)


def quorum(topology, candidate_ids, mode, sync_set, primary_id=None,
           failing_for=0, force_after=0):
    """Whether promoting one of the candidates loses nothing the cluster
    acknowledged.

    A majority of the members has to be here among the standbys, so that
    a task manager cut off from most of the cluster does not act. In
    sync and strict mode, with N the standbys the primary last reported
    streaming (a commit waited for any one of them) and R those of them
    among the candidates, R + 1 > N: one candidate surely holds every
    acknowledged commit (CloudNativePG's quorum failover; with any one
    of N, that is every one of them). When N is not known, every member
    but the primary has to be a candidate; when it was empty, the
    primary committed without any standby, and nothing guarantees a
    standby holds those commits. ``force_after`` seconds of waiting,
    when set, go ahead regardless. Returns (ok, why).
    """
    standbys = [i for i in topology.standbys
                if not topology.answer(i).get('rebuilding')]
    needed = topology.n // 2 + 1
    if len(standbys) < needed:
        return False, '%s of %s members answer as standbys, %s needed' % (
            len(standbys), topology.n, needed)
    if mode == ASYNC:
        return True, 'async mode, a majority here'
    forced = force_after and failing_for >= force_after
    if sync_set is None:
        missing = [i for i in topology.ids
                   if i not in candidate_ids and i != primary_id and
                   topology.answer(i).get('state') != PRIMARY]
        if missing and not forced:
            return False, ('the synchronous standbys are not known and %s '
                           'cannot take over' % ', '.join(missing))
        return True, 'every standby can take over'
    if not sync_set:
        if not forced:
            return False, ('the primary last committed without any standby; '
                           'a standby may lack commits')
        return True, 'forced after %s seconds' % int(failing_for)
    holding = [i for i in candidate_ids if i in sync_set]
    ok = len(holding) + 1 > len(sync_set)
    if not ok and forced:
        return True, 'forced after %s seconds' % int(failing_for)
    return ok, '%s of the %s synchronous standbys can take over' % (
        len(holding), len(sync_set))


def lag_ok(topology, member_id, last_lsn, max_lag):
    """Whether an async candidate is close enough to where the primary
    last was; unknown positions and a zero limit pass.
    """
    lsn = topology.answer(member_id).get('lsn')
    if not max_lag or last_lsn is None or lsn is None:
        return True
    return lsn >= last_lsn - max_lag
