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
The standbys' view is twofold: whether they receive the primary's WAL,
and, while they do not, which members they reach on the tenant network
(``peers``): a primary that a majority of the cluster does not reach
there is cut off from the clients too, one that they reach is not.
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


def cut_off(topology, primary_id):
    """Whether the primary is out of reach on the tenant network, where
    the standbys and the clients are: a standby here that does not
    reach it reaches, with itself, a majority of the members, so the
    primary is on the smaller side of whatever partition there is, with
    nobody the cluster answers to. A standby says what it reaches
    (``peers``) while it does not stream. Returns (cut_off, side, why):
    cut_off None when no standby says, False when one reaches the
    primary or none reaches a majority without it (the standbys are
    cut off themselves); side the members that majority is made of.
    """
    needed = topology.n // 2 + 1
    witnesses, best = [], None
    for i in topology.standbys:
        answer = topology.answer(i)
        peers = answer.get('peers')
        if answer.get('rebuilding') or peers is None:
            continue
        witnesses.append(i)
        if peers.get(primary_id):
            return False, None, ('standby %s reaches it on the tenant '
                                 'network' % i)
        if peers.get(primary_id) is not False:
            continue
        side = [i] + sorted(j for j in topology.ids
                            if j not in (i, primary_id) and peers.get(j))
        if best is None or len(side) > len(best):
            best = side
    if not witnesses:
        return None, None, 'no standby says what it reaches'
    if best is None or len(best) < needed:
        return False, None, ('no standby reaches a majority of the cluster '
                             'without it (%s of %s): the standbys are cut '
                             'off themselves' % (
                                 len(best) if best else 0, topology.n))
    return True, best, ('standby %s reaches %s other member(s) but not '
                        'it: it is cut off on the tenant network' % (
                            best[0], len(best) - 1))


def alone_check(topology, primary_id, slack, known_alone=None):
    """What a sync-mode failover has to know of a primary that went on
    without any standby: it committed without waiting for one then,
    and a commit it acknowledged is lost if it is not on the standby
    promoted. Returns None when it did not, else (safe, why): safe
    when it could not have acknowledged a commit meanwhile, that is,
    it is cut off on the tenant network and it lost its standbys no
    earlier than they lost it (give or take the ``slack`` both take to
    notice), so it was cut off the whole time. A primary that does not
    answer cannot say; ``known_alone`` is what is kept of it then.
    """
    answer = topology.answer(primary_id)
    if answer.get('state') != PRIMARY:
        if not known_alone:
            return None
        return False, ('it does not answer how long it went on without a '
                       'standby')
    alone_for = answer.get('no_standby_for') or 0
    if not alone_for:
        return None
    cut, side, why = cut_off(topology, primary_id)
    if not cut:
        return False, why
    without = max((topology.answer(i).get('disconnected_for') or 0)
                  for i in side)
    if alone_for > without + slack:
        return False, ('it went on without a standby for %d seconds, '
                       'longer than the standbys have been without it '
                       '(%d): it may have acknowledged commits no standby '
                       'holds' % (alone_for, without))
    return True, ('it has been cut off on the tenant network since it '
                  'lost its standbys (%d seconds), so nothing was '
                  'acknowledged meanwhile' % alone_for)


def primary_dead(topology, primary_id, primary_host, grace):
    """Whether the primary is gone, by both views: every standby here
    has been without its WAL for the grace period, and the primary
    itself does not answer, or answers that it has had no standby for
    the grace period, or that its server is down; and, unless its
    server is down, the standbys do not reach it on the tenant network
    while a majority of the cluster is there (``cut_off``): standbys
    that are cut off themselves say nothing against it. A standby that
    still streams from it says the primary lives, whatever the task
    manager sees. A standby that follows another primary (not yet
    pointed at this one after a failover) says nothing about it.
    Returns (dead, why).
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
        why = ('it does not answer and every standby has been without its '
               'WAL for %s seconds' % grace)
    elif answer.get('state') == STOPPED:
        return True, 'its server is down'
    elif answer.get('state') != PRIMARY:
        return False, 'it answers as %s' % answer.get('state')
    elif (answer.get('no_standby_for') or 0) < grace:
        return False, 'it answers and reports a standby'
    else:
        why = ('it answers but has had no standby for %s seconds, and '
               'every standby has been without its WAL as long' % grace)
    # The tenant network, where the clients are, has to agree.
    cut, _side, net_why = cut_off(topology, primary_id)
    if cut is False:
        return False, '%s, but %s' % (why, net_why)
    return True, '%s; %s' % (why, net_why)


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
           failing_for=0, force_after=0, alone=None):
    """Whether promoting one of the candidates loses nothing the cluster
    acknowledged.

    A majority of the members has to be here among the standbys, so that
    a task manager cut off from most of the cluster does not act. In
    sync and strict mode, with N the standbys the primary last streamed
    to (a commit waited for any one of them) and R those of them among
    the candidates, R + 1 > N: one candidate surely holds every
    acknowledged commit (CloudNativePG's quorum failover; with any one
    of N, that is every one of them). When N is not known, every member
    but the primary has to be a candidate. In sync mode, a primary that
    went on without any standby since committed without waiting for
    one: ``alone`` (from ``alone_check``) says whether it could have
    acknowledged such a commit. ``force_after`` seconds of waiting, when
    set, go ahead regardless. Returns (ok, why).
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
    if not sync_set:
        missing = [i for i in topology.ids
                   if i not in candidate_ids and i != primary_id and
                   topology.answer(i).get('state') != PRIMARY]
        if missing and not forced:
            return False, ('the synchronous standbys are not known and %s '
                           'cannot take over' % ', '.join(missing))
        return True, 'every standby can take over'
    holding = [i for i in candidate_ids if i in sync_set]
    why = '%s of the %s synchronous standbys can take over' % (
        len(holding), len(sync_set))
    if len(holding) + 1 <= len(sync_set):
        if forced:
            return True, 'forced after %s seconds' % int(failing_for)
        return False, why
    if mode == SYNC and alone is not None:
        safe, alone_why = alone
        if not safe:
            if forced:
                return True, 'forced after %s seconds' % int(failing_for)
            return False, ('the primary went on without any standby, and '
                           '%s' % alone_why)
        why += ', and %s' % alone_why
    return True, why


def lag_ok(topology, member_id, last_lsn, max_lag):
    """Whether an async candidate is close enough to where the primary
    last was; unknown positions and a zero limit pass.
    """
    lsn = topology.answer(member_id).get('lsn')
    if not max_lag or last_lsn is None or lsn is None:
        return True
    return lsn >= last_lsn - max_lag
