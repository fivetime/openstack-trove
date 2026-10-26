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

"""The health of a cluster, as its members see it and the task manager
records it.

A member tells what it sees of its cluster from its own database, which
knows the other members through the datastore's own gossip or heartbeats
(a guest agent cannot reach the other members with network isolation):

    {'status': HEALTHY | DEGRADED | DOWN,
     'out': [the members it sees out, as the datastore names them],
     'reason': why, for people,
     'me': this member, as the datastore names it}

The task manager asks every member, adds the members that do not answer
or whose heartbeat is stale, and records the cluster's health
(``combine``):

* HEALTHY: every member answers and none sees a member out;
* DEGRADED: the cluster serves, short of members (one is out, a member
  sees one out, or a part of the cluster says it is degraded);
* DOWN: no member answers, or one says the cluster, or a part of it, no
  longer serves;
* UNKNOWN: not looked at yet (shown for a cluster without a record).
"""

HEALTHY = 'HEALTHY'
DEGRADED = 'DEGRADED'
DOWN = 'DOWN'
UNKNOWN = 'UNKNOWN'
_RANK = {HEALTHY: 0, DEGRADED: 1, DOWN: 2}


def view(status, out=(), reason=None, me=None, writable=False):
    """What a member tells of its cluster; ``writable`` when the member
    itself takes the cluster's writes now.
    """
    result = {'status': status, 'out': sorted(set(out)), 'reason': reason,
              'me': me}
    if writable:
        result['writable'] = True
    return result


def combine(answers, silent, known=None):
    """The cluster's health from what its members told.

    :param answers: (Trove's name of a member, its view) of every member
                    that answered
    :param silent: Trove's names of the members that did not answer or
                   whose heartbeat is stale
    :param known: the members' names as the datastore names them, to
                  Trove's (what an earlier look learnt, for the members
                  silent now)
    :returns: a view of the cluster, ``out`` naming every member out as
              Trove does where it can, and ``members`` the names learnt
    """
    known = dict(known or {})
    for name, answer in answers:
        if answer.get('me'):
            known[answer['me']] = name
    silent = sorted(set(silent))
    if not answers:
        result = view(DOWN, silent, 'no member answers')
        result['members'] = known
        return result
    worst = max((a.get('status') for _n, a in answers),
                key=lambda s: _RANK.get(s, 0))
    if worst not in _RANK:
        worst = HEALTHY
    # A cluster one of whose members takes its writes is not down,
    # whatever another member sees: the sentinels of a Redis primary that
    # has just failed over hold the old primary down until the replicas
    # follow the promoted one, while that one takes the writes already.
    if worst == DOWN and any(a.get('writable') for _n, a in answers):
        worst = DEGRADED
    out = set(silent)
    # The same words from several members are said once, with who said
    # them.
    said = {}
    for name, answer in answers:
        out.update(known.get(x, x) for x in answer.get('out') or ())
        if answer.get('status') not in (None, HEALTHY) and answer.get(
                'reason'):
            said.setdefault(answer['reason'], []).append(name)
    reasons = ['%s (%s)' % (reason, ', '.join(sorted(names)))
               for reason, names in sorted(said.items())]
    if silent:
        reasons.insert(0, 'not answering: %s' % ', '.join(silent))
    status = worst
    if status == HEALTHY and out:
        status = DEGRADED
    result = view(status, out, '; '.join(reasons) or None)
    result['members'] = known
    return result
