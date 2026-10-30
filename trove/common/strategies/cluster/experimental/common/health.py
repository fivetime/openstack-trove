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

"""The task manager's record of the health of the clusters
(``trove.common.cluster_health``), for the API to show.

The health of a cluster whose datastore recovers it
(``ClusterRecovery``) is recorded from the views its look gets anyway.
A datastore whose clusters heal themselves has ``HealthWatch`` instead:
the recovery hub looks at its clusters as it does at the others (a
member's report, the scan of stale heartbeats and of clusters not looked
at yet), and the look asks every member what it sees of its cluster and
records it, doing nothing else. A cluster that is not healthy is looked
at again every ``cluster_health_interval`` seconds until it is.

A record is written only when it changes, in one statement (two task
managers looking at the same cluster write the same), into its own table:
a cluster saved from an older read does not write an older health back.
The task manager whose statement changed it sends the notification
``trove.cluster.health``, for the tenant's alarms (an Aodh event alarm
through Ceilometer) and the operator's.

The record also keeps who takes the writes (the members whose answer says
``writable``) and how many times a member started taking them: a primary
that fails over within one look leaves the health as it was, and the
count, its time and a notification still tell it. A look in the middle of
a failover, finding no writer, leaves the last writers as they were.
"""

from concurrent import futures
import datetime
import json
import time

from oslo_db import exception as db_exception
from oslo_log import log as logging
from oslo_utils import timeutils
import sqlalchemy.exc

from trove.cluster import models as cluster_models
from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import clients
from trove.common import cluster_health
from trove.common import context as trove_context
from trove.common import exception
from trove.common import timeutils as trove_timeutils
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove.instance import tasks as inst_tasks
from trove import rpc

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

EVENT_TYPE = 'trove.cluster.health'

# Seconds an answer may wait for the rest of a look before the member is
# asked again.
ANSWER_MAX_AGE = 5

# Times the health is worked out again when the record changed between
# reading and writing it (another task manager recorded the same cluster).
RECORD_ATTEMPTS = 5


# The tasks of a member that is up as it should be: none, or a
# configuration that waits for a restart (the member runs on, as it was).
# A member with another task is being built, resized, restarted...: its
# cluster is left alone until the task ends.
SETTLED_TASKS = (inst_tasks.InstanceTasks.NONE,
                 inst_tasks.InstanceTasks.RESTART_REQUIRED)


def _health_of(detail):
    """What the health is beside its status: the members out, why, and
    the names learnt (not who takes the writes).
    """
    return (sorted(detail.get('out') or []), detail.get('reason'),
            detail.get('members') or {})


def _detail(health, writers=None):
    detail = {'out': health.get('out') or [],
              'reason': health.get('reason'),
              'members': health.get('members') or {}}
    # Only what there is to tell: a datastore without writers to name, or
    # a guest agent that does not name them yet, leaves the record as it
    # was before writers were kept.
    if writers and (writers['writers'] or writers['last_writers'] or
                    writers['writer_changes']):
        detail.update(writers)
    return json.dumps(detail, sort_keys=True)


def _writers(before, health, now):
    """Who takes the writes, from the last record (``before``, its
    detail) and the look's ``writers``: the writers found, the last ones
    found (kept through a look that finds none, in the middle of a
    failover), how many times a member started taking the writes, and
    since when the last one did (the first writers found start it, not
    counted).
    """
    last = list(before.get('last_writers') or [])
    changes = before.get('writer_changes') or 0
    since = before.get('writer_since')
    found = list(health.get('writers') or [])
    if found:
        if not last:
            since = since or trove_timeutils.isotime(now)
        elif set(found) - set(last):
            changes += 1
            since = trove_timeutils.isotime(now)
        last = found
    return {'writers': found, 'last_writers': last,
            'writer_changes': changes, 'writer_since': since}


def _previous(cluster_id):
    """The last record: its status, its detail as stored and as read,
    and since; None, None, {}, None when there is none.
    """
    try:
        row = cluster_models.DBClusterHealth.find_by(id=cluster_id)
    except exception.NotFound:
        return None, None, {}, None
    try:
        detail = json.loads(row.detail or '{}')
    except ValueError:
        detail = {}
    return row.status, row.detail, detail, row.since


def known_members(cluster_id):
    """The names of the members as the datastore names them, to Trove's,
    as the last record has them.
    """
    try:
        row = cluster_models.DBClusterHealth.find_by(id=cluster_id)
        return json.loads(row.detail or '{}').get('members') or {}
    except (exception.NotFound, ValueError):
        return {}


def status_of(cluster_id):
    """The cluster's recorded health, or None."""
    try:
        return cluster_models.DBClusterHealth.find_by(id=cluster_id).status
    except exception.NotFound:
        return None


def notify(cluster_id, previous, health, since, writers=None,
           previous_writers=None):
    """Tell that the cluster's health changed: what it is, what it was
    (None for the first record), the members out and why; and who takes
    the writes, who did and how many times a member started to (a writer
    that changed with the health as it was is told too).
    """
    try:
        db_cluster = cluster_models.DBCluster.find_by(id=cluster_id)
        payload = {
            'cluster_id': cluster_id,
            'cluster_name': db_cluster.name,
            'tenant_id': db_cluster.tenant_id,
            'datastore_version_id': db_cluster.datastore_version_id,
            'status': health['status'],
            'previous_status': previous,
            'members_out': list(health.get('out') or []),
            'reason': health.get('reason'),
            'since': trove_timeutils.isotime(since),
            'region': CONF.region,
            'writers': list((writers or {}).get('writers') or []),
            'previous_writers': list(previous_writers or []),
            'writer_changes': (writers or {}).get('writer_changes') or 0,
        }
        rpc.get_notifier(service='taskmanager').info(
            trove_context.TroveContext(), EVENT_TYPE, payload)
    except Exception:
        LOG.exception("The change of the health of cluster %s was not "
                      "told.", cluster_id)


def _create(cluster_id, status, detail, now):
    """The first record of the cluster, inserted; False when there is one
    already (another task manager's): ``create()`` merges, and would
    write over it.
    """
    model = cluster_models.DBClusterHealth
    try:
        with model.query() as query:
            query.session.add(model(id=cluster_id, status=status,
                                    detail=detail, since=now, created=now,
                                    updated=now))
    except (db_exception.DBDuplicateEntry, sqlalchemy.exc.IntegrityError):
        return False
    return True


def record(cluster_id, health):
    """Record the cluster's health when it changed, and tell it. Returns
    whether it did.

    What is written is worked out from the last record (the writers and
    their count go on from it), so it is written only over the record it
    was worked out from: a task manager that looked at the same cluster
    at the same time and recorded first is not undone, the record is read
    again and the health worked out anew.
    """
    model = cluster_models.DBClusterHealth
    for _attempt in range(RECORD_ATTEMPTS):
        now = timeutils.utcnow()
        previous, stored, before, since = _previous(cluster_id)
        writers = _writers(before, health, now)
        detail = _detail(health, writers)
        if previous is None:
            if not _create(cluster_id, health['status'], detail, now):
                # Another task manager recorded it first.
                continue
            since = now
            break
        if previous == health['status'] and stored == detail:
            return False
        # Since when the health is what it is: a writer that changed alone
        # leaves it.
        same = (previous == health['status'] and
                _health_of(before) == _health_of(health))
        values = {'status': health['status'], 'detail': detail,
                  'updated': now}
        if not same:
            values['since'] = since = now
        unchanged = (model.detail.is_(None) if stored is None
                     else model.detail == stored)
        with model.query() as query:
            written = query.filter(model.id == cluster_id).filter(
                model.status == previous).filter(unchanged).update(
                    values, synchronize_session=False)
        if written:
            break
        # Recorded by another task manager since it was read: again.
    else:
        LOG.warning("Cluster %s: its health was not recorded, the record "
                    "changed under it %d times; the next look records it.",
                    cluster_id, RECORD_ATTEMPTS)
        return False
    moved = writers['writer_changes'] != (before.get('writer_changes') or 0)
    LOG.info("Cluster %(c)s is %(s)s%(r)s%(w)s.",
             {'c': cluster_id, 's': health['status'],
              'r': ': %s' % health['reason'] if health.get('reason')
              else '',
              'w': '; the writes moved to %s (%d times)' % (
                  ', '.join(writers['writers']), writers['writer_changes'])
              if moved else ''})
    notify(cluster_id, previous, health, since, writers,
           before.get('last_writers'))
    return True


def stale_members(member_ids):
    """Of the given members, those whose heartbeat is older than
    ``agent_heartbeat_expiry`` (or missing).
    """
    member_ids = list(member_ids)
    if not member_ids:
        return set()
    stale_before = timeutils.utcnow() - datetime.timedelta(
        seconds=CONF.agent_heartbeat_expiry)
    fresh = set()
    with InstanceServiceStatus.query() as query:
        for status in query.filter(
                InstanceServiceStatus.instance_id.in_(member_ids)).all():
            if status.updated_at and status.updated_at >= stale_before:
                fresh.add(status.instance_id)
    return set(member_ids) - fresh


def heartbeats_since(member_ids, since):
    """Of the given members, those whose heartbeat is from ``since`` or
    later.
    """
    member_ids = list(member_ids)
    if not member_ids:
        return set()
    with InstanceServiceStatus.query() as query:
        return {status.instance_id for status in query.filter(
            InstanceServiceStatus.instance_id.in_(member_ids)).all()
            if status.updated_at and status.updated_at >= since}


def seconds_to_expiry(member_ids):
    """Seconds until the oldest heartbeat of the given members is older
    than ``agent_heartbeat_expiry`` (at least 1), or None when none of
    them has a heartbeat recorded.
    """
    member_ids = list(member_ids)
    if not member_ids:
        return None
    with InstanceServiceStatus.query() as query:
        beats = [status.updated_at for status in query.filter(
            InstanceServiceStatus.instance_id.in_(member_ids)).all()
            if status.updated_at]
    if not beats:
        return None
    expires = min(beats) + datetime.timedelta(
        seconds=CONF.agent_heartbeat_expiry)
    return max(1, int((expires - timeutils.utcnow()).total_seconds()) + 1)


class HealthWatch(object):
    """The look at a cluster of a datastore whose clusters heal
    themselves: ask, record, nothing else.
    """

    def __init__(self, manager, guest_factory=None):
        self.manager = manager
        self.conf = CONF.get(manager)
        self.guest_factory = guest_factory or self._guest
        # The clusters last found not healthy: looked at again.
        self.unhealthy = set()

    def _guest(self, context, member):
        return clients.create_guest_client(context, member.id, self.manager)

    def check_now(self, context, db_cluster, reason=None):
        LOG.info("Cluster %s: %s; looking at its health.", db_cluster.id,
                 reason or 'asked for')
        cluster_id = db_cluster.id
        if db_cluster.task_status != ClusterTasks.NONE:
            # Being built, grown, shrunk: its task ends with a look.
            self.unhealthy.discard(cluster_id)
            return
        members = DBInstance.find_all(cluster_id=cluster_id,
                                      deleted=False).all()
        if not members or any(
                m.task_status not in SETTLED_TASKS for m in members):
            self.unhealthy.discard(cluster_id)
            return
        stale = stale_members(m.id for m in members)
        asked = [m for m in members if m.id not in stale]
        silent = [m.name for m in members if m.id in stale]

        def interim(told, slow):
            # A member keeps the look waiting (powered off, its heartbeat
            # not stale yet): the others are asked again now, and what
            # they say is recorded with it not answering, so that a
            # replica promoted meanwhile is known as the writer within
            # seconds, not once the slow member's call times out.
            fresh = self._ask(context, [t[0] for t in told])
            record(cluster_id, self._combine(
                cluster_id, fresh, silent + [m.name for m in slow]))

        started = timeutils.utcnow()
        told = self._ask(context, asked, interim)
        # The look waited for a member slow to answer or that did not (one
        # just restarted, or powered off, its heartbeat not stale yet):
        # what the others said is that old, a Redis replica the sentinels
        # promoted meanwhile taking the writes already. Asked again, they
        # tell how the cluster is now.
        done = time.monotonic()
        again = {m.id for m, _a, failed, at in told
                 if not failed and done - at > ANSWER_MAX_AGE}
        # A member that did not answer but whose heartbeat is from after
        # the call came back while the look waited for it (its host reset
        # and the member evacuated, production Kata 2026-10-30: the call
        # to the old guest timed out as the new one answered the other
        # task manager's look). Asked again, it answers now; recorded as
        # not answering, it would undo the record made from its answer.
        again |= heartbeats_since(
            (m.id for m, _a, failed, _at in told if failed), started)
        if again:
            told = [t for t in told if t[0].id not in again] + self._ask(
                context, [m for m in asked if m.id in again])
        health = self._combine(cluster_id, told, silent)
        record(cluster_id, health)
        if health['status'] == cluster_health.HEALTHY:
            self.unhealthy.discard(cluster_id)
        else:
            self.unhealthy.add(cluster_id)

    def _combine(self, cluster_id, told, silent):
        """The health from the members' answers, those in ``silent`` and
        those that failed to answer counted out.
        """
        silent = list(silent)
        answers = []
        for member, answer, failed, _at in told:
            if failed or (answer and answer.get('down')):
                silent.append(member.name)
            elif answer:
                answers.append((member.name, answer))
            else:
                # A member that watches nothing (a query router) or runs
                # a guest agent from before the watch: up, as far as its
                # heartbeat tells.
                answers.append((member.name,
                                cluster_health.view(cluster_health.HEALTHY)))
        return cluster_health.combine(answers, silent,
                                      known_members(cluster_id))

    def _ask(self, context, members, interim=None):
        """Every member's answer, in the members' order. While some keep
        the others waiting for more than ANSWER_MAX_AGE, ``interim`` is
        called with the answers so far and the members still asked, once
        every ANSWER_MAX_AGE.
        """
        def ask(member):
            try:
                answer = self.guest_factory(
                    context, member).get_cluster_health(
                        timeout=self.conf.cluster_health_timeout)
                return member, answer, False, time.monotonic()
            except exception.GuestError as err:
                # It answered with an error: a guest agent without the
                # call.
                LOG.debug("Member %s cannot tell: %s", member.id, err)
                return member, None, False, time.monotonic()
            except Exception as err:
                LOG.debug("Member %s did not answer: %s", member.id, err)
                return member, None, True, time.monotonic()
        if len(members) < 2:
            return [ask(member) for member in members]
        with futures.ThreadPoolExecutor(max_workers=len(members)) as pool:
            pending = {pool.submit(ask, member): member
                       for member in members}
            told = {}
            while pending:
                done, _rest = futures.wait(
                    pending, timeout=ANSWER_MAX_AGE if told else None,
                    return_when=futures.FIRST_COMPLETED)
                if not done:
                    if interim:
                        interim([told[m.id] for m in members
                                 if m.id in told],
                                list(pending.values()))
                    continue
                for future in done:
                    member = pending.pop(future)
                    told[member.id] = future.result()
            return [told[member.id] for member in members]

    def still_watching(self, cluster_id):
        return cluster_id in self.unhealthy

    def look_again_in(self, cluster_id):
        return self.conf.cluster_health_interval


def from_views(cluster_id, members, views, member_out, serving,
               down_reason=None):
    """The health of a cluster its recovery looked at: a member that did
    not answer or whose heartbeat is stale is out, and so is one its
    datastore says is out of the cluster (``member_out(answer)``); the
    cluster is down when no member answers or ``serving(answers)`` says
    none serves, why in ``down_reason(heard)`` (the members that answered
    with a fresh heartbeat and their answers) when given.
    """
    stale = stale_members(m.id for m in members)
    answers, silent, heard = [], [], []
    for member, answer in views:
        if answer is None or member.id in stale:
            silent.append(member.name)
            continue
        heard.append((member, answer))
        if member_out(answer):
            answers.append((member.name, cluster_health.view(
                cluster_health.DEGRADED, [member.name],
                'out of the cluster',
                writable=bool(answer.get('writable')))))
        else:
            answers.append((member.name, cluster_health.view(
                cluster_health.HEALTHY,
                writable=bool(answer.get('writable')))))
    health = cluster_health.combine(answers, silent)
    given = [answer for _m, answer in views if answer]
    if answers and not serving(given):
        health['status'] = cluster_health.DOWN
        health['reason'] = '; '.join(
            r for r in (down_reason(heard) if down_reason
                        else 'no member serves', health.get('reason')) if r)
    record(cluster_id, health)
    return health
