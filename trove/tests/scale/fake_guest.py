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

"""Fake PostgreSQL cluster members, by the thousand, for the scale test
of the task manager's cluster recovery. Not part of the guest image.

Each fake member answers, on the real RabbitMQ, every call the task
manager makes to a PostgreSQL cluster member (``get_recovery_view``,
``renew_lease``, ``fence_member``, ``promote_member``...), heartbeats
through the conductor like a guest agent, and reports its cluster state
through the probe's ``ClusterEventReporter``, as a guest does. A fake
cluster has one primary and two standbys; a member can be killed (it
stops answering and heartbeating, its standbys lose its WAL) and
restored.

Usage::

    trove-fake-guests --config-file trove.conf --from 0 --clusters 500
                      --port 9800 [--seed]

``--seed`` writes the clusters, instances and service statuses to the
Trove database first. The control endpoint::

    curl -XPOST localhost:9800/kill?cluster=3        # the primary of cluster 3
    curl -XPOST localhost:9800/restore?cluster=3
    curl -XPOST localhost:9800/storm?n=50            # 50 primaries at once
    curl localhost:9800/stats

The cluster ids are ``fake-cluster-<n>`` and the member ids
``fake-cluster-<n>-m<1..3>``, so that several processes may each run a
range of clusters.
"""

import argparse
import collections
import datetime
import json
import sys
import threading
import time

import eventlet
eventlet.monkey_patch(all=True)  # noqa: E402

from http import server as http_server  # noqa: E402
from urllib import parse as urlparse  # noqa: E402

import oslo_messaging as messaging  # noqa: E402
from oslo_utils import timeutils  # noqa: E402

from trove.common import cfg  # noqa: E402
from trove.common import context as trove_context  # noqa: E402
from trove.conductor import api as conductor_api  # noqa: E402
from trove.guestagent import api as guest_api  # noqa: E402
from trove.guestagent.common import cluster_probe  # noqa: E402
from trove import rpc  # noqa: E402

CONF = cfg.CONF
WAL_TIMEOUT = 10
FENCE_GRACE = 15
TICK = 3


def cluster_id_of(n):
    return 'fake-cluster-%d' % n


def member_id_of(n, i):
    return '%s-m%d' % (cluster_id_of(n), i)


def ip_of(n, i):
    return '10.%d.%d.%d' % (100 + n // 65536, (n // 256) % 256,
                            (n % 256) * 0 + i)


class FakeCluster(object):
    """One cluster of three members, and what each knows."""

    def __init__(self, n, mode='sync'):
        self.n = n
        self.id = cluster_id_of(n)
        self.mode = mode
        self.members = {member_id_of(n, i): ip_of(n, i) for i in (1, 2, 3)}
        self.primary_id = member_id_of(n, 1)
        self.timeline = 1
        self.lsn = 1000
        self.fake = {i: FakeMember(self, i) for i in self.members}

    def primary(self):
        return self.fake.get(self.primary_id)

    def standbys(self):
        return [m for m in self.fake.values()
                if m.id != self.primary_id and not m.dead]


class FakeMember(object):

    def __init__(self, cluster, member_id):
        self.cluster = cluster
        self.id = member_id
        self.ip = cluster.members[member_id]
        self.dead = False            # killed: answers nothing
        self.fenced = False
        self.rebuilding = False
        self.stopped = False
        self.lease_until = 0
        self.lease_ttl = 0
        self.no_standby_since = None
        self.detached_since = None
        self.reporter = None
        self.calls = collections.Counter()

    # What the member is

    @property
    def is_primary(self):
        return self.cluster.primary_id == self.id and not self.stopped

    def streaming(self):
        """A standby streams while the primary lives and is not
        fenced, and it is not stopped.
        """
        primary = self.cluster.primary()
        return (not self.is_primary and not self.stopped and
                not self.rebuilding and primary is not None and
                not primary.dead and not primary.fenced and
                self.cluster.primary_id != self.id)

    def streaming_standbys(self):
        return [m for m in self.cluster.standbys() if m.streaming()]

    def tick(self, now):
        """Every few seconds: notice the standbys gone (the primary) or
        the primary gone (a standby), with the WAL timeout, and report.
        """
        if self.dead:
            return
        if self.is_primary:
            if self.streaming_standbys():
                self.no_standby_since = None
            elif self.no_standby_since is None:
                self.no_standby_since = now + WAL_TIMEOUT
        else:
            if self.streaming():
                self.detached_since = None
            elif self.detached_since is None:
                self.detached_since = now + WAL_TIMEOUT
        if self.reporter is not None:
            self.reporter.observe(self.signal(now), None)

    def writable(self, now):
        if self.fenced or self.stopped or not self.is_primary:
            return False
        if self.streaming_standbys() or now < self.lease_until:
            self.confirmed_at = now
            return True
        return (getattr(self, 'confirmed_at', None) is not None and
                now - self.confirmed_at < FENCE_GRACE)

    def state(self):
        if self.rebuilding:
            return 'rebuilding'
        if self.stopped:
            return 'stopped'
        return 'primary' if self.is_primary else 'standby'

    def recovery_view(self, now):
        state = self.state()
        answer = {'ip': self.ip, 'state': state,
                  'role': 'PRIMARY' if state == 'primary' else (
                      'STREAMING' if self.streaming() else 'DETACHED'),
                  'mode': self.cluster.mode, 'self_id': self.id,
                  'primary_id': self.cluster.primary_id,
                  'sync_set': getattr(self, 'sync_set', None),
                  'writable': self.writable(now), 'fenced': self.fenced,
                  'lease_fresh': now < self.lease_until}
        if state == 'primary':
            answer['standbys'] = {
                m.id: {'state': 'streaming', 'sync_state': 'quorum',
                       'lag_bytes': 0} for m in self.streaming_standbys()}
            answer['lsn'] = self.cluster.lsn
            answer['timeline'] = self.cluster.timeline
            answer['no_standby_for'] = (
                now - self.no_standby_since
                if self.no_standby_since is not None and
                now > self.no_standby_since else None)
            if self.no_standby_since is not None:
                answer['peers'] = self.peers()
        elif state == 'standby':
            streaming = self.streaming()
            primary = self.cluster.primary()
            answer['receiver'] = {
                'status': 'streaming' if streaming else None,
                'sender_host': primary.ip if primary else None,
                'last_msg_age': 1.0, 'replay_lag_bytes': 0,
            } if streaming else None
            answer['lsn'] = self.cluster.lsn
            answer['replay_lsn'] = self.cluster.lsn
            answer['timeline'] = self.cluster.timeline
            answer['disconnected_for'] = (
                now - self.detached_since
                if self.detached_since is not None and
                now > self.detached_since else None)
            if not streaming:
                answer['peers'] = self.peers()
        return answer

    def peers(self):
        return {m.id: not m.dead for m in self.cluster.fake.values()
                if m.id != self.id}

    def signal(self, now):
        S = cluster_probe.ClusterSignal
        if self.rebuilding:
            return S(True, cluster_probe.ABNORMAL, 'rebuilding')
        if self.stopped:
            return S(True, cluster_probe.ABNORMAL,
                     'the server does not answer')
        if self.fenced:
            return S(True, cluster_probe.ABNORMAL, 'fenced')
        if not self.is_primary:
            # The receiver reports streaming until the WAL timeout.
            if (self.streaming() or self.detached_since is None or
                    now < self.detached_since):
                return cluster_probe.NORMAL
            away = now - self.detached_since
            return S(True, cluster_probe.ABNORMAL,
                     'not streaming for %d seconds' % away,
                     extra={'disconnected_for': away})
        if self.no_standby_since is None or now < self.no_standby_since:
            return cluster_probe.NORMAL
        alone_for = now - self.no_standby_since
        left = max(self.lease_until - now, 0)
        resend = FENCE_GRACE / 3.0
        if self.lease_ttl:
            resend = max(left - self.lease_ttl / 3.0, FENCE_GRACE / 3.0)
        return S(True, cluster_probe.LEASE_REQUEST,
                 'no standby streams for %d seconds; the lease runs %d '
                 'more seconds' % (alone_for, left),
                 extra={'no_standby_for': alone_for, 'lease_left': left},
                 resend_after=resend)


class FakeEndpoint(object):
    """The RPC endpoint of one fake member: what the task manager calls.
    A dead member sleeps past the caller's timeout.
    """

    target = messaging.Target(version=guest_api.API.API_LATEST_VERSION)

    def __init__(self, member, stats):
        self.member = member
        self.stats = stats

    def _alive(self, what):
        self.stats[what] += 1
        self.member.calls[what] += 1
        if self.member.dead:
            self.stats['calls to dead members'] += 1
            time.sleep(30)
            return False
        return True

    def rpc_ping(self, context):
        return self._alive('rpc_ping')

    def get_recovery_view(self, context):
        if not self._alive('get_recovery_view'):
            return None
        return self.member.recovery_view(time.monotonic())

    def get_member_role(self, context):
        if not self._alive('get_member_role'):
            return None
        view = self.member.recovery_view(time.monotonic())
        return {k: view.get(k) for k in ('state', 'role', 'writable',
                                         'mode', 'standbys')}

    def is_writable_member(self, context):
        self._alive('is_writable_member')
        return self.member.writable(time.monotonic())

    def get_cluster_context(self, context):
        self._alive('get_cluster_context')
        c = self.member.cluster
        return {'replication_user': {'name': 'replicator', 'password': 'x'},
                'mode': c.mode, 'members': c.members,
                'primary_id': c.primary_id, 'self_id': self.member.id}

    def cluster_complete(self, context):
        self._alive('cluster_complete')

    def renew_lease(self, context, ttl):
        if self._alive('renew_lease'):
            now = time.monotonic()
            self.member.lease_until = now + ttl
            self.member.lease_ttl = ttl
            self.member.confirmed_at = now

    def remember_sync_set(self, context, primary_id, members, lsn=None,
                          alone=False):
        if self._alive('remember_sync_set'):
            self.member.sync_set = {'primary_id': primary_id,
                                    'members': members, 'lsn': lsn,
                                    'alone': alone}

    def update_cluster_members(self, context, members, mode,
                               primary_id=None):
        if self._alive('update_cluster_members') and primary_id:
            self.member.cluster.primary_id = primary_id

    def create_member_slot(self, context, member_id):
        self._alive('create_member_slot')

    def drop_member_slot(self, context, member_id):
        self._alive('drop_member_slot')

    def fence_member(self, context):
        if self._alive('fence_member'):
            self.member.fenced = True
            self.member.stopped = True
            self.member.lease_until = 0

    def promote_member(self, context, members, mode):
        if not self._alive('promote_member'):
            return None
        c = self.member.cluster
        c.primary_id = self.member.id
        c.timeline += 1
        c.lsn += 100
        self.member.fenced = self.member.stopped = False
        self.member.detached_since = None
        self.member.no_standby_since = None
        self.member.confirmed_at = time.monotonic()
        self.stats['promotions'] += 1
        return {'lsn': c.lsn, 'timeline': c.timeline}

    def follow_primary(self, context, primary):
        if self._alive('follow_primary'):
            self.member.cluster.primary_id = primary['id']
            self.member.detached_since = None

    def rejoin_cluster(self, context, primary):
        if self._alive('rejoin_cluster'):
            self.member.cluster.primary_id = primary['id']
            self.member.fenced = self.member.stopped = False
            self.member.rebuilding = False
            self.member.detached_since = None
            self.stats['rejoins'] += 1


class FakeGuests(object):
    """The fake members of a range of clusters, in this process."""

    def __init__(self, first, count, mode):
        self.clusters = {}
        self.servers = []
        self.stats = collections.Counter()
        self.events = collections.Counter()
        self.started = time.monotonic()
        for n in range(first, first + count):
            self.clusters[n] = FakeCluster(n, mode)

    def _event(self, member):
        def send(kind, signal, view, since):
            self.events[kind] += 1
            context = trove_context.TroveContext()
            conductor_api.API(context).cluster_event(
                member.id, kind, sent=timeutils.utcnow_ts(microsecond=True),
                payload={'kind': kind, 'reason': signal.reason,
                         'since': since, 'datastore_manager': 'postgresql',
                         'member': {'state': member.state()},
                         'extra': signal.extra or {},
                         'view': member.recovery_view(time.monotonic())})
        return send

    def start(self):
        for cluster in self.clusters.values():
            for member in cluster.fake.values():
                member.reporter = cluster_probe.ClusterEventReporter(
                    CONF.postgresql.cluster_event_interval,
                    CONF.postgresql.cluster_event_min_interval,
                    send=self._event(member))
                target = messaging.Target(
                    topic='guestagent.%s' % member.id, server=member.id,
                    version=guest_api.API.API_LATEST_VERSION)
                # No encryption key: the seeded instances have none.
                server = rpc.get_server(
                    target, [FakeEndpoint(member, self.stats)], key=None)
                server.start()
                self.servers.append(server)
        eventlet.spawn_n(self._ticks)
        eventlet.spawn_n(self._heartbeats)

    def _ticks(self):
        while True:
            now = time.monotonic()
            for cluster in self.clusters.values():
                for member in cluster.fake.values():
                    try:
                        member.tick(now)
                    except Exception as err:
                        self.stats['tick errors'] += 1
                        print('tick %s: %s' % (member.id, err),
                              file=sys.stderr)
                eventlet.sleep(0)
            eventlet.sleep(TICK)

    def _heartbeats(self):
        interval = CONF.report_interval
        while True:
            for cluster in self.clusters.values():
                for member in cluster.fake.values():
                    if member.dead:
                        continue
                    try:
                        context = trove_context.TroveContext()
                        conductor_api.API(context).heartbeat(
                            member.id, {'service_status': 'healthy'},
                            sent=timeutils.utcnow_ts(microsecond=True))
                        self.stats['heartbeats'] += 1
                    except Exception as err:
                        self.stats['heartbeat errors'] += 1
                        print('heartbeat %s: %s' % (member.id, err),
                              file=sys.stderr)
                    eventlet.sleep(0)
            eventlet.sleep(interval)

    # Control

    def kill(self, n):
        cluster = self.clusters[n]
        primary = cluster.primary()
        if primary is not None:
            primary.dead = True
            self.stats['killed'] += 1
            return primary.id

    def restore(self, n):
        cluster = self.clusters[n]
        for member in cluster.fake.values():
            if member.dead:
                member.dead = False
                member.stopped = member.id != cluster.primary_id and \
                    member.cluster.timeline > 1
                self.stats['restored'] += 1

    def storm(self, count):
        killed = []
        for n in sorted(self.clusters):
            if len(killed) >= count:
                break
            member = self.kill(n)
            if member:
                killed.append(member)
        return killed

    def snapshot(self):
        primaries = collections.Counter(
            c.primary_id[-2:] for c in self.clusters.values())
        return {'clusters': len(self.clusters),
                'uptime': int(time.monotonic() - self.started),
                'calls': dict(self.stats), 'events': dict(self.events),
                'primaries': dict(primaries),
                'timelines': dict(collections.Counter(
                    c.timeline for c in self.clusters.values())),
                'dead': sum(1 for c in self.clusters.values()
                            for m in c.fake.values() if m.dead)}


def seed(first, count):
    """The clusters, instances and service statuses in the database,
    as the task manager and the conductor expect them.
    """
    from trove.cluster.models import DBCluster
    from trove.cluster.tasks import ClusterTasks
    from trove.datastore import models as datastore_models
    from trove.instance.models import DBInstance
    from trove.instance.models import InstanceServiceStatus
    from trove.instance.service_status import ServiceStatuses
    from trove.instance.tasks import InstanceTasks
    version = datastore_models.DatastoreVersion.load(
        datastore_models.Datastore.load('postgresql'), '17')
    made = 0
    for n in range(first, first + count):
        cluster_id = cluster_id_of(n)
        try:
            DBCluster.find_by(id=cluster_id)
            continue
        except Exception:
            pass
        DBCluster(ClusterTasks.NONE, id=cluster_id, name=cluster_id,
                  tenant_id='fake', datastore_version_id=version.id,
                  created=timeutils.utcnow()).save()
        for i in (1, 2, 3):
            member_id = member_id_of(n, i)
            DBInstance(InstanceTasks.NONE, id=member_id, name=member_id,
                       tenant_id='fake', cluster_id=cluster_id,
                       datastore_version_id=version.id, type='member',
                       deleted=False, created=timeutils.utcnow()).save()
            InstanceServiceStatus(ServiceStatuses.HEALTHY, id=member_id,
                                  instance_id=member_id).save()
        made += 1
    print('seeded %d clusters (%d already there)' % (made, count - made))


class Control(http_server.BaseHTTPRequestHandler):
    guests = None

    def log_message(self, *args):
        pass

    def _reply(self, body, code=200):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._reply(self.guests.snapshot())

    def do_POST(self):
        url = urlparse.urlparse(self.path)
        query = dict(urlparse.parse_qsl(url.query))
        try:
            if url.path == '/kill':
                killed = self.guests.kill(int(query['cluster']))
                self._reply({'killed': killed})
            elif url.path == '/restore':
                self.guests.restore(int(query['cluster']))
                self._reply({'restored': int(query['cluster'])})
            elif url.path == '/storm':
                self._reply({'killed': self.guests.storm(int(query['n'])),
                             'at': datetime.datetime.utcnow().isoformat()})
            else:
                self._reply({'error': 'unknown'}, 404)
        except Exception as err:
            self._reply({'error': str(err)}, 400)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from', dest='first', type=int, default=0)
    parser.add_argument('--clusters', type=int, default=100)
    parser.add_argument('--mode', default='sync')
    parser.add_argument('--port', type=int, default=9800)
    parser.add_argument('--seed', action='store_true')
    parser.add_argument('--seed-only', action='store_true',
                        help='write the rows and exit (the scan at scale)')
    args, rest = parser.parse_known_args()
    CONF(rest, project='trove')
    rpc.init(CONF)
    if args.seed or args.seed_only:
        from trove.db import get_db_api
        get_db_api().configure_db()
        seed(args.first, args.clusters)
        if args.seed_only:
            return
    guests = FakeGuests(args.first, args.clusters, args.mode)
    guests.start()
    Control.guests = guests
    httpd = http_server.HTTPServer(('0.0.0.0', args.port), Control)
    print('fake guests: clusters %d..%d, control on :%d' % (
        args.first, args.first + args.clusters - 1, args.port))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    while True:
        eventlet.sleep(60)
        print(json.dumps(guests.snapshot()))


if __name__ == '__main__':
    main()
