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

"""Fake Galera (MariaDB) cluster members, by the thousand, for the scale
test of the task manager's Galera recovery. Not part of the guest image.

A fake member answers what the task manager sends a Galera member during
recovery (``get_recovery_view``, a call, and ``bootstrap_cluster``, a
cast), heartbeats through the conductor and reports its cluster state
through the probe's ``ClusterEventReporter`` with the signal of the real
guest: abnormal "waiting for a primary component" while it runs outside a
primary component, normal in it. Each guest is a server of the shared
``guestagent`` topic, as the production guests are.

The model of one cluster of three: the members in the primary component
commit (their seqno grows together); a member that dies keeps the seqno it
had. When a whole cluster loses power its members stop a few transactions
apart. Back up, they wait for a primary component; their position (the
state uuid and seqno) becomes known after a short recovery. Bootstrapping
a member forms the component; the other running members join it on the
next tick. What the test must never see is counted:

* ``bootstrap from a behind member``: a member bootstrapped while another
  member (dead or alive) holds more transactions (they would be lost);
* ``bootstrap while a component exists``: a second component (split brain);
* ``joined from ahead``: a member that held more than the component joined
  it (its extra transactions are discarded by the state transfer).

Usage::

    python -m trove.tests.scale.fake_galera --config-file trove.conf \\
        --from 0 --clusters 250 --port 9800 [--seed]

    curl -XPOST 'localhost:9800/outage?n=100&down=60'   # whole clusters
    curl -XPOST 'localhost:9800/outage?n=100&down=60&members=2'
    curl -XPOST 'localhost:9800/outage?n=100&down=60&members=1'
    curl localhost:9800/stats

``members`` is how many members of each cluster lose power (3 by default);
they come back ``down`` seconds later.
"""

import argparse
import collections
import datetime
import json
import random
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
TICK = 3
MANAGER = 'mariadb'
# A member back from an outage knows its position after this many seconds
# at most (the recovery run of a member that did not log it).
POSITION_DELAY = 10
# Commits per tick while a primary component exists.
COMMITS = 3


def cluster_id_of(n):
    return 'fake-galera-%d' % n


def member_id_of(n, i):
    return '%s-m%d' % (cluster_id_of(n), i)


def ip_of(n, i):
    # Unique per cluster; within a cluster m1 < m2 < m3.
    return '10.%d.%d.%d' % (150 + n // 16384, (n // 64) % 256,
                            (n % 64) * 4 + i)


class FakeCluster(object):

    def __init__(self, n, stats):
        self.n = n
        self.id = cluster_id_of(n)
        self.uuid = 'f00d%04x-0000-4000-8000-%012d' % (n % 65536, n)
        self.stats = stats
        self.members = [FakeMember(self, member_id_of(n, i), ip_of(n, i))
                        for i in (1, 2, 3)]
        for m in self.members:
            m.in_group = True
            m.seqno = 1000
        self.formed_at = time.monotonic()
        self.lost_at = None

    def component(self):
        return [m for m in self.members if m.alive and m.in_group]

    def tick(self, now):
        component = self.component()
        if component:
            top = max(m.seqno for m in component) + COMMITS
            for m in component:
                m.seqno = top
        if component:
            for m in self.members:
                if m.alive and not m.in_group and m.known(now):
                    if m.seqno > component[0].seqno:
                        self.stats['joined from ahead'] += 1
                        print('%s joined %s from ahead: %d > %d' % (
                            m.id, self.id, m.seqno, component[0].seqno),
                            file=sys.stderr)
                    m.in_group = True
                    m.seqno = component[0].seqno
            if self.lost_at is not None and all(
                    m.in_group for m in self.members if m.alive) and all(
                    m.alive for m in self.members):
                self.stats['clusters formed again'] += 1
                self.stats['formed seconds total'] += int(now - self.lost_at)
                self.lost_at = None


class FakeMember(object):

    def __init__(self, cluster, member_id, ip):
        self.cluster = cluster
        self.id = member_id
        self.ip = ip
        self.alive = True
        self.in_group = False
        self.seqno = 0
        self.known_at = 0
        self.bootstrapped = False
        self.reporter = None

    def known(self, now):
        return self.in_group or now >= self.known_at

    def waiting(self):
        return self.alive and not self.in_group

    def state(self):
        return 'Synced' if self.in_group else 'Initialized'

    def recovery_view(self, now):
        return {'ip': self.ip, 'in_group': self.in_group,
                'waiting': self.waiting(),
                'position': ([self.cluster.uuid, self.seqno]
                             if self.known(now) else None),
                'safe_to_bootstrap': False,
                'bootstrapped': self.bootstrapped}

    def signal(self, now):
        if self.in_group:
            return cluster_probe.NORMAL
        return cluster_probe.ClusterSignal(
            True, cluster_probe.ABNORMAL, 'waiting for a primary component',
            extra={'waiting': True})


class FakeEndpoint(object):
    """What the task manager calls on one member. A dead member sleeps
    past the caller's timeout.
    """

    target = messaging.Target(version=guest_api.API.API_LATEST_VERSION)

    def __init__(self, member, stats):
        self.member = member
        self.stats = stats

    def _alive(self, what):
        self.stats[what] += 1
        if not self.member.alive:
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
        ok = self.member.in_group
        return {'state': self.member.state(),
                'role': 'PRIMARY' if ok else None, 'writable': ok}

    def is_writable_member(self, context):
        return self._alive('is_writable_member') and self.member.in_group

    def cluster_complete(self, context):
        self._alive('cluster_complete')

    def bootstrap_cluster(self, context):
        if not self._alive('bootstrap_cluster'):
            return
        m, c = self.member, self.member.cluster
        if c.component():
            self.stats['bootstrap while a component exists'] += 1
            print('%s: bootstrap of %s while a component exists' % (
                c.id, m.id), file=sys.stderr)
            return
        most = max(x.seqno for x in c.members)
        if m.seqno < most:
            self.stats['bootstrap from a behind member'] += 1
            print('%s: bootstrap of %s at %d, %d held elsewhere' % (
                c.id, m.id, m.seqno, most), file=sys.stderr)
        m.in_group = True
        m.bootstrapped = True
        self.stats['bootstraps'] += 1


class FakeGuests(object):

    def __init__(self, first, count):
        self.stats = collections.Counter()
        self.events = collections.Counter()
        self.clusters = {n: FakeCluster(n, self.stats)
                         for n in range(first, first + count)}
        self.started = time.monotonic()
        self.servers = []

    def _event(self, member):
        def send(kind, signal, view, since):
            self.events[kind] += 1
            context = trove_context.TroveContext()
            conductor_api.API(context).cluster_event(
                member.id, kind, sent=timeutils.utcnow_ts(microsecond=True),
                payload={'kind': kind, 'reason': signal.reason,
                         'since': since, 'datastore_manager': MANAGER,
                         'member': {'state': member.state()},
                         'extra': signal.extra or {},
                         'view': member.recovery_view(time.monotonic())})
        return send

    def start(self):
        conf = CONF.get(MANAGER)
        for cluster in self.clusters.values():
            for member in cluster.members:
                member.reporter = cluster_probe.ClusterEventReporter(
                    conf.cluster_event_interval,
                    conf.cluster_event_min_interval,
                    send=self._event(member))
                # A server of the shared topic, named after the instance,
                # like the production guests.
                target = messaging.Target(
                    topic=guest_api.GUEST_TOPIC, server=member.id,
                    version=guest_api.API.API_LATEST_VERSION)
                server = rpc.get_server(
                    target, [FakeEndpoint(member, self.stats)], key=None)
                server.start()
                self.servers.append(server)
                eventlet.sleep(0)
        eventlet.spawn_n(self._ticks)
        eventlet.spawn_n(self._heartbeats)

    def _ticks(self):
        while True:
            now = time.monotonic()
            for cluster in self.clusters.values():
                try:
                    cluster.tick(now)
                    for member in cluster.members:
                        if member.alive and member.reporter is not None:
                            member.reporter.observe(member.signal(now), None)
                except Exception as err:
                    self.stats['tick errors'] += 1
                    print('tick %s: %s' % (cluster.id, err), file=sys.stderr)
                eventlet.sleep(0)
            eventlet.sleep(TICK)

    def _heartbeats(self):
        interval = CONF.report_interval
        while True:
            for cluster in self.clusters.values():
                for member in cluster.members:
                    if not member.alive:
                        continue
                    try:
                        conductor_api.API(trove_context.TroveContext()).\
                            heartbeat(member.id,
                                      {'service_status': 'healthy'},
                                      sent=timeutils.utcnow_ts(
                                          microsecond=True))
                        self.stats['heartbeats'] += 1
                    except Exception as err:
                        self.stats['heartbeat errors'] += 1
                        print('heartbeat %s: %s' % (member.id, err),
                              file=sys.stderr)
                    eventlet.sleep(0)
            eventlet.sleep(interval)

    # Control

    def outage(self, count, down, members=3):
        """The first count clusters lose power on `members` of their
        members, which stop a few transactions apart and come back after
        `down` seconds.
        """
        hit = []
        now = time.monotonic()
        for n in sorted(self.clusters):
            if len(hit) >= count:
                break
            c = self.clusters[n]
            if c.lost_at is not None or not all(m.alive for m in c.members):
                continue
            victims = random.sample(c.members, members)
            for m in victims:
                m.alive = False
                m.in_group = False
                # The last committed transactions differ a little.
                m.seqno -= random.randint(0, 2)
            # Fewer than a majority left: the survivors lose the primary
            # component and wait (a bootstrapped member forms one alone).
            survivors = c.component()
            if len(survivors) * 2 <= len(c.members):
                for m in survivors:
                    m.in_group = False
                    m.known_at = now
            c.lost_at = now
            hit.append(c.id)
            eventlet.spawn_after(down, self._power_on, c, victims)
        self.stats['outages'] += len(hit)
        return hit

    def _power_on(self, cluster, victims):
        now = time.monotonic()
        for m in victims:
            m.alive = True
            m.in_group = False
            m.bootstrapped = False
            m.known_at = now + random.uniform(0, POSITION_DELAY)
        self.stats['powered on'] += len(victims)

    def snapshot(self):
        now = time.monotonic()
        lost = [c for c in self.clusters.values() if c.lost_at is not None]
        return {'clusters': len(self.clusters),
                'uptime': int(now - self.started),
                'calls': dict(self.stats), 'events': dict(self.events),
                'not formed again': len(lost),
                'oldest loss seconds': int(max(
                    (now - c.lost_at for c in lost), default=0)),
                'members down': sum(1 for c in self.clusters.values()
                                    for m in c.members if not m.alive),
                'members waiting': sum(1 for c in self.clusters.values()
                                       for m in c.members if m.waiting())}


def seed(first, count, version_name):
    """The clusters, instances and service statuses in the database."""
    from trove.cluster.models import DBCluster
    from trove.cluster.tasks import ClusterTasks
    from trove.datastore import models as datastore_models
    from trove.instance.models import DBInstance
    from trove.instance.models import InstanceServiceStatus
    from trove.instance.service_status import ServiceStatuses
    from trove.instance.tasks import InstanceTasks
    version = datastore_models.DatastoreVersion.load(
        datastore_models.Datastore.load(MANAGER), version_name)
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
            if url.path == '/outage':
                hit = self.guests.outage(int(query['n']),
                                         float(query.get('down', 60)),
                                         int(query.get('members', 3)))
                self._reply({'hit': len(hit), 'first': hit[:3],
                             'at': datetime.datetime.utcnow().isoformat()})
            else:
                self._reply({'error': 'unknown'}, 404)
        except Exception as err:
            self._reply({'error': str(err)}, 400)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--from', dest='first', type=int, default=0)
    parser.add_argument('--clusters', type=int, default=100)
    parser.add_argument('--port', type=int, default=9800)
    parser.add_argument('--version', default='11.4')
    parser.add_argument('--seed', action='store_true')
    args, rest = parser.parse_known_args()
    CONF(rest, project='trove')
    rpc.init(CONF)
    if args.seed:
        from trove.db import get_db_api
        get_db_api().configure_db()
        seed(args.first, args.clusters, args.version)
    guests = FakeGuests(args.first, args.clusters)
    guests.start()
    Control.guests = guests
    httpd = http_server.HTTPServer(('0.0.0.0', args.port), Control)
    print('fake Galera guests: clusters %d..%d, control on :%d' % (
        args.first, args.first + args.clusters - 1, args.port))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    while True:
        eventlet.sleep(60)
        print(json.dumps(guests.snapshot()))


if __name__ == '__main__':
    main()
