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

"""Which replicas of a service cover which keys: the task managers and
the clusters they watch.

The replicas join a group on the backend named by ``[coordination]
backend_url`` (tooz; the membership is a lease each replica keeps alive)
and every replica computes, by itself, which keys it covers: for each
key, the ``k`` replicas with the highest rendezvous (highest random
weight) hash of (key, replica) cover it. Every replica holds the same
member list and computes the same map, with no coordination; adding or
removing a replica moves only the keys whose top-k changes.

Coverage is k-redundant: each key is covered by k replicas at once, all
of them act, and the actions are deduplicated by compare-and-set (the
cluster's recovery task), so a replica that dies leaves the key covered
by the others with no gap, and two replicas acting is harmless. That
makes over-coverage safe and under-coverage the only danger: without a
backend URL, or until the backend first answers, a replica covers every
key; when the backend stops answering, it keeps the last membership.
"""

import hashlib
import os
import socket
import threading
import time

from oslo_config import cfg
from oslo_log import log as logging

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

coordination_group = cfg.OptGroup(
    'coordination', title='Coordination of the task manager replicas')
coordination_opts = [
    cfg.StrOpt('backend_url', secret=True, default=None,
               help='tooz backend the task manager replicas register on '
                    '(for example etcd3+http://coordination-etcd:2379), so '
                    'that each cluster is watched by '
                    'cluster_recovery_coverage of them. Empty: this '
                    'process is the only replica and watches every '
                    'cluster.'),
]

# The group the task managers join, and how long a membership lasts
# without a heartbeat.
RECOVERY_GROUP = b'trove-taskmanager-recovery'
MEMBERSHIP_TIMEOUT = 30


def _bytes(value):
    return value if isinstance(value, bytes) else value.encode('utf-8')


def _reason(ex):
    """The reason of a tooz error, which often wraps the real one."""
    return str(getattr(ex, 'cause', None) or ex) or type(ex).__name__


def mix64(x):
    """The splitmix64 finalizer: a bijective mixer with the avalanche a
    rendezvous hash needs, which FNV-1a alone lacks.
    """
    mask = (1 << 64) - 1
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9 & mask
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb & mask
    return x ^ (x >> 31)


def hrw_score(key, member):
    """The rendezvous weight of (key, member): FNV-1a 64 of the key, a
    NUL and the member, mixed. Higher is preferred.
    """
    h = 0xcbf29ce484222325
    for byte in _bytes(key) + b'\0' + _bytes(member):
        h = ((h ^ byte) * 0x100000001b3) & ((1 << 64) - 1)
    return mix64(h)


def coverers(key, members, k):
    """The k members that cover the key, highest weight first; ties
    break by id, so the result is the same everywhere. Fewer when
    there are fewer members; none when k is not positive.
    """
    if k <= 0 or not members:
        return []
    ranked = sorted(members, key=lambda m: (-hrw_score(key, m), m))
    return ranked[:k]


def covers(member, key, members, k):
    return member in coverers(key, members, k)


def digest(value):
    """A stable, short digest of a value, for logs."""
    return hashlib.sha1(_bytes(value)).hexdigest()[:8]


class Coordinator(object):
    """The membership of this process in the group of its replicas, and
    the keys it covers.

    :param group: the group the replicas share.
    :param member_id: this process's id in the group; the host name and
                      the pid by default, unique among the replicas.
    """

    def __init__(self, group=RECOVERY_GROUP, member_id=None, k=None):
        self.group = _bytes(group)
        self.member_id = _bytes(member_id or '%s:%s' % (
            socket.gethostname(), os.getpid()))
        self._k = k
        self._url = None
        self._coordinator = None
        # The last membership the backend gave, and when; None until it
        # gave one.
        self._members = None
        self._fetched_at = None
        # Whether the last call of the backend failed: logged once, not
        # at every call while it lasts.
        self._failing = False
        self._lock = threading.RLock()

    @property
    def k(self):
        return self._k if self._k is not None else \
            CONF.cluster_recovery_coverage

    @property
    def active(self):
        """Whether a backend is configured."""
        return bool(self._url)

    @property
    def connected(self):
        return self._coordinator is not None

    def start(self):
        """Join the group, or go on alone when no backend is configured.
        A backend that cannot be reached is tried again at every
        refresh; until it answers, this replica covers every key.
        """
        self._url = CONF.coordination.backend_url
        if not self._url:
            LOG.info("No [coordination] backend_url: this task manager is "
                     "the only replica and watches every cluster.")
            return
        with self._lock:
            self._connect()

    def _connect(self):
        from tooz import coordination as tooz_coordination
        coordinator = tooz_coordination.get_coordinator(
            self._url, self.member_id)
        try:
            coordinator.start(start_heart=True)
            try:
                self._join(coordinator)
            except Exception:
                coordinator.stop()
                raise
        except Exception as ex:
            if not self._failing:
                LOG.error("Cannot join group %(g)s: %(ex)s. This replica "
                          "watches every cluster until it does.",
                          {'g': self.group.decode(), 'ex': _reason(ex)})
            self._failing = True
            return
        self._failing = False
        self._coordinator = coordinator
        LOG.info("Joined group %(g)s as %(m)s.",
                 {'g': self.group.decode(), 'm': self.member_id.decode()})

    def _join(self, coordinator):
        from tooz import coordination as tooz_coordination
        try:
            coordinator.join_group_create(self.group)
        except tooz_coordination.MemberAlreadyExist:
            pass

    def stop(self):
        """Leave the group, so that the replicas take over at once."""
        with self._lock:
            coordinator = self._coordinator
            self._coordinator = None
            if coordinator is None:
                return
            try:
                coordinator.leave_group(self.group).get()
            except Exception as ex:
                LOG.warning("Failed to leave group %(g)s: %(ex)s",
                            {'g': self.group.decode(), 'ex': _reason(ex)})
            try:
                coordinator.stop()
            except Exception as ex:
                LOG.warning("Failed to stop the coordinator: %s", ex)

    def members(self, max_age=MEMBERSHIP_TIMEOUT):
        """The sorted members of the group, asked of the backend at most
        once per ``max_age`` seconds; [self] without a backend or until
        it first answers; the last membership it gave while it does not
        answer.
        """
        if not self._url:
            return [self.member_id]
        with self._lock:
            now = time.monotonic()
            if (self._members is not None and self._fetched_at is not None
                    and now - self._fetched_at < max_age):
                return self._members
            if self._coordinator is None:
                self._connect()
                if self._coordinator is None:
                    return self._members or [self.member_id]
            try:
                self._members = sorted(self._fetch())
                self._fetched_at = now
                if self._failing:
                    LOG.info("The backend of group %s answers again.",
                             self.group.decode())
                self._failing = False
            except Exception as ex:
                if self._failing:
                    LOG.debug("Still cannot get the members of group %(g)s: "
                              "%(ex)s", {'g': self.group.decode(),
                                         'ex': _reason(ex)})
                elif self._members is None:
                    LOG.error("Cannot get the members of group %(g)s: "
                              "%(ex)s. This replica watches every cluster "
                              "until the backend answers.",
                              {'g': self.group.decode(), 'ex': _reason(ex)})
                else:
                    LOG.warning("Cannot get the members of group %(g)s: "
                                "%(ex)s. Keeping the last known membership "
                                "of %(n)d until it answers.",
                                {'g': self.group.decode(),
                                 'ex': _reason(ex),
                                 'n': len(self._members)})
                self._failing = True
            return self._members or [self.member_id]

    def _fetch(self):
        """The members of the group, joining it again when needed: the
        membership expires, or the whole group is gone (a backend
        restarted without its data), after a long outage.
        """
        from tooz import coordination as tooz_coordination
        coordinator = self._coordinator
        try:
            members = set(coordinator.get_members(self.group).get())
        except tooz_coordination.GroupNotCreated:
            LOG.warning("Group %s is gone; creating and joining it again.",
                        self.group.decode())
            self._join(coordinator)
            members = set(coordinator.get_members(self.group).get())
        if self.member_id not in members:
            LOG.warning("Not a member of group %s any more; joining again.",
                        self.group.decode())
            self._join(coordinator)
            members = set(coordinator.get_members(self.group).get())
        return members

    def covered(self, keys):
        """The subset of keys this replica covers, by one look at the
        membership.
        """
        members = self.members()
        if members == [self.member_id]:
            return set(keys)
        return {key for key in keys
                if covers(self.member_id, key, members, self.k)}

    def covers(self, key):
        """Whether this replica covers the key, by the membership last
        fetched (refreshed when older than the membership timeout).
        """
        return key in self.covered([key])
