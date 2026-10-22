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

import functools
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
# With cluster_recovery_slots, a replica first holds the lock of a slot and
# is a member of the group under the slot's name; the slot's name is also
# its RPC server name, so the conductor can send it what it covers.
SLOT_PREFIX = 'recovery-'
SLOT_LOCK = b'trove-taskmanager-recovery-slot-%d'
# How often a replica without a slot tries for one again.
SLOT_RETRY = 60
# How long a slot given up because its RPC server could not start (its
# queue could not be declared) stays parked: its lock held by the replica
# that gave it up, so that no replica tries it meanwhile.
SLOT_QUARANTINE = 3600


# The lease the tooz drivers give a lock and a membership by default
# (their lock_timeout and membership_timeout), when the driver does not
# say how often to beat.
LEASE = 30
# How many times a lease is refreshed in its lifetime. tooz's own heart
# refreshes once per lifetime: the etcd driver's beats are the lease
# apart, and the refresh resets the lease, so a refresh that is late at
# all (by the wait of the beating thread, by the round trip) finds it
# lapsed; the lock is then gone from the backend while its holder, whose
# refresh of a lapsed lease raises nothing, believes it holds it.
BEATS_PER_LEASE = 3


def slot_name(index):
    return '%s%d' % (SLOT_PREFIX, index)


def start_coordinator(url, member_id):
    """A tooz coordinator on the backend, started with Trove's own heart
    (the coordinator and the heart, which stops before it).
    """
    from tooz import coordination as tooz_coordination
    coordinator = tooz_coordination.get_coordinator(url, _bytes(member_id))
    coordinator.start(start_heart=False)
    heart = Heart(coordinator)
    heart.start()
    return coordinator, heart


class _Held(object):

    def __init__(self, lock, on_lapse, probe):
        self.lock = lock
        self.on_lapse = on_lapse
        # Whether a lock of the same name acquired again tells that the
        # lease lapsed (not with a driver whose lock the process that
        # holds it can acquire again).
        self.probe = probe


class Heart(object):
    """The pump that keeps the leases of a coordinator alive: it beats
    BEATS_PER_LEASE times per lease, and at each beat looks after the
    locks the process holds for long (its liveness lock, its slot): a
    lock whose lease lapsed anyway is held again at once, and a lock
    the driver reports as held no more is given up for lost; the holder
    is told either way. A lock another holder took while the lease had
    lapsed cannot be told from one held (the driver reports it acquired
    either way): a slot is then held by two replicas, which the coverage
    allows (two replicas acting is harmless), until one of them stops.

    :param coordinator: a started tooz coordinator, its own heart off.
    """

    def __init__(self, coordinator, beats=BEATS_PER_LEASE):
        self._coordinator = coordinator
        self._beats = beats
        self._held = {}
        self._guard = threading.RLock()
        self._dead = threading.Event()
        self._thread = None
        # The lease the coordinator asks to be refreshed within, and when
        # it was last refreshed (None: never, or not since it failed).
        self.lease = LEASE
        self.period = float(LEASE) / beats
        self._beat_at = None

    def start(self):
        interval = self._beat()
        if interval is not None:
            self._beat_at = time.monotonic()
        self.lease = interval or LEASE
        self.period = max(1.0, float(self.lease) / self._beats)
        self._spawn()

    def _spawn(self):
        self._thread = threading.Thread(target=self._run, name='trove-heart')
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        while not self._dead.wait(self.period):
            try:
                self.beat()
            except Exception:
                LOG.exception("The heart skipped a beat.")

    def stop(self):
        self._dead.set()

    def _beat(self):
        """One refresh of the leases; the interval the driver asks for,
        None when the backend did not answer.
        """
        try:
            interval = self._coordinator.heartbeat()
        except Exception as ex:
            LOG.warning("Cannot refresh the leases on the coordination "
                        "backend: %s", _reason(ex))
            return None
        if isinstance(interval, (int, float)) and interval > 0:
            return interval
        return LEASE

    def beat(self):
        """A beat: the leases refreshed, the locks held looked after."""
        now = time.monotonic()
        if self._beat_at is not None and now - self._beat_at > self.lease:
            LOG.warning("The heart did not beat for %.0f s (a lease is "
                        "%d s): the leases may have lapsed.",
                        now - self._beat_at, self.lease)
        if self._beat() is not None:
            self._beat_at = now
        with self._guard:
            for name in list(self._held):
                self.check(name)

    @property
    def alive(self):
        """Whether the leases are known to be alive: refreshed within a
        lease.
        """
        return (self._beat_at is not None and
                time.monotonic() - self._beat_at <= self.lease)

    def hold(self, name, lock, on_lapse=None):
        """A lock the process holds for long, looked after from now on.
        ``on_lapse`` is called with the lock that holds it again when its
        lease lapsed, and with None when it is lost to another holder.
        """
        with self._guard:
            self._held[name] = _Held(lock, on_lapse,
                                     probe=not self._reentrant(name))

    def _reentrant(self, name):
        """Whether the process that holds a lock can acquire it again (a
        file lock, say): then a lock of the same name acquired tells
        nothing of the lease.
        """
        try:
            fresh = self._coordinator.get_lock(name)
            if fresh.acquire(blocking=False):
                fresh.release()
                LOG.debug("Lock %s can be acquired again by its holder: "
                          "its lease is not probed.", name)
                return True
        except Exception as ex:
            LOG.debug("Cannot probe lock %s: %s", name, _reason(ex))
        return False

    def release(self, name):
        """The lock of that name, looked after no more; None when it is
        not held. The holder releases it.
        """
        with self._guard:
            held = self._held.pop(name, None)
        return held.lock if held else None

    def holds(self, name):
        return name in self._held

    def check(self, name):
        """Whether the lock of that name is held, by one look: refreshed;
        held again when its lease had lapsed; given up when another
        holder has it.
        """
        with self._guard:
            held = self._held.get(name)
            if held is None:
                return False
            try:
                refreshed = bool(held.lock.heartbeat())
            except Exception as ex:
                LOG.warning("Cannot refresh lock %s: %s", name, _reason(ex))
                return self.alive
            if refreshed and not held.probe:
                return True
            try:
                fresh = self._coordinator.get_lock(name)
                got = fresh.acquire(blocking=False)
            except Exception as ex:
                LOG.warning("Cannot probe lock %s: %s", name, _reason(ex))
                return refreshed
            if got:
                LOG.warning("The lease of lock %s had lapsed: held again.",
                            name)
                held.lock = fresh
                if held.on_lapse is not None:
                    held.on_lapse(fresh)
                return True
            if refreshed:
                return True
            LOG.error("Lock %s is lost: another holder has it.", name)
            del self._held[name]
            if held.on_lapse is not None:
                held.on_lapse(None)
            return False


def slot_topic():
    """The RPC topic of the slot servers. Not the task manager's own
    topic: an RPC server consumes its topic's shared queue and fanout too,
    and a slot server takes cluster events only.
    """
    return '%s-slots' % CONF.taskmanager_queue


def is_slot(member):
    member = member.decode() if isinstance(member, bytes) else member
    rest = member[len(SLOT_PREFIX):]
    return member.startswith(SLOT_PREFIX) and rest.isdigit()


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

    def __init__(self, group=RECOVERY_GROUP, member_id=None, k=None,
                 slots=None, observer=False):
        self.group = _bytes(group)
        self.member_id = _bytes(member_id or '%s:%s' % (
            socket.gethostname(), os.getpid()))
        self._k = k
        self._slots = slots
        # An observer (the conductor) reads the membership and is not a
        # member.
        self.observer = observer
        # With slots: the slot held (its name, which is this replica's
        # member id in the group), the lock that holds it and the
        # connection that keeps the lock; when a slot was last tried for.
        self.slot = None
        # Whether the slot's RPC server is up: only then is this replica a
        # member of the group (trove-conductor sends to members only).
        self.slot_ready = False
        # Slots given up on, by index: (their lock, still held, and until
        # when).
        self._parked = {}
        self._holder = None
        self._holder_heart = None
        self._slot_lock = None
        self._slot_index = None
        self._tried_slot_at = None
        # Called with the slot's name when one is held, and with no
        # argument when it is lost (the lock lapsed).
        self.on_slot_held = None
        self.on_slot_lost = None
        self._url = None
        self._coordinator = None
        self._heart = None
        # The last membership the backend gave, and when; None until it
        # gave one.
        self._members = None
        self._fetched_at = None
        self._connect_tried_at = None
        # Whether the last call of the backend failed: logged once, not
        # at every call while it lasts.
        self._failing = False
        self._lock = threading.RLock()

    @property
    def k(self):
        return self._k if self._k is not None else \
            CONF.cluster_recovery_coverage

    @property
    def slots(self):
        if self._slots is not None:
            return self._slots
        return 0 if self.observer else CONF.cluster_recovery_slots

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
            if self.slots and not self.observer:
                self._claim_slot()
                if self.slot is None or not self.slot_ready:
                    return
            self._connect()

    def _claim_slot(self):
        """Hold the lock of the first free slot and take its name as the
        member id; without one, this replica watches no cluster and tries
        again every SLOT_RETRY seconds.
        """
        self._tried_slot_at = time.monotonic()
        try:
            if self._holder is None:
                self._holder, self._holder_heart = start_coordinator(
                    self._url, self.member_id)
            now = time.monotonic()
            self._unpark(now)
            for index in range(self.slots):
                if index in self._parked:
                    continue
                lock = self._holder.get_lock(SLOT_LOCK % index)
                if lock.acquire(blocking=False):
                    self._slot_lock = lock
                    self.slot = slot_name(index)
                    self._slot_index = index
                    self._holder_heart.hold(
                        SLOT_LOCK % index, lock,
                        functools.partial(self._slot_lapsed, index))
                    self.slot_ready = self.on_slot_held is None
                    LOG.info("Holding slot %(s)s of %(n)d as %(m)s.",
                             {'s': self.slot, 'n': self.slots,
                              'm': self.member_id.decode()})
                    if self.on_slot_held is not None:
                        self.on_slot_held(self.slot)
                    return
            LOG.warning("Every one of the %d slots is held: this task "
                        "manager watches no cluster and tries again in %d "
                        "seconds.", self.slots, SLOT_RETRY)
        except Exception as ex:
            LOG.error("Cannot get a slot: %s. This task manager watches no "
                      "cluster and tries again in %d seconds.",
                      _reason(ex), SLOT_RETRY)

    def _unpark(self, now):
        for index, (lock, until) in list(self._parked.items()):
            if until <= now:
                del self._parked[index]
                LOG.info("Releasing parked slot %s.", slot_name(index))
                try:
                    self._holder_heart.release(SLOT_LOCK % index)
                    lock.release()
                except Exception as ex:
                    LOG.warning("Failed to release slot %s: %s",
                                slot_name(index), _reason(ex))

    def _slot_held(self):
        """Whether the slot's lock still holds, by one look of the heart
        (a lapsed lease is held again there and then; a slot another
        replica took meanwhile is lost).
        """
        return (self._slot_lock is not None and
                self._holder_heart.check(SLOT_LOCK % self._slot_index))

    def _slot_lapsed(self, index, lock):
        """The heart's word on a slot's lock: the lock that holds it again
        after its lease lapsed, or None when another holder took it (the
        slot is lost at the next look at the membership).
        """
        if index in self._parked:
            if lock is None:
                del self._parked[index]
            else:
                self._parked[index] = (lock, self._parked[index][1])
        elif self.slot == slot_name(index):
            self._slot_lock = lock

    def _lose_slot(self):
        LOG.error("Lost slot %s: leaving group %s and watching no cluster "
                  "until a slot is held again.", self.slot,
                  self.group.decode())
        self._leave()
        self.slot = None
        self.slot_ready = False
        self._slot_lock = None
        self._members = None
        if self.on_slot_lost is not None:
            try:
                self.on_slot_lost()
            except Exception:
                LOG.exception("Failed to stop serving the slot.")

    def _connect(self):
        member_id = _bytes(self.slot) if self.slot else self.member_id
        if self.observer:
            member_id = _bytes('observer:%s' % self.member_id.decode())
        try:
            coordinator, heart = start_coordinator(self._url, member_id)
            if not self.observer:
                try:
                    self._join(coordinator)
                except Exception:
                    heart.stop()
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
        self._heart = heart
        if self.observer:
            LOG.info("Watching the members of group %s.", self.group.decode())
        else:
            LOG.info("Joined group %(g)s as %(m)s.",
                     {'g': self.group.decode(), 'm': member_id.decode()})

    def _join(self, coordinator):
        from tooz import coordination as tooz_coordination
        try:
            coordinator.join_group_create(self.group)
        except tooz_coordination.MemberAlreadyExist:
            pass

    def stop(self):
        """Leave the group and give the slot up, so that the replicas
        take over at once.
        """
        with self._lock:
            self._leave()
            if self._slot_lock is not None:
                try:
                    self._holder_heart.release(SLOT_LOCK % self._slot_index)
                    self._slot_lock.release()
                except Exception as ex:
                    LOG.warning("Failed to release slot %s: %s", self.slot,
                                _reason(ex))
                self._slot_lock = None
                self.slot = None
            self._unpark(float('inf'))
            if self._holder is not None:
                self._holder_heart.stop()
                try:
                    self._holder.stop()
                except Exception as ex:
                    LOG.warning("Failed to stop the slot holder: %s", ex)
                self._holder = None

    def _leave(self):
        coordinator = self._coordinator
        self._coordinator = None
        if coordinator is None:
            return
        if self._heart is not None:
            self._heart.stop()
            self._heart = None
        if not self.observer:
            try:
                coordinator.leave_group(self.group).get()
            except Exception as ex:
                LOG.warning("Failed to leave group %(g)s: %(ex)s",
                            {'g': self.group.decode(), 'ex': _reason(ex)})
        try:
            coordinator.stop()
        except Exception as ex:
            LOG.warning("Failed to stop the coordinator: %s", ex)

    @property
    def watching(self):
        """Whether this replica watches clusters at all: not without a
        slot when slots are configured.
        """
        return not (self._url and self.slots and not self.observer and
                    (self.slot is None or not self.slot_ready))

    def slot_served(self):
        """The slot's RPC server is up: join the group under its name at
        the next look at the membership.
        """
        with self._lock:
            if self.slot is None:
                return
            self.slot_ready = True
            self._members = None
            self._connect_tried_at = None
            self._connect()

    def give_up_slot(self):
        """Park the slot for SLOT_QUARANTINE seconds (its RPC server could
        not start; its lock stays held, so no replica tries it meanwhile)
        and try for another one at once.
        """
        with self._lock:
            if self.slot is None:
                return
            LOG.error("Parking slot %s for %d seconds: its RPC server did "
                      "not start. The queue of the slot may be one RabbitMQ "
                      "cannot declare any more (a quorum queue whose Raft "
                      "members outlived a failed declaration); while parked "
                      "it takes one of cluster_recovery_slots.", self.slot,
                      SLOT_QUARANTINE)
            # Parked, not released: the other replicas would only try it in
            # turn.
            self._parked[self._slot_index] = (
                self._slot_lock, time.monotonic() + SLOT_QUARANTINE)
            self._leave()
            self.slot = None
            self.slot_ready = False
            self._slot_lock = None
            self._members = None
            self._tried_slot_at = None

    def members(self, max_age=MEMBERSHIP_TIMEOUT):
        """The sorted members of the group, asked of the backend at most
        once per ``max_age`` seconds; [self] without a backend or until
        it first answers; the last membership it gave while it does not
        answer.
        """
        if not self._url:
            return [] if self.observer else [self.member_id]
        with self._lock:
            now = time.monotonic()
            if (self._members is not None and self._fetched_at is not None
                    and now - self._fetched_at < max_age):
                return self._members
            if self.slots and not self.observer:
                if self.slot is not None and not self._slot_held():
                    self._lose_slot()
                if self.slot is None:
                    if (self._tried_slot_at is None or
                            now - self._tried_slot_at >= SLOT_RETRY):
                        self._claim_slot()
                    if self.slot is None:
                        return []
                if not self.slot_ready:
                    return []
            if self._coordinator is None:
                # Not more than once per max_age: an event must not wait on
                # a backend that does not answer.
                if (self._connect_tried_at is not None and
                        now - self._connect_tried_at < max_age):
                    return self._members or self._alone()
                self._connect_tried_at = now
                self._connect()
                if self._coordinator is None:
                    return self._members or self._alone()
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
            return self._members or self._alone()

    def _alone(self):
        """The membership before the backend first answers: this
        replica alone (it watches everything), none for an observer.
        """
        if self.observer:
            return []
        return [_bytes(self.slot) if self.slot else self.member_id]

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
            if self.observer:
                return set()
            LOG.warning("Group %s is gone; creating and joining it again.",
                        self.group.decode())
            self._join(coordinator)
            members = set(coordinator.get_members(self.group).get())
        if self.observer:
            return members
        if self.me not in members:
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
        if not self.watching:
            return set()
        if members == [self.me]:
            return set(keys)
        return {key for key in keys
                if covers(self.me, key, members, self.k)}

    @property
    def me(self):
        """This replica's id in the group: its slot's name when it holds
        one.
        """
        return _bytes(self.slot) if self.slot else self.member_id

    def coverers_of(self, key):
        """The members that cover a key (an observer's question)."""
        return coverers(key, self.members(), self.k)

    def covers(self, key):
        """Whether this replica covers the key, by the membership last
        fetched (refreshed when older than the membership timeout).
        """
        return key in self.covered([key])
