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

"""The keep-alive heartbeats, written in batches.

Nearly every heartbeat a guest sends repeats the status the conductor
already wrote for it; only its time matters. Such a heartbeat is kept
in memory and the ``updated_at`` of all of them is written with one
statement per status every ``conductor_heartbeat_flush_interval``
seconds, instead of the four statements the full path costs each.

The batch is written where the row holds the status the heartbeat
repeats; the flush finds the rows that do not (the control plane set
another status, or the guest's first heartbeat after a change reached
another worker) and runs those heartbeats through the full path after
all, so that the guest's status wins as it always did, at most a flush
later. A heartbeat that changes the status this worker heard last goes
the full way at once. The workers (processes) of a conductor share
nothing: each batches what it hears.
"""

import os
import threading

from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import timeutils

from trove.common import cfg
from trove.common import exception
from trove.instance.models import InstanceServiceStatus
from trove.instance import service_status as svc_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The batch is written before it holds more than this many heartbeats.
MAX_PENDING = 5000


class Keepalives(object):

    def __init__(self, full_path):
        """``full_path(instance_id, status_description, sent)`` writes
        one heartbeat the usual way.
        """
        self.full_path = full_path
        self._lock = threading.Lock()
        # instance id -> (status id, sent) of the last heartbeat this
        # process wrote or batched; a status that was not written
        # (RESTART_REQUIRED stays) is forgotten.
        self.known = {}
        # instance id -> (status id, sent) waiting for the flush.
        self.pending = {}
        self._ticker = None
        self._pid = None
        self._flushes = 0

    @property
    def interval(self):
        return CONF.conductor_heartbeat_flush_interval

    def seen_later(self, instance_id, sent):
        """Whether this process handled a later heartbeat of the
        instance already: then this one is stale.
        """
        if sent is None:
            return False
        with self._lock:
            known = self.known.get(instance_id)
        return known is not None and known[1] >= sent

    def confirmed(self, instance_id, status_id, sent):
        """The full path wrote this status for the instance; a
        keep-alive of the status before it is overtaken.
        """
        with self._lock:
            self.known[instance_id] = (status_id, sent)
            self.pending.pop(instance_id, None)

    def forget(self, instance_id):
        with self._lock:
            self.known.pop(instance_id, None)
            self.pending.pop(instance_id, None)

    def accept(self, instance_id, status_description, sent):
        """Whether the heartbeat repeats the status this process heard
        last from the instance (or is the first it hears): then it waits
        for the flush.
        """
        if not self.interval or sent is None or status_description is None:
            return False
        try:
            status = svc_status.ServiceStatus.from_description(
                status_description)
        except ValueError:
            # The full path says so.
            return False
        flush = False
        with self._lock:
            known = self.known.get(instance_id)
            if known is not None and (known[0] != status.code or
                                      known[1] >= sent):
                return False
            self.known[instance_id] = (status.code, sent)
            self.pending[instance_id] = (status.code, sent)
            flush = len(self.pending) >= MAX_PENDING
        self._ensure_ticker()
        if flush:
            self.flush()
        return True

    def _ensure_ticker(self):
        """The flush timer of this process (a worker is forked after
        the manager is made: the timer is started where it is used).
        """
        pid = os.getpid()
        if self._ticker is not None and self._pid == pid:
            return
        with self._lock:
            if self._ticker is not None and self._pid == pid:
                return
            self._pid = pid
            self._ticker = loopingcall.FixedIntervalLoopingCall(
                self._flush_safely)
            self._ticker.start(interval=self.interval,
                               initial_delay=self.interval,
                               stop_on_exception=False)

    def stop(self):
        if self._ticker is not None:
            self._ticker.stop()
            self._ticker = None
        self.flush()

    def _flush_safely(self):
        try:
            self.flush()
        except Exception as err:
            LOG.warning("The heartbeats could not be written in a batch "
                        "(%s); they are written one by one until it works.",
                        err)
            with self._lock:
                self.known.clear()
                self.pending.clear()

    def flush(self):
        """Write the time of the batched heartbeats: one statement per
        status. The rows whose status changed meanwhile are not
        touched by it; their heartbeats go through the full path.
        """
        with self._lock:
            pending, self.pending = self.pending, {}
            self._flushes += 1
            prune = self._flushes % 100 == 0
        if prune:
            self._prune()
        if not pending:
            return 0
        now = timeutils.utcnow()
        by_status = {}
        for instance_id, (status_id, sent) in pending.items():
            by_status.setdefault(status_id, []).append(instance_id)
        written = 0
        for status_id, ids in by_status.items():
            with InstanceServiceStatus.query() as query:
                touched = query.filter(
                    InstanceServiceStatus.instance_id.in_(ids),
                    InstanceServiceStatus.status_id == status_id
                ).update({'updated_at': now}, synchronize_session=False)
            written += touched
            if touched >= len(ids):
                continue
            with InstanceServiceStatus.query() as query:
                found = {row[0] for row in query.filter(
                    InstanceServiceStatus.instance_id.in_(ids),
                    InstanceServiceStatus.status_id == status_id
                ).with_entities(InstanceServiceStatus.instance_id).all()}
            description = svc_status.ServiceStatus.from_code(
                status_id).description
            for instance_id in ids:
                if instance_id in found:
                    continue
                self.forget(instance_id)
                LOG.debug("Instance %s: its status is not %s any more; "
                          "the heartbeat is written in full.",
                          instance_id, description)
                try:
                    self.full_path(instance_id, description,
                                   pending[instance_id][1])
                except exception.ModelNotFoundError:
                    pass
        LOG.debug("%d heartbeat(s) written in %d statement(s).",
                  written, len(by_status))
        return written

    def _prune(self):
        """Forget the instances not heard from for a long time (deleted
        ones stay otherwise).
        """
        cutoff = timeutils.utcnow_ts() - 10 * CONF.agent_heartbeat_expiry
        with self._lock:
            for instance_id in [i for i, (_, sent) in self.known.items()
                                if sent < cutoff]:
                del self.known[instance_id]
