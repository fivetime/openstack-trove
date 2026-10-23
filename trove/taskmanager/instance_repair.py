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

"""The task manager's repair of standalone instances: the server of an
instance that Nova shut off by itself is started again.

The cluster repair (``common.repair``) starts a cluster member whose
server Nova shut off by itself: its domain went away (powered off,
killed), ``_sync_instance_power_state`` recorded a ``stop`` without a
user, and nothing starts it again. A standalone instance's server is
shut off the same way and stays off the same way; here it is started
by the same decision (``common.repair.decide_start``): only one that is
quiet to the control plane (its heartbeat is stale), whose latest
``stop`` in Nova has no user (one stopped through the API, on purpose,
stays stopped), on a host whose compute service is up, while the
instance has no task, at most ``cluster_repair_start_attempts`` times
since the stop, ``cluster_repair_start_interval`` apart.

The task manager scans the database every
``instance_repair_scan_interval`` seconds for the standalone instances
whose heartbeat is stale; of them each replica looks at the ones it
covers by the recovery hub's membership (``recovery.hub``), so that two
task managers do not start the same server (and when they do, Nova's
409 is no error). The repair is told as ``trove.instance.repair.start``,
``trove.instance.repair.end`` (the instance heard from again) and
``trove.instance.repair.error``.
"""

import threading
import time

from novaclient import exceptions as nova_exceptions
from oslo_log import log as logging
from oslo_utils import timeutils
from sqlalchemy import or_
from sqlalchemy.sql import expression

from trove.common import cfg
from trove.common import clients
from trove.common import context as trove_context
from trove.common.strategies.cluster.experimental.common import health
from trove.common.strategies.cluster.experimental.common import repair
from trove.instance.models import DBInstance
from trove.instance.models import InstanceServiceStatus
from trove import rpc
from trove.taskmanager import recovery

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

EVENT_START = 'trove.instance.repair.start'
EVENT_END = 'trove.instance.repair.end'
EVENT_ERROR = 'trove.instance.repair.error'

_repair = None
_repair_lock = threading.Lock()


def scan(context):
    """One scan by the one repair of this process."""
    global _repair
    with _repair_lock:
        if _repair is None:
            _repair = InstanceRepair()
    _repair.check_now(context)


def suspect_instances(stale_before):
    """The standalone instances that may need a start, from the database
    alone: (id, name, tenant id, server id) of every instance that is not
    deleted, belongs to no cluster, has a server and no task (or only a
    restart required), and whose heartbeat is older than ``stale_before``
    or was never recorded.
    """
    settled = [task.code for task in health.SETTLED_TASKS]
    with DBInstance.query() as query:
        rows = query.session.query(
            DBInstance.id, DBInstance.name, DBInstance.tenant_id,
            DBInstance.compute_instance_id
        ).select_from(DBInstance).outerjoin(
            InstanceServiceStatus,
            InstanceServiceStatus.instance_id == DBInstance.id
        ).filter(
            DBInstance.deleted == expression.false(),
            DBInstance.cluster_id.is_(None),
            DBInstance.compute_instance_id.isnot(None),
            DBInstance.task_id.in_(settled),
            or_(InstanceServiceStatus.updated_at.is_(None),
                InstanceServiceStatus.updated_at < stale_before))
        return rows.all()


class Quiet(object):
    """A suspect, as ``decide_start`` and the notification want it."""

    def __init__(self, row):
        self.id, self.name, self.tenant_id, self.compute_instance_id = row


def notify(event_type, instance, action, reason):
    try:
        payload = {
            'instance_id': instance.id,
            'instance_name': instance.name,
            'tenant_id': instance.tenant_id,
            'action': action,
            'reason': reason,
            'region': CONF.region,
        }
        rpc.get_notifier(service='taskmanager').info(
            trove_context.TroveContext(), event_type, payload)
    except Exception:
        LOG.exception("The repair of instance %s was not told.",
                      instance.id)


class InstanceRepair(object):
    """The repair of the standalone instances this replica covers."""

    def __init__(self, nova_factory=None, now=None, covered=None):
        self.nova_factory = nova_factory or clients.create_nova_client
        self._now = now or timeutils.utcnow
        # Which of the suspects this replica looks at: by default the
        # recovery hub's share of them.
        self._covered = covered or self._covered_by_hub
        # The instances this task manager started, and when: the end of
        # the repair is told when one is heard from again.
        self.started = {}

    @staticmethod
    def _covered_by_hub(context, ids):
        return recovery.hub(context).coordinator.covered(ids)

    def check_now(self, context):
        if not CONF.instance_repair_start:
            return
        stale_before = self._now() - timeutils.datetime.timedelta(
            seconds=CONF.agent_heartbeat_expiry)
        suspects = {row[0]: Quiet(row)
                    for row in suspect_instances(stale_before)}
        self._ended(suspects)
        if not suspects:
            return
        covered = self._covered(context, set(suspects))
        LOG.debug("The scan finds %d quiet instances, %d looked at here.",
                  len(suspects), len(covered))
        if not covered:
            return
        nova = self.nova_factory(context)
        computes = {}
        for instance_id in sorted(covered):
            instance = suspects[instance_id]
            try:
                decision = repair.decide_start(nova, instance, computes,
                                               now=self._now)
            except nova_exceptions.NotFound:
                # Its server is gone: not this repair's to mend.
                self.started.pop(instance_id, None)
                continue
            except Exception as err:
                LOG.warning("Instance %s not looked at for a start: %s",
                            instance_id, err)
                continue
            if decision.start:
                self._start(nova, instance, decision.reason)
            else:
                LOG.debug("Instance %s not started: %s", instance_id,
                          decision.reason)

    def _start(self, nova, instance, reason):
        LOG.info("Starting instance %s (%s): %s.", instance.name,
                 instance.id, reason)
        try:
            started = repair.start_server(nova, instance.compute_instance_id)
        except Exception as err:
            LOG.warning("Instance %s was not started: %s", instance.id, err)
            notify(EVENT_ERROR, instance, 'start',
                   '%s; the start failed: %s' % (reason, err))
            return
        if not started:
            LOG.info("Instance %s is being started already.", instance.id)
            return
        self.started[instance.id] = (instance, time.monotonic())
        notify(EVENT_START, instance, 'start', reason)

    def _ended(self, suspects):
        """The instances started here that are no longer quiet."""
        for instance_id in list(self.started):
            if instance_id in suspects:
                continue
            instance, since = self.started.pop(instance_id)
            LOG.info("Instance %s is back, %d s after its start.",
                     instance.name, time.monotonic() - since)
            notify(EVENT_END, instance, 'start',
                   'heard from again %d s after its start' %
                   (time.monotonic() - since))
