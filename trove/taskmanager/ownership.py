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

"""Which task manager process works on an instance or a cluster, and what
becomes of the work when that process is gone.

A task manager acknowledges an RPC message before it handles it, and keeps
the progress of a task (the waits for the servers, the steps left) in its
memory only. A task manager that stops on purpose finishes what it is
doing first (``trove.common.rpc.service``); one that dies (out of memory,
its node lost) leaves the instance or the cluster in its task for good,
and the API refuses to delete it.

So each task that a task manager carries out is recorded on its instance
or cluster (``task_owner``: this process, by a name no other process ever
has), and cleared when the handler returns. Every task manager holds a
lock named after its process on the ``[coordination]`` backend, kept by
the backend's lease; a task manager that can take the lock of an owner
knows that owner is gone. It then takes the task over, in one statement
against the others, and gives it up: a build ends in error, any other
task is cleared, a delete is left to be asked again, and a fault on the
instance says so. Nothing is resumed: what the task was given (passwords,
configuration) was only in the message.

Without a backend only the processes of the same host can be told gone: a
task manager that started again (its container restarted) takes over the
tasks of the processes before it there.
"""

import contextlib
import functools
import os
import socket
import uuid

from oslo_log import log as logging
from oslo_utils import timeutils

from trove.cluster import models as cluster_models
from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.instance import models as inst_models
from trove.instance.tasks import InstanceTasks

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

# This process, as no other process will ever be named: the host, the pid
# (which the next container on the host may get again) and a random part.
PROCESS = '%s:%s:%s' % (socket.gethostname(), os.getpid(),
                        uuid.uuid4().hex[:8])
LOCK = 'trove-taskmanager-%s'


def _host(owner):
    return owner.rsplit(':', 2)[0]


class Liveness(object):
    """The lock that tells the other task managers this process lives,
    and the means to tell whether the process of another task is gone.
    """

    def __init__(self):
        self._url = CONF.coordination.backend_url
        self._coordinator = None
        self._lock = None

    def start(self):
        if not self._url:
            LOG.info("No [coordination] backend_url: the tasks of a task "
                     "manager that died are taken over only by a task "
                     "manager started after it on the same host.")
            return
        self._hold()

    def _hold(self):
        from tooz import coordination as tooz_coordination
        try:
            if self._coordinator is None:
                coordinator = tooz_coordination.get_coordinator(
                    self._url, PROCESS.encode())
                coordinator.start(start_heart=True)
                self._coordinator = coordinator
            lock = self._coordinator.get_lock((LOCK % PROCESS).encode())
            if lock.acquire(blocking=False):
                self._lock = lock
                LOG.info("Holding the lock of task manager %s.", PROCESS)
            else:
                LOG.error("Cannot hold the lock of task manager %s.",
                          PROCESS)
        except Exception as ex:
            LOG.error("Cannot hold the lock of task manager %(p)s: %(ex)s. "
                      "Its tasks would be taken for abandoned only after it "
                      "is: trying again.", {'p': PROCESS, 'ex': ex})

    def _held(self):
        """Whether this process still holds its lock, held again when it
        was lost (a long stall lets the lease lapse).
        """
        if not self._url:
            return True
        if self._lock is not None:
            try:
                if self._lock.heartbeat():
                    return True
            except Exception as ex:
                LOG.warning("Cannot refresh the lock of task manager %(p)s: "
                            "%(ex)s", {'p': PROCESS, 'ex': ex})
            self._lock = None
        self._hold()
        return self._lock is not None

    def stop(self):
        if self._lock is not None:
            try:
                self._lock.release()
            except Exception as ex:
                LOG.warning("Failed to release the lock of task manager "
                            "%(p)s: %(ex)s", {'p': PROCESS, 'ex': ex})
            self._lock = None
        if self._coordinator is not None:
            try:
                self._coordinator.stop()
            except Exception:
                pass
            self._coordinator = None

    def gone(self, owner):
        """Whether the process ``owner`` is known to be gone; anything
        that cannot be told counts as alive.
        """
        if owner == PROCESS:
            return False
        if _host(owner) == _host(PROCESS):
            # The process before this one in the same container or host.
            return True
        if not self._url or not self._held():
            return False
        try:
            lock = self._coordinator.get_lock((LOCK % owner).encode())
            if not lock.acquire(blocking=False):
                return False
            lock.release()
            return True
        except Exception as ex:
            LOG.warning("Cannot tell whether task manager %(o)s lives: "
                        "%(ex)s", {'o': owner, 'ex': ex})
            return False


_liveness = None


def liveness():
    global _liveness
    if _liveness is None:
        _liveness = Liveness()
    return _liveness


def _model(kind):
    return (inst_models.DBInstance if kind == 'instance'
            else cluster_models.DBCluster)


def _set_owner(kind, target_id, owner, only_if=None):
    """``task_owner`` of the instance or cluster, in one statement; with
    ``only_if``, only while it is that owner. Whether it was set.
    """
    with _model(kind).query() as query:
        query = query.filter_by(id=target_id)
        if only_if is not None:
            query = query.filter_by(task_owner=only_if)
        return bool(query.update({'task_owner': owner},
                                 synchronize_session=False))


@contextlib.contextmanager
def owned(kind, target_id):
    """The task of the instance or cluster (``kind``) ``target_id`` is
    this process's while the block runs. Failing to record it never
    stops the task.
    """
    recorded = False
    if target_id:
        try:
            recorded = _set_owner(kind, target_id, PROCESS)
        except Exception:
            LOG.exception("Cannot record task manager %(p)s on %(k)s %(i)s.",
                          {'p': PROCESS, 'k': kind, 'i': target_id})
    try:
        yield
    finally:
        if recorded:
            try:
                _set_owner(kind, target_id, None, only_if=PROCESS)
            except Exception:
                LOG.exception("Cannot clear task manager %(p)s from %(k)s "
                              "%(i)s.", {'p': PROCESS, 'k': kind,
                                         'i': target_id})


def owning(kind):
    """The decorated task manager method carries out the task of the
    instance or cluster (``kind``) whose id is its ``<kind>_id``
    argument, or the first one after the context.
    """
    key = '%s_id' % kind

    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, context, *args, **kwargs):
            with owned(kind, kwargs.get(key, args[0] if args else None)):
                return f(self, context, *args, **kwargs)
        return wrapper
    return decorator


# Tasks of a cluster that change its members: a build that stopped half
# way leaves them unformed.
BUILDING_CLUSTER_TASKS = (ClusterTasks.BUILDING_INITIAL.code,
                          ClusterTasks.GROWING_CLUSTER.code,
                          ClusterTasks.ADDING_SHARD.code)


def take_over_abandoned():
    """Give up the tasks whose task manager is gone. Returns how many."""
    taken = 0
    for kind in ('cluster', 'instance'):
        with _model(kind).query() as query:
            rows = query.filter(
                _model(kind).task_owner.isnot(None)).filter_by(
                    deleted=False).all()
        verdicts = {}
        for row in rows:
            owner = row.task_owner
            if owner not in verdicts:
                verdicts[owner] = liveness().gone(owner)
            if not verdicts[owner]:
                continue
            # Against another task manager doing the same.
            if not _set_owner(kind, row.id, None, only_if=owner):
                continue
            taken += 1
            try:
                if kind == 'cluster':
                    _give_up_cluster(row, owner)
                else:
                    _give_up_instance(row.id, owner)
            except Exception:
                LOG.exception("Failed to give up the task of %(k)s %(i)s.",
                              {'k': kind, 'i': row.id})
    return taken


def _reason(task, owner):
    return ("The task manager doing it (%(o)s) stopped before it "
            "finished: %(t)s" % {'o': owner, 't': getattr(
                task, 'db_text', None) or task.description})


def _give_up_instance(instance_id, owner):
    db_instance = inst_models.DBInstance.find_by(id=instance_id,
                                                 deleted=False)
    task = db_instance.task_status
    if task == InstanceTasks.NONE or task.is_error:
        return
    if task == InstanceTasks.DELETING:
        LOG.warning("Instance %(i)s: task manager %(o)s stopped while "
                    "deleting it; delete it again.",
                    {'i': instance_id, 'o': owner})
        message = "The delete was not finished: delete it again."
    elif task == InstanceTasks.BUILDING:
        LOG.warning("Instance %(i)s: task manager %(o)s stopped while "
                    "building it; it is in error.",
                    {'i': instance_id, 'o': owner})
        db_instance.set_task_status(InstanceTasks.BUILDING_ERROR_ABANDONED)
        db_instance.save()
        message = "The build was not finished."
    else:
        LOG.warning("Instance %(i)s: task manager %(o)s stopped during "
                    "%(t)s; the task is cleared.",
                    {'i': instance_id, 'o': owner, 't': task.action})
        db_instance.set_task_status(InstanceTasks.NONE)
        db_instance.save()
        message = ("%s was not finished: check the instance before "
                   "going on." % task.action.capitalize())
    inst_models.save_instance_fault(instance_id, message,
                                    _reason(task, owner))


def _give_up_cluster(db_cluster, owner):
    task = db_cluster.task_status
    if task in (ClusterTasks.NONE, ClusterTasks.DELETING):
        if task == ClusterTasks.DELETING:
            LOG.warning("Cluster %(c)s: task manager %(o)s stopped while "
                        "deleting it; delete it again.",
                        {'c': db_cluster.id, 'o': owner})
        return
    LOG.warning("Cluster %(c)s: task manager %(o)s stopped during %(t)s; "
                "the task is cleared.",
                {'c': db_cluster.id, 'o': owner, 't': task.name})
    if task.code in BUILDING_CLUSTER_TASKS:
        failed = (InstanceTasks.BUILDING_ERROR_ABANDONED
                  if task == ClusterTasks.BUILDING_INITIAL
                  else InstanceTasks.GROWING_ERROR)
        for db_instance in inst_models.DBInstance.find_all(
                cluster_id=db_cluster.id, deleted=False).all():
            # A member being added is still building; a member of the
            # initial build never was formed into the cluster.
            if (task == ClusterTasks.BUILDING_INITIAL or
                    db_instance.task_status == InstanceTasks.BUILDING):
                db_instance.set_task_status(failed)
                db_instance.save()
                inst_models.save_instance_fault(
                    db_instance.id, "The cluster task was not finished.",
                    _reason(task, owner))
    with cluster_models.DBCluster.query() as query:
        query.filter_by(id=db_cluster.id, task_id=task.code).update(
            {'task_id': ClusterTasks.NONE.code,
             'updated': timeutils.utcnow()}, synchronize_session=False)
