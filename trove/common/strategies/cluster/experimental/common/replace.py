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

"""The repair's replacement of a member that is lost for good: removed
the way the cluster removes a member (its shrink), and one like it added
the way the cluster adds one (its grow).

A member is lost when two sides say so, never one alone:

* Nova: its server is gone, in ERROR, or off and the repair gave up
  starting it, while the compute service of its host is up (Nova's word
  on a host it does not hear from is old: a server deleted there may run
  on). A host that is down is Masakari's, not the repair's.
* Its peers: every other member answers and holds it out of the cluster
  (the datastore's plugin, ``can_replace``). A member only the control
  plane cannot reach, that its peers still see, is not lost.

The datastores keep a member that comes back after its replacement out
by themselves (quorum, election epochs, the replica set's configuration)
mostly; the order keeps the identity single anyway: the old member is
deleted (its server, its ports) before the cluster forgets it, and the
new one is added after. The cluster is never larger than it was, so the
replacement needs no quota beyond the tenant's.

The replacement goes in steps, each a look of the recovery hub, kept in
the cluster's properties (``repair``) so that any task manager follows
it, one that starts again too: ``removing`` (the shrink is under way),
``adding`` (the grow is), then done. A step that does not come out as it
should ends the replacement, told as an error, and leaves the cluster to
the operator: the member is not replaced again by itself (``repair_failed``
in the properties names it) until it is gone.

A datastore takes part with a plugin in the namespace
``trove.cluster.repair``, by its manager's name: a ``ReplacePlugin``.
Without one, a lost member is only told.
"""

import json

from novaclient import exceptions as nova_exceptions
from oslo_log import log as logging
from oslo_utils import timeutils
from stevedore import driver

from trove.cluster.tasks import ClusterTasks
from trove.common import cfg
from trove.common import clients
from trove.common import cluster_health
from trove.common import context as trove_context
from trove.common.notification import DBaaSClusterGrow
from trove.common.notification import DBaaSClusterShrink
from trove.common.notification import StartNotification
from trove.common.strategies.cluster.experimental.common import health

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

NAMESPACE = 'trove.cluster.repair'
STATE = 'repair'
# The member whose replacement failed: not replaced again by itself (the
# operator's, until it is gone or another replacement succeeds).
FAILED = 'repair_failed'
REMOVING, ADDING = 'removing', 'adding'
# The cluster's task while a step is under way.
STEP_TASKS = {REMOVING: ClusterTasks.SHRINKING_CLUSTER,
              ADDING: ClusterTasks.GROWING_CLUSTER}
# Seconds between two looks while a step is under way.
FOLLOW_EVERY = 30


class ReplacePlugin(object):
    """What a datastore gives for the repair to replace one of its
    members; made with the datastore's manager name.
    """

    def __init__(self, manager):
        self.manager = manager

    def can_replace(self, context, db_cluster, lost, others):
        """Whether the cluster can lose the member now, and why: every
        other member answers, holds it out, and serves without it.
        Returns (bool, reason).
        """
        raise NotImplementedError()


class PeersHoldItOut(ReplacePlugin):
    """The plugin of a datastore whose members each tell the members they
    see out (``get_cluster_health``: ``out`` and ``me`` named alike, as
    the sentinels, Galera and Group Replication do): the cluster can lose
    a member when every other member answers, none holds the cluster
    down, and each holds that member out and no other.

    A datastore narrows it with ``applies`` (the clusters it knows how to
    replace a member of) and ``vouches`` (what a member's view must say);
    ``min_others`` is the members the cluster must keep.
    """

    min_others = 2

    def __init__(self, manager, guest_factory=None):
        super(PeersHoldItOut, self).__init__(manager)
        self.guest_factory = guest_factory or clients.create_guest_client

    def applies(self, db_cluster):
        """(bool, reason): whether a member of the cluster is replaced."""
        return True, None

    def vouches(self, member, view, lost_name):
        """(bool, reason): whether the member's view holds the lost one
        out, and it alone.
        """
        if view.get('status') == cluster_health.DOWN:
            return False, 'member %s holds the cluster down: %s' % (
                member.name, view.get('reason'))
        out = set(view.get('out') or [])
        if out != {lost_name}:
            return False, 'member %s holds out %s, not %s alone' % (
                member.name, ', '.join(sorted(out)) or 'none', lost_name)
        return True, None

    def can_replace(self, context, db_cluster, lost, others):
        ok, why = self.applies(db_cluster)
        if not ok:
            return False, why
        if len(others) < self.min_others:
            return False, 'fewer than %d members would be left' % (
                self.min_others)
        # A datastore whose health its recovery records has no such
        # option: the guest's own timeout for the call then.
        timeout = getattr(CONF.get(self.manager), 'cluster_health_timeout',
                          None)
        views = []
        for member in others:
            try:
                view = self.guest_factory(
                    context, member.id, self.manager).get_cluster_health(
                        timeout=timeout)
            except Exception as err:
                return False, 'member %s does not answer: %s' % (
                    member.name, err)
            if not view:
                return False, 'member %s tells nothing' % member.name
            views.append((member, view))
        lost_name = self.name_of(db_cluster, lost, views)
        if not lost_name:
            return False, 'its address is not known'
        for member, view in views:
            ok, why = self.vouches(member, view, lost_name)
            if not ok:
                return False, why
        return True, 'every other member holds %s out' % lost_name

    @staticmethod
    def name_of(db_cluster, lost, views):
        """The lost member's name as the datastore names it: as the
        cluster's health learnt it, or, where that never learnt it (the
        health a recovery records, from views without ``me``), the one
        member every other holds out, alike, and none of them is.
        """
        known = next((name for name, trove_name in
                      health.known_members(db_cluster.id).items()
                      if trove_name == lost.name), None)
        if known:
            return known
        outs = {frozenset(view.get('out') or []) for _m, view in views}
        mine = {view.get('me') for _m, view in views}
        if len(outs) != 1:
            return None
        out = next(iter(outs))
        if len(out) != 1 or out & mine:
            return None
        return next(iter(out))


_plugins = {}


def plugin_for(manager):
    """The datastore's plugin, or None."""
    if manager not in _plugins:
        try:
            _plugins[manager] = driver.DriverManager(
                NAMESPACE, manager, invoke_on_load=True,
                invoke_args=(manager,)).driver
        except Exception as err:
            LOG.debug("No repair plugin for %s: %s", manager, err)
            _plugins[manager] = None
    return _plugins[manager]


class Lost(object):
    def __init__(self, lost, reason):
        self.lost = lost
        self.reason = reason


class Gone(object):
    """A member as the state keeps it once it is deleted: for the
    notifications.
    """

    def __init__(self, member_id, name):
        self.id = member_id
        self.name = name


def tenant_context(db_cluster, action=None, user_id=None):
    """The task manager acting on the tenant's cluster: its project, the
    service's rights, the user the cluster's members were made for (a new
    member's server names it, as theirs do); with the notification of
    the cluster's ``action`` (a ``DBaaSCluster...`` class), as the API
    sets one on a request: the task manager's step of the action ends it.
    """
    context = trove_context.TroveContext(project_id=db_cluster.tenant_id,
                                         user_id=user_id, is_admin=True)
    if action is not None:
        context.notification = action(
            context, request_id=context.request_id,
            server_type='taskmanager', tenant_id=db_cluster.tenant_id,
            client_ip=CONF.host, server_ip=CONF.host)
    return context


class MemberReplacement(object):
    """The replacement of the members of the clusters of every
    datastore that has a plugin.
    """

    def __init__(self, notify, now=None, plugins=None):
        self.notify = notify
        self._now = now or timeutils.utcnow
        self.plugins = plugins or plugin_for
        # The clusters that had a lost member lately, and when: many at
        # once is an outage of the platform, not of members.
        self.lately = {}

    # Whether a member is lost

    def lost_decision(self, nova, member, start_decision, compute_up):
        """Nova's side of whether a quiet member is lost."""
        try:
            server = nova.servers.get(member.compute_instance_id)
        except nova_exceptions.NotFound:
            host = self._host_of_deleted(nova, member.compute_instance_id)
            if not host:
                return Lost(False, 'its server is gone, on a host not known')
            if not compute_up(host):
                return Lost(False, 'its server is gone while the compute '
                                   'service of host %s is not up' % host)
            return Lost(True, 'its server is gone')
        host = getattr(server, 'OS-EXT-SRV-ATTR:host', None)
        if server.status == 'ERROR':
            if not compute_up(host):
                return Lost(False, 'in ERROR on host %s, whose compute '
                                   'service is not up' % host)
            return Lost(True, 'its server is in ERROR')
        if server.status == 'SHUTOFF' and start_decision is not None and \
                'given up' in start_decision.reason:
            return Lost(True, 'its server is off: %s' % start_decision.reason)
        return Lost(False, 'its server is %s' % server.status)

    @staticmethod
    def _host_of_deleted(nova, server_id):
        try:
            found = nova.servers.list(search_opts={
                'uuid': server_id, 'deleted': True, 'all_tenants': True})
        except Exception as err:
            LOG.debug("Deleted server %s not looked up: %s", server_id, err)
            return None
        for server in found:
            return getattr(server, 'OS-EXT-SRV-ATTR:host', None)
        return None

    def _breaker_open(self, cluster_id):
        now = self._now()
        for key, when in list(self.lately.items()):
            if (now - when).total_seconds() > CONF.cluster_repair_window:
                del self.lately[key]
        self.lately[cluster_id] = now
        return len(self.lately) > CONF.cluster_repair_breaker

    # The steps

    def begin(self, context, db_cluster, manager, members, lost, reason):
        """A member is lost as Nova tells: replace it if the plugin says
        the cluster can lose it, the breaker is closed and the cluster is
        free. Returns the seconds until the next look, or None.
        """
        plugin = self.plugins(manager)
        if plugin is None:
            LOG.warning("Cluster %s: member %s is lost (%s); its datastore "
                        "%s has no repair plugin.", db_cluster.id, lost.id,
                        reason, manager)
            return None
        failed = db_cluster.get_properties().get(FAILED) or {}
        if failed.get('lost') == lost.id:
            LOG.debug("Cluster %s: member %s is lost (%s); its replacement "
                      "failed (%s): the operator's.", db_cluster.id,
                      lost.id, reason, failed.get('error'))
            return None
        others = [m for m in members if m.id != lost.id]
        ok, why = plugin.can_replace(context, db_cluster, lost, others)
        if not ok:
            LOG.info("Cluster %s: member %s is lost (%s) but is not "
                     "replaced: %s.", db_cluster.id, lost.id, reason, why)
            return FOLLOW_EVERY * 4
        if self._breaker_open(db_cluster.id):
            LOG.warning("Cluster %s: member %s is lost, but %d clusters had "
                        "a lost member within %d s: no replacement until "
                        "fewer do (the platform, not the members?).",
                        db_cluster.id, lost.id, len(self.lately),
                        CONF.cluster_repair_window)
            return FOLLOW_EVERY * 4
        user_id = self._user_of(context, others)
        if not user_id:
            LOG.warning("Cluster %s: member %s is lost (%s), but no member "
                        "tells the user it was made for: not replaced.",
                        db_cluster.id, lost.id, reason)
            return FOLLOW_EVERY * 4
        spec = self._spec(context, db_cluster, lost, others)
        state = {'phase': REMOVING, 'lost': lost.id, 'name': lost.name,
                 'spec': spec, 'user': user_id,
                 'before': sorted(m.id for m in others),
                 'since': self._now().isoformat(),
                 'reason': '%s; %s' % (reason, why)}
        if not self._take(db_cluster, ClusterTasks.SHRINKING_CLUSTER):
            return None
        self._save(db_cluster, state)
        LOG.info("Cluster %s: replacing member %s (%s): %s.", db_cluster.id,
                 lost.name, lost.id, state['reason'])
        self.notify('trove.cluster.repair.start', db_cluster, lost,
                    'replace', state['reason'])
        try:
            from trove.taskmanager import api as task_api
            context = tenant_context(db_cluster, DBaaSClusterShrink,
                                     state['user'])
            with StartNotification(context, cluster_id=db_cluster.id,
                                   instance_ids=[lost.id]):
                task_api.load(context, manager).shrink_cluster(
                    db_cluster.id, [lost.id])
        except Exception as err:
            self._end(db_cluster, state, 'the removal was not sent: %s' % err)
            self._release(db_cluster)
        return FOLLOW_EVERY

    def follow(self, context, db_cluster, manager, members, state):
        """The next step of a replacement under way. Returns the seconds
        until the next look, or None once it ended.
        """
        since = timeutils.normalize_time(
            timeutils.parse_isotime(state['since']))
        if (self._now() - since).total_seconds() > \
                CONF.cluster_repair_replace_timeout:
            held = db_cluster.task_status == STEP_TASKS[state['phase']]
            self._end(db_cluster, state, 'not done within %d s' %
                      CONF.cluster_repair_replace_timeout)
            if held:
                # A step whose task never ended (it failed before it
                # could end it) holds the cluster no longer: the task of
                # the step is the replacement's, taken by it.
                self._release(db_cluster)
            return None
        if db_cluster.task_status != ClusterTasks.NONE:
            return FOLLOW_EVERY
        ids = {m.id for m in members}
        if state['phase'] == REMOVING:
            if state['lost'] in ids:
                self._end(db_cluster, state, 'the shrink did not remove it')
                return None
            return self._add(db_cluster, manager, state)
        new = [m for m in members if m.id not in state['before']]
        if not new:
            self._end(db_cluster, state, 'the grow added no member')
            return None
        if any(m.task_status not in health.SETTLED_TASKS for m in new):
            self._end(db_cluster, state, 'the new member %s failed' %
                      ', '.join(m.name for m in new))
            return None
        if health.status_of(db_cluster.id) != cluster_health.HEALTHY:
            return FOLLOW_EVERY
        self._end(db_cluster, state, None, new=new)
        return None

    def _add(self, db_cluster, manager, state):
        from trove.cluster import models as cluster_models
        from trove.common import glance as common_glance
        context = tenant_context(db_cluster, DBaaSClusterGrow,
                                 state.get('user'))
        # The step is the grow's task, taken as the shrink's was: of two
        # task managers that heard the shrink end, one adds.
        if not self._take(db_cluster, ClusterTasks.GROWING_CLUSTER):
            return FOLLOW_EVERY
        state = dict(state, phase=ADDING)
        self._save(db_cluster, state)
        try:
            # Loaded after the step is taken: the grow saves the cluster
            # as it loaded it, properties too.
            cluster = cluster_models.Cluster.load(context, db_cluster.id)
            image_id = cluster.ds_version.image_id
            if not image_id:
                image_id = common_glance.get_image_id(
                    clients.create_glance_client(context), None,
                    cluster.ds_version.image_tags)
            with StartNotification(context, cluster_id=db_cluster.id):
                cluster.grow([dict(state['spec'])], image_id, held=True)
        except Exception as err:
            LOG.exception("Cluster %s: the member replacing %s was not "
                          "added.", db_cluster.id, state['lost'])
            self._end(db_cluster, state, 'the grow failed: %s' % err)
            self._release(db_cluster)
            return None
        LOG.info("Cluster %s: adding a member in place of %s.",
                 db_cluster.id, state['name'])
        return FOLLOW_EVERY

    @staticmethod
    def _user_of(context, others):
        """The user the cluster's members were made for, as their
        servers tell (``trove_user_id``).
        """
        nova = clients.create_nova_client(context)
        for peer in others:
            try:
                server = nova.servers.get(peer.compute_instance_id)
            except Exception as err:
                LOG.debug("The server of %s not looked up: %s", peer.id, err)
                continue
            user_id = (getattr(server, 'metadata', None) or {}).get(
                'trove_user_id')
            if user_id:
                return user_id
        return None

    def _spec(self, context, db_cluster, lost, others):
        """The new member, like the lost one: its flavor and volume, its
        volume's type and zone, the network of the cluster.
        """
        spec = {'flavor_id': lost.flavor_id}
        if lost.volume_size:
            spec['volume_size'] = lost.volume_size
        volume_of = lost if lost.volume_id else (
            next((m for m in others if m.volume_id), None))
        if volume_of is not None:
            try:
                volume = clients.create_cinder_client(context).volumes.get(
                    volume_of.volume_id)
                if getattr(volume, 'volume_type', None):
                    spec['volume_type'] = volume.volume_type
                if lost.volume_id and getattr(volume, 'availability_zone',
                                              None):
                    spec['availability_zone'] = volume.availability_zone
            except Exception as err:
                LOG.debug("The volume of %s not looked up: %s",
                          volume_of.id, err)
        for peer in others:
            try:
                ports = clients.create_neutron_client(context).list_ports(
                    name='trove-%s' % peer.id).get('ports', [])
            except Exception as err:
                LOG.debug("The ports of %s not looked up: %s", peer.id, err)
                continue
            # A member's ports are named alike: the management network's
            # too, which Trove adds to every member by itself.
            tenant = [p['network_id'] for p in ports
                      if p['network_id'] not in CONF.management_networks]
            if tenant:
                spec['nics'] = [{'network_id': tenant[0]}]
                break
        return spec

    # The state, the cluster's task

    @staticmethod
    def _swap(db_cluster, props):
        """Saves the properties if they are still as the cluster was
        read: of two task managers that follow the same step, one moves
        it on. Returns whether this one did.
        """
        from trove.cluster import models as cluster_models
        after = json.dumps(props, sort_keys=True) if props else None
        with cluster_models.DBCluster.query() as query:
            moved = query.filter_by(
                id=db_cluster.id, properties=db_cluster.properties).update(
                    {'properties': after, 'updated': timeutils.utcnow()},
                    synchronize_session=False)
        if moved:
            db_cluster.properties = after
        return bool(moved)

    @staticmethod
    def _save(db_cluster, state):
        props = db_cluster.get_properties()
        if state is None:
            props.pop(STATE, None)
        else:
            props[STATE] = state
        db_cluster.save_properties(props)

    @staticmethod
    def _take(db_cluster, task):
        from trove.cluster import models as cluster_models
        with cluster_models.DBCluster.query() as query:
            taken = query.filter_by(
                id=db_cluster.id, task_id=ClusterTasks.NONE.code).update(
                    {'task_id': task.code, 'updated': timeutils.utcnow()},
                    synchronize_session=False)
        if taken:
            db_cluster.task_status = task
        return bool(taken)

    @staticmethod
    def _release(db_cluster):
        from trove.cluster import models as cluster_models
        with cluster_models.DBCluster.query() as query:
            query.filter_by(id=db_cluster.id).update(
                {'task_id': ClusterTasks.NONE.code,
                 'updated': timeutils.utcnow()},
                synchronize_session=False)

    def _end(self, db_cluster, state, error, new=()):
        props = db_cluster.get_properties()
        props.pop(STATE, None)
        props.pop(FAILED, None)
        if error:
            props[FAILED] = {'lost': state['lost'], 'name': state['name'],
                             'error': error, 'at': self._now().isoformat()}
        if not self._swap(db_cluster, props):
            # The other task manager ended it, and told.
            return
        gone = Gone(state['lost'], state['name'])
        if error:
            LOG.error("Cluster %s: the replacement of member %s ended: %s.",
                      db_cluster.id, state['name'], error)
            self.notify('trove.cluster.repair.error', db_cluster, gone,
                        'replace', error)
            return
        LOG.info("Cluster %s: member %s replaced by %s.", db_cluster.id,
                 state['name'], ', '.join(m.name for m in new))
        self.notify('trove.cluster.repair.end', db_cluster, gone, 'replace',
                    'replaced by %s' % ', '.join(m.name for m in new))
