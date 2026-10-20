#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

"""The cluster calls every member's guest agent manager answers, whatever
the datastore: the cluster probe that watches where the member stands and
keeps its ready port, and the calls that tell the probe and the task
manager what they need. A datastore's manager lists the mixin before the
manager class it extends and adds the calls that are its own (how a
member is installed, how the cluster is formed again).
"""

from oslo_log import log as logging

from trove.guestagent.common import cluster_probe

LOG = logging.getLogger(__name__)


def health_of(app):
    """What the member sees of its cluster, for the task manager; a
    member whose database does not answer is down.
    """
    watched = getattr(app, 'is_watched_member', None)
    if watched is not None and not watched():
        # A member that watches nothing (a MongoDB query router).
        return None
    tell = getattr(app, 'cluster_health', None)
    if tell is None:
        return None
    try:
        return tell()
    except Exception as err:
        LOG.warning("Cannot tell the health of the cluster: %s", err)
        return {'down': True, 'reason': 'the database does not answer'}


class ClusterMemberMixin(object):

    cluster_probe = None
    # The extended property the cluster's mode comes with, kept on the
    # member for the task manager to ask back; None: no mode.
    CLUSTER_MODE_KEY = None

    def init_cluster_probe(self):
        """Start the cluster probe; a manager without one is still a
        manager.
        """
        try:
            self.cluster_probe = cluster_probe.ClusterProbe(
                self.app, self.docker_client)
            self.cluster_probe.start()
        except Exception:
            LOG.exception("The cluster probe could not be set up.")

    def do_prepare(self, context, packages, databases, memory_mb, users,
                   device_path, mount_point, backup_info,
                   config_contents, root_password, overrides,
                   cluster_config, snapshot, ds_version=None):
        super(ClusterMemberMixin, self).do_prepare(
            context, packages, databases, memory_mb, users, device_path,
            mount_point, backup_info, config_contents, root_password,
            overrides, cluster_config, snapshot, ds_version=ds_version)
        self.keep_cluster_mode_of(cluster_config)

    def keep_cluster_mode_of(self, cluster_config):
        """The mode the member was created with, kept for the task
        manager to set the cluster up with; the members of a grown
        cluster get the cluster's. For a manager whose ``do_prepare``
        does not call this mixin's.
        """
        key = self.CLUSTER_MODE_KEY
        if key and cluster_config and cluster_config.get(key):
            self.app.keep_cluster_mode(cluster_config[key])

    def get_cluster_context(self, context):
        LOG.debug("Getting the cluster context.")
        return self.app.get_cluster_context()

    def cluster_complete(self, context):
        """Every member has joined."""
        super(ClusterMemberMixin, self).cluster_complete(context)
        if self.cluster_probe:
            self.cluster_probe.enable_complete()

    def leave_cluster(self, context):
        """The probe first: the port closes, and nothing brings the member
        back into the cluster it is leaving.
        """
        if self.cluster_probe:
            self.cluster_probe.disable()
        self.app.leave_group()

    def is_writable_member(self, context):
        return self.app.is_writable_member()

    def get_recovery_view(self, context):
        """Where the member stands, for the task manager's recovery."""
        return self.app.recovery_view()

    def get_member_role(self, context):
        return self.app.get_member_role()

    def get_cluster_health(self, context):
        return health_of(self.app)


class ClusterWatchMixin(object):
    """The cluster calls of a datastore whose clusters heal themselves:
    the watch that tells the control plane what the member sees of its
    cluster, and the call the task manager asks it with. Listed before the
    manager class it extends.
    """

    cluster_watch = None

    def init_cluster_watch(self):
        """Start the watch; a manager without one is still a manager."""
        try:
            self.cluster_watch = cluster_probe.ClusterWatch(self.app)
            self.cluster_watch.start()
        except Exception:
            LOG.exception("The cluster watch could not be set up.")

    def cluster_complete(self, context):
        """Every member has joined: the watch tells from now on."""
        super(ClusterWatchMixin, self).cluster_complete(context)
        if self.cluster_watch:
            self.cluster_watch.enable_complete()

    def get_cluster_health(self, context):
        return health_of(self.app)

    def get_member_role(self, context):
        """The member's state and role in its cluster as its database
        has them now (a Redis master or replica, a MongoDB primary or
        secondary); None for a member that watches nothing.
        """
        watched = getattr(self.app, 'is_watched_member', None)
        if watched is not None and not watched():
            return None
        view = self.app.member_view()
        return {'state': view.state, 'role': view.role}
