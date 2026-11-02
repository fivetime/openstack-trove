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

"""The repair's plugin of Vertica: the replacement of a lost member.

Every node sees the whole database (the nodes table): the cluster can
lose a member when every other member answers, none holds the database
down, and each holds that member's address out and no other
(``common.replace.PeersHoldItOut``; a node is out while it is not UP).

A Vertica cluster neither grows nor shrinks here (the Community Edition
allows three nodes, and three nodes at K-safety 1 cannot lose one), so
the lost member is not removed from the database: its member is deleted
and its node stays in the catalog, DOWN (the database runs on the two
others, as it did while the member was lost). A member like it is then
made, gets the cluster's secrets, and takes the node's place at its own
address: the node starts there with empty directories and recovers its
catalog and data from the others (vcluster start_node, which changes
the node's address in the catalog). The others get the configuration
with the new address.

Only Enterprise Mode: an Eon Mode node also has a depot and subscribes
to shards of the communal storage, which this was not tried with.
"""

from trove.common import exception
from trove.common.i18n import _
from trove.common.strategies.cluster.experimental.common import replace

EON = 'eon'


class VerticaReplace(replace.PeersHoldItOut):

    def applies(self, db_cluster):
        if db_cluster.get_properties().get('storage_mode') == EON:
            return False, 'a member of an Eon Mode cluster is not replaced'
        return True, None

    def remember(self, context, db_cluster, lost, others):
        """The lost node's address, which the database knows it by: the
        one every other member holds out.
        """
        outs = set()
        for member in others:
            view = self.guest_factory(
                context, member.id, self.manager).get_cluster_health()
            outs.add(frozenset((view or {}).get('out') or []))
        if len(outs) != 1:
            raise exception.TroveError(_("The members hold different "
                                         "nodes out."))
        out = next(iter(outs))
        if len(out) != 1:
            raise exception.TroveError(_("The members hold %d nodes out.")
                                       % len(out))
        return {'address': next(iter(out)), 'type': lost.type}

    def remove(self, context, db_cluster, state):
        from trove.taskmanager import api as task_api
        task_api.load(context, self.manager).vertica_remove_member(
            db_cluster.id, state['lost'])

    def add(self, context, db_cluster, state):
        from trove.cluster import models as cluster_models
        from trove.common import server_group as srv_grp
        from trove.taskmanager import api as task_api
        cluster = cluster_models.Cluster.load(context, db_cluster.id)
        spec = dict(state['spec'])
        locality = srv_grp.ServerGroup.convert_to_hint(cluster.server_group)
        instance_id = cluster.create_replacement(
            spec, state.get('type') or 'member', locality)
        task_api.load(context, self.manager).vertica_take_over_node(
            db_cluster.id, instance_id, state['address'])
