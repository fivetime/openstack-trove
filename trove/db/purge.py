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

"""Purging the rows of what was deleted long ago.

Trove deletes instances, clusters, backups, configuration groups,
modules, security groups and datastore version metadata softly: the
row stays, with ``deleted`` set and ``deleted_at`` the time, so that
the management API can still show what a deleted instance was. Nothing
removed those rows, nor the rows that only the instance's id ties to
it (its service status, the conductor's last-seen marks, its root
history, its faults and modules): at a steady churn the tables grew
without bound. ``trove-manage db_purge`` removes the rows of what was
deleted more than a number of days ago, children before parents and
never a row something live still refers to.
"""

import collections
import contextlib
import datetime

from oslo_log import log as logging
import sqlalchemy as sa

from trove.backup.models import DBBackup
from trove.cluster.models import DBCluster
from trove.common import timeutils
from trove.conductor.models import LastSeen
from trove.configuration.models import DBConfiguration
from trove.configuration.models import DBConfigurationParameter
from trove.datastore.models import DBDatastoreVersionMetadata
from trove.db import get_db_api
from trove.extensions.common.models import RootHistory
from trove.extensions.security_group import models as secgroup_models
from trove.instance.models import DBInstance
from trove.instance.models import DBInstanceFault
from trove.instance.models import InstanceServiceStatus
from trove.module.models import DBInstanceModule
from trove.module.models import DBModule

SecurityGroup = secgroup_models.SecurityGroup
SecurityGroupRule = secgroup_models.SecurityGroupRule
SecurityGroupInstanceAssociation = (
    secgroup_models.SecurityGroupInstanceAssociation)

LOG = logging.getLogger(__name__)

# Rows are removed in batches of this many parents, so that one
# statement never locks a whole table of a busy deployment.
BATCH = 1000


@contextlib.contextmanager
def _query(model):
    query = get_db_api()._base_query(model)
    yield query
    query.session.commit()


def _ids_in(column, ids):
    return column.in_(list(ids))


def _referenced(column):
    """A subquery of the values a column holds (the rows referring to
    something): what is referred to is kept.
    """
    return sa.select(column).where(column.isnot(None)).scalar_subquery()


class Purge(object):
    """One purge of what was deleted before a cutoff time; ``counts``
    holds the rows removed (or, when dry, those that would be) per
    table in the order they are removed.
    """

    def __init__(self, cutoff, dry_run=False, batch=BATCH):
        self.cutoff = cutoff
        self.dry_run = dry_run
        self.batch = batch
        self.counts = collections.OrderedDict()

    def _old(self, model):
        return sa.and_(model.deleted == sa.true(),
                       model.deleted_at < self.cutoff)

    def _count(self, table, n):
        self.counts[table] = self.counts.get(table, 0) + n

    def _remove(self, model, *conditions):
        """Remove (or count) the rows of a model that match."""
        with _query(model) as query:
            query = query.filter(*conditions)
            if self.dry_run:
                n = query.count()
            else:
                n = query.delete(synchronize_session=False)
        self._count(model._table_name, n)
        return n

    def _parents(self, model, *conditions):
        """The ids of the deleted-long-ago rows of a model that match,
        a batch at a time; what a batch removes does not come back.
        A dry run walks on instead.
        """
        offset = 0
        while True:
            with _query(model) as query:
                ids = [row[0] for row in query.filter(
                    self._old(model), *conditions
                ).with_entities(model.id).order_by(model.deleted_at)
                    .offset(offset).limit(self.batch).all()]
            if not ids:
                return
            yield ids
            if self.dry_run:
                offset += len(ids)

    def run(self):
        self.instances()
        self.clusters()
        self.backups()
        self.configurations()
        self.modules()
        self.security_groups()
        self.datastore_version_metadata()
        return self.counts

    def instances(self):
        """A deleted instance and everything only its id ties to it. An
        instance a replica still points to (its primary) stays.
        """
        for ids in self._parents(
                DBInstance,
                DBInstance.id.notin_(_referenced(DBInstance.slave_of_id))):
            self._remove(SecurityGroupInstanceAssociation,
                         _ids_in(SecurityGroupInstanceAssociation.instance_id,
                                 ids))
            self._remove(DBInstanceModule,
                         _ids_in(DBInstanceModule.instance_id, ids))
            self._remove(DBInstanceFault,
                         _ids_in(DBInstanceFault.instance_id, ids))
            self._remove(InstanceServiceStatus,
                         _ids_in(InstanceServiceStatus.instance_id, ids))
            self._remove(LastSeen, _ids_in(LastSeen.instance_id, ids))
            self._remove(RootHistory, _ids_in(RootHistory.id, ids))
            self._remove(DBInstance, _ids_in(DBInstance.id, ids))

    def clusters(self):
        """A deleted cluster no instance row (purged or not) is in."""
        for ids in self._parents(
                DBCluster,
                DBCluster.id.notin_(_referenced(DBInstance.cluster_id))):
            self._remove(DBCluster, _ids_in(DBCluster.id, ids))

    def backups(self):
        """A deleted backup that is not the parent of a live one."""
        live_children = sa.select(DBBackup.parent_id).where(
            DBBackup.parent_id.isnot(None),
            DBBackup.deleted == sa.false()).scalar_subquery()
        for ids in self._parents(DBBackup, DBBackup.id.notin_(live_children)):
            self._remove(DBBackup, _ids_in(DBBackup.id, ids))

    def configurations(self):
        """A deleted configuration group no instance or cluster row
        refers to, with its parameters; and the parameters deleted
        from live groups.
        """
        for ids in self._parents(
                DBConfiguration,
                DBConfiguration.id.notin_(
                    _referenced(DBInstance.configuration_id)),
                DBConfiguration.id.notin_(
                    _referenced(DBCluster.configuration_id))):
            self._remove(DBConfigurationParameter,
                         _ids_in(DBConfigurationParameter.configuration_id,
                                 ids))
            self._remove(DBConfiguration, _ids_in(DBConfiguration.id, ids))
        self._remove(DBConfigurationParameter,
                     self._old(DBConfigurationParameter))

    def modules(self):
        """The modules removed from instances long ago, and the deleted
        modules no instance module row refers to.
        """
        self._remove(DBInstanceModule, self._old(DBInstanceModule))
        for ids in self._parents(
                DBModule,
                DBModule.id.notin_(_referenced(DBInstanceModule.module_id))):
            self._remove(DBModule, _ids_in(DBModule.id, ids))

    def security_groups(self):
        self._remove(SecurityGroupInstanceAssociation,
                     self._old(SecurityGroupInstanceAssociation))
        self._remove(SecurityGroupRule, self._old(SecurityGroupRule))
        for ids in self._parents(
                SecurityGroup,
                SecurityGroup.id.notin_(
                    _referenced(SecurityGroupRule.group_id)),
                SecurityGroup.id.notin_(_referenced(
                    SecurityGroupInstanceAssociation.security_group_id))):
            self._remove(SecurityGroup, _ids_in(SecurityGroup.id, ids))

    def datastore_version_metadata(self):
        self._remove(DBDatastoreVersionMetadata,
                     self._old(DBDatastoreVersionMetadata))


def purge_deleted(age_in_days, dry_run=False):
    """Remove the rows of what was deleted more than ``age_in_days``
    days ago; return the rows removed per table. A dry run only
    counts them.
    """
    if age_in_days < 0:
        raise ValueError("age_in_days must be 0 or more")
    cutoff = timeutils.utcnow() - datetime.timedelta(days=age_in_days)
    purge = Purge(cutoff, dry_run=dry_run)
    counts = purge.run()
    LOG.info("%s %d row(s) of what was deleted before %s: %s",
             "Would remove" if dry_run else "Removed",
             sum(counts.values()), cutoff.isoformat(),
             ", ".join("%s %d" % item for item in counts.items() if item[1]))
    return counts
