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

"""``trove-manage db_purge`` on the real tables."""

import datetime

from trove.backup.models import DBBackup
from trove.backup.state import BackupState
from trove.cluster.models import DBCluster
from trove.cluster.tasks import ClusterTasks
from trove.common import timeutils
from trove.common import utils
from trove.conductor.models import LastSeen
from trove.configuration.models import DBConfiguration
from trove.configuration.models import DBConfigurationParameter
from trove.datastore.models import DBDatastoreVersionMetadata
from trove.db import get_db_api
from trove.db import purge
from trove.db.sqlalchemy import mappers
from trove.extensions.common.models import RootHistory
from trove.extensions.security_group import models as secgroup_models
from trove.instance.models import DBInstance
from trove.instance.models import DBInstanceFault
from trove.instance.models import InstanceServiceStatus
from trove.instance.service_status import ServiceStatuses
from trove.instance.tasks import InstanceTasks
from trove.module.models import DBInstanceModule
from trove.module.models import DBModule
from trove.tests.unittests import trove_testtools
from trove.tests.unittests.util import util

OLD = 40
YOUNG = 5


def _rows(model, **conditions):
    query = get_db_api()._base_query(model)
    n = query.filter_by(**conditions).count()
    query.session.commit()
    return n


def _mark_deleted(model, row_id, days_ago):
    query = get_db_api()._base_query(model)
    query.filter_by(id=row_id).update(
        {'deleted': True,
         'deleted_at': timeutils.utcnow() -
         datetime.timedelta(days=days_ago)})
    query.session.commit()


class PurgeTest(trove_testtools.TestCase):

    def setUp(self):
        super(PurgeTest, self).setUp()
        if not mappers.mapping_exists(DBInstance):
            util.init_db()
        self.tenant = utils.generate_uuid()
        self.made = []
        self.addCleanup(self._drop_all)

    def _drop_all(self):
        for model, key in reversed(self.made):
            query = get_db_api()._base_query(model)
            query.filter_by(**key).delete(synchronize_session=False)
            query.session.commit()

    def _make(self, model, **values):
        row = model.create(**values)
        self.made.append((model, {'id': row.id}))
        return row

    def _instance(self, deleted_days_ago=None, **values):
        row = self._make(DBInstance, name='i', tenant_id=self.tenant,
                         task_status=InstanceTasks.NONE, **values)
        status = InstanceServiceStatus(ServiceStatuses.HEALTHY,
                                       id=utils.generate_uuid(),
                                       instance_id=row.id)
        status.save()
        self.made.append((InstanceServiceStatus, {'id': status.id}))
        for method in ('heartbeat', 'cluster_event'):
            LastSeen.create(row.id, method, 1.0)
        self.made.append((LastSeen, {'instance_id': row.id}))
        RootHistory(row.id, 'root').save()
        self.made.append((RootHistory, {'id': row.id}))
        self._make(DBInstanceFault, instance_id=row.id, message='m',
                   details='d')
        if deleted_days_ago is not None:
            _mark_deleted(DBInstance, row.id, deleted_days_ago)
        return row

    def _instance_rows(self, instance_id):
        return (_rows(DBInstance, id=instance_id),
                _rows(InstanceServiceStatus, instance_id=instance_id),
                _rows(LastSeen, instance_id=instance_id),
                _rows(RootHistory, id=instance_id),
                _rows(DBInstanceFault, instance_id=instance_id))

    def _purge(self, age_in_days=30, dry_run=False):
        return purge.purge_deleted(age_in_days, dry_run=dry_run)

    def test_an_old_deleted_instance_goes_with_its_rows(self):
        old = self._instance(deleted_days_ago=OLD)
        young = self._instance(deleted_days_ago=YOUNG)
        live = self._instance()
        self.assertEqual((1, 1, 2, 1, 1), self._instance_rows(old.id))

        counts = self._purge()

        self.assertEqual((0, 0, 0, 0, 0), self._instance_rows(old.id))
        self.assertEqual((1, 1, 2, 1, 1), self._instance_rows(young.id))
        self.assertEqual((1, 1, 2, 1, 1), self._instance_rows(live.id))
        self.assertEqual(1, counts['instances'])
        self.assertEqual(1, counts['service_statuses'])
        self.assertEqual(2, counts['conductor_lastseen'])
        self.assertEqual(1, counts['root_enabled_history'])
        self.assertEqual(1, counts['instance_faults'])

    def test_a_dry_run_counts_and_keeps(self):
        old = self._instance(deleted_days_ago=OLD)
        counts = self._purge(dry_run=True)
        self.assertEqual(1, counts['instances'])
        self.assertEqual(2, counts['conductor_lastseen'])
        self.assertEqual((1, 1, 2, 1, 1), self._instance_rows(old.id))

    def test_age_zero_removes_everything_deleted(self):
        young = self._instance(deleted_days_ago=YOUNG)
        live = self._instance()
        self._purge(age_in_days=0)
        self.assertEqual(0, _rows(DBInstance, id=young.id))
        self.assertEqual(1, _rows(DBInstance, id=live.id))

    def test_the_primary_of_a_replica_stays(self):
        primary = self._instance(deleted_days_ago=OLD)
        replica = self._instance(slave_of_id=primary.id)
        self._purge()
        self.assertEqual(1, _rows(DBInstance, id=primary.id))
        self.assertEqual(1, _rows(InstanceServiceStatus,
                                  instance_id=primary.id))
        # Gone with the replica.
        _mark_deleted(DBInstance, replica.id, OLD)
        query = get_db_api()._base_query(DBInstance)
        query.filter_by(id=replica.id).update({'slave_of_id': None})
        query.session.commit()
        counts = self._purge()
        self.assertEqual(2, counts['instances'])
        self.assertEqual(0, _rows(DBInstance, id=primary.id))

    def test_batches(self):
        ids = [self._instance(deleted_days_ago=OLD).id for _ in range(5)]
        cutoff = timeutils.utcnow() - datetime.timedelta(days=30)
        counts = purge.Purge(cutoff, batch=2).run()
        self.assertEqual(5, counts['instances'])
        self.assertEqual(10, counts['conductor_lastseen'])
        for instance_id in ids:
            self.assertEqual(0, _rows(DBInstance, id=instance_id))

    def test_a_deleted_cluster_goes_once_no_instance_row_is_in_it(self):
        empty = self._make(DBCluster, name='c', tenant_id=self.tenant,
                           task_status=ClusterTasks.NONE,
                           datastore_version_id=utils.generate_uuid())
        _mark_deleted(DBCluster, empty.id, OLD)
        held = self._make(DBCluster, name='c', tenant_id=self.tenant,
                          task_status=ClusterTasks.NONE,
                          datastore_version_id=utils.generate_uuid())
        _mark_deleted(DBCluster, held.id, OLD)
        # A member row deleted the other day still points to it.
        self._instance(deleted_days_ago=YOUNG, cluster_id=held.id)
        counts = self._purge()
        self.assertEqual(1, counts['clusters'])
        self.assertEqual(0, _rows(DBCluster, id=empty.id))
        self.assertEqual(1, _rows(DBCluster, id=held.id))

    def _backup(self, days_ago=None, **values):
        row = self._make(DBBackup, name='b', tenant_id=self.tenant,
                         state=BackupState.COMPLETED,
                         instance_id=utils.generate_uuid(), **values)
        if days_ago is not None:
            _mark_deleted(DBBackup, row.id, days_ago)
        return row

    def test_a_deleted_backup_goes_unless_a_live_one_builds_on_it(self):
        alone = self._backup(OLD)
        parent = self._backup(OLD)
        self._backup(parent_id=parent.id)
        old_parent = self._backup(OLD)
        self._backup(OLD, parent_id=old_parent.id)
        counts = self._purge()
        self.assertEqual(3, counts['backups'])
        self.assertEqual(0, _rows(DBBackup, id=alone.id))
        self.assertEqual(1, _rows(DBBackup, id=parent.id))
        self.assertEqual(0, _rows(DBBackup, id=old_parent.id))

    def _configuration(self, days_ago=None):
        row = self._make(DBConfiguration, name='g', description='',
                         tenant_id=self.tenant,
                         datastore_version_id=utils.generate_uuid())
        DBConfigurationParameter.create(
            configuration_id=row.id, configuration_key='k',
            configuration_value='v')
        self.made.append((DBConfigurationParameter,
                          {'configuration_id': row.id}))
        if days_ago is not None:
            _mark_deleted(DBConfiguration, row.id, days_ago)
        return row

    def test_a_deleted_configuration_goes_with_its_parameters(self):
        gone = self._configuration(OLD)
        used = self._configuration(OLD)
        self._instance(configuration_id=used.id)
        live = self._configuration()
        # A parameter removed from a live group long ago.
        DBConfigurationParameter.create(
            configuration_id=live.id, configuration_key='old',
            configuration_value='v')
        query = get_db_api()._base_query(DBConfigurationParameter)
        query.filter_by(configuration_id=live.id, configuration_key='old'
                        ).update({'deleted': True,
                                  'deleted_at': timeutils.utcnow() -
                                  datetime.timedelta(days=OLD)})
        query.session.commit()
        counts = self._purge()
        self.assertEqual(1, counts['configurations'])
        self.assertEqual(2, counts['configuration_parameters'])
        self.assertEqual(0, _rows(DBConfiguration, id=gone.id))
        self.assertEqual(0, _rows(DBConfigurationParameter,
                                  configuration_id=gone.id))
        self.assertEqual(1, _rows(DBConfiguration, id=used.id))
        self.assertEqual(1, _rows(DBConfigurationParameter,
                                  configuration_id=used.id))
        self.assertEqual(1, _rows(DBConfigurationParameter,
                                  configuration_id=live.id))

    def _module(self, days_ago=None):
        row = self._make(DBModule, name='m', type='ping', contents='c',
                         md5='0' * 32, tenant_id=self.tenant,
                         auto_apply=False, visible=True, live_update=False,
                         priority_apply=False, apply_order=5,
                         is_admin=False)
        if days_ago is not None:
            _mark_deleted(DBModule, row.id, days_ago)
        return row

    def test_a_deleted_module_goes_once_no_instance_has_it(self):
        gone = self._module(OLD)
        applied = self._module(OLD)
        instance = self._instance()
        self._make(DBInstanceModule, instance_id=instance.id,
                   module_id=applied.id, md5='0' * 32)
        removed = self._make(DBInstanceModule, instance_id=instance.id,
                             module_id=gone.id, md5='0' * 32)
        _mark_deleted(DBInstanceModule, removed.id, OLD)
        counts = self._purge()
        self.assertEqual(1, counts['modules'])
        self.assertEqual(1, counts['instance_modules'])
        self.assertEqual(0, _rows(DBModule, id=gone.id))
        self.assertEqual(1, _rows(DBModule, id=applied.id))
        self.assertEqual(1, _rows(DBInstanceModule, module_id=applied.id))

    def test_deleted_security_groups_go_with_rules_and_associations(self):
        group = self._make(secgroup_models.SecurityGroup, name='sg',
                           description='', user='u', tenant_id=self.tenant)
        rule = self._make(secgroup_models.SecurityGroupRule,
                          group_id=group.id, protocol='tcp', from_port=1,
                          to_port=2, cidr='0.0.0.0/0')
        assoc = self._make(secgroup_models.SecurityGroupInstanceAssociation,
                           security_group_id=group.id,
                           instance_id=self._instance().id)
        for model, row in ((secgroup_models.SecurityGroup, group),
                           (secgroup_models.SecurityGroupRule, rule),
                           (secgroup_models.SecurityGroupInstanceAssociation,
                            assoc)):
            _mark_deleted(model, row.id, OLD)
        counts = self._purge()
        self.assertEqual(1, counts['security_groups'])
        self.assertEqual(1, counts['security_group_rules'])
        self.assertEqual(1, counts['security_group_instance_associations'])
        self.assertEqual(0, _rows(secgroup_models.SecurityGroup, id=group.id))

    def test_deleted_datastore_version_metadata_goes(self):
        row = self._make(DBDatastoreVersionMetadata,
                         datastore_version_id=utils.generate_uuid(),
                         key='flavor', value='f1')
        _mark_deleted(DBDatastoreVersionMetadata, row.id, OLD)
        kept = self._make(DBDatastoreVersionMetadata,
                          datastore_version_id=utils.generate_uuid(),
                          key='flavor', value='f2')
        counts = self._purge()
        self.assertEqual(1, counts['datastore_version_metadata'])
        self.assertEqual(0, _rows(DBDatastoreVersionMetadata, id=row.id))
        self.assertEqual(1, _rows(DBDatastoreVersionMetadata, id=kept.id))

    def test_a_negative_age_is_refused(self):
        self.assertRaises(ValueError, purge.purge_deleted, -1)
