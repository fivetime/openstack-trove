# Copyright 2011 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Quotas for DB instances and resources."""

from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_log import log as logging
from oslo_utils import importutils

from trove.common import exception
from trove.common import timeutils
from trove.common import utils
from trove.db.sqlalchemy import session as db_session
from trove.quota.models import Quota
from trove.quota.models import QuotaUsage
from trove.quota.models import Reservation
from trove.quota.models import Resource

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class DbQuotaDriver(object):
    """
    Driver to perform necessary checks to enforce quotas and obtain
    quota information.  The default driver utilizes the local
    database.
    """

    def __init__(self, resources):
        self.resources = resources

    def get_quota_by_tenant(self, tenant_id, resource):
        """Get a specific quota by tenant."""

        quotas = Quota.find_all(tenant_id=tenant_id, resource=resource).all()
        if len(quotas) == 0:
            return Quota(tenant_id, resource, self.resources[resource].default)

        return quotas[0]

    def get_all_quotas_by_tenant(self, tenant_id, resources):
        """
        Retrieve the quotas for the given tenant.

        :param resources: A list of the registered resource to get.
        :param tenant_id: The ID of the tenant to return quotas for.
        """

        all_quotas = Quota.find_all(tenant_id=tenant_id).all()
        result_quotas = {quota.resource: quota for quota in all_quotas
                         if quota.resource in resources}

        if len(result_quotas) != len(resources):
            for resource in resources:
                # Not in the DB, return default value
                if resource not in result_quotas:
                    quota = Quota(tenant_id,
                                  resource,
                                  self.resources[resource].default)
                    result_quotas[resource] = quota

        return result_quotas

    def get_quota_usage_by_tenant(self, tenant_id, resource):
        """Get a specific quota usage by tenant."""

        quotas = QuotaUsage.find_all(tenant_id=tenant_id,
                                     resource=resource).all()
        if len(quotas) == 0:
            return QuotaUsage.create(tenant_id=tenant_id,
                                     in_use=0,
                                     reserved=0,
                                     resource=resource)
        return quotas[0]

    def get_all_quota_usages_by_tenant(self, tenant_id, resources):
        """
        Retrieve the quota usagess for the given tenant.

        :param tenant_id: The ID of the tenant to return quotas for.
        :param resources: A list of the registered resources to get.
        """

        all_usages = QuotaUsage.find_all(tenant_id=tenant_id).all()
        result_usages = {usage.resource: usage for usage in all_usages
                         if usage.resource in resources}
        if len(result_usages) != len(resources):
            for resource in resources:
                # Not in the DB, return default value
                if resource not in result_usages:
                    usage = QuotaUsage.create(tenant_id=tenant_id,
                                              in_use=0,
                                              reserved=0,
                                              resource=resource)
                    result_usages[resource] = usage

        return result_usages

    def get_defaults(self, resources):
        """Given a list of resources, retrieve the default quotas.

        :param resources: A list of the registered resources.
        """

        quotas = {}
        for resource in resources.values():
            quotas[resource.name] = resource.default

        return quotas

    def check_quotas(self, tenant_id, resources, deltas):
        """Check quotas for a tenant.

        This method checks quotas against current usage,
        reserved resources and the desired deltas.

        If any of the proposed values is over the defined quota, an
        QuotaExceeded exception will be raised with the sorted list of the
        resources which are too high.

        :param tenant_id: The ID of the tenant reserving the resources.
        :param resources: A dictionary of the registered resources.
        :param deltas: A dictionary of the proposed delta changes.
        """

        unregistered_resources = [delta for delta in deltas
                                  if delta not in resources]
        if unregistered_resources:
            raise exception.QuotaResourceUnknown(
                unknown=unregistered_resources)

        quotas = self.get_all_quotas_by_tenant(tenant_id, deltas.keys())
        quota_usages = self.get_all_quota_usages_by_tenant(tenant_id,
                                                           deltas.keys())

        overs = [resource for resource in deltas
                 if (int(deltas[resource]) > 0 and
                     quotas[resource].hard_limit >= 0 and
                     (quota_usages[resource].in_use +
                      quota_usages[resource].reserved +
                      int(deltas[resource])) > quotas[resource].hard_limit)]

        if overs:
            raise exception.QuotaExceeded(overs=sorted(overs))

    def reserve(self, tenant_id, resources, deltas):
        """Check quotas and reserve resources for a tenant.

        This method checks quotas against current usage,
        reserved resources and the desired deltas.

        If any of the proposed values is over the defined quota, an
        QuotaExceeded exception will be raised with the sorted list of the
        resources which are too high.  Otherwise, the method returns a
        list of reservation objects which were created.

        The usages are checked and changed under a lock of their rows: read
        first and saved whole afterwards, two requests of one tenant at a
        time each wrote back the usage the other had changed in between,
        and the tenant's in_use and reserved drifted from what it has.

        :param tenant_id: The ID of the tenant reserving the resources.
        :param resources: A dictionary of the registered resources.
        :param deltas: A dictionary of the proposed delta changes.
        """

        unregistered_resources = [delta for delta in deltas
                                  if delta not in resources]
        if unregistered_resources:
            raise exception.QuotaResourceUnknown(
                unknown=unregistered_resources)

        quotas = self.get_all_quotas_by_tenant(tenant_id, deltas.keys())
        # Creates the usages a tenant does not have yet.
        self.get_all_quota_usages_by_tenant(tenant_id, deltas.keys())
        return self._reserve(tenant_id, quotas, deltas)

    @oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
    def _reserve(self, tenant_id, quotas, deltas):
        session = db_session.get_session(expire_on_commit=False)
        with session.begin():
            usages = self._locked_usages(
                session, QuotaUsage.tenant_id == tenant_id,
                QuotaUsage.resource.in_(list(deltas)))
            usages = {usage.resource: usage for usage in usages}

            overs = [resource for resource in deltas
                     if (int(deltas[resource]) > 0 and
                         quotas[resource].hard_limit >= 0 and
                         (usages[resource].in_use +
                          usages[resource].reserved +
                          int(deltas[resource])) >
                         quotas[resource].hard_limit)]
            if overs:
                raise exception.QuotaExceeded(overs=sorted(overs))

            now = timeutils.utcnow()
            reservations = []
            for resource in sorted(deltas):
                usage = usages[resource]
                usage.reserved += deltas[resource]
                usage.updated = now
                reservation = Reservation(
                    id=utils.generate_uuid(), created=now, updated=now,
                    usage_id=usage.id, delta=deltas[resource],
                    status=Reservation.Statuses.RESERVED)
                session.add(reservation)
                reservations.append(reservation)
        return reservations

    @staticmethod
    def _locked_usages(session, *criteria):
        # In the order of their ids, so that two requests lock the usages
        # they share in the same order.
        return (session.query(QuotaUsage).filter(*criteria)
                .order_by(QuotaUsage.id).with_for_update().all())

    @oslo_db_api.wrap_db_retry(max_retries=5, retry_on_deadlock=True)
    def _settle(self, reservations, status, in_use):
        session = db_session.get_session(expire_on_commit=False)
        with session.begin():
            usages = self._locked_usages(
                session, QuotaUsage.id.in_(
                    {reservation.usage_id for reservation in reservations}))
            usages = {usage.id: usage for usage in usages}
            now = timeutils.utcnow()
            for reservation in reservations:
                usage = usages[reservation.usage_id]
                if in_use:
                    usage.in_use = max(usage.in_use + reservation.delta, 0)
                usage.reserved -= reservation.delta
                usage.updated = now
                reservation.status = status
                reservation.updated = now
                session.merge(reservation)

    def commit(self, reservations):
        """Commit reservations.

        :param reservations: A list of the reservation UUIDs, as
                             returned by the reserve() method.
        """

        self._settle(reservations, Reservation.Statuses.COMMITTED, True)

    def rollback(self, reservations):
        """Roll back reservations.

        :param reservations: A list of the reservation UUIDs, as
                             returned by the reserve() method.
        """

        self._settle(reservations, Reservation.Statuses.ROLLEDBACK, False)


class QuotaEngine(object):
    """Represent the set of recognized quotas."""

    def __init__(self, quota_driver_class=None):
        """Initialize a Quota object."""

        self._resources = {}

        if not quota_driver_class:
            quota_driver_class = CONF.quota_driver
        if isinstance(quota_driver_class, str):
            quota_driver_class = importutils.import_object(quota_driver_class,
                                                           self._resources)
        self._driver = quota_driver_class

    def __contains__(self, resource):
        return resource in self._resources

    def register_resource(self, resource):
        """Register a resource."""

        self._resources[resource.name] = resource

    def register_resources(self, resources):
        """Register a dictionary of resources."""

        for resource in resources:
            self.register_resource(resource)

    def get_quota_by_tenant(self, tenant_id, resource):
        """Get a specific quota by tenant."""

        return self._driver.get_quota_by_tenant(tenant_id, resource)

    def get_quota_usage(self, quota):
        """Get the usage for a quota."""

        return self._driver.get_quota_usage_by_tenant(quota.tenant_id,
                                                      quota.resource)

    def get_defaults(self):
        """Retrieve the default quotas."""

        return self._driver.get_defaults(self._resources)

    def get_all_quotas_by_tenant(self, tenant_id):
        """Retrieve the quotas for the given tenant.

        :param tenant_id: The ID of the tenant to return quotas for.
        """

        return self._driver.get_all_quotas_by_tenant(tenant_id,
                                                     self._resources)

    def get_all_quota_usages_by_tenant(self, tenant_id):
        """Retrieve the quota usages for the given tenant.

        :param tenant_id: The ID of the tenant to return quota usages for.
        """

        return self._driver.get_all_quota_usages_by_tenant(tenant_id,
                                                           self._resources)

    def check_quotas(self, tenant_id, **deltas):
        self._driver.check_quotas(tenant_id, self._resources, deltas)

    def reserve(self, tenant_id, **deltas):
        """Check quotas and reserve resources.

        For counting quotas--those quotas for which there is a usage
        synchronization function--this method checks quotas against
        current usage and the desired deltas.  The deltas are given as
        keyword arguments, and current usage and other reservations
        are factored into the quota check.

        This method will raise a QuotaResourceUnknown exception if a
        given resource is unknown or if it does not have a usage
        synchronization function.

        If any of the proposed values is over the defined quota, an
        QuotaExceeded exception will be raised with the sorted list of the
        resources which are too high.  Otherwise, the method returns a
        list of reservation UUIDs which were created.

        :param tenant_id: The ID of the tenant to reserve quotas for.
        """

        reservations = self._driver.reserve(tenant_id, self._resources, deltas)

        LOG.debug("Created reservations %(reservations)s",
                  {'reservations': reservations})

        return reservations

    def commit(self, reservations):
        """Commit reservations.

        :param reservations: A list of the reservation UUIDs, as
                             returned by the reserve() method.
        """

        try:
            self._driver.commit(reservations)
        except Exception:
            LOG.exception("Failed to commit reservations "
                          "%(reservations)s", {'reservations': reservations})

    def rollback(self, reservations):
        """Roll back reservations.

        :param reservations: A list of the reservation UUIDs, as
                             returned by the reserve() method.
        """

        try:
            self._driver.rollback(reservations)
        except Exception:
            LOG.exception("Failed to roll back reservations "
                          "%(reservations)s", {'reservations': reservations})

    @property
    def resources(self):
        return sorted(self._resources.keys())


QUOTAS = QuotaEngine()

''' Define all kind of resources here '''

resources = [Resource(Resource.INSTANCES, 'max_instances_per_tenant'),
             Resource(Resource.RAM, 'max_ram_per_tenant'),
             Resource(Resource.BACKUPS, 'max_backups_per_tenant'),
             Resource(Resource.VOLUMES, 'max_volumes_per_tenant')]

QUOTAS.register_resources(resources)


def run_with_quotas(tenant_id, deltas, f, *args, **kwargs):
    """Quota wrapper."""

    reservations = QUOTAS.reserve(tenant_id, **deltas)
    result = None
    try:
        result = f(*args, **kwargs)
    except Exception:
        QUOTAS.rollback(reservations)
        raise
    else:
        QUOTAS.commit(reservations)
    return result


def check_quotas(tenant_id, deltas):
    QUOTAS.check_quotas(tenant_id, **deltas)
