# Copyright [2015] Hewlett-Packard Development Company, L.P.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from eventlet.timeout import Timeout
from oslo_log import log as logging

from trove.cluster.models import DBCluster
from trove.common import cfg
from trove.common.exception import TroveError
from trove.common.i18n import _
from trove.common import object_storage
from trove.common.strategies.cluster import base
from trove.common.strategies.cluster.experimental.vertica import (
    api as vertica_api)
from trove.common.strategies.cluster.experimental.vertica import communal
from trove.instance.models import DBInstance
from trove.instance.models import Instance
from trove.taskmanager import api as task_api
import trove.taskmanager.models as task_models


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VerticaTaskManagerStrategy(base.BaseTaskManagerStrategy):

    @property
    def task_manager_api_class(self):
        return VerticaTaskManagerAPI

    @property
    def task_manager_cluster_tasks_class(self):
        return VerticaClusterTasks


class VerticaClusterTasks(task_models.ClusterTasks):

    def create_cluster(self, context, cluster_id):
        LOG.debug("Begin create_cluster for id: %s.", cluster_id)

        def _create_cluster():

            # Fetch instances by cluster_id against instances table.
            db_instances = DBInstance.find_all(cluster_id=cluster_id,
                                               deleted=False).all()
            instance_ids = [db_instance.id for db_instance in db_instances]

            # Wait for cluster members to get to cluster-ready status.
            if not self._all_instances_ready(instance_ids, cluster_id):
                return

            LOG.debug("All members ready, proceeding for cluster setup.")
            instances = [Instance.load(context, instance_id) for instance_id
                         in instance_ids]

            member_ips = [self.get_ip(instance) for instance in instances]
            guests = [self.get_guest(instance) for instance in instances]

            # vcluster on the first member talks to the agent of every
            # member: all must trust the authority of the first, and the
            # admin has one password for the one database.
            try:
                master_id = next(db_instance.id for db_instance
                                 in db_instances
                                 if db_instance.type == 'master')
                master_guest = next(guest for instance, guest
                                    in zip(instances, guests)
                                    if instance.id == master_id)
                secrets = master_guest.get_cluster_secrets()
                for instance, guest in zip(instances, guests):
                    if instance.id != master_id:
                        guest.install_cluster_secrets(secrets)

                storage = self._communal_storage()
                LOG.debug("Installing cluster with members: %s.", member_ips)
                config = master_guest.install_cluster(member_ips,
                                                      communal=storage)

                # Every member starts and stops its node with vcluster,
                # which needs the configuration the creation wrote.
                for instance, guest in zip(instances, guests):
                    if instance.id != master_id:
                        guest.set_cluster_config(config)

                LOG.debug("Finalizing cluster configuration.")
                for guest in guests:
                    guest.cluster_complete()
            except Exception:
                LOG.exception("Error creating cluster.")
                self.update_statuses_on_failure(cluster_id)

        timeout = Timeout(CONF.cluster_usage_timeout)
        try:
            _create_cluster()
            self.reset_task()
        except Timeout as t:
            if t is not timeout:
                raise  # not my timeout
            LOG.exception("Timeout for building cluster.")
            self.update_statuses_on_failure(cluster_id)
        finally:
            timeout.cancel()

        LOG.debug("End create_cluster for id: %s.", cluster_id)

    def _communal_storage(self):
        """The communal storage of an Eon Mode cluster, for the guest of
        its first member to create the database on, or None. The EC2
        credential and the bucket are made once; a retry finds them.
        """
        properties = self.db_info.get_properties()
        if properties.get(vertica_api.STORAGE_MODE) != vertica_api.EON:
            return None
        storage = object_storage.ProjectObjectStorage()
        access, secret = storage.credential(
            properties[vertica_api.EC2_USER], self.db_info.tenant_id,
            properties.get('ec2_access'))
        if properties.get('ec2_access') != access:
            properties['ec2_access'] = access
            self.db_info.save_properties(properties)
        bucket = properties['communal_bucket']
        LOG.info("Creating the bucket %(bucket)s of project %(project)s "
                 "for the communal storage of cluster %(cluster)s.",
                 {'bucket': bucket, 'project': self.db_info.tenant_id,
                  'cluster': self.id})
        storage.create_bucket(storage.client(access, secret), bucket)
        endpoint, https = communal.guest_endpoint(storage,
                                                  self.ds_version.manager)
        return {'location': communal.location(bucket),
                'endpoint': endpoint, 'https': https,
                'region': CONF.s3_region,
                'access': access, 'secret': secret}

    def delete_cluster(self, context, cluster_id):
        """An Eon Mode cluster takes its communal storage with it: the
        bucket, emptied, and then the EC2 credential, once the members
        are gone. A failure is logged and leaves them to be removed by
        hand, as the load balancer of other clusters.
        """
        properties = self.db_info.get_properties()
        super(VerticaClusterTasks, self).delete_cluster(context, cluster_id)
        if (properties.get(vertica_api.STORAGE_MODE) != vertica_api.EON or
                not DBCluster.find_by(id=cluster_id).deleted):
            return
        self._remove_communal_storage(properties)

    def _remove_communal_storage(self, properties):
        user = properties.get(vertica_api.EC2_USER)
        access = properties.get('ec2_access')
        bucket = properties.get('communal_bucket')
        what = {'bucket': bucket, 'project': self.db_info.tenant_id,
                'access': access, 'user': user, 'cluster': self.id}
        if not access:
            # The cluster failed before it had any.
            return
        storage = object_storage.ProjectObjectStorage()
        try:
            secret = storage.secret(user, access)
            if secret is None:
                LOG.warning("The EC2 credential %(access)s of user %(user)s "
                            "is gone: the bucket %(bucket)s of project "
                            "%(project)s of cluster %(cluster)s is left.",
                            what)
                return
            storage.purge_bucket(storage.client(access, secret), bucket)
            storage.delete_credential(user, access)
            LOG.info("Removed the bucket %(bucket)s of project %(project)s "
                     "and the EC2 credential %(access)s of cluster "
                     "%(cluster)s.", what)
        except Exception:
            LOG.exception("Failed to remove the bucket %(bucket)s of project "
                          "%(project)s or the EC2 credential %(access)s of "
                          "user %(user)s of cluster %(cluster)s.", what)

    # Growing and shrinking went through adminTools over SSH, which the
    # image has neither of. The API refuses both until they are done with
    # vcluster (add_node, remove_node) and tried with a license: the
    # Community Edition allows three nodes, and three nodes with
    # K-safety 1 cannot lose one.
    def grow_cluster(self, context, cluster_id, new_instance_ids):
        raise TroveError(_("Growing a Vertica cluster is not supported."))

    def shrink_cluster(self, context, cluster_id, instance_ids):
        raise TroveError(_("Shrinking a Vertica cluster is not supported."))


class VerticaTaskManagerAPI(task_api.API):

    def _cast(self, method_name, version, **kwargs):
        LOG.debug("Casting %s", method_name)
        cctxt = self.client.prepare(version=version)
        cctxt.cast(self.context, method_name, **kwargs)
