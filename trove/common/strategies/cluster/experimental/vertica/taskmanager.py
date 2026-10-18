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

# Whether the bucket is the cluster's to purge with it: it made the bucket,
# or took it over by reviving from it.
COMMUNAL_OWNER = 'communal_owner'


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
                if storage and storage['revive']:
                    self._take_over_communal_storage()

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
        user = self._storage_user(storage, properties)
        access, secret = storage.credential(
            user, self.db_info.tenant_id, properties.get('ec2_access'))
        if properties.get('ec2_access') != access:
            properties['ec2_access'] = access
            self.db_info.save_properties(properties)
        bucket = properties['communal_bucket']
        what = {'bucket': bucket, 'project': self.db_info.tenant_id,
                'cluster': self.id}
        s3 = storage.client(access, secret)
        revive = bool(properties.get(vertica_api.REVIVE_FROM))
        if revive:
            if not storage.bucket_exists(s3, bucket):
                raise TroveError(_(
                    "The bucket %(bucket)s of project %(project)s to revive "
                    "cluster %(cluster)s from is not there.") % what)
            LOG.info("Reviving cluster %(cluster)s from the bucket "
                     "%(bucket)s of project %(project)s.", what)
        else:
            LOG.info("Creating the bucket %(bucket)s of project %(project)s "
                     "for the communal storage of cluster %(cluster)s.", what)
            storage.create_bucket(s3, bucket)
            if not properties.get(COMMUNAL_OWNER):
                properties[COMMUNAL_OWNER] = True
                self.db_info.save_properties(properties)
        endpoint, https = communal.guest_endpoint(storage,
                                                  self.ds_version.manager)
        return {'location': communal.location(bucket),
                'endpoint': endpoint, 'https': https,
                'region': CONF.s3_region,
                'access': access, 'secret': secret, 'revive': revive}

    def _storage_user(self, storage, properties):
        """The user of the project the cluster reaches its communal
        storage as, made for it alone (and recorded) if it has none yet:
        the database's administrator can read the credential, which must
        not be a key to the rest of the project.
        """
        if properties.get(vertica_api.DEDICATED_USER):
            return properties[vertica_api.EC2_USER]
        user = storage.storage_user(communal.user_name(self.id),
                                    self.db_info.tenant_id,
                                    communal.user_description(self.id))
        properties.update({vertica_api.EC2_USER: user,
                           vertica_api.DEDICATED_USER: True})
        self.db_info.save_properties(properties)
        return user

    def _forget_credential(self, storage, properties):
        """Remove the EC2 credential of the cluster, and its user when
        Trove made it for the cluster.
        """
        user = properties.get(vertica_api.EC2_USER)
        if properties.get('ec2_access'):
            storage.delete_credential(user, properties['ec2_access'])
        if properties.get(vertica_api.DEDICATED_USER) and user:
            storage.delete_user(user)

    def _take_over_communal_storage(self):
        """A database revived from a bucket owns it from now on: the
        clusters of the project still naming it lose their EC2 credential,
        so that their members cannot reach it if they come up again, and
        do not purge it when they are deleted.
        """
        properties = self.db_info.get_properties()
        bucket = properties['communal_bucket']
        properties[COMMUNAL_OWNER] = True
        self.db_info.save_properties(properties)
        storage = object_storage.ProjectObjectStorage()
        for other in self._clusters_naming(bucket):
            theirs = other.get_properties()
            LOG.info("Cluster %(cluster)s, revived from the bucket "
                     "%(bucket)s, takes it over from cluster %(other)s.",
                     {'cluster': self.id, 'bucket': bucket,
                      'other': other.id})
            try:
                self._forget_credential(storage, theirs)
            except Exception:
                LOG.exception("Failed to remove the EC2 credential of "
                              "cluster %s.", other.id)
            theirs.update({'communal_moved_to': self.id,
                           COMMUNAL_OWNER: False})
            for name in ('ec2_access', vertica_api.EC2_USER,
                         vertica_api.DEDICATED_USER):
                theirs.pop(name, None)
            other.save_properties(theirs)

    def _clusters_naming(self, bucket):
        """The other clusters of the project whose communal storage is
        the bucket, and that are not deleted.
        """
        clusters = DBCluster.find_all(tenant_id=self.db_info.tenant_id,
                                      deleted=False).all()
        return [c for c in clusters
                if c.id != self.id and
                c.get_properties().get('communal_bucket') == bucket]

    def reset_communal_credential(self, context, cluster_id):
        """Give the database a new EC2 credential of the user of the
        cluster, and remove the old one. Until the database has the new one
        the old one stays: a failure leaves the cluster as it was. A
        cluster whose credential is a person's (made before clusters had a
        user of their own) moves to a user of its own.
        """
        properties = self.db_info.get_properties()
        old = dict(properties)
        storage = object_storage.ProjectObjectStorage()
        bucket = properties['communal_bucket']
        user = access = None
        try:
            user = self._storage_user(storage, properties)
            access, secret = storage.credential(user, self.db_info.tenant_id)
            if not storage.bucket_exists(storage.client(access, secret),
                                         bucket):
                raise TroveError(_("The bucket %s is not there.") % bucket)
            master = next(i for i in DBInstance.find_all(
                cluster_id=cluster_id, deleted=False).all()
                if i.type == 'master')
            self.get_guest(Instance.load(context, master.id)
                           ).set_communal_credential(access, secret)
            properties['ec2_access'] = access
            self.db_info.save_properties(properties)
            access = None
            if old.get('ec2_access'):
                storage.delete_credential(old.get(vertica_api.EC2_USER),
                                          old['ec2_access'])
            LOG.info("The communal storage of cluster %(cluster)s has a new "
                     "EC2 credential of user %(user)s.",
                     {'cluster': cluster_id, 'user': user})
        except Exception:
            LOG.exception("Failed to reset the communal storage credential "
                          "of cluster %s.", cluster_id)
            try:
                if access:
                    storage.delete_credential(user, access)
                if user and not old.get(vertica_api.DEDICATED_USER):
                    # The user made for the move: back to the old one.
                    storage.delete_user(user)
                    self.db_info.save_properties(old)
            except Exception:
                LOG.exception("Failed to undo the reset of the communal "
                              "storage credential of cluster %s.", cluster_id)
        finally:
            self.reset_task()

    def delete_cluster(self, context, cluster_id):
        """An Eon Mode cluster takes its communal storage with it: the
        bucket, emptied, and then the EC2 credential, once the members
        are gone. A failure is logged and leaves them to be removed by
        hand, as the load balancer of other clusters.
        """
        properties = DBCluster.find_by(id=cluster_id).get_properties()
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
        storage = object_storage.ProjectObjectStorage()
        if not access:
            # The cluster failed before it had a credential, maybe after
            # its user was made.
            try:
                self._forget_credential(storage, properties)
            except Exception:
                LOG.exception("Failed to remove the user %(user)s of "
                              "cluster %(cluster)s.", what)
            return
        # The bucket is this cluster's to purge if it made it, or took it
        # over by reviving from it; not once another cluster revived from
        # it, nor while another one names it.
        owner = properties.get(COMMUNAL_OWNER,
                               not properties.get(vertica_api.REVIVE_FROM))
        if (not owner or properties.get('communal_moved_to') or
                self._clusters_naming(bucket)):
            LOG.info("The bucket %(bucket)s of project %(project)s is not "
                     "cluster %(cluster)s's to remove; removing its EC2 "
                     "credential %(access)s only.", what)
            try:
                self._forget_credential(storage, properties)
            except Exception:
                LOG.exception("Failed to remove the EC2 credential "
                              "%(access)s of user %(user)s of cluster "
                              "%(cluster)s.", what)
            return
        try:
            secret = storage.secret(user, access)
            if secret is None:
                LOG.warning("The EC2 credential %(access)s of user %(user)s "
                            "is gone: the bucket %(bucket)s of project "
                            "%(project)s of cluster %(cluster)s is left.",
                            what)
                self._forget_credential(storage, properties)
                return
            storage.purge_bucket(storage.client(access, secret), bucket)
            self._forget_credential(storage, properties)
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
