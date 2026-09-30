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

"""Galera clustering for the datastores built on the MySQL guest agent.

A cluster member starts life as a single instance: ``prepare`` brings the
database up on its own. The task manager then calls ``install_cluster`` on
each member in turn, the first with ``bootstrap=True``, and
``cluster_complete`` on all of them once every member has joined.
"""

import docker
from oslo_log import log as logging
from sqlalchemy import exc
from sqlalchemy.sql.expression import text

from trove.common import cfg
from trove.common import exception
from trove.guestagent.common import guestagent_utils
from trove.guestagent.common import operating_system
from trove.guestagent.common import sql_query
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.guestagent.utils import docker as docker_util
from trove.guestagent.utils import mysql as mysql_util
from trove.instance import service_status

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

CNF_CLUSTER = 'cluster'
# Makes the server form a new cluster instead of joining the one named in
# wsrep_cluster_address.
BOOTSTRAP_OPTION = '--wsrep-new-cluster'
HEALTHCHECK_FILE = 'cluster-healthcheck'
CONTAINER_NAME = 'database'


class GaleraAppMixin(object):
    """Galera clustering for an app built on ``BaseMySqlApp``.

    List it before the app class it extends, so that ``start_db`` here runs
    first.
    """

    # The section of the configuration file that holds the wsrep options.
    CLUSTER_CONF_SECTION = 'mysqld'

    @property
    def cluster_configuration(self):
        return self.configuration_manager.get_value(
            self.CLUSTER_CONF_SECTION) or {}

    def is_cluster_member(self):
        return self.configuration_manager.has_system_override(CNF_CLUSTER)

    @property
    def cluster_healthcheck_file(self):
        # In the configuration directory on the data volume, which the
        # container sees at the same path.
        return guestagent_utils.build_file_path(
            guestagent_utils.get_conf_dir(), HEALTHCHECK_FILE,
            mysql_service.CNF_EXT)

    @property
    def cluster_healthcheck(self):
        """The container health check of a cluster member.

        A member is healthy when it is synced with the cluster, not when it
        merely answers.

        The check of a single instance cannot be used. It logs in with an
        account whose password the image generates when it initializes the
        data directory and keeps in a file beside the data. A member that
        joins receives the accounts of the cluster with its state transfer
        but keeps its own file, so the two no longer match and the member
        would be unhealthy for ever although it is part of the cluster.
        """
        raise NotImplementedError()

    def start_db(self, *args, **kwargs):
        if self.is_cluster_member():
            # On the instance, so that it replaces the check of the class
            # for every container this app creates from here on.
            self.HEALTHCHECK = self.cluster_healthcheck
        return super(GaleraAppMixin, self).start_db(*args, **kwargs)

    def _create_cluster_replication_user(self, replication_user):
        LOG.info("Creating the cluster replication user.")
        name = replication_user['name']
        password = replication_user['password']
        with mysql_util.SqlClient(
                self.get_engine(), use_flush=True) as client:
            try:
                create = sql_query.CreateUser(name, clear=password)
                client.execute(text(str(create)), **create.keyArgs)
            except (exc.OperationalError, exc.InternalError) as err:
                # It is there already; make sure of its password.
                LOG.debug(err)
                update = sql_query.SetPassword(
                    name, new_password=password,
                    ds=CONF.datastore_manager,
                    ds_version=CONF.datastore_version)
                client.execute(text(str(update)))

            grant = sql_query.Grant(
                permissions=['REPLICATION CLIENT', 'RELOAD', 'LOCK TABLES'],
                user=name)
            client.execute(text(str(grant)))

    def _write_cluster_healthcheck_file(self, replication_user):
        path = self.cluster_healthcheck_file
        operating_system.write_file(
            path,
            # Over TCP: a server started only to initialize or upgrade the
            # data directory listens on the socket alone.
            {'client': {'user': replication_user['name'],
                        'password': replication_user['password'],
                        'host': '127.0.0.1',
                        'protocol': 'tcp'}},
            codec=self.CFG_CODEC, as_root=True)
        # The health check runs in the container as the database user.
        operating_system.chown(
            path, self.database_service_uid, self.database_service_gid,
            as_root=True)
        operating_system.chmod(
            path, operating_system.FileMode.SET_USR_RW, as_root=True)

    def write_cluster_configuration_overrides(self, cluster_configuration):
        self.configuration_manager.apply_system_override(
            cluster_configuration, CNF_CLUSTER)

    def reset_admin_password(self, admin_password):
        """Give the admin user the password the whole cluster uses.

        A member that joins receives the accounts of the cluster, so the
        password stored on it has to be the cluster's before it joins.
        """
        with mysql_util.SqlClient(
                self.get_engine(), use_flush=True) as client:
            self._create_admin_user(client, admin_password)
        self.save_password(mysql_service.ADMIN_USER_NAME, admin_password)
        # The cached engine holds the old password.
        mysql_service.ENGINE = None

    def started_with_bootstrap(self):
        try:
            container = self.docker_client.containers.get(CONTAINER_NAME)
        except docker.errors.NotFound:
            return False
        return BOOTSTRAP_OPTION in (container.attrs['Config'].get('Cmd') or
                                    [])

    def start_cluster_node(self, command, bootstrap=False):
        """Create the database container as a cluster member and start it.

        A container keeps the command it was created with, so an existing
        one is removed first.
        """
        docker_util.remove_container(self.docker_client)
        if bootstrap:
            command = ('%s %s' % (command or '', BOOTSTRAP_OPTION)).strip()

        try:
            self.start_db(ds_version=CONF.datastore_version, command=command)
        except exception.TroveError:
            status = docker_util.get_container_status(self.docker_client)
            if bootstrap or status != 'running':
                raise
            # A joining member is not healthy until it has received the
            # state of the cluster, which takes as long as the data is big.
            LOG.info("The database is still starting, it may be receiving "
                     "the state of the cluster.")
            if not self.status.wait_for_status(
                    service_status.ServiceStatuses.HEALTHY,
                    CONF.restore_usage_timeout):
                raise

    def install_cluster(self, replication_user, cluster_configuration,
                        command, bootstrap=False):
        LOG.info("Installing the cluster configuration, bootstrap: %s.",
                 bootstrap)
        self._create_cluster_replication_user(replication_user)
        self.stop_db()
        self.write_cluster_configuration_overrides(cluster_configuration)
        self._write_cluster_healthcheck_file(replication_user)
        self.wipe_ib_logfiles()
        self.start_cluster_node(command, bootstrap=bootstrap)

    def leave_bootstrap(self, command):
        """Start the member that formed the cluster as an ordinary member.

        It was started with the option that forms a new cluster, and its
        container would be started with it again after every reboot. Galera
        refuses to form a cluster from a member that was not the last to
        leave it, so the database would not come back.
        """
        if not self.started_with_bootstrap():
            return
        LOG.info("Starting the member that formed the cluster again, to "
                 "join the cluster like the others.")
        self.stop_db()
        self.start_cluster_node(command)

    def get_cluster_context(self):
        configuration = self.cluster_configuration
        name, _sep, password = configuration.get(
            'wsrep_sst_auth', '').replace('"', '').partition(':')
        return {
            'replication_user': {
                'name': name,
                'password': password,
            },
            'cluster_name': configuration.get('wsrep_cluster_name'),
            'admin_password': self.get_auth_password(),
        }
