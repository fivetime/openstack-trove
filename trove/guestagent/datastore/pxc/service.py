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

from oslo_log import log as logging

from trove.common import cfg
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mysql import service as mysql_service

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

# The entrypoint of the image edits this file with sed and prints it with
# grep, and fails when it is missing or empty. The image's own copy is
# hidden by the directory the guest agent mounts over /etc/mysql.
NODE_CONFIG = '/etc/mysql/node.cnf'
NODE_CONFIG_CONTENT = """\
# The entrypoint of the image edits this file. The server does not read
# it: it is started with --defaults-file.
[mysqld]
"""


class PXCApp(galera_service.GaleraAppMixin, mysql_service.MySqlApp):
    """Percona XtraDB Cluster, from the ``percona-xtradb-cluster`` image.

    Every instance is a Galera node and a single instance is a cluster of
    one: the image waits for the node to be synced when it initializes the
    data directory, which a server without Galera never is. The
    configuration template therefore loads the Galera provider and forms a
    cluster from the first start.
    """

    @property
    def _extra_envs(self):
        # The image creates these accounts when it initializes the data
        # directory, with well known passwords unless it is given others.
        # Nothing in Trove logs in with them.
        return {
            name: utils.generate_random_password()
            for name in ('XTRABACKUP_PASSWORD', 'MONITOR_PASSWORD',
                         'CLUSTERCHECK_PASSWORD')
        }

    @property
    def cluster_healthcheck(self):
        healthcheck = dict(mysql_service.MySqlApp.HEALTHCHECK)
        # Over TCP, like the check of a single instance: the server the
        # image starts to initialize the data directory listens on the
        # socket only and must not count as the database being up.
        healthcheck["test"] = [
            "CMD-SHELL",
            "mysql --defaults-file=%s -N -B "
            "-e \"SHOW STATUS LIKE 'wsrep_local_state'\" | grep -qw 4"
            % self.cluster_healthcheck_file]
        return healthcheck

    def _write_node_config(self):
        operating_system.ensure_directory(
            '/etc/mysql', user=self.database_service_uid,
            group=self.database_service_gid, force=True, as_root=True)
        operating_system.write_file(
            NODE_CONFIG, NODE_CONFIG_CONTENT, as_root=True)
        operating_system.chown(
            NODE_CONFIG, self.database_service_uid,
            self.database_service_gid, as_root=True)
        operating_system.chmod(
            NODE_CONFIG, operating_system.FileMode.SET_USR_RW, as_root=True)

    def start_db(self, *args, **kwargs):
        self._write_node_config()
        return super(PXCApp, self).start_db(*args, **kwargs)

    def get_cluster_context(self):
        # Percona XtraDB Cluster 8 has no wsrep_sst_auth: state transfers
        # use an account of their own. The replication user is kept where
        # the health check reads it.
        credentials = operating_system.read_file(
            self.cluster_healthcheck_file, codec=self.CFG_CODEC,
            as_root=True)['client']
        return {
            'replication_user': {
                'name': credentials['user'],
                'password': credentials['password'],
            },
            'cluster_name': self.cluster_configuration.get(
                'wsrep_cluster_name'),
            'admin_password': self.get_auth_password(),
            'writer_mode': self.writer_mode,
        }
