# Copyright 2015 Tesora, Inc.
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
#

import stat

from oslo_log import log as logging
from oslo_utils.excutils import save_and_reraise_exception

from trove.common import cfg
from trove.common import constants
from trove.common import exception
from trove.common import utils
from trove.guestagent.common import operating_system
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.galera_common import service as galera_service
from trove.guestagent.datastore.mysql_common import service as mysql_service
from trove.guestagent.utils import docker as docker_util
from trove.guestagent.utils import mysql as mysql_util


CONF = cfg.CONF
LOG = logging.getLogger(__name__)


# Starts a cluster member the way MariaDB's own galera_recovery does
# before mariadbd: after a crash grastate.dat holds -1 and the server
# would start from no position at all (the cluster's state becomes
# 00000000-…:0 once every member comes back, and a member behind is not
# told so), so the position is recovered from the storage engine first
# and handed to the server. In /etc/mysql, which the container mounts; the
# image's entrypoint runs what it is given when that is not the server.
START_WRAPPER = '/etc/mysql/galera-start'
START_WRAPPER_SCRIPT = r"""#!/bin/sh
# Written by the Trove guest agent: start a Galera member from the
# position the storage engine holds, as galera_recovery does. What the
# recovery run said is kept in galera-recovery.log beside the data
# directory.
datadir=/var/lib/mysql/data
for arg in "$@"; do
    case "$arg" in --datadir=*) datadir="${arg#--datadir=}" ;; esac
done
grastate="$datadir/grastate.dat"
if [ -f "$grastate" ] && grep -q '^seqno: *-1' "$grastate"; then
    log="$(dirname "$datadir")/galera-recovery.log"
    echo "galera-start: recovering the position, $(date -u)" > "$log"
    mariadbd "$@" --wsrep-recover --log-error="$log" >> "$log" 2>&1
    echo "galera-start: the recovery run exited with $?" >> "$log"
    pattern='s/.*Recovered position: *\([0-9a-f-]*:[0-9-]*\).*/\1/p'
    position=$(sed -n "$pattern" "$log" | tail -n 1)
    if [ -n "$position" ]; then
        echo "galera-start: recovered position $position"
        set -- "$@" "--wsrep_start_position=$position"
    else
        echo "galera-start: no position recovered, the end of $log:"
        tail -n 8 "$log"
    fi
fi
exec mariadbd "$@"
"""


class MariaDBApp(galera_service.GaleraAppMixin, mysql_service.BaseMySqlApp):

    # MariaDB reads the wsrep options from their own section.
    CLUSTER_CONF_SECTION = 'galera'
    # The image has MariaDB's client and server, under their own names.
    PEER_CLIENT = 'mariadb'
    SERVER_BINARY = 'mariadbd'

    def start_db(self, *args, **kwargs):
        """A cluster member's container runs the start wrapper, which
        recovers the member's position after a crash before the server.
        """
        command = kwargs.get('command')
        if command and self.is_cluster_member() and \
                not command.startswith(START_WRAPPER):
            self._write_start_wrapper()
            kwargs['command'] = '%s %s' % (START_WRAPPER, command)
        return super(MariaDBApp, self).start_db(*args, **kwargs)

    def _write_start_wrapper(self):
        operating_system.write_file(START_WRAPPER, START_WRAPPER_SCRIPT,
                                    as_root=True)
        # 0755: the server runs it as the database user.
        operating_system.chmod(
            START_WRAPPER, FileMode(reset=[stat.S_IRWXU | stat.S_IRGRP |
                                           stat.S_IXGRP | stat.S_IROTH |
                                           stat.S_IXOTH]),
            as_root=True)

    HEALTHCHECK = {
        "test": ["CMD", "healthcheck.sh", "--defaults-file",
                 "/var/lib/mysql/data/.my-healthcheck.cnf",
                 "--connect", "--innodb_initialized"],
        "start_period": 10 * 1000000000,  # 10 seconds in nanoseconds
        "interval": 10 * 1000000000,
        "timeout": 5 * 1000000000,
        "retries": 3
    }
    # # to regenerate the .my-healthcheck.cnf after restoring
    _extra_envs = {"MARIADB_AUTO_UPGRADE": 1}
    # Set to True for io_uring feature
    _previledged = True

    def __init__(self, status, docker_client):
        super(MariaDBApp, self).__init__(status, docker_client)

    @property
    def cluster_healthcheck(self):
        healthcheck = dict(MariaDBApp.HEALTHCHECK)
        healthcheck["test"] = [
            "CMD", "healthcheck.sh", "--defaults-file",
            self.cluster_healthcheck_file,
            "--connect", "--innodb_initialized", "--galera_online"]
        return healthcheck

    def wait_for_slave_status(self, status, client, max_time):
        def verify_slave_status():
            actual_status = client.execute(
                'SHOW GLOBAL STATUS like "Slave_running";').first()[1]
            return actual_status.upper() == status.upper()

        LOG.debug("Waiting for slave status %s with timeout %s",
                  status, max_time)
        try:
            utils.poll_until(verify_slave_status, sleep_time=3,
                             time_out=max_time)
            LOG.info("Replication status: %s.", status)
        except exception.PollTimeOut:
            raise RuntimeError(
                "Replication is not %(status)s after %(max)d seconds." %
                {'status': status.lower(), 'max': max_time})

    def _get_slave_status(self):
        with mysql_util.SqlClient(self.get_engine()) as client:
            return client.execute('SHOW SLAVE STATUS').first()

    def _get_master_UUID(self):
        slave_status = self._get_slave_status()
        return slave_status and slave_status['Master_Server_Id'] or None

    def _get_gtid_executed(self):
        with mysql_util.SqlClient(self.get_engine()) as client:
            return client.execute('SELECT @@global.gtid_binlog_pos').first()[0]

    def _get_gtid_slave_executed(self):
        with mysql_util.SqlClient(self.get_engine()) as client:
            return client.execute('SELECT @@global.gtid_slave_pos').first()[0]

    def get_last_txn(self):
        master_UUID = self._get_master_UUID()
        last_txn_id = '0'
        gtid_executed = self._get_gtid_slave_executed()
        for gtid_set in gtid_executed.split(','):
            uuid_set = gtid_set.split('-')
            if str(uuid_set[1]) == str(master_UUID):
                last_txn_id = uuid_set[-1]
                break
        return master_UUID, int(last_txn_id)

    def get_latest_txn_id(self):
        return self._get_gtid_executed()

    def wait_for_txn(self, txn):
        cmd = "SELECT MASTER_GTID_WAIT('%s')" % txn
        with mysql_util.SqlClient(self.get_engine()) as client:
            client.execute(cmd)

    def wipe_ib_logfiles(self):
        # mariadb_backup doesn't need to delete this file
        pass

    def get_extra_conf_dir(self):
        return '/etc/mysql/mariadb.conf.d'

    def reset_data_for_restore_snapshot(self, data_dir):
        """This function try remove slave status in database"""
        command = ("--defaults-file=/etc/mysql/my.cnf "
                   " --skip-slave-start=ON --datadir=%s" % data_dir)

        extra_volumes = {
            "/etc/mysql": {"bind": "/etc/mysql", "mode": "rw"},
            constants.MYSQL_HOST_SOCKET_PATH: {
                "bind": "/var/run/mysqld", "mode": "rw"},
            data_dir: {"bind": data_dir, "mode": "rw"},
        }

        try:
            self.start_db(ds_version=CONF.datastore_version, command=command,
                          extra_volumes=extra_volumes)
            self.stop_slave(for_failover=False)
        except Exception as err:
            with save_and_reraise_exception():
                LOG.error("Failed to remove slave status: %s", str(err))
        finally:
            try:
                LOG.debug(
                    'The init container log: %s',
                    docker_util.get_container_logs(self.docker_client))
                docker_util.remove_container(self.docker_client)
            except Exception as err:
                LOG.error('Failed to remove container. error: %s', str(err))


class MariaDBRootAccess(mysql_service.BaseMySqlRootAccess):
    def __init__(self, app):
        super(MariaDBRootAccess, self).__init__(app)


class MariaDBAdmin(mysql_service.BaseMySqlAdmin):
    def __init__(self, app):
        root_access = MariaDBRootAccess(app)
        super(MariaDBAdmin, self).__init__(root_access, app)
