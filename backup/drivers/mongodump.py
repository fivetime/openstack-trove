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

import os
import urllib.parse

from oslo_config import cfg
from oslo_log import log as logging

from backup.drivers import base

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class MongoDump(base.BaseRunner):
    """Backup and restore with mongodump and mongorestore.

    The whole server goes into one archive, users and roles included,
    streamed out and in. Both tools talk to the server given as db-host:
    the guest agent gives the socket of its container, which is what it
    mounts into this one.
    """

    def __init__(self, *args, **kwargs):
        self.datadir = '/var/lib/mongodb'
        self.backup_log = '/tmp/mongodump.log'
        super(MongoDump, self).__init__(*args, **kwargs)

    @property
    def uri(self):
        host = CONF.db_host or 'localhost'
        if host.endswith('.sock'):
            host = urllib.parse.quote(host, safe='')
        if CONF.db_user:
            auth = '%s:%s@' % (urllib.parse.quote(CONF.db_user, safe=''),
                               urllib.parse.quote(CONF.db_password or '',
                                                  safe=''))
            return 'mongodb://%s%s/?authSource=admin' % (auth, host)
        return 'mongodb://%s' % host

    @property
    def _uri_arg(self):
        # The command goes through %-formatting in the base class, and the
        # URI has the socket path percent-encoded in it.
        return "--uri=%s" % self.uri.replace('%', '%%')

    @property
    def cmd(self):
        return f'mongodump {self._uri_arg} --archive'

    @property
    def restore_cmd(self):
        # --drop replaces what the temporary server holds, the users of
        # the source instance included; the guest agent sets the admin
        # user afterwards.
        return f'mongorestore {self._uri_arg} --archive --drop'

    @property
    def filename(self):
        return f'{self.base_filename}.archive'

    def pre_backup(self):
        self._gzip = True

    def check_process(self):
        """mongodump reports failures on its standard error and does not
        always exit non-zero for a partial dump.
"""
        if not os.path.exists(self.backup_log):
            return True
        with open(self.backup_log, 'r') as fp:
            log = fp.read()
        if 'Failed:' in log or 'error' in log.lower():
            LOG.error('mongodump reported an error: %s', log)
            return False
        return True

    def check_restore_process(self):
        return self.process.returncode == 0

    def run_restore(self):
        self._gzip = True
        return self.unpack(self.location, self.checksum, self.restore_command)
