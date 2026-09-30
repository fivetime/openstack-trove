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

from oslo_config import cfg
from oslo_log import log as logging

from backup.drivers import base

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class CouchDBBackup(base.BaseRunner):
    """Backup and restore of CouchDB by copying its database files.

    A database file is only ever appended to and the server reads it from
    its last complete header, so a copy taken while the server runs is a
    database as of some moment during the copy. tar notices that such a
    file grew while it was read; that is expected here and not reported.

    The archive holds the contents of the data directory and is restored by
    unpacking it into an empty one.
    """

    def __init__(self, *args, **kwargs):
        self.datadir = kwargs.pop('db_datadir', None) or CONF.db_datadir \
            or '/var/lib/couchdb/data'
        self.backup_log = '/tmp/couchdb-backup.log'
        super(CouchDBBackup, self).__init__(*args, **kwargs)

    @property
    def cmd(self):
        return 'tar --warning=no-file-changed -cpf - -C %s .' % self.datadir

    @property
    def restore_cmd(self):
        return 'tar -xpf - -C %s' % self.datadir

    @property
    def filename(self):
        return '%s.tar' % self.base_filename

    def pre_backup(self):
        self._gzip = True

    def check_process(self):
        if not os.path.exists(self.backup_log):
            return True
        with open(self.backup_log, 'r') as fp:
            log = fp.read()
        if log.strip():
            LOG.error('tar reported: %s', log)
            return False
        return True

    def check_restore_process(self):
        return self.process.returncode == 0

    def run_restore(self):
        self._gzip = True
        return self.unpack(self.location, self.checksum, self.restore_command)
