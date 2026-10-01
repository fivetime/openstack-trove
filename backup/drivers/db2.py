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


class DB2Backup(base.BaseRunner):
    """Backup and restore of Db2 as the online backup images the instance
    writes.

    The guest agent has the instance write an online backup image of
    every database, with the logs that make it consistent, into a
    directory of the volume, and the users of the instance next to them.
    This packs that directory; a restore unpacks it into an empty one,
    from which the guest agent restores the databases.
    """

    def __init__(self, *args, **kwargs):
        self.datadir = kwargs.pop('db_datadir', None) or CONF.db_datadir \
            or '/var/lib/db2/trove/backup'
        self.backup_log = '/tmp/db2-backup.log'
        super(DB2Backup, self).__init__(*args, **kwargs)

    @property
    def cmd(self):
        return 'tar -cpf - -C %s .' % self.datadir

    @property
    def restore_cmd(self):
        return 'tar -xpf - -C %s' % self.datadir

    @property
    def filename(self):
        return '%s.tar' % self.base_filename

    def pre_backup(self):
        # The images are compressed by the instance already.
        self._gzip = False

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
        self._gzip = False
        return self.unpack(self.location, self.checksum, self.restore_command)
