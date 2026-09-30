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


class NodetoolSnapshot(base.BaseRunner):
    """Backup and restore of Cassandra from a snapshot of every table.

    The guest agent has the server take the snapshot, named after the
    backup, before it starts this container and clears it afterwards. A
    snapshot is a directory of hard links next to the files of each table:

        <data dir>/<keyspace>/<table>/snapshots/<snapshot>/<file>

    The files are packed without the 'snapshots/<snapshot>' part of their
    path, so that a backup is restored by unpacking it into an empty data
    directory.
    """

    def __init__(self, *args, **kwargs):
        self.datadir = kwargs.pop('db_datadir', None) or CONF.db_datadir \
            or '/var/lib/cassandra/data'
        self.snapshot = kwargs.get('filename') or CONF.backup_id
        self.backup_log = '/tmp/cassandra-backup.log'
        self.file_list = '/tmp/cassandra-backup.files'
        super(NodetoolSnapshot, self).__init__(*args, **kwargs)

    @property
    def cmd(self):
        return ('tar --transform=s#snapshots/%s/## -cpf - -C %s -T %s'
                % (self.snapshot, self.datadir, self.file_list))

    @property
    def restore_cmd(self):
        return 'tar -xpf - -C %s' % self.datadir

    @property
    def filename(self):
        return '%s.tar' % self.base_filename

    def _snapshot_files(self):
        """The files of the snapshot, relative to the data directory.

        A table keeps its snapshots next to its files, and so does each
        of its secondary indexes, one directory further down.
        """
        found = []
        for root, _dirs, files in os.walk(self.datadir):
            parts = os.path.relpath(root, self.datadir).split(os.sep)
            if not any(parts[i] == 'snapshots' and
                       parts[i + 1] == self.snapshot
                       for i in range(len(parts) - 1)):
                continue
            found.extend(os.path.join(*parts, name) for name in files)
        return sorted(found)

    def pre_backup(self):
        self._gzip = True
        files = self._snapshot_files()
        LOG.info('Found %d files of snapshot %s.', len(files), self.snapshot)
        if not files:
            # There is always the system keyspace.
            raise Exception('No files of snapshot %s under %s.'
                            % (self.snapshot, self.datadir))
        with open(self.file_list, 'w') as fp:
            fp.write('\n'.join(files) + '\n')

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
