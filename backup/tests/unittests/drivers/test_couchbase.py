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
import shutil
import subprocess
import tarfile
import tempfile
import unittest

from oslo_config import cfg
from oslo_utils import importutils

from backup import main

CONF = cfg.CONF
CONF.backup_encryption_key = None
CONF.backup_id = "backup_unittest"
CONF.db_datadir = None


class TestCouchbaseBackup(unittest.TestCase):

    def setUp(self):
        self.runner_cls = importutils.import_class(
            main.driver_mapping['couchbasebackup'])
        self.datadir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.datadir, ignore_errors=True)

    def _touch(self, *parts):
        path = os.path.join(self.datadir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(parts[-1])

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]
        self.assertIn('couchbasebackup', driver_opt.type.choices)

    def test_default_data_directory(self):
        self.assertEqual('/var/lib/couchbase', self.runner_cls().datadir)

    def test_archive_is_the_node_without_its_logs(self):
        self._touch('lib', 'couchbase', 'config', 'config.dat')
        self._touch('lib', 'couchbase', 'data', 'appdb', '0.couch.1')
        self._touch('lib', 'couchbase', 'logs', 'debug.log')
        self._touch('lib', 'couchbase', 'stats_data', 'wal')
        self._touch('lib', 'couchbase', 'tmp', 'scratch')
        self._touch('lib', 'couchbase', 'crash', 'dump')
        self._touch('lost+found', 'x')
        self._touch('conf.d', 'os_admin.cnf')
        runner = self.runner_cls(filename='b1', db_datadir=self.datadir)
        runner.pre_backup()
        self.assertTrue(runner._gzip)
        self.assertEqual('b1.tar.gz', runner.manifest)
        # A data file that grows or is compacted away while it is read is
        # expected.
        self.assertIn('--warning=no-file-changed', runner.command.split())
        self.assertIn('--warning=no-file-removed', runner.command.split())

        archive = tempfile.mktemp(suffix='.tar')
        self.addCleanup(os.remove, archive)
        with open(archive, 'wb') as out:
            subprocess.check_call(runner.command.split(), stdout=out)
        with tarfile.open(archive) as tar:
            names = tar.getnames()
        self.assertIn('./lib/couchbase/config/config.dat', names)
        self.assertIn('./lib/couchbase/data/appdb/0.couch.1', names)
        for excluded in ('logs/debug.log', 'stats_data/wal', 'tmp/scratch',
                         'crash/dump'):
            self.assertNotIn('./lib/couchbase/' + excluded, names)
        self.assertNotIn('./lost+found/x', names)
        # The guest agent's files, the admin password among them.
        self.assertNotIn('./conf.d/os_admin.cnf', names)
        # The server expects the directories themselves.
        for kept in ('logs', 'stats_data', 'tmp', 'crash'):
            self.assertIn('./lib/couchbase/' + kept, names)

        target = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target, ignore_errors=True)
        restore = self.runner_cls(db_datadir=target)
        self.assertEqual(['tar', '-xpf', '-', '-C', target],
                         restore.restore_command.split())
        with open(archive, 'rb') as stream:
            subprocess.check_call(restore.restore_command.split(),
                                  stdin=stream)
        self.assertTrue(os.path.exists(
            os.path.join(target, 'lib', 'couchbase', 'data', 'appdb',
                         '0.couch.1')))
        self.assertTrue(os.path.isdir(
            os.path.join(target, 'lib', 'couchbase', 'logs')))
