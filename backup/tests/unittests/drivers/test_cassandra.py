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


class TestNodetoolSnapshot(unittest.TestCase):

    def setUp(self):
        self.runner_cls = importutils.import_class(
            main.driver_mapping['nodetoolsnapshot'])
        self.datadir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.datadir, ignore_errors=True)

    def _touch(self, *parts):
        path = os.path.join(self.datadir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(parts[-1])

    def _runner(self, **kwargs):
        runner = self.runner_cls(filename='b1', db_datadir=self.datadir,
                                 **kwargs)
        runner.file_list = os.path.join(self.datadir, 'files')
        runner.command = runner.cmd
        return runner

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]
        self.assertIn('nodetoolsnapshot', driver_opt.type.choices)

    def test_only_the_files_of_this_snapshot(self):
        self._touch('appdb', 't-1', 'nb-1-big-Data.db')
        self._touch('appdb', 't-1', 'snapshots', 'b1', 'nb-1-big-Data.db')
        self._touch('appdb', 't-1', 'snapshots', 'b1', 'schema.cql')
        self._touch('appdb', 't-1', 'snapshots', 'other', 'nb-1-big-Data.db')
        # the snapshot of a secondary index, one directory further down
        self._touch('appdb', 't-1', '.idx', 'snapshots', 'b1', 'nb-1-Data.db')
        self._touch('appdb', 't-1', 'backups', 'nb-2-big-Data.db')

        self.assertEqual(
            ['appdb/t-1/.idx/snapshots/b1/nb-1-Data.db',
             'appdb/t-1/snapshots/b1/nb-1-big-Data.db',
             'appdb/t-1/snapshots/b1/schema.cql'],
            self._runner()._snapshot_files())

    def test_an_empty_snapshot_is_an_error(self):
        self._touch('appdb', 't-1', 'nb-1-big-Data.db')
        self.assertRaisesRegex(Exception, 'No files of snapshot b1',
                               self._runner().pre_backup)

    def test_archive_unpacks_into_a_data_directory(self):
        self._touch('appdb', 't-1', 'snapshots', 'b1', 'nb-1-big-Data.db')
        self._touch('system', 'local-7', 'snapshots', 'b1', 'nb-3-Data.db')
        runner = self._runner()
        runner.pre_backup()
        self.assertTrue(runner._gzip)
        self.assertEqual('b1.tar.gz', runner.manifest)

        # The command as the base class runs it: split on whitespace.
        archive = os.path.join(self.datadir, 'out.tar')
        with open(archive, 'wb') as out:
            subprocess.check_call(runner.command.split(), stdout=out)
        with tarfile.open(archive) as tar:
            self.assertEqual(
                ['appdb/t-1/nb-1-big-Data.db', 'system/local-7/nb-3-Data.db'],
                sorted(tar.getnames()))

    def test_restore_unpacks_into_the_data_directory(self):
        runner = self._runner()
        self.assertEqual(['tar', '-xpf', '-', '-C', self.datadir],
                         runner.restore_command.split())
