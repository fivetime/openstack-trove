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


class TestDB2Backup(unittest.TestCase):

    def setUp(self):
        self.runner_cls = importutils.import_class(
            main.driver_mapping['db2backup'])
        self.datadir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.datadir, ignore_errors=True)

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]
        self.assertIn('db2backup', driver_opt.type.choices)

    def test_default_directory(self):
        self.assertEqual('/var/lib/db2/trove/backup',
                         self.runner_cls().datadir)

    def test_archive_is_the_images_and_the_users(self):
        for name in ('APPDB.0.db2inst1.DBPART000.20261001080037.001',
                     'users'):
            with open(os.path.join(self.datadir, name), 'w') as f:
                f.write(name)
        runner = self.runner_cls(filename='b1', db_datadir=self.datadir)
        runner.pre_backup()
        # The images are compressed by the instance: not again.
        self.assertFalse(runner._gzip)
        self.assertNotIn('gzip', runner.command)
        archive = tempfile.mktemp(suffix='.tar')
        self.addCleanup(os.remove, archive)
        with open(archive, 'wb') as out:
            subprocess.check_call(runner.command.split(), stdout=out)
        with tarfile.open(archive) as tar:
            self.assertEqual(
                {'.', './users',
                 './APPDB.0.db2inst1.DBPART000.20261001080037.001'},
                set(tar.getnames()))
        target = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target, ignore_errors=True)
        restore = self.runner_cls(db_datadir=target)
        with open(archive, 'rb') as stream:
            subprocess.check_call(restore.restore_command.split(),
                                  stdin=stream)
        self.assertTrue(os.path.exists(os.path.join(target, 'users')))
