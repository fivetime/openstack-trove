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


class TestVerticaBackup(unittest.TestCase):

    def setUp(self):
        self.runner_cls = importutils.import_class(
            main.driver_mapping['verticabackup'])
        self.datadir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.datadir, ignore_errors=True)

    def _touch(self, *parts):
        path = os.path.join(self.datadir, *parts)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            f.write(parts[-1])

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]
        self.assertIn('verticabackup', driver_opt.type.choices)

    def test_default_data_directory(self):
        self.assertEqual('/var/lib/vertica', self.runner_cls().datadir)

    def test_archive_is_the_volume_without_the_logs(self):
        node = os.path.join('db_srvr', 'v_db_srvr_node0001_catalog')
        self._touch('vertica_cluster.yaml')
        self._touch(node, 'Catalog', 'catalog.cat')
        self._touch(node, 'vertica.log')
        self._touch('db_srvr', 'dbLog')
        self._touch('db_srvr', 'v_db_srvr_node0001_data', '123.gt')
        runner = self.runner_cls(filename='b1', db_datadir=self.datadir)
        runner.pre_backup()
        self.assertTrue(runner._gzip)
        self.assertEqual('b1.tar.gz', runner.manifest)

        archive = tempfile.mktemp(suffix='.tar')
        self.addCleanup(os.remove, archive)
        with open(archive, 'wb') as out:
            subprocess.check_call(runner.command.split(), stdout=out)
        with tarfile.open(archive) as tar:
            names = tar.getnames()
        self.assertIn('./vertica_cluster.yaml', names)
        self.assertIn('./%s/Catalog/catalog.cat' % node, names)
        self.assertIn('./db_srvr/v_db_srvr_node0001_data/123.gt', names)
        self.assertNotIn('./%s/vertica.log' % node, names)
        self.assertNotIn('./db_srvr/dbLog', names)

        target = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, target, ignore_errors=True)
        restore = self.runner_cls(db_datadir=target)
        self.assertEqual(['tar', '-xpf', '-', '-C', target],
                         restore.restore_command.split())
        with open(archive, 'rb') as stream:
            subprocess.check_call(restore.restore_command.split(),
                                  stdin=stream)
        self.assertTrue(os.path.exists(
            os.path.join(target, 'vertica_cluster.yaml')))
