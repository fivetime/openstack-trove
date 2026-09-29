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

import unittest

from oslo_config import cfg
from oslo_utils import importutils

from backup import main

CONF = cfg.CONF
CONF.backup_encryption_key = None
CONF.backup_id = "backup_unittest"


class TestRedisBackup(unittest.TestCase):
    def setUp(self):
        # The mapping the backup container really uses, not a copy of it.
        self.runner_cls = importutils.import_class(
            main.driver_mapping['redisbackup'])

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]

        self.assertIn('redisbackup', driver_opt.type.choices)

    def test_datadir(self):
        runner = self.runner_cls()

        self.assertEqual('redis', runner.DATASTORE_NAME)
        self.assertEqual('/var/lib/redis', runner.datadir)
        self.assertEqual('/var/lib/redis/backup.log', runner.backup_log)

    def test_datadir_can_be_overridden(self):
        runner = self.runner_cls(db_datadir='/var/lib/redis/data')

        self.assertEqual('/var/lib/redis/data', runner.datadir)

    def test_streams_are_compressed(self):
        runner = self.runner_cls()

        runner.pre_backup()

        self.assertTrue(runner._gzip)
