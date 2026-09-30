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

import shlex
import unittest
from unittest import mock

from oslo_config import cfg
from oslo_utils import importutils

from backup import main

CONF = cfg.CONF
CONF.backup_encryption_key = None
CONF.backup_id = "backup_unittest"


class TestMongoDump(unittest.TestCase):

    def setUp(self):
        self.runner_cls = importutils.import_class(
            main.driver_mapping['mongodump'])
        self.addCleanup(self._reset)
        self._reset()

    @staticmethod
    def _reset():
        for name in ('db_host', 'db_user', 'db_password'):
            try:
                CONF.clear_override(name)
            except cfg.NoSuchOptError:
                pass

    @staticmethod
    def _set(**values):
        for name, value in values.items():
            try:
                CONF.set_override(name, value)
            except cfg.NoSuchOptError:
                setattr(CONF, name, value)

    def test_driver_is_selectable(self):
        driver_opt = [opt for opt in main.cli_opts if opt.name == 'driver'][0]
        self.assertIn('mongodump', driver_opt.type.choices)
        self.assertNotIn('mongodump_inc', main.driver_mapping)

    def test_socket_and_credentials_go_into_the_uri(self):
        self._set(db_host='/var/run/mongodb/mongodb-27017.sock',
                  db_user='os_admin', db_password='pw')
        runner = self.runner_cls()
        uri = 'mongodb://os_admin:pw@%2Fvar%2Frun%2Fmongodb%2Fmongodb-27017' \
              '.sock/?authSource=admin'
        self.assertEqual(uri, runner.uri)
        self.assertEqual(['mongodump', '--uri=' + uri, '--archive'],
                         shlex.split(runner.command))
        self.assertEqual(
            ['mongorestore', '--uri=' + uri, '--archive', '--drop'],
            shlex.split(runner.restore_command))
        # The base class splits the command on whitespace.
        self.assertEqual(runner.command.split(), shlex.split(runner.command))

    def test_no_credentials_for_the_temporary_server(self):
        self._set(db_host='/var/run/mongodb/mongodb-27017.sock',
                  db_user=None, db_password=None)
        runner = self.runner_cls()
        self.assertEqual(
            'mongodb://%2Fvar%2Frun%2Fmongodb%2Fmongodb-27017.sock',
            runner.uri)
        self.assertNotIn('authSource', runner.restore_command)

    def test_manifest(self):
        self._set(db_host='localhost', db_user=None, db_password=None)
        runner = self.runner_cls(filename='b1')
        runner.pre_backup()
        self.assertTrue(runner._gzip)
        self.assertEqual('b1.archive.gz', runner.manifest)

    def test_backup_errors_are_read_from_the_log(self):
        self._set(db_host='localhost', db_user=None, db_password=None)
        runner = self.runner_cls()
        with mock.patch('builtins.open', mock.mock_open(
                read_data='done dumping appdb.t (2 documents)\n')), \
                mock.patch('os.path.exists', return_value=True):
            self.assertTrue(runner.check_process())
        with mock.patch('builtins.open', mock.mock_open(
                read_data='Failed: error connecting to db server\n')), \
                mock.patch('os.path.exists', return_value=True):
            self.assertFalse(runner.check_process())

    def test_restore_is_checked_by_exit_code(self):
        self._set(db_host='localhost', db_user=None, db_password=None)
        runner = self.runner_cls()
        runner.process = mock.Mock(returncode=0)
        self.assertTrue(runner.check_restore_process())
        runner.process = mock.Mock(returncode=1)
        self.assertFalse(runner.check_restore_process())
