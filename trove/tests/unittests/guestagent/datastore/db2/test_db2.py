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

import json
import subprocess
import tempfile
from unittest import mock

from oslo_config import cfg as oslo_cfg
from oslo_utils import importutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.db2 import models
from trove.common import exception
from trove.extensions.common import models as extension_models
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore.db2 import manager as db2_manager
from trove.guestagent.datastore.db2 import service as db2_service
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.guestagent.module import driver_manager
from trove.guestagent.module.drivers import db2_license_driver
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF


class DB2GuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a db2 instance sees it."""

    def setUp(self):
        super(DB2GuestTestCase, self).setUp()
        self.patch_datastore_manager('db2')
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass

    def _app(self, settings=None):
        status = mock.create_autospec(base_service.BaseDbStatus,
                                      instance=True)
        app = db2_service.DB2App(status, mock.Mock())
        app._configuration_manager = mock.Mock()
        app._configuration_manager.parse_configuration.return_value = \
            dict(settings or {})
        return app


class FakeInstance(object):
    """The users file, the databases and the authorities, in memory, and
    the commands the guest agent runs.
    """

    def __init__(self):
        self.users_file = None
        self.databases = []
        self.grants = {}
        self.commands = []
        self.statements = []
        self.system_users = {}

    def as_owner(self, command):
        self.commands.append(command)
        if command == 'db2 list database directory':
            if not self.databases:
                raise exception.TroveError('SQL1031N  The database '
                                           'directory cannot be found')
            out = []
            for name in self.databases:
                out += [' Database name                 = %s' % name,
                        ' Directory entry type          = Indirect']
            return '\n'.join(out) + '\n'
        if command.startswith('db2 CREATE DATABASE '):
            self.databases.append(command.split()[-1])
        if command.startswith('db2 DEACTIVATE DATABASE '):
            self.databases.remove(command.split()[-1])
        return ''

    def run(self, statements, database=None):
        self.statements.append((database, list(statements)))
        rows = []
        for statement in statements:
            if statement.startswith('GRANT '):
                user = statement.split('TO USER ')[1].strip('"')
                self.grants.setdefault(user, set()).add(database)
            elif statement.startswith('REVOKE '):
                user = statement.split('FROM USER ')[1].strip('"')
                self.grants.get(user, set()).discard(database)
            elif statement.startswith('SELECT '):
                user = statement.split("GRANTEE = '")[1].split("'")[0]
                if database in self.grants.get(user, set()):
                    rows.append([user])
        return rows

    def execute(self, command, environment=None, ok_codes=(0,)):
        self.commands.append(command)
        if command[0] == 'useradd':
            self.system_users[command[-1]] = '!'
        elif command[0] == 'userdel':
            self.system_users.pop(command[-1], None)
        elif command[0] == 'getent':
            return '%s:$6$hash-of-%s:1::::::\n' % (command[2], command[2])
        return ''


class TestDB2DatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['db2'])
        self.assertIs(db2_manager.Manager, manager_cls)

    def test_options(self):
        self.assertEqual('icr.io/db2_community/db2', CONF.db2.docker_image)
        self.assertEqual('db2backup', CONF.db2.backup_strategy)
        self.assertEqual('/var/lib/db2', CONF.db2.mount_point)
        # The image makes db2inst1 the first user and group of the
        # container.
        self.assertEqual('1000', CONF.db2.database_service_uid)
        self.assertEqual('1000', CONF.db2.database_service_gid)
        self.assertEqual({'db2inst1', 'db2fenc1', 'db2root'},
                         set(CONF.db2.ignore_users))

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('db2',
                      extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_the_license_module_type_is_allowed_and_has_a_driver(self):
        self.assertIn('db2_license', CONF.module_types)
        driver = driver_manager.ModuleDriverManager().get_driver(
            'db2_license')
        self.assertIsInstance(driver, db2_license_driver.DB2LicenseDriver)

    def test_rules_leave_out_what_the_tenant_must_not_set(self):
        with open('trove/templates/db2/validation-rules.json') as f:
            rules = [rule['name']
                     for rule in json.load(f)['configuration-parameters']]
        self.assertEqual(len(rules), len(set(rules)))
        for name in ('SYSADM_GROUP', 'AUTHENTICATION', 'SRVCON_AUTH',
                     'TRUST_ALLCLNTS', 'CLNT_PW_PLUGIN', 'SVCENAME',
                     'COMM_EXIT_LIST', 'SSL_SVR_KEYDB'):
            self.assertNotIn(name, rules)
        self.assertIn('MAX_CONNECTIONS', rules)


class TestDB2Models(trove_testtools.TestCase):

    def test_names(self):
        for name in ('appdb', 'A1', '@x', 'abcdefgh'):
            models.DB2Schema(name)
        for name in ('1db', 'abcdefghi', 'a-b', 'a b', 'a;b'):
            self.assertRaises(ValueError, models.DB2Schema, name)
        for name in ('appuser', '_u1'):
            models.DB2User(name, 'pw')
        for name in ('AppUser', '1u', 'a-b', 'a' * 31, 'a;b'):
            self.assertRaises(ValueError, models.DB2User, name, 'pw')

    def test_passwords_chpasswd_takes(self):
        models.DB2User('u', 'a"b\'c$d&e')
        for password in ('a:b', 'a\nb'):
            self.assertRaises(ValueError, models.DB2User, 'u', password)


class TestDB2App(DB2GuestTestCase):

    @mock.patch.object(db2_service, 'docker_util')
    @mock.patch.object(db2_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.return_value = True
        mock_os.read_file.return_value = 'pw'
        app.start_db(ds_version='12.1.5.0')

        args, kwargs = mock_docker.start_container.call_args
        self.assertEqual('icr.io/db2_community/db2:12.1.5.0', args[1])
        # Capabilities rather than a privileged container.
        self.assertEqual(['IPC_OWNER', 'SYS_RESOURCE', 'SYS_NICE'],
                         kwargs['cap_add'])
        self.assertNotIn('privileged', kwargs)
        self.assertEqual([{'Name': 'nofile', 'Soft': 65536,
                           'Hard': 65536}], kwargs['ulimits'])
        self.assertEqual('accept', kwargs['environment']['LICENSE'])
        self.assertEqual('pw', kwargs['environment']['DB2INST1_PASSWORD'])
        volumes = kwargs['volumes']
        self.assertEqual('/database', volumes['/var/lib/db2']['bind'])
        self.assertEqual({'bind': '/var/custom', 'mode': 'ro'},
                         volumes['/etc/db2-trove/custom'])
        self.assertEqual({'50000/tcp': 50000}, kwargs['ports'])
        # The image runs as root and makes its users itself.
        self.assertNotIn('user', kwargs)
        # The first start sets the instance up: longer than usual.
        self.assertEqual(900, app.status.wait_for_status.call_args[0][1])
        scripts = [call[0] for call in mock_os.write_file.call_args_list
                   if call[0][0].endswith('10-trove-users.sh')]
        self.assertEqual(1, len(scripts))

    def test_users_script_is_valid_bash(self):
        script = db2_service.USERS_SCRIPT_CONTENT % {
            'users': '/database/trove/users', 'group': 'db2iadm1'}
        with tempfile.NamedTemporaryFile('w', suffix='.sh') as f:
            f.write(script)
            f.flush()
            subprocess.check_call(['bash', '-n', f.name])
        self.assertIn('usermod -p "$hash" "$name"', script)

    def test_clp_no_row_and_warning_are_not_errors(self):
        app = self._app()
        container = app.docker_client.containers.get.return_value
        for code in (0, 1, 2):
            container.exec_run.return_value = (code, (b'out', None))
            self.assertEqual('out', app.as_owner('db2 x'))
        container.exec_run.return_value = (4, (b'SQL0204N x', None))
        self.assertRaisesRegex(exception.TroveError, 'SQL0204N',
                               app.as_owner, 'db2 x')
        # Plain commands keep 0 as the only success.
        container.exec_run.return_value = (1, (None, b'no'))
        self.assertRaises(exception.TroveError, app.execute, ['useradd'])

    @mock.patch.object(db2_service, 'operating_system')
    def test_a_license_is_written_as_text(self, mock_os):
        app = self._app()
        app.write_license(b'[LicenseCertificate]\n')
        written = {call[0][0]: call[0][1]
                   for call in mock_os.write_file.call_args_list}
        self.assertEqual('[LicenseCertificate]\n',
                         written['/var/lib/db2/trove/license.lic'])

    def test_install_license_keeps_it_on_the_volume(self):
        app = self._app()
        with mock.patch.object(db2_service.DB2App, 'execute') as execute, \
                mock.patch.object(db2_service.DB2App, 'as_owner',
                                  return_value='Product name: X'):
            self.assertEqual('Product name: X', app.install_license())
        self.assertEqual(
            [['/opt/ibm/db2/V12.1/adm/db2licm', '-a',
              '/database/trove/license.lic'],
             ['/var/db2_setup/lib/backup_cfg.sh']],
            [call[0][0] for call in execute.call_args_list])

    @mock.patch.object(db2_service, 'operating_system')
    def test_backup_writes_images_and_users_then_cleans(self, mock_os):
        app = self._app()
        order = mock.Mock()
        with mock.patch.object(db2_service.DB2Admin,
                               'backup_databases') as images, \
                mock.patch.object(base_service.BaseDbApp,
                                  'create_backup') as base:
            order.attach_mock(images, 'images')
            order.attach_mock(base, 'pack')
            order.attach_mock(mock_os.remove_dir_contents, 'clean')
            app.create_backup(mock.Mock(), {'id': 'b1'})
        self.assertEqual(['clean', 'images', 'pack', 'clean'],
                         [call[0] for call in order.mock_calls])
        images.assert_called_once_with('/database/trove/backup')
        mock_os.copy.assert_called_once_with(
            '/var/lib/db2/trove/users', '/var/lib/db2/trove/backup/users',
            preserve=True, as_root=True)
        self.assertEqual('--db-datadir=/var/lib/db2/trove/backup',
                         base.call_args[1]['extra_params'])

    @mock.patch.object(db2_service, 'operating_system')
    def test_restore_databases(self, mock_os):
        app = self._app()
        mock_os.exists.return_value = True
        listing = ('APPDB.0.db2inst1.DBPART000.20261001074619.001\n'
                   'users\nlogs_APPDB\nevil;rm.0.x\n')
        with mock.patch.object(db2_service.DB2App, 'execute',
                               return_value=listing), \
                mock.patch.object(db2_service.DB2Admin,
                                  'restore_database') as restore, \
                mock.patch.object(db2_service.DB2Admin,
                                  'apply_users') as apply_users:
            app.restore_databases()
        restore.assert_called_once_with(
            'APPDB.0.db2inst1.DBPART000.20261001074619.001',
            '/database/trove/restore')
        apply_users.assert_called_once_with()
        mock_os.remove_dir_contents.assert_called_once_with(
            '/var/lib/db2/trove/restore')


class TestDB2Admin(DB2GuestTestCase):

    def _adm(self):
        app = self._app()
        fake = FakeInstance()
        app.as_owner = fake.as_owner
        app.execute = fake.execute
        app.write_run_file = mock.Mock(return_value=('/h/x', '/c/x'))
        adm = db2_service.DB2Admin(app)
        adm.run = fake.run
        users = {}

        def read():
            return dict(users)

        def write(new):
            users.clear()
            users.update(new)
        adm._read_users = read
        adm._write_users = write
        self.patch = mock.patch.object(db2_service, 'operating_system')
        self.patch.start()
        self.addCleanup(self.patch.stop)
        return adm, fake, users

    def _user(self, name, password='secret', databases=()):
        user = models.DB2User(name, password)
        for database in databases:
            user.databases = database
        return user.serialize()

    def test_run(self):
        app = self._app()
        with mock.patch.object(db2_service.DB2App, 'write_run_file',
                               return_value=('/h/f', '/c/f')) as write, \
                mock.patch.object(db2_service.DB2App, 'as_owner',
                                  return_value='Database Connection\n'
                                               'TROVE_ROW\tA \tY\n'
                                               'DB20000I done\n') as owner, \
                mock.patch.object(db2_service, 'operating_system') as osys:
            rows = db2_service.DB2Admin(app).run(['SELECT 1'],
                                                 database='APPDB')
        self.assertEqual([['A', 'Y']], rows)
        self.assertEqual('CONNECT TO APPDB;\nSELECT 1;\nCONNECT RESET;\n',
                         write.call_args[0][0])
        # The statements are in a file, the command line is fixed.
        owner.assert_called_once_with('db2 -txf /c/f')
        osys.remove.assert_called_once_with('/h/f', force=True,
                                            as_root=True)

    def test_databases(self):
        adm, fake, _ = self._adm()
        self.assertEqual([], adm.list_databases()[0])
        adm.create_database([models.DB2Schema('appdb').serialize()])
        self.assertEqual(['APPDB'],
                         [d['_name'] for d in adm.list_databases()[0]])
        # Ready for online backups.
        self.assertIn('db2 UPDATE DB CFG FOR APPDB USING LOGARCHMETH1 '
                      'DISK:/database/trove/archive', fake.commands)
        self.assertIn('db2 BACKUP DATABASE APPDB TO /dev/null',
                      fake.commands)
        adm.delete_database(models.DB2Schema('appdb').serialize())
        self.assertEqual([], adm.list_databases()[0])

    def test_users_are_system_users_with_authorities(self):
        adm, fake, users = self._adm()
        adm.create_database([models.DB2Schema('appdb').serialize(),
                             models.DB2Schema('otherdb').serialize()])
        adm.create_user([self._user('appuser', "it's", ['appdb'])])

        self.assertIn(['useradd', '-M', '-s', '/sbin/nologin', 'appuser'],
                      fake.commands)
        # The password goes through a file, not a command line.
        adm.app.write_run_file.assert_called_with("appuser:it's\n")
        self.assertIn(['sh', '-c', 'chpasswd < /c/x'], fake.commands)
        self.assertEqual({'appuser': ('$6$hash-of-appuser', False)}, users)
        self.assertEqual({'APPDB'}, fake.grants['APPUSER'])

        listed = adm.list_users()[0]
        self.assertEqual(['appuser'], [u['_name'] for u in listed])
        self.assertEqual(['APPDB'],
                         [d['_name'] for d in listed[0]['_databases']])
        adm.grant_access('appuser', None, ['otherdb'])
        self.assertEqual(['APPDB', 'OTHERDB'],
                         [d['_name'] for d in adm.list_access('appuser')])
        adm.revoke_access('appuser', None, 'appdb')
        self.assertEqual(['OTHERDB'],
                         [d['_name'] for d in adm.list_access('appuser')])

    def test_a_database_being_created_is_left_out(self):
        # Created, then backed up offline: in exclusive use for minutes.
        adm, fake, users = self._adm()
        adm.create_database([models.DB2Schema('appdb').serialize(),
                             models.DB2Schema('newdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])
        real_run = fake.run

        def run(statements, database=None):
            if database == 'NEWDB':
                raise exception.TroveError(
                    'SQL1035N  The operation failed because the specified '
                    'database cannot be connected to in the mode requested.')
            return real_run(statements, database)
        adm.run = run
        self.assertEqual(['APPDB'],
                         [d['_name'] for d in adm.list_access('appuser')])
        # Other errors are errors.
        adm.run = mock.Mock(side_effect=exception.TroveError('SQL0204N'))
        self.assertRaises(exception.TroveError, adm.list_access, 'appuser')

    def test_delete_user_revokes_first(self):
        adm, fake, users = self._adm()
        adm.create_database([models.DB2Schema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])
        adm.delete_user(self._user('appuser'))
        self.assertEqual(set(), fake.grants['APPUSER'])
        self.assertEqual({}, users)
        self.assertIn(['userdel', 'appuser'], fake.commands)
        adm.delete_user(self._user('appuser'))

    def test_reserved_missing_and_renamed_users(self):
        adm, fake, users = self._adm()
        for name in ('db2inst1', 'db2fenc1', 'db2root'):
            self.assertRaises(exception.BadRequest, adm.create_user,
                              [self._user(name)])
        self.assertRaises(exception.UserNotFound, adm.grant_access,
                          'nobody', None, ['appdb'])
        adm.create_user([self._user('appuser')])
        self.assertRaises(exception.UnprocessableEntity,
                          adm.update_attributes, 'appuser', None,
                          {'name': 'other'})

    def test_root_is_in_the_owners_group(self):
        adm, fake, users = self._adm()
        self.assertFalse(adm.is_root_enabled())
        root = adm.enable_root('rootpw')
        self.assertEqual('db2root', root['_name'])
        self.assertIn(['usermod', '-a', '-G', 'db2iadm1', 'db2root'],
                      fake.commands)
        self.assertEqual(('$6$hash-of-db2root', True), users['db2root'])
        self.assertTrue(adm.is_root_enabled())
        self.assertEqual([], adm.list_users()[0])
        adm.disable_root()
        self.assertFalse(adm.is_root_enabled())

    def test_parameters(self):
        adm, fake, _ = self._adm()
        adm.set_parameter('MAX_CONNECTIONS', 100)
        self.assertIn('db2 UPDATE DBM CFG USING MAX_CONNECTIONS 100 '
                      'IMMEDIATE', fake.commands)
        self.assertRaises(exception.BadRequest, adm.set_parameter,
                          'MAX_CONNECTIONS', '1; rm -rf /')

    def test_restore_database(self):
        adm, fake, _ = self._adm()
        adm.restore_database(
            'APPDB.0.db2inst1.DBPART000.20261001074619.001', '/r')
        self.assertIn('db2 RESTORE DATABASE APPDB FROM /r TAKEN AT '
                      '20261001074619 LOGTARGET /r/logs_APPDB WITHOUT '
                      'PROMPTING', fake.commands)
        self.assertIn('db2 "ROLLFORWARD DATABASE APPDB TO END OF BACKUP AND '
                      'COMPLETE OVERFLOW LOG PATH (/r/logs_APPDB)"',
                      fake.commands)
        self.assertRaises(exception.TroveError, adm.restore_database,
                          'x;rm -rf.0.db2inst1.DBPART000.1.001', '/r')


class TestDB2LicenseDriver(DB2GuestTestCase):

    def test_type(self):
        self.assertEqual('db2_license',
                         db2_license_driver.DB2LicenseDriver().get_type())

    def test_installs_the_license(self):
        driver = db2_license_driver.DB2LicenseDriver()
        app = mock.Mock()
        app.install_license.return_value = 'Product name: Db2 Standard'
        with mock.patch.object(db2_license_driver.DB2LicenseDriver, '_app',
                               return_value=app), \
                mock.patch.object(db2_license_driver.operating_system,
                                  'read_file', return_value=b'LIC'):
            self.assertEqual((True, 'Product name: Db2 Standard'),
                             driver.apply('lic', 'db2', '12.1', '/x', False))
        app.write_license.assert_called_once_with(b'LIC')

    def test_other_datastores_are_refused(self):
        self.patch_datastore_manager('mysql')
        driver = db2_license_driver.DB2LicenseDriver()
        self.assertFalse(driver.apply('lic', 'db2', '12.1', '/x', False)[0])


class TestDB2Manager(DB2GuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = db2_manager.Manager()
        manager.app = mock.Mock()
        manager.app.mount_point = '/var/lib/db2'
        manager.app.has_admin.return_value = False
        manager.adm = manager.app.adm
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=4096, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/db2', backup_info=None,
                    config_contents='', root_password=None, overrides=None,
                    cluster_config=None, snapshot=None,
                    ds_version='12.1.5.0')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_new_instance(self):
        manager = self._manager()
        self._prepare(manager)
        manager.app.secure.assert_called_once_with()
        manager.app.start_db.assert_called_once_with(ds_version='12.1.5.0')
        manager.app.restore_databases.assert_not_called()

    @mock.patch.object(db2_manager.Manager, 'perform_restore')
    def test_restored_instance(self, mock_restore):
        manager = self._manager()
        order = mock.Mock()
        order.attach_mock(mock_restore, 'unpack')
        order.attach_mock(manager.app.start_db, 'start')
        order.attach_mock(manager.app.restore_databases, 'restore')
        self._prepare(manager, backup_info={'id': 'b1'})
        self.assertEqual(['unpack', 'start', 'restore'],
                         [call[0] for call in order.mock_calls])


class TestDB2Files(DB2GuestTestCase):

    @mock.patch.object(db2_service, 'operating_system')
    def test_run_files_belong_to_the_owner_alone(self, mock_os):
        app = self._app()
        host, container = app.write_run_file('x')
        self.assertTrue(host.startswith('/var/lib/db2/trove/run/'))
        self.assertTrue(container.startswith('/database/trove/run/'))
        mock_os.chown.assert_called_once_with(host, '1000', '1000',
                                              as_root=True)
        mock_os.chmod.assert_called_once_with(host, FileMode.SET_USR_RW,
                                              as_root=True)
