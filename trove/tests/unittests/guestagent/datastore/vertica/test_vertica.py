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

from cryptography import x509
from oslo_config import cfg as oslo_cfg
from oslo_serialization import base64
from oslo_utils import importutils

from trove.common import cfg
from trove.common import constants
from trove.common.db.vertica import models
from trove.common import exception
from trove.common import stream_codecs
from trove.common import template
from trove.extensions.common import models as extension_models
from trove.guestagent.common import configuration
from trove.guestagent.common.operating_system import FileMode
from trove.guestagent.datastore import manager as base_manager
from trove.guestagent.datastore import service as base_service
from trove.guestagent.datastore.vertica import manager as vertica_manager
from trove.guestagent.datastore.vertica import service as vertica_service
from trove.guestagent.module import driver_manager
from trove.guestagent.module.drivers import vertica_license_driver
from trove.tests.unittests import trove_testtools

CONF = cfg.CONF
CODEC = stream_codecs.KeyValueCodec(line_terminator='\n')


def _template(ram=4096):
    ds_version = mock.Mock(datastore_name='vertica', manager='vertica',
                           version='25.4.0-0-minimal')
    ds_version.name = '25.4'
    return template.SingleInstanceConfigTemplate(
        ds_version, {'ram': ram, 'vcpus': 2},
        'c2a8a4a5-7a8e-4a0c-9d5e-0b4c5d1f2a3b')


class VerticaGuestTestCase(trove_testtools.TestCase):
    """CONF as the guest agent of a vertica instance sees it."""

    def setUp(self):
        super(VerticaGuestTestCase, self).setUp()
        self.patch_datastore_manager('vertica')
        # Registered by the guest agent process, not by trove.common.cfg.
        try:
            CONF.register_opts([oslo_cfg.BoolOpt('network_isolation',
                                                 default=False)])
        except oslo_cfg.DuplicateOptError:
            pass

    def _app(self, settings=None, overrides=None):
        # The status is the real class behind a mock: a call the container
        # based status does not have must fail here, not on a guest.
        status = mock.create_autospec(base_service.BaseDbStatus,
                                      instance=True)
        app = vertica_service.VerticaApp(status, mock.Mock())
        app._configuration_manager = mock.Mock()
        app._configuration_manager.parse_configuration.return_value = \
            dict(settings or {})
        app._configuration_manager.get_user_override.return_value = \
            dict(overrides or {})
        return app


class FakeServer(object):
    """The part of the catalog the guest agent reads, in memory, and the
    statements it runs.
    """

    def __init__(self):
        self.schemas = []
        self.users = []
        self.grants = {}
        self.statements = []

    def run(self, statements, password=None):
        rows = []
        for statement in statements:
            self.statements.append(statement)
            rows += self._handle(statement)
        return rows

    def query(self, statement):
        return self.run([statement])

    @staticmethod
    def _names(statement):
        return [part for i, part in enumerate(statement.split('"'))
                if i % 2]

    def _handle(self, statement):
        names = self._names(statement)
        if statement.startswith('SELECT schema_name'):
            return [[name] for name in sorted(
                self.schemas + ['public', 'v_func', 'v_txtindex'])]
        if statement.startswith('SELECT user_name FROM v_catalog.users '
                                'WHERE'):
            wanted = statement.split("lower('")[1].split("')")[0]
            return [[name] for name in self.users
                    if name.lower() == wanted.lower()]
        if statement.startswith('SELECT user_name'):
            return [[name] for name in sorted(self.users + ['dbadmin'])]
        if statement.startswith('SELECT object_name FROM v_catalog.grants'):
            wanted = statement.split("lower('")[1].split("')")[0]
            return [[schema] for schema in sorted(
                self.grants.get(wanted.lower(), []))]
        if statement.startswith('CREATE SCHEMA'):
            if names[0] in self.schemas:
                raise exception.TroveError('Object already exists')
            self.schemas.append(names[0])
        elif statement.startswith('DROP SCHEMA'):
            self.schemas.remove(names[0])
            for schemas in self.grants.values():
                if names[0] in schemas:
                    schemas.remove(names[0])
        elif statement.startswith('CREATE USER'):
            self.users.append(names[0])
        elif statement.startswith('DROP USER'):
            self.users.remove(names[0])
            self.grants.pop(names[0].lower(), None)
        elif statement.startswith('ALTER USER') and ' RENAME TO ' in \
                statement:
            self.users[self.users.index(names[0])] = names[1]
            self.grants[names[1].lower()] = self.grants.pop(
                names[0].lower(), [])
        elif statement.startswith('GRANT ALL PRIVILEGES EXTEND'):
            if names[0] not in self.schemas:
                raise exception.TroveError('Schema does not exist')
            schemas = self.grants.setdefault(names[1].lower(), [])
            if names[0] not in schemas:
                schemas.append(names[0])
        elif statement.startswith('REVOKE ALL PRIVILEGES'):
            schemas = self.grants.get(names[1].lower(), [])
            if names[0] in schemas:
                schemas.remove(names[0])
        return []


class TestVerticaDatastoreWiring(trove_testtools.TestCase):

    def test_default_manager_resolves(self):
        manager_cls = importutils.import_class(
            constants.REGISTRY_EXT_DEFAULTS['vertica'])
        self.assertIs(vertica_manager.Manager, manager_cls)
        self.assertTrue(issubclass(manager_cls, base_manager.Manager))

    def test_options(self):
        self.assertEqual('opentext/vertica-k8s', CONF.vertica.docker_image)
        self.assertEqual('verticabackup', CONF.vertica.backup_strategy)
        self.assertEqual('/var/lib/vertica', CONF.vertica.mount_point)
        # The image runs as the dbadmin user, whose ids it has.
        self.assertEqual('997', CONF.vertica.database_service_uid)
        self.assertEqual('995', CONF.vertica.database_service_gid)
        ports = {port for port_range in CONF.vertica.tcp_ports
                 for port in port_range}
        # The clients' port alone: the agent and the HTTPS service of a
        # single node are for the node itself.
        self.assertEqual({5433}, ports)
        self.assertEqual([], CONF.vertica.udp_ports)
        # The guest side of clusters is not there.
        self.assertFalse(CONF.vertica.cluster_support)

    def test_the_admins_and_the_system_schemas_are_hidden(self):
        self.assertEqual({'dbadmin', 'root'},
                         set(CONF.vertica.ignore_users))
        self.assertEqual({'public', 'v_func', 'v_txtindex'},
                         set(CONF.vertica.ignore_dbs))

    def test_user_and_database_apis_are_enabled(self):
        self.assertIn('vertica',
                      extension_models.EXTENSIONS_SUPPORTED_DATASTORES)
        self.assertIn(
            'vertica', extension_models.SCHEMA_MANAGEMENT_SUPPORTED_DATASTORES)

    def test_the_license_module_type_is_allowed_and_has_a_driver(self):
        self.assertIn('vertica_license', CONF.module_types)
        driver = driver_manager.ModuleDriverManager().get_driver(
            'vertica_license')
        self.assertIsInstance(driver,
                              vertica_license_driver.VerticaLicenseDriver)

    def test_template_is_parsed_on_both_sides(self):
        config = _template()
        self.assertEqual({'MaxClientSessions': '50'},
                         CODEC.deserialize(config.render()))
        # The API side parses it when a configuration group is detached.
        self.assertEqual({'MaxClientSessions': 50},
                         dict(config.render_dict()))

    def test_rules_leave_out_what_the_tenant_must_not_set(self):
        with open('trove/templates/vertica/validation-rules.json') as f:
            rules = {rule['name'].lower(): rule
                     for rule in json.load(f)['configuration-parameters']}
        for name in ('enablessl', 'sslprivatekey', 'securityalgorithm',
                     'restrictsystemtables', 'disableinheritedprivileges',
                     'globalheirusername', 'javabinaryforudx',
                     'eeverticaoptions'):
            self.assertNotIn(name, rules)
        self.assertIn('maxclientsessions', rules)
        # As the server says, not as the rules of 7.x did.
        self.assertTrue(rules['blockcachesize']['restart_required'])
        self.assertFalse(rules['maxclientsessions']['restart_required'])


class TestVerticaApp(VerticaGuestTestCase):

    def test_configuration_manager(self):
        app = vertica_service.VerticaApp(mock.Mock(), mock.Mock())
        with mock.patch.object(configuration.OneFileOverrideStrategy,
                               'configure'):
            manager = app.configuration_manager
        self.assertIsInstance(manager._codec, stream_codecs.KeyValueCodec)
        self.assertEqual('/etc/vertica-trove/vertica.conf',
                         manager._base_config_path)

    @mock.patch.object(vertica_service.VerticaApp, 'save_password')
    @mock.patch.object(vertica_service, 'operating_system')
    def test_secure_writes_what_the_container_needs(self, mock_os,
                                                    mock_save):
        app = self._app()
        with mock.patch.object(vertica_service.VerticaApp, 'address',
                               new_callable=mock.PropertyMock,
                               return_value='192.168.1.5'):
            app.secure('pw')

        written = {call[0][0]: call[0][1]
                   for call in mock_os.write_file.call_args_list}
        self.assertEqual('pw', written['/etc/vertica-trove/admin.secret'])
        self.assertEqual('192.168.1.5', written['/etc/vertica-trove/host'])
        script = written['/etc/vertica-trove/start.sh']
        self.assertIn('/opt/vertica/bin/node_management_agent &', script)
        self.assertIn('vcluster re_ip --db-name db_srvr', script)
        self.assertIn('--password-file $CONF/admin.secret',
                      script)
        for name in ('rootca.pem', 'dbadmin.pem', 'dbadmin.key',
                     'vertica_https.pem', 'nma_client.pem',
                     'httpstls.json'):
            self.assertIn('/etc/vertica-trove/certs/' + name, written)
        # Every file belongs to the database user, and the password and
        # the keys to it alone.
        self.assertEqual(
            {'997'}, {call[0][1] for call in mock_os.chown.call_args_list})
        modes = {}
        for call in mock_os.chmod.call_args_list:
            modes.setdefault(call[0][0], []).append(call[0][1])
        self.assertEqual([FileMode.SET_USR_RW],
                         modes['/etc/vertica-trove/admin.secret'])
        self.assertEqual([FileMode.SET_USR_RW],
                         modes['/etc/vertica-trove/certs/dbadmin.key'])
        self.assertIn(FileMode.ADD_READ_ALL,
                      modes['/etc/vertica-trove/start.sh'])
        mock_save.assert_called_once_with('dbadmin', 'pw')

    @mock.patch.object(vertica_service, 'operating_system')
    def test_a_license_is_written_as_text(self, mock_os):
        # The module hands the guest bytes; the file is written in text
        # mode.
        app = self._app()
        app.write_license(b'Acme\n10TB\n')
        written = {call[0][0]: call[0][1]
                   for call in mock_os.write_file.call_args_list}
        self.assertEqual('Acme\n10TB\n',
                         written['/etc/vertica-trove/license.key'])

    def test_the_admin_password_is_read_where_it_is_saved(self):
        app = self._app()
        with mock.patch.object(base_service.BaseDbApp, 'save_password'), \
                mock.patch.object(base_service.guestagent_utils,
                                  'get_conf_dir', return_value='/c'), \
                mock.patch.object(base_service.operating_system,
                                  'read_file',
                                  return_value={'client': {'password': 'pw'}}
                                  ) as read_file:
            self.assertEqual('pw', app.admin_password)
        self.assertEqual('/c/dbadmin.cnf', read_file.call_args[0][0])
        # Where save_password writes it.
        with mock.patch.object(base_service.guestagent_utils,
                               'get_conf_dir', return_value='/c'), \
                mock.patch.object(base_service.operating_system,
                                  'write_file') as write_file:
            base_service.BaseDbApp.save_password('dbadmin', 'pw')
        self.assertEqual('/c/dbadmin.cnf', write_file.call_args[0][0])

    def test_start_script_is_valid_bash(self):
        app = self._app()
        written = {}
        with mock.patch.object(
                vertica_service.VerticaApp, '_write_owned',
                side_effect=lambda path, content, mode=None:
                written.__setitem__(path, content)), \
                mock.patch.object(vertica_service.VerticaApp, 'address',
                                  new_callable=mock.PropertyMock,
                                  return_value='10.0.0.1'):
            app.write_node_files()
        with tempfile.NamedTemporaryFile('w', suffix='.sh') as f:
            f.write(written['/etc/vertica-trove/start.sh'])
            f.flush()
            subprocess.check_call(['bash', '-n', f.name])

    def test_certificates_chain_to_the_authority(self):
        files = vertica_service.generate_certificates()
        ca = x509.load_pem_x509_certificate(files['rootca.pem'].encode())
        for name in ('dbadmin', 'vertica_https', 'nma_client'):
            cert = x509.load_pem_x509_certificate(
                files[name + '.pem'].encode())
            self.assertEqual(ca.subject, cert.issuer)
            cert.verify_directly_issued_by(ca)
        tls = json.loads(files['httpstls.json'])
        self.assertEqual(files['vertica_https.pem'], tls['certificate'])
        self.assertEqual([files['rootca.pem']], tls['ca_certificates'])

    @mock.patch.object(vertica_service, 'docker_util')
    @mock.patch.object(vertica_service, 'operating_system')
    def test_start_db(self, mock_os, mock_docker):
        app = self._app()
        app.status.wait_for_status.return_value = True
        mock_os.exists.return_value = True
        with mock.patch.object(vertica_service.VerticaApp,
                               'write_node_files') as write_node_files:
            app.start_db(ds_version='25.4.0-0-minimal')

        write_node_files.assert_called_once_with()
        args, kwargs = mock_docker.start_container.call_args
        self.assertEqual('opentext/vertica-k8s:25.4.0-0-minimal', args[1])
        self.assertEqual(['/bin/bash', '/etc/vertica-trove/start.sh'],
                         kwargs['command'])
        self.assertEqual('997:995', kwargs['user'])
        volumes = kwargs['volumes']
        self.assertEqual('/data', volumes['/var/lib/vertica']['bind'])
        self.assertEqual('/opt/vertica/config/https_certs',
                         volumes['/etc/vertica-trove/certs']['bind'])
        self.assertEqual({'5433/tcp': 5433}, kwargs['ports'])
        # The server requires 32768 open files and docker gives 1024.
        self.assertEqual([{'Name': 'nofile', 'Soft': 65536,
                           'Hard': 65536}], kwargs['ulimits'])
        # The health check needs no password.
        self.assertNotIn('VSQL_PASSWORD', kwargs['healthcheck']['test'][1])
        self.assertNotIn('-h', kwargs['healthcheck']['test'][1].split())
        app.status.wait_for_status.assert_called_once()

    @mock.patch.object(vertica_service, 'docker_util')
    @mock.patch.object(vertica_service, 'operating_system')
    def test_start_db_without_a_database_does_not_wait(
            self, mock_os, mock_docker):
        app = self._app()
        mock_os.exists.return_value = False
        with mock.patch.object(vertica_service.VerticaApp,
                               'write_node_files'):
            app.start_db()
        app.status.wait_for_status.assert_not_called()

    def test_create_database(self):
        app = self._app()
        app.status.wait_for_status.return_value = True
        with mock.patch.object(vertica_service.VerticaApp, 'vcluster') as \
                vcluster, \
                mock.patch.object(vertica_service.VerticaApp,
                                  'agent_is_up', return_value=True), \
                mock.patch.object(vertica_service.VerticaApp, 'has_license',
                                  return_value=True), \
                mock.patch.object(vertica_service.VerticaApp, 'address',
                                  new_callable=mock.PropertyMock,
                                  return_value='10.0.0.1'), \
                mock.patch.object(vertica_service.VerticaApp,
                                  'get_auth_password', return_value='pw'), \
                mock.patch.object(vertica_service.VerticaAdmin,
                                  'run') as run:
            app.create_database()

        args = vcluster.call_args[0]
        self.assertEqual('create_db', args[0])
        self.assertEqual('10.0.0.1', args[args.index('--hosts') + 1])
        self.assertEqual('/etc/vertica-trove/license.key',
                         args[args.index('--license') + 1])
        # The password stays in its file.
        self.assertNotIn('pw', args)
        statements, kwargs = run.call_args[0][0], run.call_args[1]
        self.assertEqual('pw', kwargs['password'])
        self.assertIn('''CREATE AUTHENTICATION "trove_local" METHOD 'trust' '''
                      'LOCAL', statements)
        self.assertIn('GRANT AUTHENTICATION "trove_local" TO "dbadmin"',
                      statements)

    def test_a_version_without_the_community_edition_needs_a_license(self):
        app = self._app()
        with mock.patch.object(
                vertica_service.VerticaApp, 'vcluster',
                side_effect=exception.TroveError(
                    'Community Edition (CE) license is deprecated')), \
                mock.patch.object(vertica_service.VerticaApp,
                                  'agent_is_up', return_value=True), \
                mock.patch.object(vertica_service.VerticaApp, 'has_license',
                                  return_value=False), \
                mock.patch.object(vertica_service.VerticaApp, 'address',
                                  new_callable=mock.PropertyMock,
                                  return_value='10.0.0.1'):
            self.assertRaisesRegex(exception.TroveError, 'vertica_license',
                                   app.create_database)

    def test_settings_are_set_on_the_database(self):
        app = self._app(settings={'MaxClientSessions': 50,
                                  'DefaultSessionLocale': 'en_US'})
        with mock.patch.object(vertica_service.VerticaAdmin, 'run') as run:
            app.apply_settings()
        statements = [call[0][0][0] for call in run.call_args_list]
        self.assertIn('ALTER DATABASE DEFAULT SET PARAMETER '
                      '"GlobalHeirUsername" = \'dbadmin\'', statements)
        self.assertIn('ALTER DATABASE DEFAULT SET PARAMETER '
                      '"MaxClientSessions" = 50', statements)
        self.assertIn('ALTER DATABASE DEFAULT SET PARAMETER '
                      '"DefaultSessionLocale" = \'en_US\'', statements)

    def test_removed_overrides_are_cleared(self):
        app = self._app(overrides={'MaxClientSessions': 77})
        with mock.patch.object(vertica_service.VerticaAdmin, 'run') as run:
            app.remove_overrides()
        app.configuration_manager.remove_user_override.assert_called_once()
        run.assert_called_once_with([
            'ALTER DATABASE DEFAULT CLEAR PARAMETER "MaxClientSessions"'])

    def test_backup_stops_the_database_for_the_copy(self):
        app = self._app()
        order = mock.Mock()
        with mock.patch.object(vertica_service.VerticaApp,
                               'shutdown_database') as shutdown, \
                mock.patch.object(vertica_service.VerticaApp,
                                  'start_database') as start, \
                mock.patch.object(base_service.BaseDbApp,
                                  'create_backup',
                                  side_effect=exception.TroveError('swift')
                                  ) as base:
            order.attach_mock(shutdown, 'shutdown')
            order.attach_mock(base, 'backup')
            order.attach_mock(start, 'start')
            self.assertRaises(exception.TroveError, app.create_backup,
                              mock.Mock(),
                              {'id': 'b1'})
        # Started again even when the copy fails.
        self.assertEqual(['shutdown', 'backup', 'start'],
                         [call[0] for call in order.mock_calls])
        kwargs = base.call_args[1]
        self.assertFalse(kwargs['need_dbuser'])
        self.assertEqual('--db-datadir=/var/lib/vertica',
                         kwargs['extra_params'])

    def test_execute(self):
        app = self._app()
        container = app.docker_client.containers.get.return_value
        container.exec_run.return_value = (0, (b'out', None))
        self.assertEqual('out', app.execute(['vsql'], environment={'A': 1}))
        container.exec_run.assert_called_once_with(
            ['vsql'], environment={'A': 1}, demux=True)

        container.exec_run.return_value = (1, (None, b'ERROR 1: x'))
        self.assertRaisesRegex(exception.TroveError, 'ERROR 1',
                               app.execute, ['vsql'])


class TestVerticaAdmin(VerticaGuestTestCase):

    def _adm(self):
        adm = vertica_service.VerticaAdmin(self._app())
        server = FakeServer()
        adm.run = server.run
        adm.query = server.query
        return adm, server

    def _user(self, name, password='secret', databases=()):
        user = models.VerticaUser(name, password)
        for database in databases:
            user.databases = database
        return user.serialize()

    def test_run(self):
        app = self._app()
        adm = vertica_service.VerticaAdmin(app)
        with mock.patch.object(vertica_service.VerticaApp, 'execute',
                               return_value='a\x1fb\nc\x1fd\n') as execute:
            self.assertEqual([['a', 'b'], ['c', 'd']],
                             adm.run(['SELECT 1', 'SELECT 2']))
        command, kwargs = execute.call_args[0][0], execute.call_args[1]
        # Over the socket, as the admin, without a password.
        self.assertEqual(['vsql', '-U', 'dbadmin'], command[:3])
        self.assertNotIn('-h', command)
        self.assertIsNone(kwargs['environment'])
        self.assertIn('ON_ERROR_STOP=1', command)
        self.assertEqual('SELECT 1; SELECT 2', command[-1])

        with mock.patch.object(vertica_service.VerticaApp, 'execute',
                               return_value='') as execute:
            adm.run(['SELECT 1'], password='pw')
        command, kwargs = execute.call_args[0][0], execute.call_args[1]
        # The password goes in the environment, not on the command line.
        self.assertEqual({'VSQL_PASSWORD': 'pw'}, kwargs['environment'])
        self.assertNotIn('pw', command)
        self.assertIn('-h', command)

    def test_quoting(self):
        self.assertEqual('"a""b"', vertica_service.quote_identifier('a"b'))
        self.assertEqual("'it''s'", vertica_service.quote_literal("it's"))

    def test_databases_are_schemas(self):
        adm, server = self._adm()
        adm.create_database([models.VerticaSchema('appdb').serialize()])
        self.assertIn('CREATE SCHEMA "appdb" DEFAULT INCLUDE SCHEMA '
                      'PRIVILEGES', server.statements)
        # The schemas of the system and the default one are hidden.
        self.assertEqual(['appdb'],
                         [d['_name'] for d in adm.list_databases()[0]])
        adm.delete_database(models.VerticaSchema('appdb').serialize())
        self.assertIn('DROP SCHEMA "appdb" CASCADE', server.statements)
        self.assertRaises(ValueError, adm.create_database,
                          [models.VerticaSchema('public').serialize()])

    def test_invalid_names(self):
        for name in ('a b', 'a"b', '1a', 'a;b', 'a' * 129):
            self.assertRaises(ValueError, models.VerticaSchema, name)
            self.assertRaises(ValueError, models.VerticaUser, name, 'pw')
        models.VerticaSchema('App_DB$1')

    def test_users_and_their_schemas(self):
        adm, server = self._adm()
        adm.create_database([models.VerticaSchema('appdb').serialize(),
                             models.VerticaSchema('otherdb').serialize()])
        adm.create_user([self._user('appuser', "it's", ['appdb'])])
        self.assertIn('''CREATE USER "appuser" IDENTIFIED BY 'it''s\'''',
                      server.statements)
        self.assertIn('GRANT ALL PRIVILEGES EXTEND ON SCHEMA "appdb" TO '
                      '"appuser"', server.statements)

        users = adm.list_users()[0]
        self.assertEqual(['appuser'], [u['_name'] for u in users])
        self.assertEqual(['appdb'],
                         [d['_name'] for d in users[0]['_databases']])

        adm.grant_access('appuser', None, ['otherdb'])
        self.assertEqual(['appdb', 'otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        adm.revoke_access('appuser', None, 'appdb')
        self.assertIn('REVOKE ALL PRIVILEGES ON SCHEMA "appdb" FROM '
                      '"appuser"', server.statements)
        self.assertEqual(['otherdb'],
                         [d['_name'] for d in adm.list_access('appuser')])
        # The server compares names without regard to case.
        self.assertEqual('appuser', adm.get_user('AppUser')['_name'])
        self.assertIsNone(adm.get_user('nobody'))
        self.assertIsNone(adm.get_user('DBADMIN'))

    def test_a_user_can_be_renamed_and_keeps_its_schemas(self):
        adm, server = self._adm()
        adm.create_database([models.VerticaSchema('appdb').serialize()])
        adm.create_user([self._user('appuser', databases=['appdb'])])

        adm.update_attributes('appuser', None, {'name': 'newname',
                                                'password': 'newpw'})

        self.assertIn('''ALTER USER "appuser" IDENTIFIED BY 'newpw\'''',
                      server.statements)
        self.assertIn('ALTER USER "appuser" RENAME TO "newname"',
                      server.statements)
        self.assertEqual(['appdb'],
                         [d['_name'] for d in adm.list_access('newname')])
        self.assertRaises(exception.BadRequest, adm.update_attributes,
                          'newname', None, {'name': 'root'})

    def test_a_dropped_user_leaves_its_objects(self):
        # The objects go to the heir the guest agent sets, not with it.
        adm, server = self._adm()
        adm.create_user([self._user('appuser')])
        adm.delete_user(self._user('appuser'))
        self.assertIn('DROP USER "appuser" CASCADE', server.statements)
        self.assertEqual('dbadmin',
                         vertica_service.SYSTEM_PARAMETERS[
                             'GlobalHeirUsername'])
        # Deleting what is not there is not an error.
        adm.delete_user(self._user('appuser'))

    def test_reserved_and_missing_users(self):
        adm, server = self._adm()
        for name in ('dbadmin', 'root', 'DBAdmin'):
            self.assertRaises(exception.BadRequest, adm.create_user,
                              [self._user(name)])
            self.assertRaises(exception.BadRequest, adm.delete_user,
                              self._user(name))
        self.assertEqual([], server.users)
        self.assertRaises(exception.UserNotFound, adm.grant_access,
                          'nobody', None, ['appdb'])
        self.assertRaises(exception.UserNotFound, adm.change_passwords,
                          [self._user('nobody')])

    def test_root_is_a_pseudosuperuser(self):
        adm, server = self._adm()
        self.assertFalse(adm.is_root_enabled())

        root = adm.enable_root('rootpw')

        self.assertEqual('root', root['_name'])
        self.assertIn('GRANT PSEUDOSUPERUSER TO "root"', server.statements)
        self.assertIn('ALTER USER "root" DEFAULT ROLE PSEUDOSUPERUSER',
                      server.statements)
        self.assertTrue(adm.is_root_enabled())
        self.assertEqual([], adm.list_users()[0])

        adm.enable_root('otherpw')
        self.assertIn('''ALTER USER "root" IDENTIFIED BY 'otherpw\'''',
                      server.statements)
        adm.disable_root()
        self.assertFalse(adm.is_root_enabled())

    def test_parameters(self):
        adm, server = self._adm()
        adm.set_parameter('MaxClientSessions', '77')
        adm.set_parameter('DefaultSessionLocale', "x'y")
        adm.set_parameter('EnableJIT', True)
        self.assertEqual([
            'ALTER DATABASE DEFAULT SET PARAMETER "MaxClientSessions" = 77',
            'ALTER DATABASE DEFAULT SET PARAMETER "DefaultSessionLocale" = '
            "'x''y'",
            'ALTER DATABASE DEFAULT SET PARAMETER "EnableJIT" = 1',
        ], server.statements)

    def test_install_license(self):
        adm, server = self._adm()
        with mock.patch.object(FakeServer, 'query',
                               return_value=[['Vertica'], ['Perpetual']]):
            adm.query = server.query
            self.assertEqual('Vertica Perpetual', adm.install_license())
        self.assertEqual(
            ["SELECT INSTALL_LICENSE('/etc/vertica-trove/license.key')"],
            server.statements)


class TestVerticaLicenseDriver(VerticaGuestTestCase):

    def setUp(self):
        super(TestVerticaLicenseDriver, self).setUp()
        self.driver = vertica_license_driver.VerticaLicenseDriver()
        self.app = mock.Mock()
        self.app.adm.install_license.return_value = 'Acme 10TB'
        self.patch_app = mock.patch.object(
            vertica_license_driver.VerticaLicenseDriver, '_app',
            return_value=self.app)
        self.patch_app.start()
        self.addCleanup(self.patch_app.stop)

    def _apply(self, content=b'LICENSE'):
        with mock.patch.object(vertica_license_driver.operating_system,
                               'read_file', return_value=content):
            return self.driver.apply('lic', 'vertica', '25.4', '/x', False)

    def test_type(self):
        self.assertEqual('vertica_license', self.driver.get_type())

    def test_installs_into_a_running_database(self):
        self.app.database_exists.return_value = True
        self.assertEqual((True, 'Acme 10TB'), self._apply())
        self.app.write_license.assert_called_once_with(b'LICENSE')
        self.app.adm.install_license.assert_called_once_with()

    def test_kept_for_a_database_yet_to_be_created(self):
        self.app.database_exists.return_value = False
        success, _message = self._apply()
        self.assertTrue(success)
        self.app.write_license.assert_called_once_with(b'LICENSE')
        self.app.adm.install_license.assert_not_called()

    def test_other_datastores_and_empty_modules_are_refused(self):
        self.assertFalse(self._apply(b'  ')[0])
        self.patch_datastore_manager('mysql')
        self.assertFalse(self._apply()[0])
        self.app.write_license.assert_not_called()

    def test_a_license_cannot_be_removed(self):
        success, message = self.driver.remove('lic', 'vertica', '25.4', '/x')
        self.assertFalse(success)
        self.assertIn('replace', message)


class TestVerticaManager(VerticaGuestTestCase):

    def _manager(self):
        with mock.patch.object(base_manager.Manager, 'docker_client',
                               new_callable=mock.PropertyMock):
            manager = vertica_manager.Manager()
        manager.app = mock.Mock()
        manager.app.mount_point = '/var/lib/vertica'
        manager.app.has_admin.return_value = False
        manager.app.database_exists.return_value = False
        manager.adm = manager.app.adm
        manager.status = mock.Mock()
        return manager

    def _prepare(self, manager, **kwargs):
        args = dict(context=mock.Mock(), packages=None, databases=None,
                    memory_mb=4096, users=None, device_path='/dev/vdb',
                    mount_point='/var/lib/vertica', backup_info=None,
                    config_contents='MaxClientSessions=50\n',
                    root_password=None, overrides=None, cluster_config=None,
                    snapshot=None, ds_version='25.4.0-0-minimal')
        args.update(kwargs)
        manager.do_prepare(**args)

    def test_a_new_instance_creates_its_database(self):
        manager = self._manager()
        order = mock.Mock()
        for name in ('secure', 'start_db', 'create_database',
                     'apply_settings'):
            order.attach_mock(getattr(manager.app, name), name)

        self._prepare(manager)

        self.assertEqual(['secure', 'start_db', 'create_database',
                          'apply_settings'],
                         [call[0] for call in order.mock_calls])
        manager.app.reset_admin_password.assert_not_called()

    @mock.patch.object(vertica_manager.Manager, 'perform_restore')
    def test_a_restored_instance_takes_its_own_password(self, mock_restore):
        manager = self._manager()
        manager.app.database_exists.return_value = True
        order = mock.Mock()
        for name in ('secure', 'start_db', 'reset_admin_password',
                     'apply_settings'):
            order.attach_mock(getattr(manager.app, name), name)
        order.attach_mock(mock_restore, 'restore')

        self._prepare(manager, backup_info={'id': 'b1'})

        self.assertEqual(['secure', 'restore', 'start_db',
                          'reset_admin_password', 'apply_settings'],
                         [call[0] for call in order.mock_calls])
        manager.app.create_database.assert_not_called()
        self.assertEqual('/var/lib/vertica', mock_restore.call_args[0][1])

    def test_a_license_module_is_kept_before_the_database_exists(self):
        manager = self._manager()
        modules = [
            {'module': {'type': 'ping', 'name': 'p',
                        'contents': base64.encode_as_text('message=x')}},
            {'module': {'type': 'vertica_license', 'name': 'lic',
                        'contents': base64.encode_as_text(b'LICENSE')}},
        ]
        with mock.patch.object(base_manager.Manager, 'prepare') as prepare:
            manager.prepare(mock.Mock(), None, None, 4096, None,
                            modules=modules)
        manager.app.write_license.assert_called_once_with(b'LICENSE')
        # The modules are then applied as usual.
        self.assertEqual(modules, prepare.call_args[1]['modules'])

    def test_a_license_that_cannot_be_kept_does_not_stop_the_prepare(self):
        manager = self._manager()
        manager.app.write_license.side_effect = OSError('disk')
        modules = [{'module': {'type': 'vertica_license', 'name': 'lic',
                               'contents': base64.encode_as_text(b'L')}}]
        with mock.patch.object(base_manager.Manager, 'prepare') as prepare:
            manager.prepare(mock.Mock(), None, None, 4096, None,
                            modules=modules)
        prepare.assert_called_once()

    def test_configuration_changes_reach_the_running_server(self):
        manager = self._manager()
        manager.apply_overrides(mock.Mock(), {'MaxClientSessions': 70})
        manager.app.apply_overrides.assert_called_once_with(
            {'MaxClientSessions': 70})
