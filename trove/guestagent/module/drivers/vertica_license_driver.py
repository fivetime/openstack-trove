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

from datetime import date

from oslo_log import log as logging

from trove.common import cfg
from trove.guestagent.common import operating_system
from trove.guestagent.module.drivers import module_driver

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VerticaLicenseDriver(module_driver.ModuleDriver):
    """Install a Vertica license: the contents of the module are the
    license file as the vendor issues it.

    The license is kept on the guest and installed into the database. An
    instance created with the module has the license before its database
    is created, as a version of Vertica that no longer accepts the
    license of the Community Edition requires.
    """

    def get_description(self):
        return "Vertica License Module Driver"

    def get_updated(self):
        return date(2026, 10, 1)

    @staticmethod
    def _app():
        # Only a guest of a vertica instance imports the datastore.
        import docker

        from trove.guestagent.datastore import service as base_service
        from trove.guestagent.datastore.vertica import service

        client = docker.from_env()
        return service.VerticaApp(base_service.BaseDbStatus(client), client)

    @module_driver.output(
        log_message='Installing the Vertica license',
        success_message='Vertica license installed',
        fail_message='Vertica license not installed')
    def apply(self, name, datastore, ds_version, data_file, admin_module):
        if CONF.datastore_manager != 'vertica':
            return False, "A Vertica license applies to Vertica instances"
        content = operating_system.read_file(data_file, as_root=True)
        if not content.strip():
            return False, "The module has no license"
        app = self._app()
        app.write_license(content)
        if not app.database_exists():
            # The database is created with it.
            return True, "Vertica license kept for the database"
        return True, app.adm.install_license()

    @module_driver.output(
        log_message='Removing the Vertica license')
    def remove(self, name, datastore, ds_version, data_file):
        return False, ("Vertica has no way to remove a license: apply a "
                       "module with another license to replace it")
