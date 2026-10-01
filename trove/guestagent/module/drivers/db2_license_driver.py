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


class DB2LicenseDriver(module_driver.ModuleDriver):
    """Install a Db2 license: the contents of the module are the license
    file (.lic) as IBM issues it.

    One image serves every edition: the license makes the edition. The
    instance keeps an installed license on its volume and takes it up
    again when its container is new.
    """

    def get_description(self):
        return "Db2 License Module Driver"

    def get_updated(self):
        return date(2026, 10, 1)

    @staticmethod
    def _app():
        # Only a guest of a db2 instance imports the datastore.
        import docker

        from trove.guestagent.datastore.db2 import service
        from trove.guestagent.datastore import service as base_service

        client = docker.from_env()
        return service.DB2App(base_service.BaseDbStatus(client), client)

    @module_driver.output(
        log_message='Installing the Db2 license',
        success_message='Db2 license installed',
        fail_message='Db2 license not installed')
    def apply(self, name, datastore, ds_version, data_file, admin_module):
        if CONF.datastore_manager != 'db2':
            return False, "A Db2 license applies to Db2 instances"
        content = operating_system.read_file(data_file, as_root=True,
                                             decode=False)
        if not content.strip():
            return False, "The module has no license"
        app = self._app()
        app.write_license(content)
        return True, app.install_license()

    @module_driver.output(
        log_message='Removing the Db2 license')
    def remove(self, name, datastore, ds_version, data_file):
        return False, ("Removing a license is not supported: apply a "
                       "module with another license to replace it")
