#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from trove.guestagent.datastore.galera_common import manager as galera_manager
from trove.guestagent.datastore.mysql import manager
from trove.guestagent.datastore.mysql import service as mysql_service
from trove.guestagent.datastore.mysql_common import manager as common_manager
from trove.guestagent.datastore.pxc import service
from trove.guestagent.datastore import service as base_service

DEFAULTS_FILE = '/etc/mysql/my.cnf'


class Manager(galera_manager.GaleraManagerMixin, manager.Manager):
    def __init__(self):
        status = base_service.BaseDbStatus(self.docker_client)
        app = service.PXCApp(status, self.docker_client)
        adm = mysql_service.MySqlAdmin(app)

        # Not super().__init__(): the MySQL manager builds its own app there.
        common_manager.MySqlManager.__init__(self, app, status, adm)

    def get_start_db_params(self, data_dir):
        # The image has an /etc/my.cnf of its own, which is read first and
        # includes more files. Read the configuration Trove wrote, alone.
        return (f'--defaults-file={DEFAULTS_FILE} '
                f'--datadir={data_dir}')
