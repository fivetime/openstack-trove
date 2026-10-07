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

from trove.guestagent.datastore.mysql import manager
from trove.guestagent.datastore.mysql import service as mysql_service
from trove.guestagent.datastore.mysql_common import manager as common_manager
from trove.guestagent.datastore.percona import service
from trove.guestagent.datastore import service as base_service


class Manager(manager.Manager):
    def __init__(self):
        status = base_service.BaseDbStatus(self.docker_client)
        app = service.PerconaApp(status, self.docker_client)
        adm = mysql_service.MySqlAdmin(app)

        # Not super().__init__(): the MySQL manager builds its own app there.
        common_manager.MySqlManager.__init__(self, app, status, adm)
        self.init_cluster_probe()
