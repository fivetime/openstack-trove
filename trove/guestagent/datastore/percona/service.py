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

from trove.guestagent.datastore.group_replication import service as \
    gr_service
from trove.guestagent.datastore.mysql import service


class PerconaApp(gr_service.GroupReplicationAppMixin, service.MySqlApp):
    """Percona Server, run from the official ``percona/percona-server`` image.

    The image follows the conventions of the ``mysql`` image that the MySQL
    implementation relies on: the root password comes from
    ``MYSQL_ROOT_PASSWORD``, ``/etc/mysql/my.cnf`` is read, and the server
    runs as the user the container is started with. Nothing is overridden.
    """
