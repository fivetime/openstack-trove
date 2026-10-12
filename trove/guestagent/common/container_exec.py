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

import eventlet
from oslo_utils import encodeutils

from trove.common import exception
from trove.guestagent.common import readyport


def exec_client_in_container(docker_client, command, environment, timeout,
                             container_name=readyport.CONTAINER_NAME):
    """Run a client command in the database container and return what it
    printed; raise when it failed, did not finish in time or the container
    is not there.

    For a cluster member asking a peer: the tenant NIC, through which the
    members see each other, lives in the container (DOCKER_HOST_NIC_MODE),
    and the guest agent's own namespace has only the management network.
    The client in the image is used, with the password in the environment
    and not on the command line.
    """
    container = docker_client.containers.get(container_name)
    with eventlet.Timeout(timeout):
        code, output = container.exec_run(command, environment=environment)
    output = encodeutils.safe_decode(output or b'')
    if code != 0:
        raise exception.TroveError(output.strip())
    return output
