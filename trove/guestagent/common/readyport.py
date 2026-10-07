# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The ready port of a cluster member.

A member that takes writes answers on a ready port, redirected to the
database port inside the database container's network namespace: the
tenant NIC lives there, moved in by the docker-hostnic plugin. A load
balancer in front of the cluster checks that port, so that it sends writes
to a member that takes them and to no other. Which member that is depends
on the datastore; the port is the same for all of them.
"""

from oslo_log import log as logging

from trove.common import exception
from trove.common import utils

LOG = logging.getLogger(__name__)

CONTAINER_NAME = 'database'


class ReadyPort(object):
    """An iptables REDIRECT of the ready port to the database port in the
    database container's network namespace, present on a member that takes
    writes and absent otherwise.

    The rule is on PREROUTING only: the traffic comes in through the tenant
    NIC in the container. Connections already established through the port
    go on after the rule is removed (conntrack keeps their translation); a
    member that stopped taking writes refuses their writes itself, and new
    connections are refused: nothing listens on the port, which is what the
    load balancer's check sees.
    """

    def __init__(self, docker_client, port, target_port,
                 container_name=CONTAINER_NAME):
        self.docker_client = docker_client
        self.port = port
        self.target_port = target_port
        self.container_name = container_name
        self.rule = ['PREROUTING', '-p', 'tcp', '--dport', str(port),
                     '-j', 'REDIRECT', '--to-ports', str(target_port)]
        # The container the rule was last applied in, and whether the rule
        # is there; None when not known.
        self.pid = None
        self.applied = None

    def container_pid(self):
        """The PID of the database container, None when it is not running:
        its network namespace, and any rule in it, are gone with it.
        """
        try:
            container = self.docker_client.containers.get(self.container_name)
            return container.attrs.get('State', {}).get('Pid') or None
        except Exception as err:
            LOG.debug("No database container: %s", err)
            return None

    def _iptables(self, pid, action):
        # -w waits for the xtables lock instead of failing on it.
        utils.execute_with_timeout(
            'nsenter', '-t', str(pid), '-n', 'iptables', '-w', '5',
            '-t', 'nat', action, *self.rule,
            run_as_root=True, root_helper='sudo', timeout=15)

    def _present(self, pid):
        try:
            self._iptables(pid, '-C')
            return True
        except exception.ProcessExecutionError as err:
            # 1 is "no such rule"; anything else is a failure.
            if err.exit_code == 1:
                return False
            raise

    def ensure(self, wanted, pid=None):
        """Make the rule present or absent. Returns what it is, or None
        when there is no container to hold it.
        """
        pid = pid or self.container_pid()
        self.pid = pid
        if pid is None:
            self.applied = None
            return None
        try:
            present = self._present(pid)
            if present != wanted:
                self._iptables(pid, '-A' if wanted else '-D')
                LOG.info("Ready port %s %s.", self.port,
                         'opened' if wanted else 'closed')
            self.applied = wanted
        except Exception:
            # Not known any more; the next check puts it right.
            self.applied = None
            raise
        return wanted
