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

"""More than one process in the database container: the database server
and a process that goes with it, such as a Redis sentinel, which has to
share the container's network (the tenant NIC docker-hostnic gives it)
and live and die with the server.

``script`` gives the container's command: a POSIX shell script (the
images may be busybox, without bash or ``wait -n``) that starts every
process and ends the container when any of them ends, so that the
container's restart policy starts them all again; stopped (TERM or INT,
as ``docker stop`` does), it stops them and ends cleanly, so that a
container stopped on purpose stays stopped.
"""

import shlex

HEADER = '#!/bin/sh\n'


def script(commands, purpose=None):
    """The script that runs the commands (each a list of arguments) side
    by side in one container.
    """
    lines = [HEADER.rstrip('\n')]
    if purpose:
        lines.append('# %s' % purpose)
    lines += [
        '# Every process ends the container when it ends; stopped, the '
        'container stops them.',
        'PIDS=""',
        'stop() { kill $PIDS 2>/dev/null; wait; exit 0; }',
        'trap stop TERM INT',
    ]
    for command in commands:
        lines.append('%s &' % ' '.join(shlex.quote(str(a)) for a in command))
        lines.append('PIDS="$PIDS $!"')
    lines += [
        'while :; do',
        '    for p in $PIDS; do',
        '        kill -0 $p 2>/dev/null || '
        '{ kill $PIDS 2>/dev/null; wait; exit 1; }',
        '    done',
        # In the background, so that a signal is taken at once.
        '    sleep 1 &',
        '    wait $!',
        'done',
    ]
    return '\n'.join(lines) + '\n'
