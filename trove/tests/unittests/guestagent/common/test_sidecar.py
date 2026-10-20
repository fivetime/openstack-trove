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

import os
import signal
import subprocess
import tempfile
import time

from trove.guestagent.common import sidecar
from trove.tests.unittests import trove_testtools


class SidecarScriptTest(trove_testtools.TestCase):
    """The script run with /bin/sh, as a database container runs it."""

    def _start(self, commands):
        fd, path = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            f.write(sidecar.script(commands, purpose='test'))
        self.addCleanup(os.remove, path)
        return subprocess.Popen(['/bin/sh', path])

    def _alive(self, marker):
        out = subprocess.run(['ps', '-eo', 'args'], capture_output=True,
                             text=True).stdout
        return any(line.startswith('sleep ' + marker)
                   for line in out.splitlines())

    def test_a_process_that_ends_ends_the_container(self):
        proc = self._start([['sleep', '300.1'], ['sh', '-c', 'exit 3']])
        self.assertEqual(1, proc.wait(timeout=10))
        # The other went with it.
        self.assertFalse(self._alive('300.1'))

    def test_stopped_it_stops_them_and_ends_cleanly(self):
        proc = self._start([['sleep', '300.2'], ['sleep', '300.3']])
        for _ in range(50):
            if self._alive('300.2') and self._alive('300.3'):
                break
            time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(0, proc.wait(timeout=10))
        self.assertFalse(self._alive('300.2'))
        self.assertFalse(self._alive('300.3'))

    def test_arguments_are_quoted(self):
        text = sidecar.script([['valkey-server', '/conf/a b.conf'],
                               ['echo', "it's"]])
        self.assertIn("valkey-server '/conf/a b.conf' &", text)
        self.assertIn("echo 'it'\"'\"'s' &", text)
        self.assertTrue(text.startswith('#!/bin/sh\n'))
        # No bash: the images may be busybox.
        self.assertNotIn('wait -n', text)
