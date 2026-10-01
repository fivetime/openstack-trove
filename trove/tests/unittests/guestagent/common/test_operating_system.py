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

from unittest import mock

from trove.common import exception
from trove.guestagent.common import operating_system
from trove.tests.unittests import trove_testtools


class TestCreateUser(trove_testtools.TestCase):

    def _run(self, useradd_error=None):
        def execute(cmd, *args, **kwargs):
            if cmd == 'useradd' and useradd_error:
                raise exception.ProcessExecutionError(
                    stderr=useradd_error, stdout='', exit_code=4,
                    cmd='useradd')
            return '', ''
        with mock.patch.object(operating_system, 'execute_shell_cmd',
                               side_effect=execute) as mock_execute:
            operating_system.create_user('database', '1000')
        return [call[0][0] for call in mock_execute.call_args_list]

    def test_creates_group_and_user(self):
        self.assertEqual(['groupadd', 'useradd'], self._run())

    def test_an_existing_user_is_kept(self):
        self._run("useradd: user 'database' already exists\n")

    def test_an_id_the_guest_already_has_is_used(self):
        # uid 1000 is the login user of the guest image, and the id some
        # database images run as.
        with mock.patch.object(operating_system, 'LOG') as log:
            self._run('useradd: UID 1000 is not unique\n')
        log.warning.assert_called_once()

    def test_other_failures_are_errors(self):
        self.assertRaises(exception.UnprocessableEntity, self._run,
                          'useradd: cannot lock /etc/passwd\n')
