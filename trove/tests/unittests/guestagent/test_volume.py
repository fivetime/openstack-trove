#    Copyright 2026 PS Cloud Services
#
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

import os
import shutil
import tempfile
from unittest import mock

import testtools

from oslo_concurrency import processutils

from trove.common import exception
from trove.guestagent import volume


class FSExtTest(testtools.TestCase):

    def setUp(self):
        super(FSExtTest, self).setUp()

        self.fs = volume.FSExt("ext4", "")
        self.device_path = "/dev/vdb"

    @mock.patch.object(volume.utils, "execute")
    def test_check_format_blkid_success(self, mock_execute):
        mock_execute.side_effect = [
            ("ext4\n", ""),
            ("", ""),
        ]

        self.fs.check_format(self.device_path)

        mock_execute.assert_has_calls([
            mock.call(
                "blkid", "-o", "value", "-s", "TYPE",
                self.device_path,
                run_as_root=True,
                root_helper="sudo"),
            mock.call(
                "e2fsck", "-p", self.device_path,
                run_as_root=True,
                root_helper="sudo"),
        ])

    # blkid exit code 2 -  Not Found / Cannot Read. FORMATTING REQUIRED.
    @mock.patch.object(volume.utils, "execute")
    def test_check_format_blkid_exit_code_2(self, mock_execute):
        mock_execute.side_effect = processutils.ProcessExecutionError(
            exit_code=2)

        self.assertRaises(
            exception.GuestError,
            self.fs.check_format,
            self.device_path)

        mock_execute.assert_called_once_with(
            "blkid", "-o", "value", "-s", "TYPE",
            self.device_path,
            run_as_root=True,
            root_helper="sudo")

    # blkid exit code 4 - Usage Error. No formatting. Just throw error.
    @mock.patch.object(volume.utils, "execute")
    def test_check_format_blkid_exit_code_4(self, mock_execute):
        mock_execute.side_effect = processutils.ProcessExecutionError(
            exit_code=4)

        self.assertRaises(
            exception.ProcessExecutionError,
            self.fs.check_format,
            self.device_path)

        mock_execute.assert_called_once()

    # blkid exit code 8 - Ambivalent Result. No formatting. Just throw error.
    @mock.patch.object(volume.utils, "execute")
    def test_check_format_blkid_exit_code_8(self, mock_execute):
        mock_execute.side_effect = processutils.ProcessExecutionError(
            exit_code=8)

        self.assertRaises(
            exception.ProcessExecutionError,
            self.fs.check_format,
            self.device_path)

        mock_execute.assert_called_once()

    @mock.patch.object(volume.utils, "execute")
    def test_check_format_e2fsck_success(self, mock_execute):
        mock_execute.side_effect = [
            ("ext4\n", ""),
            ("", ""),
        ]

        self.fs.check_format(self.device_path)

        self.assertEqual(2, mock_execute.call_count)

    @mock.patch.object(volume.utils, "execute")
    def test_check_format_e2fsck_exit_codes(self, mock_execute):
        exit_codes = {
            1: False,
            2: False,
            4: True,
            8: True,
            16: True,
            32: True,
            128: True,
        }

        for exit_code, should_raise in exit_codes.items():
            mock_execute.reset_mock()
            mock_execute.side_effect = [
                ("ext4\n", ""),
                processutils.ProcessExecutionError(
                    exit_code=exit_code),
            ]

            if should_raise:
                self.assertRaises(
                    processutils.ProcessExecutionError,
                    self.fs.check_format,
                    self.device_path)
            else:
                self.fs.check_format(self.device_path)

            self.assertEqual(2, mock_execute.call_count)


class FstabTest(testtools.TestCase):
    """The mount options of a data volume, through a real fstab file."""

    def setUp(self):
        super(FstabTest, self).setUp()
        fd, self.fstab = tempfile.mkstemp()
        os.close(fd)
        self.addCleanup(os.remove, self.fstab)
        patcher = mock.patch.object(volume, 'FSTAB', self.fstab)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write(self, content):
        with open(self.fstab, 'w') as f:
            f.write(content)

    def _read(self):
        with open(self.fstab) as f:
            return f.read()

    def _install_copies(self):
        # The guest agent installs the new fstab with sudo; here it copies.
        def execute(*args, **kwargs):
            if args[0] == 'install':
                shutil.copyfile(args[-2], args[-1])
            return '', ''
        return mock.patch.object(volume.utils, 'execute',
                                 side_effect=execute)

    def test_options_of_the_mount_point(self):
        self._write("# /var/lib/vertica none\n"
                    "LABEL=root\t/\text4\tdefaults\t0\t1\n"
                    "/dev/vdb\t/var/lib/vertica\text4\tdefaults,noatime"
                    "\t0\t0\n")
        self.assertEqual('defaults,noatime',
                         volume.fstab_options('/var/lib/vertica'))
        self.assertIsNone(volume.fstab_options('/var/lib/mysql'))

    def test_last_entry_wins(self):
        # What mount -a ends with.
        self._write("/dev/vdb /var/lib/vertica ext4 defaults 0 0\n"
                    "/dev/vdb /var/lib/vertica ext4 defaults,sync 0 0\n")
        self.assertEqual('defaults,sync',
                         volume.fstab_options('/var/lib/vertica'))

    def test_no_fstab(self):
        os.remove(self.fstab)
        self.addCleanup(self._write, '')
        self.assertIsNone(volume.fstab_options('/var/lib/vertica'))

    def test_an_entry_replaces_the_old_ones_of_its_mount_point(self):
        content = volume.fstab_with(
            "LABEL=root / ext4 defaults 0 1\n"
            "/dev/vdb /var/lib/vertica ext4 defaults,noatime 0 0\n"
            "/dev/vdb /var/lib/vertica ext4 defaults 0 0",
            "/dev/vdb\t/var/lib/vertica\text4\tdefaults,noatime,sync\t0\t0",
            '/var/lib/vertica')
        self.assertEqual(
            "LABEL=root / ext4 defaults 0 1\n"
            "/dev/vdb\t/var/lib/vertica\text4\tdefaults,noatime,sync\t0\t0\n",
            content)

    def test_with_option(self):
        self.assertEqual('defaults,noatime,sync',
                         volume.with_option('defaults,noatime', 'sync'))
        self.assertEqual('defaults,noatime,sync',
                         volume.with_option('defaults,async,noatime', 'sync'))
        self.assertEqual('defaults,sync',
                         volume.with_option('defaults,sync', 'sync'))

    def test_a_mount_point_mounted_again_keeps_its_options(self):
        # A resize unmounts the volume and mounts it again.
        self._write("/dev/vdb /var/lib/vertica ext4 defaults,noatime,sync"
                    " 0 0\n")
        self.assertEqual('defaults,noatime,sync', volume.VolumeMountPoint(
            '/dev/vdb', '/var/lib/vertica').mount_options)
        self.assertEqual(volume.CONF.mount_options, volume.VolumeMountPoint(
            '/dev/vdb', '/mnt/volume').mount_options)

    def test_remount_in_place_and_for_the_next_boot(self):
        self._write("LABEL=root / ext4 defaults 0 1\n"
                    "/dev/vdb\t/var/lib/vertica\text4\tdefaults,noatime"
                    "\t0\t0\n")
        with self._install_copies() as mock_execute:
            volume.VolumeDevice('/dev/vdb').remount(
                '/var/lib/vertica', 'defaults,noatime,sync')
        mock_execute.assert_any_call(
            'mount', '-o', 'remount,defaults,noatime,sync',
            '/var/lib/vertica', run_as_root=True, root_helper='sudo')
        self.assertEqual(
            "LABEL=root / ext4 defaults 0 1\n"
            "/dev/vdb\t/var/lib/vertica\text4\tdefaults,noatime,sync\t0\t0\n",
            self._read())
        self.assertEqual('defaults,noatime,sync',
                         volume.fstab_options('/var/lib/vertica'))
