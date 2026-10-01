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

import re

from trove.common.db import models

# The server takes up to eight letters, digits and @#$ for the name of a
# database, not starting with a digit, and folds it to upper case.
_DATABASE = re.compile(r'^[A-Za-z@#$][A-Za-z0-9@#$]{0,7}$')
# A user is a user of the operating system of the container.
_USER = re.compile(r'^[a-z_][a-z0-9_]{0,29}$')


class DB2Schema(models.DatastoreSchema):
    """A Db2 database."""

    @property
    def _max_schema_name_length(self):
        return 8

    def _is_valid_schema_name(self, value):
        return bool(_DATABASE.match(value))


class DB2User(models.DatastoreUser):
    """A user of the container's operating system, with authorities on
    each of its databases.
    """

    # The instance owner is the administrator; root is a user in its group.
    root_username = 'db2root'

    @property
    def _max_user_name_length(self):
        return 30

    def _is_valid_user_name(self, value):
        return bool(_USER.match(value))

    def _is_valid_password(self, value):
        # chpasswd takes name:password lines.
        return ':' not in value and '\n' not in value

    @property
    def schema_model(self):
        return DB2Schema
