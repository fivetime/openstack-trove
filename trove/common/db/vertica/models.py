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

# An identifier the server takes without quoting. Trove quotes them all
# the same; the server compares identifiers without regard to case.
_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')


class VerticaSchema(models.DatastoreSchema):
    """A schema of the one database of a Vertica instance."""

    @property
    def _max_schema_name_length(self):
        return 128

    def _is_valid_schema_name(self, value):
        return bool(_IDENTIFIER.match(value))


class VerticaUser(models.DatastoreUser):
    """A user of the database, with all privileges on its schemas."""

    @property
    def _max_user_name_length(self):
        return 128

    def _is_valid_user_name(self, value):
        return bool(_IDENTIFIER.match(value))

    @property
    def schema_model(self):
        return VerticaSchema
