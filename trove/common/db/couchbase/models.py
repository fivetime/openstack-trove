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


class CouchbaseSchema(models.DatastoreSchema):
    """A Couchbase bucket.

    The server accepts up to 100 letters, digits, underscores, periods,
    dashes and percent signs as the name of a bucket.
    """

    _NAME = re.compile(r'^[A-Za-z0-9_.%-]+$')

    @property
    def _max_schema_name_length(self):
        return 100

    def _is_valid_schema_name(self, value):
        return bool(self._NAME.match(value))


class CouchbaseUser(models.DatastoreUser):
    """A local user of the server, with a role on each of its buckets.

    The server rejects a name with whitespace or one of the characters
    ()<>,;:\\"/[]?={} in it.
    """

    _INVALID = re.compile(r'[\s()<>,;:\\"/\[\]?={}]')

    @property
    def _max_user_name_length(self):
        return 128

    def _is_valid_user_name(self, value):
        return not self._INVALID.search(value)

    @property
    def schema_model(self):
        return CouchbaseSchema
