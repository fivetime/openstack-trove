# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The communal storage of an Eon Mode Vertica cluster: a bucket of the
project of the cluster, named after it (trove.common.object_storage).
"""

from trove.common import cfg

CONF = cfg.CONF

BUCKET_PREFIX = 'trove-vertica-'
DATA_PREFIX = 'db'


def bucket_name(cluster_id):
    return BUCKET_PREFIX + cluster_id


def location(bucket):
    return 's3://%s/%s' % (bucket, DATA_PREFIX)


def guest_endpoint(storage, manager):
    """host:port of the S3 service and whether it is HTTPS, as the
    database instances reach it: the S3 service's own address unless the
    datastore says otherwise.
    """
    conf = CONF.get(manager)
    endpoint, https = storage.address()
    if conf.eon_communal_endpoint:
        endpoint = conf.eon_communal_endpoint
    if conf.eon_communal_https is not None:
        https = conf.eon_communal_https
    return endpoint, https
