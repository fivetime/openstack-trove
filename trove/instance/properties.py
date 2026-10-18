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

"""The extended properties of a new instance: what it is created with that
its datastore keeps for its life, such as whether a Vertica instance syncs
its writes. Each datastore knows its own; a property it does not know is
refused rather than ignored, as a misspelled one would otherwise go unseen.
"""

from oslo_utils import strutils

from trove.common import exception
from trove.common.i18n import _

# Vertica does not sync what it commits to disk. A single instance that
# loses power, or whose host does, loses recent commits or comes back
# unable to start. Mounted with sync, every write reaches the disk before
# it returns, at a cost to write throughput.
SYNC_WRITES = 'sync_writes'


def _vertica(props):
    unknown = sorted(set(props) - {SYNC_WRITES})
    if unknown:
        raise exception.BadRequest(_(
            "Unknown extended properties for a Vertica instance: "
            "%(unknown)s. Known: %(known)s.") % {
                'unknown': ', '.join(unknown), 'known': SYNC_WRITES})
    try:
        sync = strutils.bool_from_string(props.get(SYNC_WRITES, False),
                                         strict=True)
    except ValueError:
        raise exception.BadRequest(_(
            "%s must be true or false.") % SYNC_WRITES)
    return {SYNC_WRITES: sync}


# The datastores (by manager) whose instances take extended properties,
# and what they make of them: always what to keep, even when none were
# given, so that an instance shows the defaults it was created with.
DATASTORES = {
    'vertica': _vertica,
}


def instance_properties(manager, extended_properties):
    """What a new instance of the datastore keeps of the extended
    properties it is created with.
    """
    props = dict(extended_properties or {})
    parse = DATASTORES.get(manager)
    if parse is None:
        if props:
            raise exception.BadRequest(_(
                "Instances of datastore %s take no extended "
                "properties.") % manager)
        return {}
    return parse(props)


def guest_properties(properties):
    """What the guest of the instance is told when it is prepared."""
    return {SYNC_WRITES: True} if properties.get(SYNC_WRITES) else {}
