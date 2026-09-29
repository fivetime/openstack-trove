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

from trove.guestagent.datastore.redis_common import service


class RedisApp(service.RedisApp):
    """Redis itself, run from the official ``redis`` image.

    The Redis family base class already carries the Redis names, so
    nothing is overridden here. The class exists so that the datastore has
    its own manager module, like KeyDB and Valkey.
    """
