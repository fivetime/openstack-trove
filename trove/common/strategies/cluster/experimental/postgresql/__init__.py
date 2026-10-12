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

"""PostgreSQL clusters: a primary and its streaming standbys.

The members are alike; the first of them, by address, is the primary and
the others take a base backup from it and stream its WAL, each through a
replication slot of its own. The endpoint (a load balancer) sends writes to
the primary, which answers on its ready port, and the read endpoint sends
to the standbys that keep up, which answer on their read port. The tenant
chooses how a commit waits for the standbys with the extended property
``replication_mode``: ``async``, ``sync`` (the default: one standby, none
when no standby is there) or ``strict`` (one standby, always).
"""
