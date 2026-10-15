# Copyright 2026 Simon Zhou
#
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

"""Guest message partitions: one RabbitMQ broker each for guest agents.

A single broker holds a bounded number of guest agents (every guest has a
quorum queue of its own and a connection; a node restart recovers every Raft
member, so the recovery window grows with the guest count). Partitions spread
the guests over several brokers. An instance is given a partition when it is
created and keeps it: the guest agent config injected into the instance names
the partition's broker, the control plane reaches the guest through it and
trove-conductor listens on every partition for what guests send.

The default partition, named None, is the broker of [DEFAULT] transport_url,
which the control plane also uses for its own RPC; instances made before
partitions existed belong to it.
"""

import collections
import re

from oslo_config import cfg
from oslo_log import log as logging

from trove.common import exception

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

GROUP_PREFIX = 'guest_message_partition_'
NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$')

partition_opts = [
    cfg.StrOpt('transport_url', secret=True,
               help='The oslo.messaging transport URL through which the '
                    'control plane reaches this partition\'s broker.'),
    cfg.StrOpt('guest_transport_url', secret=True,
               help='The transport URL written into the guest agent config '
                    'of the instances on this partition (an address the '
                    'guests can reach, e.g. the broker\'s load balancer).'),
    cfg.ListOpt('availability_zones', default=[],
                help='Availability zones this partition serves; empty '
                     'serves any zone no other partition is named for.'),
    cfg.IntOpt('max_instances', default=2500, min=0,
               help='Most instances placed on this partition.'),
    cfg.BoolOpt('accepts_new', default=True,
                help='Whether new instances may be placed on this partition '
                     '(False drains it).'),
]

Partition = collections.namedtuple(
    'Partition', ['name', 'transport_url', 'guest_transport_url',
                  'availability_zones', 'max_instances', 'accepts_new'])


def names():
    """The names of the configured partitions, the default one excluded."""
    return list(CONF.guest_message_partitions or [])


def enabled():
    return bool(names())


def _register(name):
    if not NAME_RE.match(name):
        raise exception.TroveError(
            "Invalid guest message partition name %r." % name)
    group = GROUP_PREFIX + name
    if group not in CONF:
        CONF.register_opts(partition_opts, group=group)
    return CONF[group]


def get(name):
    """The partition of a name; None is the default partition."""
    if name is None:
        return Partition(None, None, None,
                         list(CONF.default_partition_availability_zones),
                         CONF.default_partition_max_instances,
                         CONF.default_partition_accepts_new)
    if name not in names():
        raise exception.TroveError(
            "Guest message partition %r is not configured "
            "([DEFAULT] guest_message_partitions)." % name)
    conf = _register(name)
    if not conf.transport_url or not conf.guest_transport_url:
        raise exception.TroveError(
            "Guest message partition %r needs transport_url and "
            "guest_transport_url in [%s]." % (name, GROUP_PREFIX + name))
    return Partition(name, conf.transport_url, conf.guest_transport_url,
                     list(conf.availability_zones), conf.max_instances,
                     conf.accepts_new)


def all_partitions():
    """Every partition, the default one first."""
    return [get(None)] + [get(name) for name in names()]


def usage(partition_name):
    """The instances placed on a partition and not deleted."""
    from trove.instance.models import DBInstance
    return DBInstance.find_all(message_partition=partition_name,
                               deleted=False).count()


def _label(name):
    return 'default' if name is None else name


def choose(availability_zone=None):
    """The partition for a new instance in an availability zone.

    The candidates are the partitions named for the zone or, when none is,
    those that serve any zone; among those that accept new instances and are
    not full, the least used one (as a share of its maximum) wins. Raises
    GuestMessagePartitionsFull when none can take the instance. Without
    configured partitions every instance stays on the default one and nothing
    is counted.
    """
    if not enabled():
        return None
    partitions = all_partitions()
    named = [p for p in partitions
             if availability_zone and
             availability_zone in p.availability_zones]
    candidates = named or [p for p in partitions if not p.availability_zones]
    report = []
    best = None
    for p in candidates:
        used = usage(p.name)
        report.append('%s %d/%d%s' % (_label(p.name), used, p.max_instances,
                                      '' if p.accepts_new else ' draining'))
        if not p.accepts_new or used >= p.max_instances:
            continue
        share = float(used) / p.max_instances
        if best is None or share < best[0]:
            best = (share, p)
    if best is None:
        raise exception.GuestMessagePartitionsFull(
            zone=availability_zone or 'any',
            usage=', '.join(report) or 'no partition serves it')
    LOG.info("Guest message partition for a new instance in zone %(zone)s: "
             "%(name)s (%(usage)s).",
             {'zone': availability_zone or 'any',
              'name': _label(best[1].name), 'usage': ', '.join(report)})
    return best[1].name


def guest_config(content, partition_name):
    """The guest agent config of an instance on a partition.

    The default partition keeps the file as it is; any other gets its
    guest_transport_url as [DEFAULT] transport_url, replacing the line the
    file has there or adding one.
    """
    if partition_name is None:
        return content
    url = get(partition_name).guest_transport_url
    lines = content.splitlines()
    section = None
    default_at = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('[') and stripped.endswith(']'):
            section = stripped[1:-1].strip()
            if section == 'DEFAULT' and default_at is None:
                default_at = i
            continue
        if section == 'DEFAULT' and re.match(r'^transport_url\s*=', stripped):
            lines[i] = 'transport_url = %s' % url
            return '\n'.join(lines) + '\n'
    if default_at is None:
        lines = ['[DEFAULT]', 'transport_url = %s' % url, ''] + lines
    else:
        lines.insert(default_at + 1, 'transport_url = %s' % url)
    return '\n'.join(lines) + '\n'
