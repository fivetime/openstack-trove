# Copyright 2014 OpenStack Foundation
# All Rights Reserved.
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


# NOTE(esp): This code was taken from nova

__all__ = [
    'init',
    'cleanup',
    'set_defaults',
    'add_extra_exmods',
    'clear_extra_exmods',
    'get_allowed_exmods',
    'get_client',
    'get_server',
    'delete_server_queues',
    'get_notifier',
]


from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging
from oslo_messaging.rpc import dispatcher

import trove.common.exception
from trove.common.rpc import secure_serializer as ssz
from trove.common.rpc import serializer as sz

CONF = cfg.CONF
LOG = logging.getLogger(__name__)
TRANSPORT = None
NOTIFICATION_TRANSPORT = None
NOTIFIER = None
# The RPC transports of the guest message partitions other than the default
# one (trove.common.message_partition), by partition name, made on first use.
PARTITION_TRANSPORTS = {}

ALLOWED_EXMODS = [
    trove.common.exception.__name__,
]

EXTRA_EXMODS = []


def init(conf):
    global TRANSPORT, NOTIFICATION_TRANSPORT, NOTIFIER
    exmods = get_allowed_exmods()
    TRANSPORT = messaging.get_rpc_transport(conf,
                                            allowed_remote_exmods=exmods)

    NOTIFICATION_TRANSPORT = messaging.get_notification_transport(
        conf,
        allowed_remote_exmods=exmods)

    serializer = sz.TroveRequestContextSerializer(
        messaging.JsonPayloadSerializer())
    NOTIFIER = messaging.Notifier(NOTIFICATION_TRANSPORT,
                                  serializer=serializer)


def cleanup():
    global TRANSPORT, NOTIFICATION_TRANSPORT, NOTIFIER
    assert TRANSPORT is not None
    assert NOTIFICATION_TRANSPORT is not None
    assert NOTIFIER is not None
    TRANSPORT.cleanup()
    NOTIFICATION_TRANSPORT.cleanup()
    for transport in PARTITION_TRANSPORTS.values():
        transport.cleanup()
    PARTITION_TRANSPORTS.clear()
    TRANSPORT = NOTIFICATION_TRANSPORT = NOTIFIER = None


def get_transport(partition=None):
    """The RPC transport of a guest message partition.

    None is the default partition: the transport of [DEFAULT] transport_url,
    which the control plane also uses for its own RPC. Any other name is a
    partition of [DEFAULT] guest_message_partitions, whose broker the control
    plane reaches through the partition's transport_url.
    """
    assert TRANSPORT is not None
    if partition is None:
        return TRANSPORT
    transport = PARTITION_TRANSPORTS.get(partition)
    if transport is None:
        from trove.common import message_partition
        url = message_partition.get(partition).transport_url
        transport = messaging.get_rpc_transport(
            CONF, url=url, allowed_remote_exmods=get_allowed_exmods())
        PARTITION_TRANSPORTS[partition] = transport
    return transport


def set_defaults(control_exchange):
    messaging.set_transport_defaults(control_exchange)


def add_extra_exmods(*args):
    EXTRA_EXMODS.extend(args)


def clear_extra_exmods():
    del EXTRA_EXMODS[:]


def get_allowed_exmods():
    return ALLOWED_EXMODS + EXTRA_EXMODS


def get_transport_url(url_str=None):
    return messaging.TransportURL.parse(CONF, url_str)


def get_client(target, key, version_cap=None, serializer=None,
               secure_serializer=ssz.SecureSerializer, transport=None):
    assert TRANSPORT is not None
    # BUG(1650518): Cleanup in the Pike release
    # uncomment this (following) line in the pike release
    # assert key is not None
    serializer = secure_serializer(
        sz.TroveRequestContextSerializer(serializer), key)
    return messaging.get_rpc_client(
        transport or TRANSPORT, target, version_cap=version_cap,
        serializer=serializer)


def get_server(target, endpoints, key, serializer=None,
               secure_serializer=ssz.SecureSerializer, transport=None):
    assert TRANSPORT is not None

    # Thread module is not monkeypatched if remote debugging is enabled.
    # Using eventlet executor without monkepatching thread module will
    # lead to unpredictable results.
    from trove.common import debug_utils
    debug_utils.setup()

    executor = "blocking" if debug_utils.enabled() else "eventlet"

    # BUG(1650518): Cleanup in the Pike release
    # uncomment this (following) line in the pike release
    # assert key is not None
    serializer = secure_serializer(
        sz.TroveRequestContextSerializer(serializer), key)

    return messaging.get_rpc_server(
        transport or TRANSPORT,
        target,
        endpoints,
        executor=executor,
        serializer=serializer,
        access_policy=dispatcher.DefaultRPCAccessPolicy)


def delete_server_queues(target, retry=None, transport=None):
    """Delete the queues the RPC servers of a target consume from.

    For a topic that is not used again. Does nothing with an oslo.messaging
    too old to provide delete_rpc_server_queues.
    """
    assert TRANSPORT is not None
    delete = getattr(messaging, 'delete_rpc_server_queues', None)
    if delete is None:
        LOG.debug("oslo.messaging has no delete_rpc_server_queues, not "
                  "deleting the queues of %s", target)
        return
    delete(transport or TRANSPORT, target, retry=retry)


def get_notifier(service=None, host=None, publisher_id=None):
    assert NOTIFIER is not None
    if not publisher_id:
        publisher_id = "%s.%s" % (service, host or CONF.host)
    return NOTIFIER.prepare(publisher_id=publisher_id)
