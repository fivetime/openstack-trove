# Copyright 2020 Catalyst Cloud
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
import json
import re
import time

import docker
from docker import errors as derros
from docker import types
from oslo_log import log as logging
from oslo_utils import encodeutils
import requests

from trove.common import cfg
from trove.common import constants

LOG = logging.getLogger(__name__)
CONF = cfg.CONF
ANSI_ESCAPE = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')


def stop_container(client, name="database"):
    try:
        container = client.containers.get(name)
    except docker.errors.NotFound:
        LOG.warning("Failed to get container %s", name)
        return

    container.stop(timeout=CONF.state_change_wait_time)


def create_network(client: docker.client.DockerClient,
                   name: str) -> str:
    networks = client.networks.list()
    for net in networks:
        if net.name == name:
            return net.id
    LOG.debug("Creating docker network: %s", name)
    with open(constants.ETH1_CONFIG_PATH) as fd:
        eth1_config = json.load(fd)
    enable_ipv6 = False
    ipam_pool = list()
    if eth1_config.get("ipv4_address"):
        ipam_pool.append(types.IPAMPool(
            subnet=eth1_config.get("ipv4_cidr"),
            gateway=eth1_config.get("ipv4_gateway"))
        )
    if eth1_config.get("ipv6_address"):
        enable_ipv6 = True
        ipam_pool.append(types.IPAMPool(
            subnet=eth1_config.get("ipv6_cidr"),
            gateway=eth1_config.get("ipv6_gateway")
        ))

    ipam_config = docker.types.IPAMConfig(pool_configs=ipam_pool)
    mac_address = eth1_config.get("mac_address")
    net = client.networks.create(name=name,
                                 driver=constants.DOCKER_HOST_NIC_MODE,
                                 ipam=ipam_config,
                                 enable_ipv6=enable_ipv6,
                                 options=dict(hostnic_mac=mac_address))
    LOG.debug("docker network: %s created successfully", net.id)
    return net.id


PULL_ATTEMPTS = 5
PULL_WAIT = 5  # seconds before the second attempt, doubled after each


def _pull_once(api, image):
    """One pull through the low-level client, the daemon's stream read
    to its end: a failure on the way (the registry, the network, the
    unpacking of a layer) is an 'error' message in it, with HTTP 200,
    which the SDK's images.pull does not raise; it fails afterwards on
    the look-up of the image it did not get.
    """
    repository, tag = docker.utils.parse_repository_tag(image)
    for message in api.pull(repository, tag=tag, stream=True, decode=True):
        if isinstance(message, dict) and message.get('error'):
            raise derros.DockerException(message['error'])


def pull_image(client, image, attempts=PULL_ATTEMPTS, wait=PULL_WAIT):
    """Pull the image, again after a failure, and return it (an Image of
    a DockerClient, the inspection of an APIClient). Production,
    2026-10-22: a query router's prepare failed at its only pull, the
    unpacking of a layer having failed ("snapshot extract-... does not
    exist"), and the member made in place of a lost one was in ERROR.
    """
    high_level = isinstance(client, docker.DockerClient)
    api = client.api if high_level else client

    def pulled():
        if high_level:
            return client.images.get(image)
        return api.inspect_image(image)

    for attempt in range(1, attempts + 1):
        try:
            _pull_once(api, image)
            return pulled()
        except (derros.DockerException,
                requests.exceptions.RequestException) as err:
            if attempt >= attempts:
                LOG.error("The image %s was not pulled in %d attempts: %s",
                          image, attempts, err)
                raise
            LOG.warning("The pull of the image %s failed (attempt %d of "
                        "%d): %s; again in %d s.", image, attempt,
                        attempts, err, wait)
            time.sleep(wait)
            wait *= 2


def ensure_image(client, image):
    """The image, pulled when it is not here: a container started from an
    image that is here needs no registry.
    """
    try:
        if isinstance(client, docker.DockerClient):
            return client.images.get(image)
        return client.inspect_image(image)
    except derros.ImageNotFound:
        return pull_image(client, image)


def _create_container_with_low_level_api(image: str, param: dict) -> None:
    # create a low-level docker api object
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    host_config_kwargs = dict()
    if param.get("restart_policy"):
        host_config_kwargs["restart_policy"] = param.get("restart_policy")
    if param.get("privileged"):
        host_config_kwargs["privileged"] = param.get("privileged")
    if param.get("volumes"):
        host_config_kwargs["binds"] = param.get("volumes")
    if param.get("ipc_mode"):
        host_config_kwargs["ipc_mode"] = param.get("ipc_mode")
    if param.get("ulimits"):
        host_config_kwargs["ulimits"] = param.get("ulimits")
    if param.get("cap_add"):
        host_config_kwargs["cap_add"] = param.get("cap_add")
    host_config = client.create_host_config(**host_config_kwargs)

    network_config_kwargs = dict()
    with open(constants.ETH1_CONFIG_PATH) as fd:
        eth1_config = json.load(fd)
    if eth1_config.get("ipv4_address"):
        network_config_kwargs["ipv4_address"] = eth1_config.get("ipv4_address")
    if eth1_config.get("ipv6_address"):
        network_config_kwargs["ipv6_address"] = eth1_config.get("ipv6_address")
        # The router answers the interface's solicitation with an
        # advertisement as soon as the interface comes up in the container's
        # namespace, the kernel installs a default route from it, and
        # docker's own default route through the gateway then fails with
        # "file exists". Docker applies these sysctls before it brings the
        # interface up.
        network_config_kwargs["driver_opt"] = {
            constants.DOCKER_ENDPOINT_SYSCTLS:
                "net.ipv6.conf.IFNAME.accept_ra=0"}

    networking_config = client.create_networking_config(
        {param.get("network"):
            client.create_endpoint_config(**network_config_kwargs)})
    healthcheck = param.get("healthcheck", None)

    # NOTE(wuchunyang): the low-level api doesn't support RUN interface,
    # so we need pull image first, then start the container
    ensure_image(client, image)
    LOG.debug("Creating container: %s", param.get("name"))
    container = client.create_container(image=image,
                                        name=param.get("name"),
                                        detach=param.get("detach"),
                                        user=param.get("user"),
                                        environment=param.get("environment"),
                                        command=param.get("command"),
                                        host_config=host_config,
                                        healthcheck=healthcheck,
                                        networking_config=networking_config,
                                        stop_timeout=param.get(
                                            "stop_timeout"))
    LOG.debug("Starting container: %s", param.get("name"))
    client.start(container=container)


def start_container(client, image, name="database",
                    restart_policy="unless-stopped",
                    volumes={}, ports={}, user="", network_mode="host",
                    environment={}, command="", healthcheck=None,
                    privileged=False, ulimits=None, cap_add=None,
                    stop_timeout=None):
    """Start a docker container.

    :param client: docker client obj.
    :param image: docker image.
    :param name: container name.
    :param restart_policy: restart policy.
    :param volumes: e.g.
           {"/host/trove": {"bind": "/container/trove", "mode": "rw"}}
    :param ports: ports is ignored when network_mode="host". e.g.
           {"3306/tcp": 3306}
    :param user: e.g. "1000.1001"
    :param network_mode: One of bridge, none, host
    :param environment: Environment variables
    :param command:
    :param privileged: docker privileged
    :param ulimits: resource limits of the container, a list of
           docker.types.Ulimit; the daemon's defaults when not given
    :param cap_add: capabilities to add, e.g. ["IPC_OWNER"]: narrower than
           a privileged container
    :param stop_timeout: seconds a stop (the daemon's at shutdown too)
           waits for the server to stop by itself before killing it; the
           time Trove gives a stop, by default
    :return:
    """
    if stop_timeout is None:
        stop_timeout = CONF.state_change_wait_time
    try:
        container = client.containers.get(name)
        LOG.info(f'Starting existing container {name}')
        container.start()
        return
    except docker.errors.NotFound:
        pass

    LOG.info(
        f"Creating docker container, image: {image}, "
        f"volumes: {volumes}, ports: {ports}, user: {user}, "
        f"network_mode: {network_mode}, environment: {environment}, "
        f"health_check: {healthcheck}, "
        f"command: {command}")
    kwargs = dict(name=name,
                  restart_policy={"Name": restart_policy},
                  privileged=privileged,
                  detach=True,
                  volumes=volumes,
                  ports=ports,
                  user=user,
                  ipc_mode="host",
                  environment=environment,
                  healthcheck=healthcheck,
                  command=command,
                  stop_timeout=stop_timeout)
    if ulimits:
        kwargs["ulimits"] = ulimits
    if cap_add:
        kwargs["cap_add"] = cap_add
    if network_mode == constants.DOCKER_HOST_NIC_MODE:
        create_network(client, constants.DOCKER_NETWORK_NAME)
        kwargs["network"] = constants.DOCKER_NETWORK_NAME
        return _create_container_with_low_level_api(image, kwargs)
    else:
        kwargs["network_mode"] = network_mode
        ensure_image(client, image)
        return client.containers.run(image, **kwargs)


def _decode_output(output):
    output = encodeutils.safe_decode(output)
    output = ANSI_ESCAPE.sub('', output.strip())
    return output.split('\n')


def run_container(client, image, name, network_mode="host", volumes={},
                  command="", user=""):
    """Run command in a container and return the string output list.

    :returns output: The log output.
    :returns ret: True if no error occurs, otherwise False.
    """
    try:
        container = client.containers.get(name)
        LOG.debug(f'Removing existing container {name}')
        container.remove(force=True)
    except docker.errors.NotFound:
        pass

    try:
        LOG.info(
            f'Running container {name}, image: {image}, '
            f'network_mode: {network_mode}, volumes: {volumes}, '
            f'command: {command}')
        ensure_image(client, image)
        output = client.containers.run(
            image,
            name=name,
            network_mode=network_mode,
            volumes=volumes,
            remove=False,
            command=command,
            user=user,
        )
    except docker.errors.ContainerError as err:
        output = err.container.logs()
        return _decode_output(output), False

    return _decode_output(output), True


def get_container_status(client, name="database"):
    try:
        container = client.containers.get(name)
        # One of created, restarting, running, removing, paused, exited, or
        # dead
        return container.status
    except docker.errors.NotFound:
        return "not running"
    except Exception:
        return "unknown"


def get_container_health(client: docker.DockerClient, name="database") -> str:
    try:
        container = client.containers.get(name)
        # One of staring, healthy, unhealthy, unknown
        return container.health
    except docker.errors.NotFound:
        return "not running"
    except Exception:
        return "unknown"


def run_command(client, command, name="database"):
    container = client.containers.get(name)
    # output is Bytes type
    ret, output = container.exec_run(command)
    if ret == 1:
        raise Exception('Running command error: %s' % output)

    return output


def restart_container(client, name="database"):
    container = client.containers.get(name)
    container.restart(timeout=CONF.state_change_wait_time)


def get_container_image(client, name="database"):
    """The image a container was created from, as it was named then
    (repository:tag), or None without the container.
    """
    try:
        return client.containers.get(name).attrs['Config']['Image']
    except docker.errors.NotFound:
        return None


def remove_container(client, name="database"):
    try:
        container = client.containers.get(name)
        container.remove(force=True)
    except docker.errors.NotFound:
        pass


def get_container_logs(client, name='database', tail=50):
    container = client.containers.get(name)
    output = container.logs(tail=tail)
    return _decode_output(output)


def get_image_registry(client, image):
    """Fetch image data from registry."""
    try:
        registry_data = client.images.get_registry_data(image)
    except docker.errors.NotFound as e:
        LOG.warning(f"Image was not found in registry: {str(e)}")
        registry_data = None
    except docker.errors.APIError as e:
        LOG.error(f"APIError occurred while trying to fetch image: {str(e)}")
        raise e
    return registry_data


def prune_images(client):
    """Remove unused images."""
    try:
        client.images.prune(filters={'dangling': False})
    except Exception as e:
        LOG.warning(f"Prune image failed, error: {str(e)}")
