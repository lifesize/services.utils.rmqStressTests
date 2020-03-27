import asyncio
import json
from contextlib import asynccontextmanager, AsyncExitStack

import aio_pika
import asynqp
import aiormq
import logging
import random
import secrets
import tempfile

import aiodocker
import aiodocker.exceptions
import aiofiles

ENABLE_MGMT_PLUGINS = True  # To better simulate CloudAMQP's setup
CLUSTER_DOCKER_NETWORK_CONFIG = {
    'driver': 'bridge'
}
CONTAINER_READY_DELAY = 20.0
RMQ_CONFIG_PATH = '/etc/rabbitmq/rabbitmq.conf'

container_logger = logging.getLogger('rmq_stress_tests.containers')


class RabbitMQNodeContainer:
    def __init__(self, cluster, name, host_name, conf_path):
        self.cluster = cluster
        self.name = name
        self.host_name = host_name
        self.conf_path = conf_path
        self.amqp_port = None
        self._container_tasks = set()
        self.docker = cluster.docker

        self._container = None
        self._starting_complete = asyncio.Event()
        self._starting_errors = []

    async def process_stats(self):
        output_path = (
            self.cluster.test_run.output_dir
            / f'{self.name}.stats.json'
        )
        stats_stream = self._container.stats(stream=True)

        async with aiofiles.open(
            output_path,
            mode='a',
            encoding='utf-8'
        ) as output_file:
            await output_file.write('[')
            try:
                async for stats in stats_stream:
                    await output_file.write(f'{json.dumps(stats)},\n')
            finally:
                await output_file.write(']')

    async def process_stdout(self):
        output_path = (
            self.cluster.test_run.output_dir
            / f'{self.name}.stdout.log'
        )
        log_stream = self._container.log(stdout=True, follow=True)

        async with aiofiles.open(
            output_path,
            mode='a',
            encoding='utf-8'
        ) as output_file:
            async for entry in log_stream:
                await output_file.write(entry)
                if 'Server startup complete' in entry:
                    self._starting_complete.set()
                    break
            else:
                self._starting_errors.append(
                    'Container ended without RabbitMQ start.'
                )
                self._starting_complete.set()
                return

            # After startup, the only processing to do is capturing
            async for entry in log_stream:
                await output_file.write(entry)

    async def process_stderr(self):
        output_path = (
                self.cluster.test_run.output_dir
                / f'{self.name}.stderr.log'
        )
        log_stream = self._container.log(stderr=True, follow=True)

        async with aiofiles.open(
            output_path,
            mode='a',
            encoding='utf-8'
        ) as output_file:
            async for entry in log_stream:
                await output_file.write(entry)

    async def start(self):
        container_conf = {
            'Env': (
                f'RABBITMQ_ERLANG_COOKIE={self.cluster.cookie}',
            ),
            'Hostname': self.host_name,
            'Image': self.cluster.image,
            'HostConfig': {
                'NetworkMode': self.cluster.network_name,
                'PortBindings': {
                    '5672/tcp': (None,)
                }
            }
        }
        if self.conf_path:
            container_conf['HostConfig']['Binds'] = (
                f'{self.conf_path}:{RMQ_CONFIG_PATH}',
            )
        self._container = await self.docker.containers.create(
            config=container_conf,
            name=self.name
        )
        self._container_tasks |= {
            asyncio.create_task(self.process_stats()),
            asyncio.create_task(self.process_stdout()),
            asyncio.create_task(self.process_stderr())
        }

        await self._container.start()
        await self._container.show()
        amqp_port_publications = await self._container.port('5672/tcp')
        self.amqp_port = int(amqp_port_publications[0]['HostPort'])

        try:
            await asyncio.wait_for(
                self._starting_complete.wait(),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            raise Exception(f"Container for {self.name} hasn't started within a minute.")
        if self._starting_errors:
            raise Exception(*self._starting_errors)

    async def close(self):
        await self._container.stop()
        await self._container.delete()
        _done_tasks, pending_container_tasks = await asyncio.wait(
            self._container_tasks,
            timeout=10.0
        )
        if pending_container_tasks:
            print(f"Cancelling node {self.name}'s pending container tasks:")
            for task in pending_container_tasks:
                print(f'\t• {task}')
                task.cancel()
        self._container_tasks.clear()

        self._container = None


class RMQCluster:
    def __init__(self, test_run, size, version, exit_stack):
        self.test_run = test_run
        self.size = size
        self.version = version
        self.docker = test_run.docker
        self.exit_stack = exit_stack

        self.cookie = secrets.token_urlsafe(10)
        self.name = (
            f'rmq_stress-{test_run.name}-{size}_node-v{version.replace(".", "_")}'
        )
        self.image = f'rabbitmq:{version}-management'

        self.nodes = []
        self.ports = []

        self.network = None
        self.network_name = f'{self.name}-net'

    @asynccontextmanager
    async def connect_with_asynqp(self):
        port = self.random_amqp_port()
        connection = await asynqp.connect(port=port)
        async with connection:
            yield connection

    @asynccontextmanager
    async def connect_with_aio_pika(self):
        port = self.random_amqp_port()
        connection = await aio_pika.connect(port=port)
        async with connection:
            yield connection

    @asynccontextmanager
    async def connect_with_aiormq(self):
        port = self.random_amqp_port()
        connection = await aiormq.connect(
            f'amqp://guest:guest@127.0.0.1:{port}//'
        )
        async with connection:
            yield connection

    async def start(self):
        print(f'Creating Docker network {self.network_name} for {self.name}…')
        await self.start_network()
        print(f'Created Docker network {self.network_name} for {self.name}.')
        await self.start_nodes()

    async def close(self):
        nodes = self.nodes[:]
        self.nodes.clear()
        await asyncio.gather(*[node.close() for node in nodes])
        if self.network:
            await self.network.delete()

    async def start_network(self):
        self.network = await self.docker.networks.create(
            dict(Name=self.network_name, **CLUSTER_DOCKER_NETWORK_CONFIG)
        )

    async def start_nodes(self):
        await self.docker.images.pull(self.image)
        node_names = [
            f'{self.name}-rmq{i}'
            for i in range(1, self.size + 1)
        ]
        config_file = await self.exit_stack.enter_async_context(
            config_for_rmq_cluster(self.image, node_names)
        )

        for node_index, node_name in zip(
            range(1, self.size + 1),
            node_names
        ):
            node = RabbitMQNodeContainer(
                self,
                name=node_name,
                host_name=node_name,
                conf_path=config_file.name if config_file else None
            )

            print(f'Starting node {node.name}…')
            await node.start()
            print(f'Started node {node.name}.')

            self.nodes.append(node)

    def random_amqp_port(self):
        node = random.choice(self.nodes)
        return node.amqp_port


@asynccontextmanager
async def launch_cluster(test_run, size, version):
    async with AsyncExitStack() as exit_stack:
        cluster = RMQCluster(test_run, size, version, exit_stack)
        exit_stack.push_async_callback(cluster.close)

        print(f'Creating cluster {cluster.name}…')
        await cluster.start()
        print(f'Created cluster {cluster.name}.')

        yield cluster


@asynccontextmanager
async def config_for_rmq_cluster(image, host_names):
    with tempfile.NamedTemporaryFile(
        mode='w',
        encoding='utf-8',
        prefix='rmq_stress-',
        dir='/tmp'
    ) as conf_file:
        if len(host_names) < 2:
            # Default image config is fine for standalone nodes.
            yield None
            return

        # In custom bridge networks, the DNS resolver resolves container
        # node names. Knowing this, we don't have to assign IP addresses
        # ahead of time.
        conf_file.write(
            'loopback_users.guest = false\n'
            'listeners.tcp.default = 5672\n'
            'cluster_formation.peer_discovery_backend = rabbit_peer_discovery_classic_config\n'
        )
        for i, host in enumerate(host_names):
            conf_file.write(
                f'cluster_formation.classic_config.nodes.{i} = rabbit@{host}\n'
            )
        conf_file.flush()
        try:
            yield conf_file
        except Exception as e:
            print(e)
            raise
        finally:
            print(f'Deleting temporary cluster conf file {conf_file.name}')


@asynccontextmanager
async def docker_client():
    docker = aiodocker.Docker()
    try:
        yield docker
    finally:
        await docker.close()