#!/usr/bin/env python3
import asyncio
import json
import random
from dataclasses import dataclass

from itertools import product
from pathlib import Path
from string import ascii_letters, digits

import aiodocker
import aiofiles

from rmq_stress_tests.env import launch_cluster, docker_client
from rmq_stress_tests.tests import test_slow_binds
from rmq_stress_tests.types import TestFunction

TESTS_TO_RUN: TestFunction = (
    test_slow_binds,
)
VERSIONS_TO_TEST = ('3.7.0', '3.8.2', '3.8.3')
CLUSTER_SIZES_TO_TEST = (1, 2)

json_encoder = json.encoder.JSONEncoder()


async def main():
    session_name = ''.join(random.choices(ascii_letters + digits, k=5))
    output_dir = Path() / f'results-{session_name}'
    async with docker_client() as docker:
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f'Starting tests session {session_name}.')

        for test, ver, size in product(
            TESTS_TO_RUN,
            VERSIONS_TO_TEST,
            CLUSTER_SIZES_TO_TEST
        ):
            test_name = test.__name__
            test_run = TestRun(
                name=f'{test_name}-{session_name}',
                output_dir=output_dir,
                docker=docker
            )

            async with launch_cluster(test_run, size, ver) as cluster:
                print(f'Starting {test_name} against {cluster.name}…')
                results = await test(cluster)
                print(f'Finished {test_name} against {cluster.name}.')

            await asyncio.gather(*[
                write_result(test_run, test_name, result_name, result_data,
                             cluster)
                for result_name, result_data in results
            ])
    print(
        f'Finished tests run {test_run.name}. '
        f'Results in {test_run.output_dir.absolute()}'
    )


async def write_result(test_run, test_name, result_name, result_data, cluster):
    async with aiofiles.open(
        test_run.output_dir
            / f'{test_name} {result_name} '
              f'on {cluster.size}-node '
              f'RMQ {cluster.version}.json',
        mode='w'
    ) as result_file:
        for chunk in json_encoder.iterencode(result_data):
            await result_file.write(chunk)


@dataclass
class TestRun:
    name: str
    output_dir: Path
    docker: aiodocker.Docker = None


if __name__ == '__main__':
    asyncio.run(main())
