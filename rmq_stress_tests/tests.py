import random
from string import ascii_letters, digits
from time import perf_counter

from rmq_stress_tests.env import RMQCluster

MAX_CONCURRENT_SLOW_BINDS = 10


async def test_slow_binds(cluster: RMQCluster):
    print(
        f'Starting slow binds test for up to {MAX_CONCURRENT_SLOW_BINDS} binds…'
    )
    total_time_by_binds_created = {}
    bind_times_by_binds_created = {}

    for binds_count in range(1, MAX_CONCURRENT_SLOW_BINDS + 1):
        bind_times = []

        async with cluster.connect_with_aio_pika() as rmq:
            channel = await rmq.channel()
            exchange = await channel.declare_exchange(
                ''.join(random.choices(ascii_letters+digits, k=5))
            )
            queue = await channel.declare_queue()
            bind = queue.bind  # To reduce lookup time noise
            try:
                for i in range(1, binds_count + 1):
                    binding_key = f'binding{i:06}'

                    bind_start = perf_counter()
                    await bind(exchange, routing_key=binding_key)
                    bind_end = perf_counter()

                    bind_times.append(bind_end - bind_start)
            except Exception as e:
                print(f'Test exception {e}')
                raise
            finally:
                await queue.delete(if_unused=False)
                await exchange.delete()

        bind_times_by_binds_created[binds_count] = bind_times
        total_time_by_binds_created[binds_count] = sum(bind_times)

    results = (
        ('Binding Times by Binds Created', bind_times_by_binds_created),
        ('Total Binding Time by Binds Created', total_time_by_binds_created)
    )
    return results
