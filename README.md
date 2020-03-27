# RabbitMQ Stress Testing Harness

## Usage
This is meant to be checked out and played with locally. It doesn't have an
installer.

To get started:
1. Create a Python 3.7+ Virtual Environment for this project using your favorite
technique.

        # Example using built-in virtual env creator:
        python3 -m venv venv
        source venv/bin/activate
        pip install -r requirements.txt

2. Make a test function or two in a Python module (or modify
rmq_stress_tests/tests). It should be an async function that takes a `cluster`
argument and returns an iterable of `(result_name: str, result_data: any)`
tuples–these get written out to JSON files. Your test function can connect to
the cluster using any of the common async libraries:

    ```python
   async with cluster.connect_with_aio_pika() as rmq:
        await rmq.…
    ```

    ```python
    async with cluster.connect_with_aiormq() as rmq:
        await rmq.…
    ```

    ```python
    async with cluster.connect_with_asynqp() as rmq:
        await rmq.…
    ```

3. Edit the cluster sizes and RabbitMQ versions to test against in the
`run_tests.py` constants.
4. `./run_tests.py` in your virtual environment.
5. Check the results in the `results-<run id>` folder. There will be test
result data JSONs, container CPU/memory stats JSONs and log output.
