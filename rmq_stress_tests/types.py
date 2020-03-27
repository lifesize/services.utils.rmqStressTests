"""
Just enough typing to give developers IDE hints about how to implement their
own tests.
"""
from typing import Iterable, Callable, Awaitable, Sequence
from rmq_stress_tests.env import RMQCluster

TestFunction = Iterable[Callable[[RMQCluster], Awaitable[Iterable[Sequence]]]]
