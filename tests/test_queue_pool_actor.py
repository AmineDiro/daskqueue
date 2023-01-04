import pytest

from daskqueue.QueuePool import QueuePoolActor


@pytest.mark.asyncio
async def test_queuepoolactor_batch_submit():
    queue_pool = QueuePoolActor(n_queues=1)
    res = await librcwary.do_something()
    assert b"expected result" == res
