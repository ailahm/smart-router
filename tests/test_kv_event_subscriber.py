import asyncio

from smart_router.cache import KVCacheState
from smart_router.cache.vllm_kv_event_subscriber import (
    KVEventSubscriber,
    WorkerEventEndpoint,
)


def test_kv_event_subscriber_can_add_and_remove_workers_after_start():
    async def run_test():
        subscriber = KVEventSubscriber(KVCacheState(), [])
        started = []

        async def fake_run_endpoint(endpoint):
            started.append(endpoint.worker_id)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise

        subscriber._run_endpoint = fake_run_endpoint
        subscriber.start()
        subscriber.add_endpoints(
            [WorkerEventEndpoint("worker-0", "inproc://worker-0")]
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert started == ["worker-0"]
        assert [endpoint.worker_id for endpoint in subscriber.endpoints] == [
            "worker-0"
        ]
        assert "worker-0" in subscriber._tasks_by_worker

        subscriber.remove_workers(["worker-0"])
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert subscriber.endpoints == []
        assert subscriber._tasks_by_worker == {}

        await subscriber.stop()

    asyncio.run(run_test())


def test_kv_event_subscriber_readds_worker_after_pending_remove():
    async def run_test():
        endpoint = WorkerEventEndpoint("worker-0", "inproc://worker-0")
        subscriber = KVEventSubscriber(KVCacheState(), [endpoint])
        subscriber._loop = asyncio.get_running_loop()

        subscriber.remove_workers(["worker-0"])
        subscriber.add_endpoints([endpoint])
        await asyncio.sleep(0)

        assert [endpoint.worker_id for endpoint in subscriber.endpoints] == [
            "worker-0"
        ]
        assert subscriber._endpoints_by_worker["worker-0"] == endpoint

    asyncio.run(run_test())
