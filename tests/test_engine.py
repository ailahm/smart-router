import asyncio

from smart_router.engine.engine import Engine
from smart_router.engine.engine import EngineRequest, RequestType


def test_engine_request_request_body_round_trips_and_defaults_empty():
    request = EngineRequest(
        request_id="req-1",
        identity="test",
        request_type=RequestType.SCHEDULE,
        request_body={"session_params": {"session_id": "session-a"}},
    )

    restored = EngineRequest.from_dict(request.to_dict())

    assert restored.request_body == {"session_params": {"session_id": "session-a"}}

    legacy = EngineRequest.from_dict(
        {
            "request_id": "req-legacy",
            "identity": "test",
            "request_type": RequestType.SCHEDULE,
        }
    )
    assert legacy.request_body == {}


def test_engine_run_starts_core_loops_and_can_be_cancelled():
    class TestEngine(Engine):
        def __init__(self):
            self.worker_discovery = None
            self.refresh_calls = 0
            self.receive_started = asyncio.Event()
            self.schedule_started = asyncio.Event()
            self.health_started = asyncio.Event()
            self.stop_event = asyncio.Event()

        async def refresh_worker_health(self):
            self.refresh_calls += 1

        async def receive_loop(self):
            self.receive_started.set()
            await self.stop_event.wait()

        async def schedule_loop(self):
            self.schedule_started.set()
            await self.stop_event.wait()

        async def health_check_loop(self):
            self.health_started.set()
            await self.stop_event.wait()

    async def run():
        engine = TestEngine()
        task = asyncio.create_task(engine.run())

        await asyncio.wait_for(engine.receive_started.wait(), timeout=1)
        await asyncio.wait_for(engine.schedule_started.wait(), timeout=1)
        await asyncio.wait_for(engine.health_started.wait(), timeout=1)

        assert engine.refresh_calls == 1

        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=1)
        except asyncio.CancelledError:
            pass

    asyncio.run(run())


if __name__ == "__main__":
    test_engine_run_starts_core_loops_and_can_be_cancelled()
