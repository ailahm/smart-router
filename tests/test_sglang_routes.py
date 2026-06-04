import asyncio
from types import SimpleNamespace

from smart_router.config import SmartRouterConfig
from smart_router.engine.engine import EngineResponse, RequestType
from smart_router.entrypoints.serve.sglang_routes import SGLangRoutes


class FakeEngineClient:
    def __init__(self):
        self.identity = "test-engine-client"
        self.requests = []

    async def send_request(self, request):
        self.requests.append(request)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        future.set_result(
            EngineResponse(
                request_id=request.request_id,
                prefill_url="http://prefill",
                prefill_rank=0,
                decode_url="http://decode",
                decode_rank=1,
            )
        )
        return future


def test_sglang_schedule_workers_passes_request_body_to_engine():
    routes = SGLangRoutes(SmartRouterConfig())
    engine_client = FakeEngineClient()
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(engine_client=engine_client))
    )
    body = {
        "model": "demo-model",
        "messages": [{"role": "user", "content": "hello"}],
        "session_params": {"session_id": "session-a"},
    }

    async def run_test():
        return await routes._schedule_workers(
            request,
            request_text="hello",
            headers={"Authorization": "Bearer test"},
            request_body=body,
        )

    result = asyncio.run(run_test())

    assert result["prefill_url"] == "http://prefill"
    assert result["decode_url"] == "http://decode"
    schedule_request = engine_client.requests[0]
    assert schedule_request.request_type == RequestType.SCHEDULE
    assert schedule_request.request_body == body
