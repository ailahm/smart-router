from smart_router.config import SmartRouterConfig
from smart_router.config.kv_events import KVEventsConfig
from smart_router.config.worker import WorkerGroupConfig
from smart_router.engine.vllm_engine import VLLMEngine
from smart_router.worker import WorkerRegistry, WorkerType
from smart_router.worker.factory import register_workers_for_url


class RecordingSubscriber:
    def __init__(self):
        self.added = []

    def add_endpoints(self, endpoints):
        self.added.extend(endpoints)


def test_dynamic_k8s_kv_event_subscription_uses_full_worker_endpoint_mapping():
    config = SmartRouterConfig(
        prefill_worker_config=WorkerGroupConfig(intra_dp_size=2),
        decode_worker_config=WorkerGroupConfig(intra_dp_size=2),
        kv_events_config=KVEventsConfig(
            enabled=True,
            endpoints=[
                "tcp://10.0.0.21:5557",
                "tcp://10.0.0.31:5657",
            ],
        ),
    )
    registry = WorkerRegistry()
    register_workers_for_url(
        registry, "http://10.0.0.21:18000", WorkerType.PREFILL, config
    )
    decode_worker_ids = register_workers_for_url(
        registry, "http://10.0.0.31:28000", WorkerType.DECODE, config
    )

    engine = VLLMEngine.__new__(VLLMEngine)
    engine.config = config
    engine.worker_registry = registry
    engine.kv_event_subscriber = RecordingSubscriber()

    engine._subscribe_kv_events_for_workers(decode_worker_ids)

    assert [
        (endpoint.worker_id, endpoint.endpoint)
        for endpoint in engine.kv_event_subscriber.added
    ] == [
        ("http://10.0.0.31:28000@0", "tcp://10.0.0.31:5657"),
        ("http://10.0.0.31:28000@1", "tcp://10.0.0.31:5658"),
    ]
