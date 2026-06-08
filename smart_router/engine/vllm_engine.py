import asyncio
import logging
from typing import Any, Dict, List, Optional

from smart_router.cache import KVCacheState
from smart_router.cache.vllm_kv_event_subscriber import (
    KVEventSubscriber,
    build_worker_event_endpoints,
)
from smart_router.engine.engine import Engine, EngineRequest
from smart_router.config import SmartRouterConfig
from smart_router.policies import Policy, get_policy_config, select_worker_with_context
from smart_router.worker import Worker, WorkerRegistry, WorkerType
from smart_router.worker.factory import register_workers_for_url

DECODE_URL_PLACEHOLDER = "DECODE_URL_PLACEHOLDER"

logger = logging.getLogger(__name__)

class VLLMEngine(Engine):
    def __init__(
        self,
        config: SmartRouterConfig,
        input_socket_address: str,
        output_socket_address: str,
    ) -> None:
        super().__init__(
            input_socket_address=input_socket_address,
            output_socket_address=output_socket_address,
        )

        self.config: SmartRouterConfig = config
        self.worker_registry: WorkerRegistry = WorkerRegistry()
        self.prefill_policy: Policy = get_policy_config(config.prefill_policy_config)
        self.decode_policy: Policy = get_policy_config(config.decode_policy_config)
        self.kv_cache_state = KVCacheState()
        self.kv_event_subscriber: Optional[KVEventSubscriber] = None

        # Initialize prefill workers.
        for url in config.prefill_worker_config.urls or []:
            register_workers_for_url(self.worker_registry, url, WorkerType.PREFILL, config)

        # Initialize decode workers.
        for url in config.decode_worker_config.urls or []:
            register_workers_for_url(self.worker_registry, url, WorkerType.DECODE, config)

        self.configure_worker_discovery(config)
        self._attach_kv_cache_state()

        logger.info("registered workers: %s", self.worker_registry.get_all_urls())

    def build_schedule_context(self, request: EngineRequest) -> Dict[str, Any]:
        context: Dict[str, Any] = {}
        if request.request_token_ids:
            worker_ids = [worker.url() for worker in self.worker_registry.get_all()]
            context["kv_match_scores"] = self.kv_cache_state.best_workers_by_tokens(
                worker_ids, request.request_token_ids)
        return context

    def schedule_prefill(
        self,
        request_text: str,
        headers: Dict[str, str],
        request_body: Optional[Dict[str, Any]] = None,
        request_token_ids: Optional[List[int]] = None,
        schedule_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Worker]:
        workers = self.worker_registry.get_healthy_by_type(WorkerType.PREFILL)
        prefill = self._select_with_policy(
            self.prefill_policy,
            workers,
            request_text=request_text,
            headers=headers,
            request_body=request_body,
            request_token_ids=request_token_ids,
            schedule_context=schedule_context,
        )
        return prefill

    def schedule_decode(
        self,
        request_text: str,
        headers: Dict[str, str],
        request_body: Optional[Dict[str, Any]] = None,
        request_token_ids: Optional[List[int]] = None,
        schedule_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Worker]:
        workers = self.worker_registry.get_healthy_by_type(WorkerType.DECODE)
        decode = self._select_with_policy(
            self.decode_policy,
            workers,
            request_text=request_text,
            headers=headers,
            request_body=request_body,
            request_token_ids=request_token_ids,
            schedule_context=schedule_context,
        )
        return decode

    def _select_with_policy(
        self,
        policy: Policy,
        workers: List[Worker],
        request_text: str,
        headers: Dict[str, str],
        request_body: Optional[Dict[str, Any]],
        request_token_ids: Optional[List[int]],
        schedule_context: Optional[Dict[str, Any]],
    ) -> Optional[Worker]:
        if self._uses_kv_event_policy(policy):
            return select_worker_with_context(
                policy,
                workers,
                request_text=request_text,
                headers=headers,
                request_body=request_body,
                request_token_ids=request_token_ids,
                kv_match_scores=(schedule_context or {}).get("kv_match_scores"),
            )
        return select_worker_with_context(
            policy,
            workers,
            request_text=request_text,
            headers=headers,
            request_body=request_body,
        )

    def _uses_kv_event_policy(self, policy: Policy) -> bool:
        return callable(getattr(policy, "set_kv_cache_state", None))

    def _attach_kv_cache_state(self) -> None:
        for policy in (self.prefill_policy, self.decode_policy):
            setter = getattr(policy, "set_kv_cache_state", None)
            if setter is not None:
                setter(self.kv_cache_state)

    def configure_worker_discovery(self, config) -> None:
        discovery_config = getattr(config, "k8s_discovery_config", None)
        if discovery_config is None or not discovery_config.enabled:
            self.worker_discovery = None
            return

        from smart_router.discovery import K8SPodDiscovery

        self.worker_discovery = K8SPodDiscovery(
            config,
            self.worker_registry,
            on_workers_added=self._handle_discovered_workers_added,
            on_workers_removed=self._handle_discovered_workers_removed,
        )

    def _handle_discovered_workers_added(self, worker_ids: List[str]) -> None:
        self._subscribe_kv_events_for_workers(worker_ids)
        self.request_debounced_health_refresh(worker_ids)

    def _handle_discovered_workers_removed(self, worker_ids: List[str]) -> None:
        self._remove_workers_from_policies(worker_ids)
        for worker_id in worker_ids:
            self.kv_cache_state.clear_worker(worker_id)
        if self.kv_event_subscriber is not None:
            self.kv_event_subscriber.remove_workers(worker_ids)

    def _subscribe_kv_events_for_workers(self, worker_ids: List[str]) -> None:
        if (not self.config.kv_events_config.enabled
                or self.kv_event_subscriber is None):
            return

        worker_id_set = set(worker_ids)
        if not worker_id_set:
            return

        endpoint_map = build_worker_event_endpoints(
            self.config,
            self.worker_registry.get_all(),
        )
        endpoints = [
            endpoint for endpoint in endpoint_map
            if endpoint.worker_id in worker_id_set
        ]
        if endpoints:
            self.kv_event_subscriber.add_endpoints(endpoints)

    async def run(self):
        self._event_loop = asyncio.get_running_loop()
        tasks = []
        try:
            if self.worker_discovery is not None:
                await self.worker_discovery.sync_once()

            if self.config.kv_events_config.enabled:
                endpoint_map = build_worker_event_endpoints(
                    self.config,
                    self.worker_registry.get_all(),
                )
                self.kv_event_subscriber = KVEventSubscriber(
                    self.kv_cache_state,
                    endpoint_map,
                )
                self.kv_event_subscriber.start()

            await self.refresh_worker_health()
            tasks.extend(
                [
                    self.receive_loop(),
                    self.schedule_loop(),
                    self.health_check_loop(),
                ]
            )
            if self.worker_discovery is not None:
                tasks.append(self.worker_discovery.run_loop())
            await asyncio.gather(*tasks)
        finally:
            if self.kv_event_subscriber is not None:
                await self.kv_event_subscriber.stop()

def start_engine(config: SmartRouterConfig, input_addr: str, output_addr: str) -> None:
    engine = VLLMEngine(
        config,
        input_socket_address=input_addr,
        output_socket_address=output_addr
    )
    asyncio.run(engine.run())
