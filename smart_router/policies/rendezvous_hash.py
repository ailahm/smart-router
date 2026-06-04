import logging
from typing import List, Optional

from smart_router.config import PolicyConfig
from smart_router.policies.consistent_hash import stable_hash
from smart_router.policies.policy import Policy
from smart_router.policies.routing_key import extract_routing_key
from smart_router.worker import Worker

logger = logging.getLogger(__name__)


class RendezvousHashPolicy(Policy):
    def __init__(self, config: PolicyConfig):
        self.config = config

    def name(self) -> str:
        return "rendezvous_hash"

    def select_worker(
        self,
        workers: List[Worker],
        request_text: Optional[str] = None,
        headers: Optional[dict] = None,
        request_body: Optional[dict] = None,
    ) -> Optional[Worker]:
        candidates = [worker for worker in workers if worker.is_available()]
        if not candidates:
            return None

        routing_key, key_source = self.extract_routing_key(
            request_text=request_text,
            headers=headers,
            request_body=request_body,
        )

        selected_worker: Optional[Worker] = None
        selected_identity = ""
        best_score: Optional[int] = None

        for worker in candidates:
            worker_identity = worker.url()
            score = stable_hash(f"{routing_key}:{worker_identity}")
            if (
                best_score is None
                or score > best_score
                or (score == best_score and worker_identity < selected_identity)
            ):
                best_score = score
                selected_worker = worker
                selected_identity = worker_identity

        if selected_worker is not None:
            logger.debug(
                "[POLICY: %s] key_source=%s key_hash=%016x selected=%s",
                self.name(),
                key_source,
                stable_hash(routing_key),
                selected_worker.url(),
            )

        return selected_worker

    def extract_routing_key(
        self,
        request_text: Optional[str] = None,
        headers: Optional[dict] = None,
        request_body: Optional[dict] = None,
    ) -> tuple[str, str]:
        return extract_routing_key(
            request_text=request_text,
            headers=headers,
            request_body=request_body,
            stable_hash_fn=stable_hash,
        )
