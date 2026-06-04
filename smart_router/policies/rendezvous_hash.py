import json
import logging
from typing import Any, List, Optional

from smart_router.config import PolicyConfig
from smart_router.policies.consistent_hash import stable_hash
from smart_router.policies.policy import Policy
from smart_router.worker import Worker

logger = logging.getLogger(__name__)

FALLBACK_RAW_CONTENT_MAX_CHARS = 1024
HEADER_KEY_PRIORITY = (
    "x-session-id",
    "x-user-id",
    "x-tenant-id",
    "x-correlation-id",
    "x-request-id",
    "x-trace-id",
)


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
        normalized_headers = self._normalize_headers(headers)
        for header_name in HEADER_KEY_PRIORITY:
            value = self._string_value(normalized_headers.get(header_name))
            if value is not None:
                return f"header:{header_name}:{value}", f"header:{header_name}"

        if isinstance(request_body, dict):
            session_params = request_body.get("session_params")
            if isinstance(session_params, dict):
                value = self._string_value(session_params.get("session_id"))
                if value is not None:
                    return f"session:{value}", "body:session_params.session_id"

            value = self._string_value(request_body.get("user"))
            if value is not None:
                return f"user:{value}", "body:user"

            value = self._string_value(request_body.get("session_id"))
            if value is not None:
                return f"session:{value}", "body:session_id"

            value = self._string_value(request_body.get("user_id"))
            if value is not None:
                return f"user:{value}", "body:user_id"

        return self._fallback_routing_key(request_text, request_body)

    def _fallback_routing_key(
        self,
        request_text: Optional[str],
        request_body: Optional[dict],
    ) -> tuple[str, str]:
        content = request_text if request_text is not None else ""
        source = "request_text"
        if not content and request_body:
            content = self._canonical_body(request_body)
            source = "request_body"

        if len(content) <= FALLBACK_RAW_CONTENT_MAX_CHARS:
            return f"request:{content}", f"fallback:{source}"

        return (
            f"request_hash:{stable_hash(content):016x}",
            f"fallback_hash:{source}",
        )

    def _normalize_headers(self, headers: Optional[dict]) -> dict[str, Any]:
        if not headers:
            return {}
        return {str(key).lower(): value for key, value in headers.items()}

    def _string_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value if value else None
        if isinstance(value, (dict, list, tuple, set)):
            return None
        return str(value)

    def _canonical_body(self, request_body: dict) -> str:
        try:
            return json.dumps(
                request_body,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            return str(request_body)
