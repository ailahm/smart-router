import json
from typing import Any, Callable, Optional

FALLBACK_RAW_CONTENT_MAX_CHARS = 1024
HEADER_KEY_PRIORITY = (
    "x-session-id",
    "x-user-id",
    "x-tenant-id",
    "x-correlation-id",
    "x-request-id",
    "x-trace-id",
)


def extract_routing_key(
    *,
    request_text: Optional[str] = None,
    headers: Optional[dict] = None,
    request_body: Optional[dict] = None,
    stable_hash_fn: Callable[[str], int],
) -> tuple[str, str]:
    normalized_headers = _normalize_headers(headers)
    for header_name in HEADER_KEY_PRIORITY:
        value = _string_value(normalized_headers.get(header_name))
        if value is not None:
            return f"header:{header_name}:{value}", f"header:{header_name}"

    if isinstance(request_body, dict):
        session_params = request_body.get("session_params")
        if isinstance(session_params, dict):
            value = _string_value(session_params.get("session_id"))
            if value is not None:
                return f"session:{value}", "body:session_params.session_id"

        value = _string_value(request_body.get("user"))
        if value is not None:
            return f"user:{value}", "body:user"

        value = _string_value(request_body.get("session_id"))
        if value is not None:
            return f"session:{value}", "body:session_id"

        value = _string_value(request_body.get("user_id"))
        if value is not None:
            return f"user:{value}", "body:user_id"

    return _fallback_routing_key(
        request_text=request_text,
        request_body=request_body,
        stable_hash_fn=stable_hash_fn,
    )


def _fallback_routing_key(
    *,
    request_text: Optional[str],
    request_body: Optional[dict],
    stable_hash_fn: Callable[[str], int],
) -> tuple[str, str]:
    content = request_text if request_text is not None else ""
    source = "request_text"
    if not content and request_body:
        content = _canonical_body(request_body)
        source = "request_body"

    if len(content) <= FALLBACK_RAW_CONTENT_MAX_CHARS:
        return f"request:{content}", f"fallback:{source}"

    return (
        f"request_hash:{stable_hash_fn(content):016x}",
        f"fallback_hash:{source}",
    )


def _normalize_headers(headers: Optional[dict]) -> dict[str, Any]:
    if not headers:
        return {}
    return {str(key).lower(): value for key, value in headers.items()}


def _string_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value if value else None
    if isinstance(value, (dict, list, tuple, set)):
        return None
    return str(value)


def _canonical_body(request_body: dict) -> str:
    try:
        return json.dumps(
            request_body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError):
        return str(request_body)
