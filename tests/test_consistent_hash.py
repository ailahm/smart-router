from smart_router.config import PolicyConfig, SmartRouterConfig
from smart_router.policies.consistent_hash import ConsistentHashPolicy
from smart_router.worker import BasicWorker, WorkerType


def _workers(count: int):
    config = SmartRouterConfig()
    return [
        BasicWorker(f"http://worker-{index}", WorkerType.REGULAR, config)
        for index in range(count)
    ]


def _selected_lookup_key(monkeypatch, policy: ConsistentHashPolicy) -> list[str]:
    lookup_keys = []
    original_hash = ConsistentHashPolicy.fbi_hash

    def recording_hash(key: str) -> int:
        if not key.startswith("http://worker-"):
            lookup_keys.append(key)
        return original_hash(key)

    monkeypatch.setattr(policy, "fbi_hash", recording_hash)
    return lookup_keys


def test_consistent_hash_uses_header_priority_for_routing_key(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(
        _workers(2),
        request_text="prompt",
        headers={"x-user-id": "header-user"},
        request_body={"session_params": {"session_id": "body-session"}},
    )

    assert lookup_keys[-1] == "header:x-user-id:header-user"


def test_consistent_hash_header_lookup_is_case_insensitive(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(
        _workers(2),
        headers={"X-Session-ID": "session-a"},
    )

    assert lookup_keys[-1] == "header:x-session-id:session-a"


def test_consistent_hash_uses_body_priority_for_routing_key(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(
        _workers(2),
        request_text="prompt",
        request_body={
            "session_params": {"session_id": "session-a"},
            "user": "alice",
            "session_id": "session-b",
            "user_id": "bob",
        },
    )
    policy.select_worker(
        _workers(2),
        request_text="prompt",
        request_body={
            "user": "alice",
            "session_id": "session-b",
            "user_id": "bob",
        },
    )
    policy.select_worker(
        _workers(2),
        request_text="prompt",
        request_body={"session_id": "session-b", "user_id": "bob"},
    )
    policy.select_worker(
        _workers(2),
        request_text="prompt",
        request_body={"user_id": "bob"},
    )

    assert lookup_keys[-4:] == [
        "session:session-a",
        "user:alice",
        "session:session-b",
        "user:bob",
    ]


def test_consistent_hash_falls_back_to_request_text(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(_workers(2), request_text="hello")

    assert lookup_keys[-1] == "request:hello"


def test_consistent_hash_falls_back_to_canonical_body(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(_workers(2), request_body={"b": 2, "a": 1})

    assert lookup_keys[-1] == 'request:{"a":1,"b":2}'


def test_consistent_hash_hashes_long_fallback_text(monkeypatch):
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    lookup_keys = _selected_lookup_key(monkeypatch, policy)

    policy.select_worker(_workers(2), request_text="x" * 1025)

    assert lookup_keys[-1].startswith("request_hash:")
    assert len(lookup_keys[-1].removeprefix("request_hash:")) == 16


def test_consistent_hash_same_session_key_selects_same_worker():
    policy = ConsistentHashPolicy(PolicyConfig(policy="consistent_hash"))
    workers = _workers(4)
    selected = policy.select_worker(workers, headers={"x-session-id": "session-a"})

    for _ in range(10):
        assert policy.select_worker(
            workers,
            headers={"x-session-id": "session-a"},
        ) is selected
