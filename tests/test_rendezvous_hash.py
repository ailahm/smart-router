from smart_router.config import PolicyConfig, SmartRouterConfig
from smart_router.policies.policy import get_policy_config
from smart_router.policies.rendezvous_hash import RendezvousHashPolicy
from smart_router.worker import BasicWorker, DPAwareWorker, WorkerType


def _policy() -> RendezvousHashPolicy:
    return RendezvousHashPolicy(PolicyConfig(policy="rendezvous_hash"))


def _workers(count: int):
    config = SmartRouterConfig()
    return [
        BasicWorker(f"http://worker-{index}", WorkerType.REGULAR, config)
        for index in range(count)
    ]


def test_factory_builds_rendezvous_hash_policy():
    policy = get_policy_config(PolicyConfig(policy="rendezvous_hash"))

    assert isinstance(policy, RendezvousHashPolicy)
    assert policy.name() == "rendezvous_hash"


def test_same_header_key_selects_same_worker_for_stable_worker_set():
    policy = _policy()
    workers = _workers(4)
    headers = {"x-session-id": "session-a"}

    selected = policy.select_worker(workers, headers=headers)

    for _ in range(10):
        assert policy.select_worker(workers, headers=headers) is selected


def test_header_priority_beats_body_key():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(
        headers={"x-user-id": "header-user"},
        request_body={"session_params": {"session_id": "body-session"}},
    )

    assert routing_key == "header:x-user-id:header-user"
    assert key_source == "header:x-user-id"


def test_header_lookup_is_case_insensitive():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(
        headers={"X-Session-ID": "session-a"},
    )

    assert routing_key == "header:x-session-id:session-a"
    assert key_source == "header:x-session-id"


def test_body_key_priority_and_namespaces():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(
        request_body={
            "session_params": {"session_id": "session-a"},
            "user": "alice",
            "session_id": "session-b",
            "user_id": "bob",
        },
    )
    assert routing_key == "session:session-a"
    assert key_source == "body:session_params.session_id"

    routing_key, key_source = policy.extract_routing_key(
        request_body={
            "user": "alice",
            "session_id": "session-b",
            "user_id": "bob",
        },
    )
    assert routing_key == "user:alice"
    assert key_source == "body:user"

    routing_key, key_source = policy.extract_routing_key(
        request_body={"session_id": "session-b", "user_id": "bob"},
    )
    assert routing_key == "session:session-b"
    assert key_source == "body:session_id"

    routing_key, key_source = policy.extract_routing_key(
        request_body={"user_id": "bob"},
    )
    assert routing_key == "user:bob"
    assert key_source == "body:user_id"


def test_fallback_uses_short_request_text_raw_content():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(request_text="hello")

    assert routing_key == "request:hello"
    assert key_source == "fallback:request_text"


def test_fallback_uses_hash_for_long_request_text():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(request_text="x" * 1025)

    assert routing_key.startswith("request_hash:")
    assert len(routing_key.removeprefix("request_hash:")) == 16
    assert key_source == "fallback_hash:request_text"


def test_fallback_empty_request_key_when_no_content():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key()

    assert routing_key == "request:"
    assert key_source == "fallback:request_text"


def test_fallback_uses_canonical_body_when_request_text_missing():
    policy = _policy()

    routing_key, key_source = policy.extract_routing_key(
        request_body={"b": 2, "a": 1},
    )

    assert routing_key == 'request:{"a":1,"b":2}'
    assert key_source == "fallback:request_body"


def test_unavailable_workers_are_not_selected():
    policy = _policy()
    workers = _workers(2)
    selected = policy.select_worker(
        workers,
        headers={"x-session-id": "session-a"},
    )
    selected.set_healthy(False)

    next_selected = policy.select_worker(
        workers,
        headers={"x-session-id": "session-a"},
    )

    assert next_selected is not selected
    assert next_selected.is_healthy()


def test_no_available_worker_returns_none():
    policy = _policy()
    workers = _workers(2)
    for worker in workers:
        worker.set_healthy(False)

    assert policy.select_worker(workers, headers={"x-session-id": "session-a"}) is None


def test_removing_non_selected_worker_keeps_selection():
    policy = _policy()
    workers = _workers(3)
    selected = policy.select_worker(workers, headers={"x-session-id": "session-a"})
    remaining = [worker for worker in workers if worker is selected]
    remaining.append(next(worker for worker in workers if worker is not selected))

    assert policy.select_worker(
        remaining,
        headers={"x-session-id": "session-a"},
    ) is selected


def test_adding_worker_only_moves_key_when_new_worker_wins():
    policy = _policy()
    workers = _workers(3)
    added_worker = _workers(4)[3]

    stable_key = None
    selected_before = None
    for index in range(100):
        headers = {"x-session-id": f"session-{index}"}
        before = policy.select_worker(workers, headers=headers)
        after = policy.select_worker(workers + [added_worker], headers=headers)
        if before is after:
            stable_key = headers
            selected_before = before
            break

    assert stable_key is not None
    assert policy.select_worker(workers + [added_worker], headers=stable_key) is selected_before


def test_tie_breaker_selects_lexicographically_smaller_worker(monkeypatch):
    policy = _policy()
    config = SmartRouterConfig()
    workers = [
        BasicWorker("http://worker-b", WorkerType.REGULAR, config),
        BasicWorker("http://worker-a", WorkerType.REGULAR, config),
    ]

    def fake_stable_hash(key: str) -> int:
        if key.startswith("header:x-session-id:session-a:"):
            return 10
        return 1

    monkeypatch.setattr("smart_router.policies.rendezvous_hash.stable_hash", fake_stable_hash)

    selected = policy.select_worker(
        workers,
        headers={"x-session-id": "session-a"},
    )

    assert selected.url() == "http://worker-a"


def test_dp_worker_identity_includes_rank():
    policy = _policy()
    config = SmartRouterConfig()
    workers = [
        DPAwareWorker("http://worker", WorkerType.PREFILL, config, 0, 2),
        DPAwareWorker("http://worker", WorkerType.PREFILL, config, 1, 2),
    ]

    selected = policy.select_worker(
        workers,
        headers={"x-session-id": "session-a"},
    )

    assert selected.url() in {"http://worker@0", "http://worker@1"}
