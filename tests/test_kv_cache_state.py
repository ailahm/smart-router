from smart_router.cache.kv_cache_state import (
    BlockRemovedEvent,
    BlockStoredEvent,
    KVCacheState,
)


def _store_event(block_hashes):
    return BlockStoredEvent(
        block_hashes=block_hashes,
        parent_block_hash=None,
        token_ids=[1, 2, 3, 4],
        block_size=2,
        lora_id=None,
        medium="GPU",
    )


def test_remove_blocks_prunes_unowned_block_records_and_token_index():
    state = KVCacheState()
    state.store_blocks("worker-0", _store_event([101, 102]), ts=1.0)

    assert state.count_matched_tokens("worker-0", [1, 2, 3, 4]) == 4

    state.remove_blocks("worker-0", [101, 102], "GPU")

    assert state.count_matched_tokens("worker-0", [1, 2, 3, 4]) == 0
    assert state._blocks == {}
    assert state._block_owners == {}
    assert state._token_index == {}


def test_remove_blocks_keeps_shared_block_until_last_owner_is_removed():
    state = KVCacheState()
    event = _store_event([201, 202])
    state.store_blocks("worker-0", event, ts=1.0)
    state.store_blocks("worker-1", event, ts=1.0)

    state.apply_event(
        "worker-0",
        BlockRemovedEvent(block_hashes=[201, 202], medium="GPU"),
        ts=2.0,
    )

    assert state.count_matched_tokens("worker-0", [1, 2, 3, 4]) == 0
    assert state.count_matched_tokens("worker-1", [1, 2, 3, 4]) == 4
    assert set(state._blocks) == {201, 202}

    state.apply_event(
        "worker-1",
        BlockRemovedEvent(block_hashes=[201, 202], medium="GPU"),
        ts=3.0,
    )

    assert state._blocks == {}
    assert state._block_owners == {}
    assert state._token_index == {}
