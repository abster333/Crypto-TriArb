from v5.ingest.event_decoder import decode_swap, SWAP_TOPIC_UNIV3


def test_decode_valid_swap():
    log = {
        "address": "0xpool",
        "topics": [SWAP_TOPIC_UNIV3],
        "data": "0x" + "00"*64 + "00"*64 + ("01" + "00"*31) + ("02" + "00"*31) + ("00"*31 + "01"),
        "blockNumber": 10,
    }
    decoded = decode_swap(log)
    assert decoded["pool"] == "0xpool"
    assert decoded["block_number"] == 10
    assert decoded["sqrt_price_x96"] > 0
    assert decoded["liquidity"] > 0


def test_decode_invalid_topic():
    decoded = decode_swap({"topics": ["0xdead"], "data": "0x"})
    assert decoded is None
