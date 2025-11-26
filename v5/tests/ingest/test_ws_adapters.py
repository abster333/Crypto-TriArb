import pytest
import json

from v5.ingest.ws_adapters import UniswapV3WsAdapter
from v5.ingest.event_decoder import SWAP_TOPIC_UNIV3


class DummyWS:
    def __init__(self, msg):
        self.msg = msg
        self.sent = []

    async def send(self, data):
        self.sent.append(data)

    async def recv(self):
        return self.msg


@pytest.mark.asyncio
async def test_adapter_parses_event(monkeypatch):
    msg = {
        "params": {
            "result": {
                "address": "0xpool",
                "topics": [SWAP_TOPIC_UNIV3],
                "data": "0x" + "00"*64 + "00"*64 + ("01" + "00"*31) + ("02" + "00"*31) + ("00"*31 + "01"),
                "blockNumber": 7,
            }
        }
    }
    adapter = UniswapV3WsAdapter("ws://example")
    dummy = DummyWS(json_dumps(msg))
    import websockets

    async def fake_connect(url):
        return dummy

    monkeypatch.setattr(websockets, "connect", fake_connect)
    await adapter.connect()
    evt = await adapter.recv_event()
    assert evt.pool == "0xpool"
    assert evt.block_number == 7


def json_dumps(obj):
    import json
    return json.dumps(obj)
