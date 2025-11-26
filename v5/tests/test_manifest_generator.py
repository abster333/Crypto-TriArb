import json
import os
import pytest

import v5.manifest_generator as gen


def test_pagination_cap(monkeypatch):
    calls = []

    def fake_fetch_page(network, dex, page, per_page, api_key):
        calls.append((page, per_page))
        return [{"attributes": {"name": "A/B 0.01%", "address": f"0x{page}"},
                 "relationships": {"base_token": {"data": {"id": f"{network}_0x1"}},
                                   "quote_token": {"data": {"id": f"{network}_0x2"}}}}] * per_page

    def fake_fetch_token(network, addr, api_key):
        return {"attributes": {"symbol": addr[-3:], "decimals": 18}}

    targets = [gen.Target("eth", "uniswap_v3", 250)]
    tokens, pools = gen.build_manifests(targets, "demo", "free", fetch_page_fn=fake_fetch_page, fetch_token_fn=fake_fetch_token, rate_sleep_fn=lambda tier, sleep_fn=None: None, sleep_fn=lambda d: None)
    # capped to 200 -> 10 pages
    assert len(calls) == 10
    assert len(pools["pairs"]) == 200
    assert len(tokens["tokens"]) >= 2


def test_skip_token_meta(monkeypatch):
    def fake_fetch_page(network, dex, page, per_page, api_key):
        return [{"attributes": {"name": "A/B 0.01%", "address": "0xpool"},
                 "relationships": {"base_token": {"data": {"id": f"{network}_0x1"}},
                                   "quote_token": {"data": {"id": f"{network}_0x2"}}}}]

    targets = [gen.Target("eth", "uniswap_v3", 1)]
    tokens, pools = gen.build_manifests(targets, "demo", "free", fetch_page_fn=fake_fetch_page, fetch_token_fn=lambda *a, **k: None, skip_token_meta=True)
    # decimals default to 18 when skipping
    t = list(tokens["tokens"].values())[0]
    assert t["decimals"] == 18
    assert len(pools["pairs"]) == 1


def test_pagination_50_pools(monkeypatch):
    calls = []

    def fake_fetch_page(network, dex, page, per_page, api_key):
        calls.append(page)
        return [{"attributes": {"name": "A/B 0.01%", "address": f"0x{page}"},
                 "relationships": {"base_token": {"data": {"id": f"{network}_0x1"}},
                                   "quote_token": {"data": {"id": f"{network}_0x2"}}}}] * per_page

    def fake_fetch_token(network, addr, api_key):
        return {"attributes": {"symbol": addr[-3:], "decimals": 18}}

    targets = [gen.Target("eth", "uniswap_v3", 50)]
    tokens, pools = gen.build_manifests(targets, "demo", "free", fetch_page_fn=fake_fetch_page, fetch_token_fn=fake_fetch_token, rate_sleep_fn=lambda tier, sleep_fn=None: None, sleep_fn=lambda d: None)
    assert len(set(calls)) == 3  # pages 1-3
    assert len(pools["pairs"]) == 50


def test_missing_decimals_fallback(monkeypatch):
    def fake_fetch_page(network, dex, page, per_page, api_key):
        return [{"attributes": {"name": "A/B 0.01%", "address": "0xpool"},
                 "relationships": {"base_token": {"data": {"id": f"{network}_0x1"}},
                                   "quote_token": {"data": {"id": f"{network}_0x2"}}}}]

    def fake_fetch_token(network, addr, api_key):
        return {"attributes": {"symbol": "X"}}  # no decimals

    targets = [gen.Target("eth", "uniswap_v3", 1)]
    tokens, pools = gen.build_manifests(targets, "demo", "free", fetch_page_fn=fake_fetch_page, fetch_token_fn=fake_fetch_token, rate_sleep_fn=lambda tier, sleep_fn=None: None, sleep_fn=lambda d: None)
    for t in tokens["tokens"].values():
        assert t["decimals"] == 18


def test_rate_limiting(monkeypatch):
    sleep_calls = []

    def fake_fetch_page(network, dex, page, per_page, api_key):
        return [{"attributes": {"name": "A/B 0.01%", "address": f"0x{page}"},
                 "relationships": {"base_token": {"data": {"id": f"{network}_0x1"}},
                                   "quote_token": {"data": {"id": f"{network}_0x2"}}}}] * per_page

    def fake_fetch_token(network, addr, api_key):
        return {"attributes": {"symbol": addr[-3:], "decimals": 18}}

    targets = [gen.Target("eth", "uniswap_v3", 25)]
    gen.build_manifests(
        targets,
        "demo",
        "free",
        fetch_page_fn=fake_fetch_page,
        fetch_token_fn=fake_fetch_token,
        rate_sleep_fn=lambda tier, sleep_fn=None: sleep_calls.append(1),
        sleep_fn=lambda d: None,
    )
    assert len(sleep_calls) >= 1
