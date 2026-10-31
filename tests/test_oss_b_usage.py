"""Regression coverage with inert workers and mocked provider boundaries."""
import pytest

from ccx.claude import usage


@pytest.mark.parametrize('saved', ['oauth', 'invalid'])
def test_usage_selector_repairs_invalid_saved_choice(tmp_path, monkeypatch, saved):
    monkeypatch.setattr(usage, 'usage_source_path', lambda: tmp_path / 'source.json')
    monkeypatch.setattr(usage, 'usage_cache_path', lambda: tmp_path / 'cache.json')
    monkeypatch.setattr(usage.oslayer, 'direct_oauth_usage_supported', lambda: False)
    usage._atomic_private_json(usage.usage_source_path(), {'version': 1, 'source': saved})
    usage._write_usage_cache({'five_hour': {'utilization': 80}})
    assert usage.set_usage_source('get_usage') == 'get_usage'
    assert usage._usage_source() == 'get_usage'
    assert 'five_hour' not in usage._read_usage_cache()
