from ccx.codex import delivery as D


def test_delivery_id_rejects_actual_nul_but_allows_literal_escape():
    assert D.valid_delivery_id("plain-id") is True
    assert D.valid_delivery_id("a\\x00b") is True
    assert D.valid_delivery_id("a\x00b") is False
