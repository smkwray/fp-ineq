from __future__ import annotations

import pytest

from fp_ineq.phase1_compose import _apply_manifest_patches


def test_apply_manifest_patches_supports_selected_experimental_patch_groups() -> None:
    manifest = {
        "patches": [
            {"id": "base", "kind": "literal_replace", "search": "CREATE C=1;", "replace": "CREATE C=1;\nINPUT X;"},
        ],
        "experimental_patches": [
            {
                "id": "credit_effective_rates",
                "kind": "literal_replace_group",
                "replacements": [
                    {"search": "RSA", "replace": "RSAEFF"},
                    {"search": "RMA", "replace": "RMAEFF"},
                ],
            }
        ],
    }
    stock_text = "CREATE C=1;\nIDENT THG=D1G*YT;\nIDENT THS=D1S*YT;\nRSA\nRMA\n"
    composed = _apply_manifest_patches(
        stock_text,
        manifest,
        experimental_patch_ids=["credit_effective_rates"],
    )
    assert "INPUT X;" in composed
    assert "IDENT THG=D1G*YT;" in composed
    assert "IDENT THS=D1S*YT;" in composed
    assert "RSAEFF" in composed
    assert "RMAEFF" in composed


def test_apply_manifest_patches_rejects_unknown_experimental_patch_id() -> None:
    with pytest.raises(ValueError, match="Unknown experimental phase1 patch ids"):
        _apply_manifest_patches(
            "CREATE C=1;",
            {"patches": [], "experimental_patches": []},
            experimental_patch_ids=["missing"],
        )
