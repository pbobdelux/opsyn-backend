"""Tests validating that all count queries use plain string comparisons for
brand_id and org_id (VARCHAR(120) columns), not CAST(:brand_id AS UUID).

These tests guard against the regression:
    operator does not exist: uuid = character varying

which occurs when asyncpg infers a UUID type for a parameter that is compared
against a VARCHAR column.

Run with:
    python -m pytest tests/test_brand_id_varchar_counts.py -v
"""
import sys
import os
import inspect
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_leaflink_sync_source() -> str:
    """Return the full source of services/leaflink_sync.py."""
    src_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "services",
        "leaflink_sync.py",
    )
    with open(src_path, "r", encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Test: no CAST(:brand_id AS UUID) in count queries
# ---------------------------------------------------------------------------

class TestNoCastInCountQueries:
    """Ensure count queries do not use CAST(:brand_id AS UUID) or CAST(:org_id AS UUID)."""

    def _count_query_blocks(self, source: str) -> list[str]:
        """Extract lines that contain both COUNT and brand_id/org_id references."""
        lines = source.splitlines()
        blocks = []
        for i, line in enumerate(lines):
            if "COUNT" in line or "count" in line:
                # Grab a small window around the COUNT line
                start = max(0, i - 2)
                end = min(len(lines), i + 5)
                blocks.append("\n".join(lines[start:end]))
        return blocks

    def test_no_cast_brand_id_as_uuid_in_pre_commit_count(self):
        """PRE_COMMIT_ROW_COUNT must not use CAST(:brand_id AS UUID)."""
        source = _get_leaflink_sync_source()
        # Find the pre-commit block
        pre_commit_match = re.search(
            r"PRE_COMMIT_ROW_COUNT.*?(?=POST_COMMIT_ROW_COUNT|# Visibility)",
            source,
            re.DOTALL,
        )
        if pre_commit_match:
            block = pre_commit_match.group(0)
            assert "CAST(:brand_id AS UUID)" not in block, (
                "PRE_COMMIT_ROW_COUNT block must not use CAST(:brand_id AS UUID). "
                "Use Order.brand_id == _brand_id_str instead."
            )
            assert "CAST(:org_id AS UUID)" not in block, (
                "PRE_COMMIT_ROW_COUNT block must not use CAST(:org_id AS UUID)."
            )

    def test_no_cast_brand_id_as_uuid_in_post_commit_count(self):
        """POST_COMMIT_ROW_COUNT must not use CAST(:brand_id AS UUID)."""
        source = _get_leaflink_sync_source()
        post_commit_match = re.search(
            r"POST_COMMIT_ROW_COUNT.*?(?=BATCH_RECONCILIATION|# Visibility)",
            source,
            re.DOTALL,
        )
        if post_commit_match:
            block = post_commit_match.group(0)
            assert "CAST(:brand_id AS UUID)" not in block, (
                "POST_COMMIT_ROW_COUNT block must not use CAST(:brand_id AS UUID). "
                "Use Order.brand_id == _brand_id_str instead."
            )
            assert "CAST(:org_id AS UUID)" not in block, (
                "POST_COMMIT_ROW_COUNT block must not use CAST(:org_id AS UUID)."
            )

    def test_no_cast_brand_id_in_sync_status_snapshot_counts(self):
        """upsert_sync_metrics_snapshot count queries must not use CAST(:brand_id AS UUID)."""
        source = _get_leaflink_sync_source()
        # Find the upsert_sync_metrics_snapshot function body
        fn_match = re.search(
            r"async def upsert_sync_metrics_snapshot.*?(?=\nasync def |\Z)",
            source,
            re.DOTALL,
        )
        assert fn_match, "upsert_sync_metrics_snapshot function not found"
        fn_body = fn_match.group(0)

        # The count queries inside this function must not use CAST
        assert "CAST(:brand_id AS UUID)" not in fn_body, (
            "upsert_sync_metrics_snapshot count queries must not use "
            "CAST(:brand_id AS UUID). Use Order.brand_id == _brand_id_str instead."
        )
        assert "CAST(:org_id AS UUID)" not in fn_body, (
            "upsert_sync_metrics_snapshot count queries must not use CAST(:org_id AS UUID)."
        )

    def test_no_cast_brand_id_in_final_accounting_count(self):
        """Final accounting visible count must not use CAST(:brand_id AS UUID)."""
        source = _get_leaflink_sync_source()
        # Find the SYNC_FINAL_ACCOUNTING block
        accounting_match = re.search(
            r"final_accounting_visible_count.*?(?=# Aggregate exception|# Aggregate)",
            source,
            re.DOTALL,
        )
        # If the old comment is gone (because we replaced it), check the new block
        if not accounting_match:
            accounting_match = re.search(
                r"_log_count_query_params\(\"final_accounting\".*?_final_visible_count\s*=",
                source,
                re.DOTALL,
            )
        if accounting_match:
            block = accounting_match.group(0)
            assert "CAST(:brand_id AS UUID)" not in block, (
                "Final accounting count must not use CAST(:brand_id AS UUID). "
                "Use Order.brand_id == _brand_id_str instead."
            )


# ---------------------------------------------------------------------------
# Test: _log_count_query_params helper exists and is correct
# ---------------------------------------------------------------------------

class TestLogCountQueryParamsHelper:
    """Validate the _log_count_query_params helper function."""

    def test_helper_exists(self):
        """_log_count_query_params must be importable from services.leaflink_sync."""
        try:
            from services.leaflink_sync import _log_count_query_params
        except ImportError as exc:
            pytest.fail(f"_log_count_query_params not importable: {exc}")

    def test_helper_signature(self):
        """_log_count_query_params must accept label, brand_id, and optional org_id."""
        from services.leaflink_sync import _log_count_query_params
        sig = inspect.signature(_log_count_query_params)
        params = list(sig.parameters.keys())
        assert "label" in params, "Missing 'label' parameter"
        assert "brand_id" in params, "Missing 'brand_id' parameter"
        assert "org_id" in params, "Missing 'org_id' parameter"

    def test_helper_org_id_is_optional(self):
        """org_id must be optional (default None)."""
        from services.leaflink_sync import _log_count_query_params
        sig = inspect.signature(_log_count_query_params)
        org_id_param = sig.parameters.get("org_id")
        assert org_id_param is not None
        assert org_id_param.default is None, (
            "org_id must default to None"
        )

    def test_helper_callable_with_string_brand_id(self):
        """_log_count_query_params must not raise when called with a plain string."""
        from services.leaflink_sync import _log_count_query_params
        # Should not raise
        _log_count_query_params("test_label", "some-brand-id-string")

    def test_helper_callable_with_uuid_string(self):
        """_log_count_query_params must not raise when called with a UUID-format string."""
        from services.leaflink_sync import _log_count_query_params
        _log_count_query_params(
            "test_label",
            "12345678-1234-5678-1234-567812345678",
            org_id="87654321-4321-8765-4321-876543218765",
        )


# ---------------------------------------------------------------------------
# Test: parameters are passed as strings, not UUID objects
# ---------------------------------------------------------------------------

class TestBrandIdIsString:
    """Validate that brand_id is coerced to str before count queries."""

    def test_str_coercion_pattern_present_in_pre_commit(self):
        """PRE_COMMIT block must coerce brand_id_value to str."""
        source = _get_leaflink_sync_source()
        # Look for the str() coercion pattern near PRE_COMMIT
        pre_commit_idx = source.find("PRE_COMMIT_ROW_COUNT_ERROR")
        assert pre_commit_idx != -1, "PRE_COMMIT_ROW_COUNT_ERROR log not found"
        # Check the surrounding 500 chars for str() coercion
        surrounding = source[max(0, pre_commit_idx - 500):pre_commit_idx + 200]
        assert "str(brand_id_value)" in surrounding, (
            "PRE_COMMIT block must coerce brand_id_value to str() before the query"
        )

    def test_str_coercion_pattern_present_in_post_commit(self):
        """POST_COMMIT block must coerce brand_id_value to str."""
        source = _get_leaflink_sync_source()
        post_commit_idx = source.find("POST_COMMIT_ROW_COUNT_ERROR")
        assert post_commit_idx != -1, "POST_COMMIT_ROW_COUNT_ERROR log not found"
        surrounding = source[max(0, post_commit_idx - 500):post_commit_idx + 200]
        assert "str(brand_id_value)" in surrounding, (
            "POST_COMMIT block must coerce brand_id_value to str() before the query"
        )

    def test_str_coercion_pattern_present_in_final_accounting(self):
        """Final accounting block must coerce brand_id_value to str."""
        source = _get_leaflink_sync_source()
        assert "final_accounting" in source, (
            "_log_count_query_params('final_accounting', ...) call not found"
        )
        final_idx = source.find('"final_accounting"')
        assert final_idx != -1
        surrounding = source[max(0, final_idx - 200):final_idx + 200]
        assert "str(brand_id_value)" in surrounding, (
            "Final accounting block must coerce brand_id_value to str()"
        )

    def test_str_coercion_pattern_present_in_sync_status_snapshot(self):
        """sync_status_snapshot block must coerce brand_id to str."""
        source = _get_leaflink_sync_source()
        snapshot_idx = source.find('"sync_status_snapshot"')
        assert snapshot_idx != -1, (
            "_log_count_query_params('sync_status_snapshot', ...) call not found"
        )
        surrounding = source[max(0, snapshot_idx - 200):snapshot_idx + 200]
        assert "str(brand_id)" in surrounding, (
            "sync_status_snapshot block must coerce brand_id to str()"
        )


# ---------------------------------------------------------------------------
# Test: ORM-style queries used (not raw SQL with CAST)
# ---------------------------------------------------------------------------

class TestOrmStyleCountQueries:
    """Validate that count queries use ORM-style where clauses."""

    def test_pre_commit_uses_orm_where(self):
        """PRE_COMMIT count must use Order.brand_id == _brand_id_str (ORM style)."""
        source = _get_leaflink_sync_source()
        # Find the pre-commit block
        pre_err_idx = source.find("PRE_COMMIT_ROW_COUNT_ERROR")
        assert pre_err_idx != -1
        # Look backwards for the ORM query
        block = source[max(0, pre_err_idx - 600):pre_err_idx + 50]
        assert "Order.brand_id ==" in block, (
            "PRE_COMMIT count must use ORM-style Order.brand_id == _brand_id_str"
        )

    def test_post_commit_uses_orm_where(self):
        """POST_COMMIT count must use Order.brand_id == _brand_id_str (ORM style)."""
        source = _get_leaflink_sync_source()
        post_err_idx = source.find("POST_COMMIT_ROW_COUNT_ERROR")
        assert post_err_idx != -1
        block = source[max(0, post_err_idx - 600):post_err_idx + 50]
        assert "Order.brand_id ==" in block, (
            "POST_COMMIT count must use ORM-style Order.brand_id == _brand_id_str"
        )

    def test_sync_status_snapshot_uses_orm_where(self):
        """upsert_sync_metrics_snapshot counts must use ORM-style where clauses."""
        source = _get_leaflink_sync_source()
        fn_match = re.search(
            r"async def upsert_sync_metrics_snapshot.*?(?=\nasync def |\Z)",
            source,
            re.DOTALL,
        )
        assert fn_match, "upsert_sync_metrics_snapshot not found"
        fn_body = fn_match.group(0)
        # Should use _Order.brand_id == _brand_id_str
        assert "_Order.brand_id ==" in fn_body, (
            "upsert_sync_metrics_snapshot must use ORM-style _Order.brand_id == _brand_id_str"
        )

    def test_batch_reconciliation_log_present(self):
        """BATCH_RECONCILIATION log must be present after POST_COMMIT count."""
        source = _get_leaflink_sync_source()
        assert "BATCH_RECONCILIATION" in source, (
            "[BATCH_RECONCILIATION] log line not found in leaflink_sync.py"
        )

    def test_sync_status_count_result_log_present(self):
        """SYNC_STATUS_COUNT_RESULT log must be present in upsert_sync_metrics_snapshot."""
        source = _get_leaflink_sync_source()
        assert "SYNC_STATUS_COUNT_RESULT" in source, (
            "[SYNC_STATUS_COUNT_RESULT] log line not found in leaflink_sync.py"
        )

    def test_row_count_sql_safe_log_present(self):
        """ROW_COUNT_SQL_SAFE log must be emitted by _log_count_query_params."""
        source = _get_leaflink_sync_source()
        assert "ROW_COUNT_SQL_SAFE" in source, (
            "[ROW_COUNT_SQL_SAFE] log tag not found — _log_count_query_params may be missing"
        )
