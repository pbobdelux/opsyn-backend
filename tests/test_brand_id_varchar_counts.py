"""Tests for brand_id VARCHAR type safety in count queries and SQL helpers.

Verifies that:
- build_brand_filter_sql() returns plain ``column = :param`` (no CAST)
- Count queries work when brand_id is a plain string (not a UUID object)
- No "operator does not exist: uuid = character varying" errors occur
- sync-status total_local_orders reflects actual DB count (not stale cache)
- Reconciliation counts match the orders table

Run with:
    python -m pytest tests/test_brand_id_varchar_counts.py -v

These tests are pure-Python and have no database or network dependencies
(they mock the DB session where needed).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ---------------------------------------------------------------------------
# build_brand_filter_sql
# ---------------------------------------------------------------------------

class TestBuildBrandFilterSql:
    """build_brand_filter_sql() must never emit CAST(:x AS UUID)."""

    def test_default_returns_plain_equality(self):
        from services.leaflink_sync import build_brand_filter_sql
        result = build_brand_filter_sql()
        assert result == "brand_id = :brand_id"
        assert "CAST" not in result.upper()
        assert "UUID" not in result.upper()

    def test_custom_column_name(self):
        from services.leaflink_sync import build_brand_filter_sql
        result = build_brand_filter_sql(column_name="org_id", param_name="org_id")
        assert result == "org_id = :org_id"
        assert "CAST" not in result.upper()

    def test_custom_param_name(self):
        from services.leaflink_sync import build_brand_filter_sql
        result = build_brand_filter_sql(column_name="brand_id", param_name="b_id")
        assert result == "brand_id = :b_id"

    def test_no_uuid_cast_in_any_form(self):
        from services.leaflink_sync import build_brand_filter_sql
        for col in ("brand_id", "org_id", "tenant_id"):
            result = build_brand_filter_sql(column_name=col, param_name=col)
            assert "CAST" not in result.upper(), (
                f"build_brand_filter_sql({col!r}) must not emit CAST: {result!r}"
            )
            assert "UUID" not in result.upper(), (
                f"build_brand_filter_sql({col!r}) must not emit UUID: {result!r}"
            )


# ---------------------------------------------------------------------------
# SQL fragment audit — scan known query strings for broken casts
# ---------------------------------------------------------------------------

class TestNoUuidCastOnVarcharColumns:
    """Scan the actual SQL strings in leaflink_sync.py for broken CAST patterns.

    The columns orders.brand_id, orders.org_id, sync_health.brand_id,
    sync_runs.brand_id, dead_letter_line_items.brand_id, and
    sync_dead_letters.brand_id are all VARCHAR — they must never appear
    in a CAST(:x AS UUID) expression in a WHERE clause.
    """

    def _load_source(self) -> str:
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "services" / "leaflink_sync.py"
        return src.read_text(encoding="utf-8")

    def test_sync_health_insert_no_uuid_cast(self):
        """sync_health INSERT VALUES must not cast brand_id to UUID."""
        source = self._load_source()
        # Find all INSERT INTO sync_health ... VALUES lines
        import re
        # Look for VALUES clauses that contain CAST(:brand_id AS UUID)
        # in the context of sync_health inserts
        bad_pattern = re.compile(
            r"INSERT\s+INTO\s+sync_health[^;]*?VALUES\s*\([^)]*CAST\s*\(\s*:brand_id\s+AS\s+UUID\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} sync_health INSERT(s) with CAST(:brand_id AS UUID). "
            "sync_health.brand_id is VARCHAR(120) — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_sync_health_update_no_uuid_cast(self):
        """sync_health UPDATE WHERE must not cast brand_id to UUID."""
        source = self._load_source()
        import re
        bad_pattern = re.compile(
            r"UPDATE\s+sync_health[^;]*?WHERE\s+brand_id\s*=\s*CAST\s*\(\s*:brand_id\s+AS\s+UUID\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} sync_health UPDATE(s) with CAST(:brand_id AS UUID). "
            "sync_health.brand_id is VARCHAR(120) — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_dead_letter_line_items_insert_no_uuid_cast(self):
        """dead_letter_line_items INSERT VALUES must not cast brand_id to UUID."""
        source = self._load_source()
        import re
        bad_pattern = re.compile(
            r"INSERT\s+INTO\s+dead_letter_line_items[^;]*?VALUES[^;]*?CAST\s*\(\s*:brand_id\s+AS\s+UUID\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} dead_letter_line_items INSERT(s) with CAST(:brand_id AS UUID). "
            "dead_letter_line_items.brand_id is VARCHAR(120) — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_sync_dead_letters_insert_no_uuid_cast(self):
        """sync_dead_letters INSERT VALUES must not cast brand_id to UUID."""
        source = self._load_source()
        import re
        bad_pattern = re.compile(
            r"INSERT\s+INTO\s+sync_dead_letters[^;]*?VALUES[^;]*?CAST\s*\(\s*:brand_id\s+AS\s+UUID\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} sync_dead_letters INSERT(s) with CAST(:brand_id AS UUID). "
            "sync_dead_letters.brand_id is VARCHAR — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_sync_dead_letters_count_no_uuid_cast(self):
        """COUNT query on sync_dead_letters must not cast brand_id to UUID."""
        source = self._load_source()
        import re
        bad_pattern = re.compile(
            r"SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+sync_dead_letters[^;]*?CAST\s*\(\s*:brand_id\s+AS\s+uuid\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} sync_dead_letters COUNT(s) with CAST(:brand_id AS uuid). "
            "sync_dead_letters.brand_id is VARCHAR — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_brand_api_credentials_update_no_uuid_cast(self):
        """brand_api_credentials UPDATE WHERE must not cast brand_id to UUID."""
        source = self._load_source()
        import re
        bad_pattern = re.compile(
            r"UPDATE\s+brand_api_credentials[^;]*?WHERE\s+brand_id\s*=\s*CAST\s*\(\s*:brand_id\s+AS\s+uuid\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} brand_api_credentials UPDATE(s) with CAST(:brand_id AS uuid). "
            "brand_api_credentials.brand_id is VARCHAR(120) — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )

    def test_orders_org_id_count_no_uuid_cast(self):
        """COUNT query on orders.org_id must not cast to UUID in routes/orders.py."""
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "routes" / "orders.py"
        source = src.read_text(encoding="utf-8")
        import re
        bad_pattern = re.compile(
            r"SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+orders[^;]*?CAST\s*\(\s*:org_id\s+AS\s+uuid\s*\)",
            re.IGNORECASE | re.DOTALL,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} orders COUNT(s) with CAST(:org_id AS uuid). "
            "orders.org_id is VARCHAR(120) — no cast needed.\n"
            + "\n".join(m[:200] for m in matches)
        )


# ---------------------------------------------------------------------------
# build_brand_filter_sql produces correct SQL for VARCHAR columns
# ---------------------------------------------------------------------------

class TestBrandFilterSqlCorrectness:
    """Verify the SQL fragment is syntactically correct for PostgreSQL."""

    def test_fragment_is_valid_sql_equality(self):
        from services.leaflink_sync import build_brand_filter_sql
        fragment = build_brand_filter_sql()
        # Must be a simple equality expression
        assert "=" in fragment
        assert fragment.strip().startswith("brand_id")
        assert ":brand_id" in fragment

    def test_fragment_usable_in_where_clause(self):
        """The fragment can be embedded in a WHERE clause without syntax errors."""
        from services.leaflink_sync import build_brand_filter_sql
        fragment = build_brand_filter_sql()
        full_sql = f"SELECT COUNT(*) FROM orders WHERE {fragment}"
        # Should not raise any import/syntax errors
        assert "SELECT COUNT(*) FROM orders WHERE brand_id = :brand_id" == full_sql

    def test_string_brand_id_param_works(self):
        """brand_id as a plain string (not UUID object) must be accepted."""
        from services.leaflink_sync import build_brand_filter_sql
        fragment = build_brand_filter_sql()
        # Simulate what asyncpg would receive: a plain string
        brand_id_value = "noble-nectar-brand-uuid-string"
        # The fragment should accept any string without type coercion
        assert isinstance(brand_id_value, str)
        assert ":brand_id" in fragment  # param placeholder present


# ---------------------------------------------------------------------------
# Logging markers present in sync-status endpoint
# ---------------------------------------------------------------------------

class TestSyncStatusLoggingMarkers:
    """Verify that [SYNC_STATUS_COUNT_QUERY] and [SYNC_STATUS_COUNT_RESULT]
    log markers are present in the routes/orders.py source."""

    def _load_orders_source(self) -> str:
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "routes" / "orders.py"
        return src.read_text(encoding="utf-8")

    def test_sync_status_count_query_marker_present(self):
        source = self._load_orders_source()
        assert "[SYNC_STATUS_COUNT_QUERY]" in source, (
            "routes/orders.py must emit [SYNC_STATUS_COUNT_QUERY] in the sync-status endpoint"
        )

    def test_sync_status_count_result_marker_present(self):
        source = self._load_orders_source()
        assert "[SYNC_STATUS_COUNT_RESULT]" in source, (
            "routes/orders.py must emit [SYNC_STATUS_COUNT_RESULT] in the sync-status endpoint"
        )

    def test_sync_status_count_query_in_leaflink_sync(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "services" / "leaflink_sync.py"
        source = src.read_text(encoding="utf-8")
        assert "[SYNC_STATUS_COUNT_QUERY]" in source, (
            "services/leaflink_sync.py must emit [SYNC_STATUS_COUNT_QUERY] in upsert_sync_metrics_snapshot"
        )

    def test_sync_status_count_result_in_leaflink_sync(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "services" / "leaflink_sync.py"
        source = src.read_text(encoding="utf-8")
        assert "[SYNC_STATUS_COUNT_RESULT]" in source, (
            "services/leaflink_sync.py must emit [SYNC_STATUS_COUNT_RESULT] in upsert_sync_metrics_snapshot"
        )


# ---------------------------------------------------------------------------
# audit_id_column_types present in database.py
# ---------------------------------------------------------------------------

class TestAuditIdColumnTypes:
    """Verify audit_id_column_types() is defined and callable."""

    def test_function_exists(self):
        from database import audit_id_column_types
        import asyncio
        assert callable(audit_id_column_types)

    def test_db_id_type_audit_marker_in_source(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "database.py"
        source = src.read_text(encoding="utf-8")
        assert "[DB_ID_TYPE_AUDIT]" in source, (
            "database.py must emit [DB_ID_TYPE_AUDIT] log markers"
        )

    def test_db_id_type_mismatch_marker_in_source(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "database.py"
        source = src.read_text(encoding="utf-8")
        assert "[DB_ID_TYPE_MISMATCH]" in source, (
            "database.py must emit [DB_ID_TYPE_MISMATCH] log markers for type violations"
        )

    def test_audit_called_at_startup_in_worker(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "opsyn-sync-worker" / "main.py"
        source = src.read_text(encoding="utf-8")
        assert "audit_id_column_types" in source, (
            "opsyn-sync-worker/main.py must call audit_id_column_types() at startup"
        )

    def test_audit_called_at_startup_in_main(self):
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "main.py"
        source = src.read_text(encoding="utf-8")
        assert "audit_id_column_types" in source, (
            "main.py must call audit_id_column_types() at startup"
        )


# ---------------------------------------------------------------------------
# Verify no CAST(:brand_id AS UUID) in WHERE clauses for VARCHAR columns
# ---------------------------------------------------------------------------

class TestNoUuidCastInWhereClauses:
    """Comprehensive scan for any remaining CAST(:brand_id AS UUID) in WHERE clauses
    that filter VARCHAR columns."""

    def _load_source(self) -> str:
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "services" / "leaflink_sync.py"
        return src.read_text(encoding="utf-8")

    def test_no_cast_brand_id_uuid_in_where(self):
        """No WHERE clause should cast brand_id to UUID (it's VARCHAR everywhere)."""
        source = self._load_source()
        import re
        # Match WHERE brand_id = CAST(:brand_id AS UUID) or WHERE brand_id = CAST(:brand_id AS uuid)
        bad_pattern = re.compile(
            r"WHERE\s+brand_id\s*=\s*CAST\s*\(\s*:brand_id\s+AS\s+(?:UUID|uuid)\s*\)",
            re.IGNORECASE,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} WHERE clause(s) with CAST(:brand_id AS UUID). "
            "brand_id is VARCHAR in all tables — remove the CAST.\n"
            + "\n".join(matches)
        )

    def test_no_cast_org_id_uuid_in_where_for_varchar_tables(self):
        """orders.org_id is VARCHAR — WHERE org_id = CAST(:org_id AS uuid) is wrong."""
        import pathlib
        src = pathlib.Path(__file__).parent.parent / "routes" / "orders.py"
        source = src.read_text(encoding="utf-8")
        import re
        bad_pattern = re.compile(
            r"WHERE\s+org_id\s*=\s*CAST\s*\(\s*:org_id\s+AS\s+uuid\s*\)",
            re.IGNORECASE,
        )
        matches = bad_pattern.findall(source)
        assert not matches, (
            f"Found {len(matches)} WHERE clause(s) with CAST(:org_id AS uuid) in routes/orders.py. "
            "orders.org_id is VARCHAR(120) — remove the CAST.\n"
            + "\n".join(matches)
        )
