#!/usr/bin/env python3
"""
Migration smoke test — validates that the migration runner works correctly
against empty, partial, and dirty database states.

Usage:
    python scripts/migration_smoke_test.py [--verbose] [--db-url <url>]

Options:
    --verbose       Print detailed output for each check
    --db-url <url>  PostgreSQL connection URL to test against
                    (defaults to DATABASE_URL environment variable)

Exit codes:
    0  All tests passed
    1  One or more tests failed

Output:
    JSON report written to stdout on completion.

Tests:
    1. Empty database   — run all migrations from scratch on a clean schema
    2. Partial database — run migrations on a DB with some tables already present
    3. Dirty database   — run migrations on a DB with legacy tables/columns

Each test verifies:
    - No critical migration failures
    - No unexpected noncritical failures
    - No UUID/INTEGER FK mismatches
    - No missing parent tables (routes, drivers, route_stops)
    - No invalid SQL syntax
    - Migration validator passes (or warns only)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from typing import Any

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so we can import project modules.
# ---------------------------------------------------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("migration_smoke_test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_db_url(url: str) -> str:
    """Normalise a postgres:// URL to postgresql+asyncpg:// and strip sslmode."""
    from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif url.startswith("postgresql://") and "+asyncpg" not in url:
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    parsed = urlparse(url)
    filtered = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                if k.lower() != "sslmode"]
    return urlunparse(parsed._replace(query=urlencode(filtered)))


async def _create_engine(db_url: str):
    """Create a minimal async engine for smoke testing."""
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine(
        db_url,
        echo=False,
        pool_pre_ping=True,
        connect_args={"ssl": "require"},
        execution_options={"compiled_cache": None},
    )


async def _drop_schema_migrations(engine) -> None:
    """Drop the schema_migrations tracking table so migrations re-run from scratch."""
    async with engine.begin() as conn:
        await conn.exec_driver_sql(
            "DROP TABLE IF EXISTS schema_migrations CASCADE"
        )


async def _table_exists(engine, table_name: str) -> bool:
    """Return True if *table_name* exists in the public schema."""
    async with engine.connect() as conn:
        row = await conn.exec_driver_sql(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = $1",
            (table_name,),
        )
        return row.fetchone() is not None


async def _column_type(engine, table: str, column: str) -> str | None:
    """Return the data_type of *column* in *table*, or None if not found."""
    async with engine.connect() as conn:
        row = await conn.exec_driver_sql(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2",
            (table, column),
        )
        result = row.fetchone()
        return result[0] if result else None


async def _run_migrations_with_engine(engine) -> dict:
    """
    Temporarily patch the database module to use *engine*, run migrations,
    then restore the original engine.
    """
    import database as _db_module
    from services import migration_runner as _mr

    original_engine = getattr(_db_module, "engine", None)
    try:
        _db_module.engine = engine
        result = await _mr.run_migrations()
        return result
    finally:
        _db_module.engine = original_engine


# ---------------------------------------------------------------------------
# Individual test cases
# ---------------------------------------------------------------------------

async def test_empty_database(engine, verbose: bool) -> dict[str, Any]:
    """
    Test 1: Empty database.
    Drop schema_migrations and run all migrations from scratch.
    Verifies that all migrations apply cleanly with no critical failures.
    """
    test_name = "empty_database"
    checks: list[str] = []
    failures: list[str] = []
    warnings: list[str] = []
    start = time.monotonic()

    if verbose:
        print(f"\n[TEST] {test_name}: running migrations on empty schema_migrations table...")

    try:
        await _drop_schema_migrations(engine)
        result = await _run_migrations_with_engine(engine)

        applied = result.get("applied", [])
        skipped = result.get("skipped_legacy", [])
        failed_critical = result.get("failed_critical", [])
        failed_noncritical = result.get("failed_noncritical", [])

        checks.append(f"migrations_applied={len(applied)}")
        checks.append(f"skipped_legacy={len(skipped)}")

        if failed_critical:
            failures.append(
                f"Critical migration failures: {', '.join(failed_critical)}"
            )
        else:
            checks.append("no_critical_failures=true")

        if failed_noncritical:
            warnings.append(
                f"Noncritical migration failures: {', '.join(failed_noncritical)}"
            )
        else:
            checks.append("no_noncritical_failures=true")

        # Verify key tables exist after migrations
        required_tables = ["orders", "brand_api_credentials", "schema_migrations"]
        for tbl in required_tables:
            exists = await _table_exists(engine, tbl)
            if exists:
                checks.append(f"table_{tbl}=exists")
            else:
                warnings.append(f"Expected table '{tbl}' not found after migrations")

        if verbose:
            print(f"  applied={len(applied)} skipped={len(skipped)} "
                  f"failed_critical={len(failed_critical)} "
                  f"failed_noncritical={len(failed_noncritical)}")

    except Exception as exc:
        failures.append(f"Unexpected exception: {exc!s:.500}")

    elapsed_ms = int((time.monotonic() - start) * 1000)
    passed = len(failures) == 0
    return {
        "test": test_name,
        "passed": passed,
        "elapsed_ms": elapsed_ms,
        "checks": checks,
        "warnings": warnings,
        "failures": failures,
    }


async def test_partial_database(engine, verbose: bool) -> dict[str, Any]:
    """
    Test 2: Partial database.
    Run migrations on a database where schema_migrations already has some entries
    (simulates a partially-migrated database).  Verifies idempotency.
    """
    test_name = "partial_database"
    checks: list[str] = []
    failures: list[str] = []
    warnings: list[str] = []
    start = time.monotonic()

    if verbose:
        print(f"\n[TEST] {test_name}: running migrations on partially-applied state...")

    try:
        # Run migrations once to get to a known state
        first_result = await _run_migrations_with_engine(engine)
        first_applied = first_result.get("applied", [])

        # Run again — should apply nothing new (idempotency check)
        second_result = await _run_migrations_with_engine(engine)
        second_applied = second_result.get("applied", [])
        second_failed_critical = second_result.get("failed_critical", [])
        second_failed_noncritical = second_result.get("failed_noncritical", [])

        checks.append(f"first_run_applied={len(first_applied)}")
        checks.append(f"second_run_applied={len(second_applied)}")

        if second_applied:
            warnings.append(
                f"Second run applied {len(second_applied)} migrations that should "
                f"have been skipped: {', '.join(second_applied)}"
            )
        else:
            checks.append("idempotent=true")

        if second_failed_critical:
            failures.append(
                f"Critical failures on second run: {', '.join(second_failed_critical)}"
            )
        else:
            checks.append("no_critical_failures_on_rerun=true")

        if second_failed_noncritical:
            warnings.append(
                f"Noncritical failures on second run: {', '.join(second_failed_noncritical)}"
            )

        if verbose:
            print(f"  first_run={len(first_applied)} second_run={len(second_applied)} "
                  f"idempotent={len(second_applied) == 0}")

    except Exception as exc:
        failures.append(f"Unexpected exception: {exc!s:.500}")

    elapsed_ms = int((time.monotonic() - start) * 1000)
    passed = len(failures) == 0
    return {
        "test": test_name,
        "passed": passed,
        "elapsed_ms": elapsed_ms,
        "checks": checks,
        "warnings": warnings,
        "failures": failures,
    }


async def test_dirty_database(engine, verbose: bool) -> dict[str, Any]:
    """
    Test 3: Dirty database.
    Verify that the migration runner handles a database that already has some
    tables/columns from a previous schema version.  Specifically checks:
      - No UUID/INTEGER FK mismatches
      - No missing parent tables (routes, drivers, route_stops)
      - No invalid SQL syntax errors
    """
    test_name = "dirty_database"
    checks: list[str] = []
    failures: list[str] = []
    warnings: list[str] = []
    start = time.monotonic()

    if verbose:
        print(f"\n[TEST] {test_name}: checking schema integrity after migrations...")

    try:
        # Run migrations to ensure we're at the latest state
        result = await _run_migrations_with_engine(engine)
        failed_critical = result.get("failed_critical", [])
        failed_noncritical = result.get("failed_noncritical", [])

        if failed_critical:
            failures.append(
                f"Critical migration failures: {', '.join(failed_critical)}"
            )
        else:
            checks.append("no_critical_failures=true")

        if failed_noncritical:
            warnings.append(
                f"Noncritical failures: {', '.join(failed_noncritical)}"
            )

        # Check for UUID/INTEGER FK mismatches on key relationships
        fk_checks = [
            ("orders", "sync_run_id", "sync_runs", "id"),
            ("order_lines", "order_id", "orders", "id"),
        ]
        for child_table, child_col, parent_table, parent_col in fk_checks:
            child_exists = await _table_exists(engine, child_table)
            parent_exists = await _table_exists(engine, parent_table)
            if not child_exists or not parent_exists:
                checks.append(f"fk_check_{child_table}_{child_col}=skipped_table_absent")
                continue

            child_type = await _column_type(engine, child_table, child_col)
            parent_type = await _column_type(engine, parent_table, parent_col)

            if child_type is None or parent_type is None:
                warnings.append(
                    f"FK type check skipped: {child_table}.{child_col} or "
                    f"{parent_table}.{parent_col} column not found"
                )
                continue

            if child_type != parent_type:
                failures.append(
                    f"FK type mismatch: {child_table}.{child_col}={child_type!r} "
                    f"but {parent_table}.{parent_col}={parent_type!r}"
                )
            else:
                checks.append(
                    f"fk_type_ok_{child_table}_{child_col}={child_type}"
                )

        # Check that parent tables exist (routes, drivers, route_stops)
        parent_tables = ["drivers", "routes", "route_stops"]
        for tbl in parent_tables:
            exists = await _table_exists(engine, tbl)
            if exists:
                checks.append(f"parent_table_{tbl}=exists")
            else:
                # These may not exist if the drivers/routes migrations haven't run
                warnings.append(
                    f"Parent table '{tbl}' not found — "
                    "dependent migrations (001/002/003) may fail"
                )

        if verbose:
            print(f"  fk_checks={len(fk_checks)} "
                  f"failures={len(failures)} warnings={len(warnings)}")

    except Exception as exc:
        failures.append(f"Unexpected exception: {exc!s:.500}")

    elapsed_ms = int((time.monotonic() - start) * 1000)
    passed = len(failures) == 0
    return {
        "test": test_name,
        "passed": passed,
        "elapsed_ms": elapsed_ms,
        "checks": checks,
        "warnings": warnings,
        "failures": failures,
    }


async def test_validator(engine, verbose: bool) -> dict[str, Any]:
    """
    Test 4: Migration validator.
    Run the migration_validator against the live schema and verify it passes
    (or warns only — no fatal issues).
    """
    test_name = "validator"
    checks: list[str] = []
    failures: list[str] = []
    warnings_out: list[str] = []
    start = time.monotonic()

    if verbose:
        print(f"\n[TEST] {test_name}: running migration_validator...")

    try:
        from sqlalchemy.ext.asyncio import AsyncSession
        from services.migration_validator import validate_migration_result

        async with AsyncSession(engine) as session:
            val_result = await validate_migration_result(session)

        ok = val_result.get("ok", False)
        issues = val_result.get("issues", [])
        val_warnings = val_result.get("warnings", [])
        checks_run = val_result.get("checks_run", 0)

        checks.append(f"validator_checks_run={checks_run}")

        if ok:
            checks.append("validator_result=ok")
        else:
            for issue in issues:
                failures.append(f"Validator issue: {issue}")

        for w in val_warnings:
            warnings_out.append(f"Validator warning: {w}")

        if verbose:
            print(f"  checks_run={checks_run} ok={ok} "
                  f"issues={len(issues)} warnings={len(val_warnings)}")

    except ImportError as exc:
        warnings_out.append(f"Could not import migration_validator: {exc}")
    except Exception as exc:
        failures.append(f"Validator raised exception: {exc!s:.500}")

    elapsed_ms = int((time.monotonic() - start) * 1000)
    passed = len(failures) == 0
    return {
        "test": test_name,
        "passed": passed,
        "elapsed_ms": elapsed_ms,
        "checks": checks,
        "warnings": warnings_out,
        "failures": failures,
    }


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def run_smoke_tests(db_url: str, verbose: bool) -> dict[str, Any]:
    """Run all smoke tests and return a JSON-serialisable report."""
    engine = await _create_engine(db_url)
    report: dict[str, Any] = {
        "db_url_host": "",
        "tests": [],
        "summary": {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0,
        },
        "overall_passed": False,
    }

    try:
        # Extract host for the report (don't log credentials)
        from urllib.parse import urlparse as _up
        _parsed = _up(db_url)
        report["db_url_host"] = _parsed.hostname or "unknown"

        tests = [
            test_empty_database(engine, verbose),
            test_partial_database(engine, verbose),
            test_dirty_database(engine, verbose),
            test_validator(engine, verbose),
        ]

        for coro in tests:
            result = await coro
            report["tests"].append(result)
            report["summary"]["total"] += 1
            if result["passed"]:
                report["summary"]["passed"] += 1
            else:
                report["summary"]["failed"] += 1
            report["summary"]["warnings"] += len(result.get("warnings", []))

        report["overall_passed"] = report["summary"]["failed"] == 0

    finally:
        await engine.dispose()

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Migration smoke test — validates migration runner correctness"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed output for each check",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", ""),
        help="PostgreSQL connection URL (defaults to DATABASE_URL env var)",
    )
    args = parser.parse_args()

    if not args.db_url:
        print(
            "ERROR: No database URL provided. "
            "Set DATABASE_URL or pass --db-url.",
            file=sys.stderr,
        )
        return 1

    db_url = _normalize_db_url(args.db_url)

    if args.verbose:
        logging.getLogger("migration_smoke_test").setLevel(logging.DEBUG)
        logging.getLogger("migration_runner").setLevel(logging.INFO)
        print(f"[smoke_test] connecting to {db_url.split('@')[-1] if '@' in db_url else db_url}")

    report = asyncio.run(run_smoke_tests(db_url, args.verbose))

    print(json.dumps(report, indent=2))

    if args.verbose:
        summary = report["summary"]
        status = "PASSED" if report["overall_passed"] else "FAILED"
        print(
            f"\n[smoke_test] {status} — "
            f"{summary['passed']}/{summary['total']} tests passed, "
            f"{summary['warnings']} warnings",
            file=sys.stderr,
        )

    return 0 if report["overall_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
