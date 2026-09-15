#!/usr/bin/env python3
"""
Copy every table in ALL_MODELS from a SQLite file into a Postgres database,
using the shared ORM schema in snowforecast.storage.models (Postgres
migration Phase 2 -- Phase 1 added the schema itself, see PR #7).

Creates the schema on the Postgres side via Base.metadata.create_all, then
copies rows table-by-table (parent tables before children, per
models.ALL_MODELS order, so FK inserts don't fail) in chunks.

SAFETY: dry-run by default -- only reads the SQLite source and reports what
would happen. Nothing is written to Postgres unless --execute is passed.
The SQLite source should be a COPY, not the live NAS file (see --db-path
convention in the other scripts/collect and scripts/backtest tools) --
this script never writes to its source, but reading a multi-GB file while
cron is mid-write elsewhere can still race.

FK CHECK: SQLite doesn't enforce the foreign keys declared in models.py, so
the source file can (and, in practice, does) contain rows that violate them
-- e.g. an atmospheric_daily row for a station_id with no matching stations
row. Postgres enforces these for real, so any such row fails the whole
insert batch it's in. This script checks every declared FK against the
source up front and reports violations before touching Postgres -- fix the
source data (add the missing parent rows) or pass --skip-fk-violations to
drop the offending rows during copy instead.

Re-running --execute against a database that already has rows will fail on
primary-key conflicts (this is a one-shot copy, not an upsert) -- drop and
recreate the target database between attempts.

Usage:
    # dry run: just report source row counts, FK violations, and the target that would be used
    python scripts/storage/migrate_sqlite_to_postgres.py --sqlite-path /path/to/copy.db

    # actually copy, against an explicit target
    python scripts/storage/migrate_sqlite_to_postgres.py \\
        --sqlite-path /path/to/copy.db \\
        --postgres-url postgresql+psycopg2://snowforecast:pw@localhost:5432/snowforecast \\
        --execute

    # same, but drop rows that violate a FK instead of aborting on them
    python scripts/storage/migrate_sqlite_to_postgres.py \\
        --sqlite-path /path/to/copy.db \\
        --postgres-url postgresql+psycopg2://snowforecast:pw@localhost:5432/snowforecast \\
        --execute --skip-fk-violations
"""

import argparse
import os

from sqlalchemy import create_engine, func, select, text

from snowforecast.storage.models import ALL_MODELS, Base

MAX_VIOLATION_GROUPS_SHOWN = 20


def table_count(engine, model) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(model)).scalar_one()


def find_fk_violations(engine) -> list[tuple[str, str, str, object, int]]:
    """For every FK declared in the ORM schema, find child rows whose value
    has no matching parent row. Returns (table, column, references, value, count) tuples."""
    violations = []
    for table in Base.metadata.sorted_tables:
        for fk in table.foreign_keys:
            child_col, parent_col = fk.parent, fk.column
            query = (
                select(child_col, func.count().label("n"))
                .where(child_col.isnot(None))
                .where(~child_col.in_(select(parent_col)))
                .group_by(child_col)
                .order_by(func.count().desc())
            )
            with engine.connect() as conn:
                rows = conn.execute(query).fetchall()
            reference = f"{parent_col.table.name}.{parent_col.name}"
            violations.extend((table.name, child_col.name, reference, value, n) for value, n in rows)
    return violations


def report_fk_violations(violations: list[tuple[str, str, str, object, int]]) -> None:
    print(f"\nFK violations found: {len(violations)} distinct value(s) in the source with no matching parent row:")
    print(f"  {'table':<20} {'column':<16} {'references':<22} {'value':<24} {'rows':>8}")
    for table, column, reference, value, n in violations[:MAX_VIOLATION_GROUPS_SHOWN]:
        print(f"  {table:<20} {column:<16} {reference:<22} {str(value):<24} {n:>8}")
    if len(violations) > MAX_VIOLATION_GROUPS_SHOWN:
        print(f"  ... and {len(violations) - MAX_VIOLATION_GROUPS_SHOWN} more")


def fk_safe_select(table):
    """A SELECT over `table` excluding any row that would violate one of its FKs."""
    stmt = select(table)
    for fk in table.foreign_keys:
        child_col, parent_col = fk.parent, fk.column
        stmt = stmt.where(child_col.is_(None) | child_col.in_(select(parent_col)))
    return stmt


def copy_table(source_engine, dest_engine, model, chunk_size: int, skip_fk: bool) -> int:
    table = model.__table__
    stmt = fk_safe_select(table) if skip_fk else select(table)
    copied = 0
    with source_engine.connect() as src_conn:
        result = src_conn.execution_options(stream_results=True).execute(stmt)
        while True:
            rows = result.fetchmany(chunk_size)
            if not rows:
                break
            with dest_engine.begin() as dest_conn:
                dest_conn.execute(table.insert(), [dict(r._mapping) for r in rows])
            copied += len(rows)
            print(f"    ...{copied} rows copied", end="\r")
    print(f"    {copied} rows copied" + " " * 10)
    return copied


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sqlite-path", required=True, help="Path to the SQLite file to migrate FROM (a copy, not the live NAS file)")
    parser.add_argument("--postgres-url", default=os.environ.get("DATABASE_URL"), help="SQLAlchemy URL to migrate TO (defaults to $DATABASE_URL if set)")
    parser.add_argument("--chunk-size", type=int, default=20000, help="Rows per read/write batch")
    parser.add_argument("--execute", action="store_true", help="Actually create the schema and copy rows (default: dry run, source-only)")
    parser.add_argument("--skip-fk-violations", action="store_true", help="Drop rows that violate a declared FK instead of aborting when any are found")
    args = parser.parse_args()

    if not os.path.exists(args.sqlite_path):
        print(f"SQLite source not found: {args.sqlite_path}")
        return

    source_engine = create_engine(f"sqlite:///{args.sqlite_path}", connect_args={"timeout": 30})

    print(f"Source: sqlite:///{args.sqlite_path}")
    print("Source table counts:")
    source_counts = {}
    for model in ALL_MODELS:
        n = table_count(source_engine, model)
        source_counts[model.__tablename__] = n
        print(f"  {model.__tablename__:<28} {n:>10}")

    violations = find_fk_violations(source_engine)
    if violations:
        report_fk_violations(violations)
    else:
        print("\nFK check: no violations found.")

    if not args.execute:
        print("\nDRY RUN -- no Postgres connection was made. Re-run with --execute and --postgres-url to actually copy.")
        return

    if violations and not args.skip_fk_violations:
        print(
            "\nRefusing to copy: fix the source data (add the missing parent rows) "
            "or re-run with --skip-fk-violations to drop the offending rows instead. Aborting."
        )
        return

    if not args.postgres_url:
        print("\n--postgres-url not given and $DATABASE_URL is not set -- nothing to copy to. Aborting.")
        return

    dest_engine = create_engine(args.postgres_url, pool_pre_ping=True)
    print(f"\nTarget: {dest_engine.url.render_as_string(hide_password=True)}")

    print("\nCreating schema on target (create_all -- no-op for tables that already exist)...")
    Base.metadata.create_all(dest_engine)

    print("\nCopying tables:")
    dest_counts = {}
    for model in ALL_MODELS:
        print(f"  {model.__tablename__}:")
        dest_counts[model.__tablename__] = copy_table(source_engine, dest_engine, model, args.chunk_size, args.skip_fk_violations)

    skipped_by_table: dict[str, int] = {}
    for table, _column, _reference, _value, n in violations:
        skipped_by_table[table] = skipped_by_table.get(table, 0) + n

    print("\n" + "=" * 70)
    print(f"{'table':<24} {'source':>10} {'skipped':>10} {'target':>10}")
    print("-" * 70)
    all_match = True
    for name in source_counts:
        skipped = skipped_by_table.get(name, 0)
        expected = source_counts[name] - skipped
        match = expected == dest_counts[name]
        all_match &= match
        flag = " " if match else "  <-- MISMATCH"
        print(f"{name:<24} {source_counts[name]:>10} {skipped:>10} {dest_counts[name]:>10}{flag}")
    print("=" * 70)
    print("All row counts match (accounting for skipped FK violations)." if all_match else "Row count MISMATCH -- do not treat this as a verified copy.")


if __name__ == "__main__":
    main()
