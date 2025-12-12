"""
SQL Server 3NF audit & normalization proposal tool.

This script is intentionally self contained so it can be dropped into a data
engineering repo and invoked as `python sqlserver_3nf_audit.py` without
additional CLIs. All configuration lives in the CONFIG constant below.

The implementation focuses on explainability and auditability: every metric is
written to disk and most steps are documented with reasoning comments so that
analysts can trace how decisions were made. No DDL statements are produced and
no schema changes are executed.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
CONFIG: Dict[str, Any] = {
    "SOURCES": [
        {
            # Update with a real SQL Server ODBC connection string. The Trusted_Connection
            # example is compatible with on-prem AD; switch to UID/PWD as required.
            "name": "ExampleSQL",
            "sqlalchemy_url": (
                "mssql+pyodbc:///?odbc_connect="
                "Driver={ODBC Driver 17 for SQL Server};"
                "Server=YOUR_SERVER;Database=YOUR_DB;Trusted_Connection=yes;"
            ),
        }
    ],
    "SCOPE": {
        "INCLUDE_SCHEMAS": None,  # regex or None
        "EXCLUDE_SCHEMAS": None,
        "INCLUDE_TABLES": None,
        "EXCLUDE_TABLES": None,
        # Optional explicit allowlist of fully qualified names (schema.table)
        "TABLE_ALLOWLIST": None,
    },
    "OVERRIDES": {
        # Force a working key for a specific table when business knowledge is available
        # (schema.table -> list of columns)
        "FORCE_KEY": {},
        # Force inclusion of otherwise skipped columns (schema.table -> list)
        "FORCE_INCLUDE_COLUMNS": {},
        # Ignore columns entirely for FD/key analysis (schema.table -> list)
        "IGNORE_COLUMNS": {},
    },
    "LIMITS": {
        "MAX_DETERMINANT_SIZE": 3,
        "DETERMINANT_POOL_SIZE": 15,
        "MAX_DEPENDENTS_TESTED": 60,
        "CONFIRM_TOP_N_KEYS": 5,
        "CONFIRM_TOP_N_FDS_PER_TABLE": 50,
    },
    "SAMPLING": {
        # Stage 1: triage using sampling for large tables
        "FULL_SCAN_MAX_ROWS": 2_000_000,
        "SAMPLE_TARGET_ROWS": 200_000,
        "SAMPLE_MIN_PCT": 0.2,
        "SAMPLE_MAX_PCT": 2.0,
    },
    "THRESHOLDS": {
        # Keys
        "KEY_MAX_DUP_ROW_PCT": 0.01,
        "KEY_MAX_NULL_ROW_PCT": 0.0,
        # FDs
        "FD_MAX_VIOLATING_GROUP_PCT": 0.1,
        "FD_MAX_VIOLATING_ROW_PCT": 0.01,
        "FD_MIN_COVERAGE_PCT": 20,
        # Reliability
        "MIN_ROWS_FOR_CONFIDENT_RESULTS": 200,
    },
    "OUTPUT": {
        "BASE_PATH": "output",
    },
    "EXCLUDE_COLUMNS_REGEX": r"(?i)^(created|createdon|created_at|updated|updatedon|updated_at|load_.*|etl_.*|dw_.*|hash_.*|rowversion|timestamp)$",
    "BLOB_TYPES": {
        "xml",
        "image",
        "text",
        "ntext",
        "geography",
        "geometry",
        "hierarchyid",
        "sql_variant",
        "varbinary",
        "varbinary(max)",
        "varchar(max)",
        "nvarchar(max)",
    },
}


# --------------------------------------------------------------------------------------
# Utility helpers
# --------------------------------------------------------------------------------------
def build_sample_clause(total_rows: int) -> str:
    cfg = CONFIG["SAMPLING"]
    if total_rows <= cfg["FULL_SCAN_MAX_ROWS"]:
        return ""  # Full scan is acceptable
    pct = max(cfg["SAMPLE_MIN_PCT"], min(cfg["SAMPLE_TARGET_ROWS"] / total_rows * 100, cfg["SAMPLE_MAX_PCT"]))
    return f"TABLESAMPLE ({pct:.4f} PERCENT)"


def quote_ident(name: str) -> str:
    """Safely quote SQL Server identifiers using bracket escaping.

    Using brackets avoids issues with reserved words and special characters. Any
    embedded closing bracket is escaped by doubling it (`]` -> `]]`).
    """
    escaped = name.replace("]", "]]" )
    return f"[{escaped}]"


def qualifies(scope_regex: Optional[str], value: str) -> bool:
    """Helper to evaluate regex filters while treating None as pass-through."""
    if scope_regex is None:
        return True
    return re.search(scope_regex, value) is not None


# --------------------------------------------------------------------------------------
# Data containers
# --------------------------------------------------------------------------------------
@dataclass
class ColumnProfile:
    name: str
    data_type: str
    nullable: bool
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    null_pct: Optional[float] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    score: float = 0.0


@dataclass
class TableProfile:
    schema: str
    table: str
    row_count: int
    sample_clause: str
    columns: List[ColumnProfile]
    determinant_pool: List[str] = field(default_factory=list)


@dataclass
class KeyCandidate:
    columns: Tuple[str, ...]
    tested_rows: int
    duplicate_excess_rows: int
    dup_pct: float
    null_rows: int
    null_pct: float

    @property
    def is_strong(self) -> bool:
        return self.dup_pct <= CONFIG["THRESHOLDS"]["KEY_MAX_DUP_ROW_PCT"] and self.null_pct <= CONFIG["THRESHOLDS"]["KEY_MAX_NULL_ROW_PCT"]


@dataclass
class FunctionalDependency:
    determinant: Tuple[str, ...]
    dependent: str
    tested_rows: int
    coverage_pct: float
    total_groups: int
    violating_groups: int
    violating_groups_pct: float
    violating_rows: int
    violating_rows_pct: float
    sample_violations: List[Dict[str, Any]]

    @property
    def is_strong(self) -> bool:
        th = CONFIG["THRESHOLDS"]
        return (
            self.coverage_pct >= th["FD_MIN_COVERAGE_PCT"]
            and self.violating_groups_pct <= th["FD_MAX_VIOLATING_GROUP_PCT"]
            and self.violating_rows_pct <= th["FD_MAX_VIOLATING_ROW_PCT"]
        )


@dataclass
class Proposal:
    determinant: Tuple[str, ...]
    dependents: List[str]
    confidence: float
    notes: List[str]


# --------------------------------------------------------------------------------------
# SQL Server client
# --------------------------------------------------------------------------------------
class SqlServerClient:
    """Thin wrapper around SQLAlchemy for SQL Server with guarded execution."""

    def __init__(self, url: str) -> None:
        self.engine: Engine = create_engine(url, fast_executemany=False, future=True)

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Result:
        """Execute a SQL statement using a context-managed connection.

        Parameters are separated from identifiers to avoid injection. Identifiers
        must be bracket-quoted by the caller.
        """
        with self.engine.connect() as conn:
            return conn.execute(text(sql), params or {})

    def fetch_value(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        result = self.execute(sql, params)
        row = result.fetchone()
        return None if row is None else row[0]


# --------------------------------------------------------------------------------------
# Metadata reader
# --------------------------------------------------------------------------------------
class MetadataReader:
    """Retrieves schema and column metadata and respects scope filters."""

    def __init__(self, client: SqlServerClient) -> None:
        self.client = client

    def list_tables(self) -> List[Tuple[str, str]]:
        sql = """
        SELECT s.name AS schema_name, t.name AS table_name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name
        """
        rows = self.client.execute(sql).fetchall()
        return [(r[0], r[1]) for r in rows]

    def list_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        sql = """
        SELECT c.name, typ.name AS data_type, c.is_nullable
        FROM sys.columns c
        JOIN sys.types typ ON c.user_type_id = typ.user_type_id
        WHERE c.object_id = OBJECT_ID(:fqname)
        ORDER BY c.column_id
        """
        fqname = f"{schema}.{table}"
        rows = self.client.execute(sql, {"fqname": fqname}).fetchall()
        return [
            {"name": r[0], "data_type": r[1].lower(), "nullable": bool(r[2])}
            for r in rows
        ]

    def get_rowcount(self, schema: str, table: str) -> int:
        sql = f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)}"
        return int(self.client.fetch_value(sql))


# --------------------------------------------------------------------------------------
# Profiler
# --------------------------------------------------------------------------------------
class Profiler:
    """Profiles columns with a focus on FD/key suitability rather than full stats."""

    def __init__(self, client: SqlServerClient) -> None:
        self.client = client

    def _sample_clause(self, total_rows: int) -> str:
        return build_sample_clause(total_rows)

    def profile_table(self, schema: str, table: str) -> TableProfile:
        total_rows = self.client.get_rowcount(schema, table)
        sample_clause = self._sample_clause(total_rows)

        columns_meta = MetadataReader(self.client).list_columns(schema, table)
        column_profiles: List[ColumnProfile] = []

        for col in columns_meta:
            name = col["name"]
            data_type = col["data_type"]
            nullable = col["nullable"]

            # Skip expensive/blobby types for FD checks, but still record basics.
            is_blob = data_type in CONFIG["BLOB_TYPES"]

            null_count = None
            null_pct = None
            distinct_count = None
            min_value = None
            max_value = None

            # Null counts using sample/full depending on table size.
            sql_null = f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)} {sample_clause} WHERE {quote_ident(name)} IS NULL"
            null_count = int(self.client.fetch_value(sql_null))
            tested_rows = total_rows if not sample_clause else int(self.client.fetch_value(f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)} {sample_clause}"))
            null_pct = (null_count / tested_rows * 100) if tested_rows else 0

            if not is_blob:
                # Prefer APPROX_COUNT_DISTINCT for large sets; fall back to COUNT DISTINCT on sample.
                distinct_sql = f"SELECT APPROX_COUNT_DISTINCT({quote_ident(name)}) FROM {quote_ident(schema)}.{quote_ident(table)} {sample_clause}"
                try:
                    distinct_count = int(self.client.fetch_value(distinct_sql))
                except Exception:
                    fallback_sql = f"SELECT COUNT(DISTINCT {quote_ident(name)}) FROM {quote_ident(schema)}.{quote_ident(table)} {sample_clause}"
                    distinct_count = int(self.client.fetch_value(fallback_sql))

                # Lightweight min/max for numeric/date columns provides context when reviewing violations.
                minmax_sql = f"SELECT MIN({quote_ident(name)}), MAX({quote_ident(name)}) FROM {quote_ident(schema)}.{quote_ident(table)} {sample_clause}"
                try:
                    min_val, max_val = self.client.execute(minmax_sql).fetchone()
                    min_value, max_value = min_val, max_val
                except Exception:
                    # Swallow type errors (e.g., unsupported types) while keeping the audit running.
                    min_value = max_value = None

            column_profiles.append(
                ColumnProfile(
                    name=name,
                    data_type=data_type,
                    nullable=nullable,
                    distinct_count=distinct_count,
                    null_count=null_count,
                    null_pct=null_pct,
                    min_value=min_value,
                    max_value=max_value,
                )
            )

        return TableProfile(
            schema=schema,
            table=table,
            row_count=total_rows,
            sample_clause=sample_clause,
            columns=column_profiles,
        )


# --------------------------------------------------------------------------------------
# Determinant selector
# --------------------------------------------------------------------------------------
class DeterminantSelector:
    """Ranks columns for use as determinants based on stability, uniqueness, and naming."""

    NAME_HINT_RE = re.compile(r"(?i)(id|code|nr|key|number|uuid|guid)")

    def __init__(self, profile: TableProfile) -> None:
        self.profile = profile

    def score_column(self, col: ColumnProfile) -> float:
        # Base score uses non-null ratio and distinct ratio (capped to avoid over-weighting huge domains).
        row_count = max(self.profile.row_count, 1)
        non_null_ratio = 1 - (col.null_count or 0) / row_count
        distinct_ratio = min((col.distinct_count or 0) / row_count, 1.5)

        type_bonus = 0.0
        if col.data_type in {"int", "bigint", "uniqueidentifier", "date", "datetime", "datetime2"}:
            type_bonus = 0.2
        elif col.data_type.startswith("nvarchar") or col.data_type.startswith("varchar"):
            type_bonus = -0.05

        name_bonus = 0.15 if self.NAME_HINT_RE.search(col.name) else 0.0
        blob_penalty = -0.3 if col.data_type in CONFIG["BLOB_TYPES"] else 0.0

        score = non_null_ratio * 0.6 + distinct_ratio * 0.6 + type_bonus + name_bonus + blob_penalty
        return score

    def build_pool(self) -> List[str]:
        for col in self.profile.columns:
            col.score = self.score_column(col)
        sorted_cols = sorted(self.profile.columns, key=lambda c: c.score, reverse=True)
        pool = [c.name for c in sorted_cols if not re.search(CONFIG["EXCLUDE_COLUMNS_REGEX"], c.name)]
        pool = pool[: CONFIG["LIMITS"]["DETERMINANT_POOL_SIZE"]]
        self.profile.determinant_pool = pool
        return pool


# --------------------------------------------------------------------------------------
# Key discovery
# --------------------------------------------------------------------------------------
class KeyFinder:
    """Evaluates uniqueness of column combinations using approximate thresholds."""

    def __init__(self, client: SqlServerClient, profile: TableProfile) -> None:
        self.client = client
        self.profile = profile
        self.sample_clause = profile.sample_clause

    def _combination_stats(self, schema: str, table: str, cols: Sequence[str]) -> KeyCandidate:
        # Ignore rows where any key column is NULL.
        not_null_filter = " AND ".join(f"{quote_ident(c)} IS NOT NULL" for c in cols)
        base_sql = f"FROM {quote_ident(schema)}.{quote_ident(table)} {self.sample_clause} WHERE {not_null_filter}"
        count_sql = f"SELECT COUNT(*) {base_sql}"
        tested_rows = int(self.client.fetch_value(count_sql))

        dup_sql = (
            f"SELECT SUM(cnt - 1) AS dup_rows FROM ("
            f"SELECT COUNT(*) AS cnt {base_sql} GROUP BY {', '.join(quote_ident(c) for c in cols)}"  # noqa: E501
            ") d"
        )
        duplicate_excess_rows = int(self.client.fetch_value(dup_sql) or 0)

        null_sql = (
            f"SELECT COUNT(*) FROM {quote_ident(schema)}.{quote_ident(table)} {self.sample_clause} WHERE NOT ({not_null_filter})"
        )
        null_rows = int(self.client.fetch_value(null_sql))

        dup_pct = (duplicate_excess_rows / tested_rows) if tested_rows else 1.0
        null_pct = (null_rows / (tested_rows + null_rows)) if (tested_rows + null_rows) else 0.0

        return KeyCandidate(columns=tuple(cols), tested_rows=tested_rows, duplicate_excess_rows=duplicate_excess_rows, dup_pct=dup_pct, null_rows=null_rows, null_pct=null_pct)

    def find_candidates(self) -> List[KeyCandidate]:
        candidates: List[KeyCandidate] = []
        schema, table = self.profile.schema, self.profile.table
        pool = self.profile.determinant_pool
        for r in range(1, CONFIG["LIMITS"]["MAX_DETERMINANT_SIZE"] + 1):
            for cols in combinations(pool, r):
                try:
                    candidate = self._combination_stats(schema, table, cols)
                    candidates.append(candidate)
                except Exception as exc:
                    print(f"[WARN] Failed key test for {schema}.{table} columns {cols}: {exc}")
        candidates.sort(key=lambda k: (k.dup_pct, k.null_pct, len(k.columns)))
        return candidates


# --------------------------------------------------------------------------------------
# FD discovery
# --------------------------------------------------------------------------------------
class FDDiscoverer:
    """Discovers functional dependencies up to determinant size 3 with evidence."""

    def __init__(self, client: SqlServerClient, profile: TableProfile) -> None:
        self.client = client
        self.profile = profile
        self.sample_clause = profile.sample_clause

    def _dependent_candidates(self, determinant: Sequence[str]) -> List[str]:
        # Dependents exclude determinant columns and blob/excluded columns.
        excluded = set(determinant)
        dependents = []
        for col in self.profile.columns:
            if col.name in excluded:
                continue
            if col.data_type in CONFIG["BLOB_TYPES"] and col.name not in CONFIG["OVERRIDES"]["FORCE_INCLUDE_COLUMNS"].get(f"{self.profile.schema}.{self.profile.table}", []):
                continue
            if re.search(CONFIG["EXCLUDE_COLUMNS_REGEX"], col.name):
                continue
            dependents.append(col.name)
        return dependents[: CONFIG["LIMITS"]["MAX_DEPENDENTS_TESTED"]]

    def _fd_stats(self, determinant: Sequence[str], dependent: str) -> FunctionalDependency:
        schema, table = self.profile.schema, self.profile.table
        not_null_filter = " AND ".join(f"{quote_ident(c)} IS NOT NULL" for c in list(determinant) + [dependent])
        base_sql = f"FROM {quote_ident(schema)}.{quote_ident(table)} {self.sample_clause} WHERE {not_null_filter}"

        tested_rows = int(self.client.fetch_value(f"SELECT COUNT(*) {base_sql}"))
        total_rows = self.profile.row_count
        coverage_pct = (tested_rows / total_rows * 100) if total_rows else 0

        # For FD X -> Y, groups where COUNT(DISTINCT Y) > 1 are violating.
        agg_sql = (
            f"SELECT COUNT(*) AS total_groups,"
            f" SUM(CASE WHEN cnty > 1 THEN 1 ELSE 0 END) AS violating_groups,"
            f" SUM(CASE WHEN cnty > 1 THEN cnt_group ELSE 0 END) AS violating_rows"
            f" FROM ("
            f"SELECT COUNT(*) AS cnt_group, COUNT(DISTINCT {quote_ident(dependent)}) AS cnty"
            f" {base_sql} GROUP BY {', '.join(quote_ident(c) for c in determinant)}"
            ") s"
        )
        total_groups, violating_groups, violating_rows = self.client.execute(agg_sql).fetchone()
        violating_groups_pct = (violating_groups / total_groups * 100) if total_groups else 0
        violating_rows_pct = (violating_rows / tested_rows * 100) if tested_rows else 0

        # Collect a small sample of violating determinant values for evidence.
        sample_sql = (
            f"WITH src AS ("
            f" SELECT {', '.join(quote_ident(c) for c in determinant)}, {quote_ident(dependent)}"
            f" FROM {quote_ident(schema)}.{quote_ident(table)} {self.sample_clause}"
            f" WHERE {not_null_filter}"
            f")"
            f" SELECT TOP (5) {', '.join(f'g.{quote_ident(c)}' for c in determinant)},"
            f" STUFF(("
            f"    SELECT DISTINCT ', ' + CAST(s.{quote_ident(dependent)} AS NVARCHAR(4000))"
            f"    FROM src s"
            f"    WHERE " + " AND ".join(f"s.{quote_ident(c)} = g.{quote_ident(c)}" for c in determinant) +
            f"    ORDER BY ', ' + CAST(s.{quote_ident(dependent)} AS NVARCHAR(4000))"
            f"    FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')"
            f", 1, 2, '') AS values_seen"
            f" FROM ("
            f"    SELECT {', '.join(quote_ident(c) for c in determinant)},"
            f"           COUNT(DISTINCT {quote_ident(dependent)}) AS dep_count"
            f"    FROM src"
            f"    GROUP BY {', '.join(quote_ident(c) for c in determinant)}"
            f" ) g"
            f" WHERE g.dep_count > 1"
        )
        sample_rows = self.client.execute(sample_sql).fetchall()
        sample = []
        for row in sample_rows:
            entry = {col: row[idx] for idx, col in enumerate(determinant)}
            entry[dependent] = row[-1]
            sample.append(entry)

        return FunctionalDependency(
            determinant=tuple(determinant),
            dependent=dependent,
            tested_rows=tested_rows,
            coverage_pct=coverage_pct,
            total_groups=total_groups,
            violating_groups=violating_groups,
            violating_groups_pct=violating_groups_pct,
            violating_rows=violating_rows,
            violating_rows_pct=violating_rows_pct,
            sample_violations=sample,
        )

    def discover(self) -> List[FunctionalDependency]:
        fds: List[FunctionalDependency] = []
        pool = self.profile.determinant_pool
        max_size = CONFIG["LIMITS"]["MAX_DETERMINANT_SIZE"]
        for r in range(1, max_size + 1):
            for determinant in combinations(pool, r):
                dependents = self._dependent_candidates(determinant)
                for dep in dependents:
                    try:
                        fd = self._fd_stats(determinant, dep)
                        fds.append(fd)
                    except Exception as exc:
                        print(f"[WARN] FD test failed for {self.profile.schema}.{self.profile.table} {determinant}->{dep}: {exc}")
        # Apply minimization: drop supersets when subset already strong.
        return self._minimize(fds)

    def _minimize(self, fds: List[FunctionalDependency]) -> List[FunctionalDependency]:
        # Sort deterministically for stable output.
        fds_sorted = sorted(fds, key=lambda f: (len(f.determinant), f.determinant, f.dependent))
        accepted: List[FunctionalDependency] = []
        for fd in fds_sorted:
            if not fd.is_strong:
                continue
            redundant = False
            for acc in accepted:
                if set(acc.determinant).issubset(fd.determinant) and acc.dependent == fd.dependent:
                    redundant = True
                    break
            if not redundant:
                accepted.append(fd)
        return accepted


# --------------------------------------------------------------------------------------
# Normalization analysis (2NF/3NF)
# --------------------------------------------------------------------------------------
class NormalizationAnalyzer:
    """Derives 2NF/3NF issues based on keys and discovered FDs."""

    def __init__(self, profile: TableProfile, keys: List[KeyCandidate], fds: List[FunctionalDependency]) -> None:
        self.profile = profile
        self.keys = keys
        self.fds = fds

    def working_key(self) -> Tuple[str, ...]:
        fq = f"{self.profile.schema}.{self.profile.table}"
        forced = CONFIG["OVERRIDES"]["FORCE_KEY"].get(fq)
        if forced:
            return tuple(forced)
        if self.keys:
            return self.keys[0].columns
        # Fallback to best determinant heuristic if no candidates exist.
        return tuple(self.profile.determinant_pool[:1]) if self.profile.determinant_pool else tuple()

    def analyze(self) -> Dict[str, Any]:
        key_cols = set(self.working_key())
        prime_cols = key_cols

        second_nf = []
        third_nf = []

        for fd in self.fds:
            det_set = set(fd.determinant)
            if not key_cols:
                continue
            # 2NF: determinant is a proper subset of composite key determining non-prime
            if len(key_cols) > 1 and det_set.issubset(key_cols) and det_set != key_cols and fd.dependent not in prime_cols:
                second_nf.append(fd)
            # 3NF: determinant is not a superkey and dependent is non-prime
            if not det_set.issuperset(key_cols) and fd.dependent not in prime_cols:
                third_nf.append(fd)

        return {
            "working_key": list(self.working_key()),
            "prime_columns": list(prime_cols),
            "second_nf_issues": [self._fd_summary(fd) for fd in second_nf],
            "third_nf_issues": [self._fd_summary(fd) for fd in third_nf],
        }

    @staticmethod
    def _fd_summary(fd: FunctionalDependency) -> Dict[str, Any]:
        return {
            "determinant": list(fd.determinant),
            "dependent": fd.dependent,
            "coverage_pct": fd.coverage_pct,
            "violating_groups_pct": fd.violating_groups_pct,
            "violating_rows_pct": fd.violating_rows_pct,
        }


# --------------------------------------------------------------------------------------
# Proposal builder
# --------------------------------------------------------------------------------------
class ProposalBuilder:
    """Generates human-reviewable decomposition proposals (no DDL)."""

    def __init__(self, profile: TableProfile, normalization: Dict[str, Any]) -> None:
        self.profile = profile
        self.normalization = normalization

    def build(self) -> List[Proposal]:
        proposals: List[Proposal] = []
        for issue in self.normalization.get("third_nf_issues", []):
            determinant = tuple(issue["determinant"])
            dependent = issue["dependent"]
            confidence = max(0.1, 1 - issue["violating_rows_pct"])
            notes = [
                "Review semantics and ensure determinant uniquely identifies dependent attributes.",
                "Validate coverage and row counts before applying any schema change.",
            ]
            proposals.append(Proposal(determinant=determinant, dependents=[dependent], confidence=confidence, notes=notes))
        return proposals


# --------------------------------------------------------------------------------------
# Artifact writer
# --------------------------------------------------------------------------------------
class ArtifactWriter:
    """Handles filesystem output for both machine-readable and human-readable artifacts."""

    def __init__(self, base_path: Path) -> None:
        self.base_path = base_path
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.manifest: Dict[str, Any] = {"tables": []}
        self.summary_rows: List[List[Any]] = []

    def table_folder(self, source: str, schema: str, table: str) -> Path:
        return self.base_path / f"source_{source}" / f"{schema}.{table}"

    def write_json(self, path: Path, obj: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2, default=str))

    def append_manifest(self, entry: Dict[str, Any]) -> None:
        self.manifest["tables"].append(entry)

    def finalize(self) -> None:
        (self.base_path / "manifest.json").write_text(json.dumps(self.manifest, indent=2, default=str))
        with (self.base_path / "summary.csv").open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["source", "schema", "table", "row_count", "working_key", "accepted_fds"])
            for row in self.summary_rows:
                writer.writerow(row)

    def write_report(self, path: Path, context: Dict[str, Any]) -> None:
        lines = [
            f"# 3NF Audit Report: {context['schema']}.{context['table']}",
            "",
            "## Table Profile",
            f"- Row count: {context['profile'].row_count}",
            "- Determinant pool: " + ", ".join(context["profile"].determinant_pool),
            "",
            "## Key Candidates",
        ]
        for kc in context["keys"][:5]:
            lines.append(
                f"- {kc.columns}: dup_pct={kc.dup_pct:.4%}, null_pct={kc.null_pct:.4%}, tested_rows={kc.tested_rows}"
            )
        lines.append("")
        lines.append("## Accepted Functional Dependencies")
        for fd in context["fds"]:
            lines.append(
                f"- {fd.determinant} -> {fd.dependent} | coverage={fd.coverage_pct:.2f}% | viol_groups={fd.violating_groups_pct:.2f}% | viol_rows={fd.violating_rows_pct:.2f}%"
            )
        lines.append("")
        lines.append("## Normalization Findings")
        norm = context["normalization"]
        lines.append(f"- Working key: {norm['working_key']}")
        lines.append(f"- 2NF issues: {len(norm['second_nf_issues'])}")
        lines.append(f"- 3NF issues: {len(norm['third_nf_issues'])}")
        lines.append("")
        lines.append("## Decomposition Proposals")
        if context["proposals"]:
            for p in context["proposals"]:
                lines.append(
                    f"- New table T_{'_'.join(p.determinant)} with PK {p.determinant}; move {', '.join(p.dependents)} (confidence {p.confidence:.2f})"
                )
                for note in p.notes:
                    lines.append(f"  - Note: {note}")
        else:
            lines.append("- No proposals. Table appears 3NF-compliant under tested constraints.")

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines))


# --------------------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------------------
class Runner:
    """Orchestrates the entire audit across configured sources."""

    def __init__(self) -> None:
        ts = datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")
        self.output_root = Path(CONFIG["OUTPUT"]["BASE_PATH"]) / ts
        self.writer = ArtifactWriter(self.output_root)

    def run(self) -> None:
        for source in CONFIG["SOURCES"]:
            print(f"[INFO] Connecting to source {source['name']}")
            client = SqlServerClient(source["sqlalchemy_url"])
            metadata = MetadataReader(client)
            tables = metadata.list_tables()
            for schema, table in tables:
                if not self._in_scope(schema, table):
                    continue
                fq = f"{schema}.{table}"
                print(f"[INFO] Auditing {fq}")
                try:
                    profile = Profiler(client).profile_table(schema, table)
                    DeterminantSelector(profile).build_pool()
                    key_candidates = KeyFinder(client, profile).find_candidates()
                    fds = FDDiscoverer(client, profile).discover()
                    normalization = NormalizationAnalyzer(profile, key_candidates, fds).analyze()
                    proposals = ProposalBuilder(profile, normalization).build()

                    table_folder = self.writer.table_folder(source["name"], schema, table)
                    self.writer.write_json(table_folder / "profile.json", self._profile_to_dict(profile))
                    self.writer.write_json(table_folder / "key_candidates.json", [self._key_to_dict(k) for k in key_candidates])
                    self.writer.write_json(table_folder / "fds.json", [self._fd_to_dict(fd) for fd in fds])
                    self.writer.write_json(table_folder / "proposals.json", [self._proposal_to_dict(p) for p in proposals])
                    self.writer.write_report(
                        table_folder / "report.md",
                        {
                            "schema": schema,
                            "table": table,
                            "profile": profile,
                            "keys": key_candidates,
                            "fds": fds,
                            "normalization": normalization,
                            "proposals": proposals,
                        },
                    )
                    self.writer.append_manifest(
                        {
                            "source": source["name"],
                            "schema": schema,
                            "table": table,
                            "row_count": profile.row_count,
                        }
                    )
                    self.writer.summary_rows.append(
                        [source["name"], schema, table, profile.row_count, normalization.get("working_key"), len(fds)]
                    )
                except Exception as exc:
                    print(f"[ERROR] Failed auditing {fq}: {exc}")
                    self.writer.append_manifest(
                        {"source": source["name"], "schema": schema, "table": table, "error": str(exc)}
                    )
        self.writer.finalize()
        print(f"[INFO] Run complete. Artifacts at {self.output_root}")

    def _in_scope(self, schema: str, table: str) -> bool:
        scope = CONFIG["SCOPE"]
        fq = f"{schema}.{table}"
        if scope.get("TABLE_ALLOWLIST") and fq not in scope["TABLE_ALLOWLIST"]:
            return False
        if not qualifies(scope.get("INCLUDE_SCHEMAS"), schema):
            return False
        if not qualifies(scope.get("INCLUDE_TABLES"), table):
            return False
        if scope.get("EXCLUDE_SCHEMAS") and re.search(scope["EXCLUDE_SCHEMAS"], schema):
            return False
        if scope.get("EXCLUDE_TABLES") and re.search(scope["EXCLUDE_TABLES"], table):
            return False
        return True

    @staticmethod
    def _profile_to_dict(profile: TableProfile) -> Dict[str, Any]:
        return {
            "schema": profile.schema,
            "table": profile.table,
            "row_count": profile.row_count,
            "sample_clause": profile.sample_clause,
            "determinant_pool": profile.determinant_pool,
            "columns": [vars(c) for c in profile.columns],
        }

    @staticmethod
    def _key_to_dict(k: KeyCandidate) -> Dict[str, Any]:
        return {
            "columns": list(k.columns),
            "tested_rows": k.tested_rows,
            "duplicate_excess_rows": k.duplicate_excess_rows,
            "dup_pct": k.dup_pct,
            "null_rows": k.null_rows,
            "null_pct": k.null_pct,
            "is_strong": k.is_strong,
        }

    @staticmethod
    def _fd_to_dict(fd: FunctionalDependency) -> Dict[str, Any]:
        return {
            "determinant": list(fd.determinant),
            "dependent": fd.dependent,
            "tested_rows": fd.tested_rows,
            "coverage_pct": fd.coverage_pct,
            "total_groups": fd.total_groups,
            "violating_groups": fd.violating_groups,
            "violating_groups_pct": fd.violating_groups_pct,
            "violating_rows": fd.violating_rows,
            "violating_rows_pct": fd.violating_rows_pct,
            "sample_violations": fd.sample_violations,
            "is_strong": fd.is_strong,
        }

    @staticmethod
    def _proposal_to_dict(p: Proposal) -> Dict[str, Any]:
        return {
            "determinant": list(p.determinant),
            "dependents": p.dependents,
            "confidence": p.confidence,
            "notes": p.notes,
        }


def _configure_test_source() -> None:
    """Point CONFIG to the dockerized SQL Server test fixture when requested."""

    password = os.getenv("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")
    test_url = f"mssql+pymssql://sa:{password}@localhost:1433/OperationsDemo"
    CONFIG["SOURCES"] = [{"name": "DockerSQL", "sqlalchemy_url": test_url}]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit SQL Server tables for 3NF readiness.")
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["test"],
        help="Use 'test' to run against the dockerized OperationsDemo database.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.mode == "test":
        _configure_test_source()
    Runner().run()
