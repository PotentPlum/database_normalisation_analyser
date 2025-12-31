
import pytest
from dataclasses import dataclass
from typing import List
from sqlserver_3nf_audit import ProposalBuilder, TableProfile, ColumnProfile

@dataclass
class MockTableProfile:
    schema: str = "dbo"
    table: str = "TestTable"
    row_count: int = 100
    sample_clause: str = ""
    columns: List[ColumnProfile] = None
    determinant_pool: List[str] = None

def test_proposal_builder_2nf():
    profile = MockTableProfile()
    normalization = {
        "working_key": ["A", "B"],
        "prime_columns": ["A", "B"],
        "second_nf_issues": [
            {
                "determinant": ["A"],
                "dependent": "C",
                "coverage_pct": 100.0,
                "violating_groups_pct": 0.0,
                "violating_rows_pct": 0.0
            }
        ],
        "third_nf_issues": []
    }

    builder = ProposalBuilder(profile, normalization)
    proposals = builder.build()

    assert len(proposals) == 1
    p = proposals[0]
    assert p.type == "2NF"
    assert p.determinant == ("A",)
    assert p.dependents == ["C"]

def test_proposal_builder_3nf():
    profile = MockTableProfile()
    normalization = {
        "working_key": ["ID"],
        "prime_columns": ["ID"],
        "second_nf_issues": [],
        "third_nf_issues": [
            {
                "determinant": ["ZipCode"],
                "dependent": "City",
                "coverage_pct": 100.0,
                "violating_groups_pct": 0.0,
                "violating_rows_pct": 0.0
            }
        ]
    }

    builder = ProposalBuilder(profile, normalization)
    proposals = builder.build()

    assert len(proposals) == 1
    p = proposals[0]
    assert p.type == "3NF"
    assert p.determinant == ("ZipCode",)
    assert p.dependents == ["City"]
