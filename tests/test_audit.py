import unittest
from unittest.mock import MagicMock
from sqlserver_3nf_audit import (
    DeterminantSelector,
    NormalizationAnalyzer,
    ProposalBuilder,
    ColumnProfile,
    TableProfile,
    KeyCandidate,
    FunctionalDependency,
    CONFIG
)

class TestDeterminantSelector(unittest.TestCase):
    def test_score_column(self):
        profile = TableProfile(
            schema="dbo",
            table="users",
            row_count=100,
            sample_clause="",
            columns=[]
        )
        selector = DeterminantSelector(profile)

        # Test ID column (should have high score)
        col_id = ColumnProfile(
            name="UserID",
            data_type="int",
            nullable=False,
            distinct_count=100,
            null_count=0
        )
        score_id = selector.score_column(col_id)

        # Test some random text column (should have lower score)
        col_desc = ColumnProfile(
            name="Description",
            data_type="varchar(max)",
            nullable=True,
            distinct_count=10,
            null_count=50
        )
        score_desc = selector.score_column(col_desc)

        self.assertGreater(score_id, score_desc)

class TestNormalizationAnalyzer(unittest.TestCase):
    def setUp(self):
        self.profile = TableProfile(
            schema="dbo",
            table="orders",
            row_count=1000,
            sample_clause="",
            columns=[],
            determinant_pool=["OrderID", "CustomerID", "OrderDate"]
        )

    def test_working_key_inference(self):
        # Case 1: No candidates, fallback to pool
        analyzer = NormalizationAnalyzer(self.profile, [], [])
        self.assertEqual(analyzer.working_key(), ("OrderID",))

        # Case 2: Candidate exists
        candidate = KeyCandidate(
            columns=("OrderID",),
            tested_rows=1000,
            duplicate_excess_rows=0,
            dup_pct=0.0,
            null_rows=0,
            null_pct=0.0
        )
        analyzer = NormalizationAnalyzer(self.profile, [candidate], [])
        self.assertEqual(analyzer.working_key(), ("OrderID",))

    def test_analyze_2nf_violation(self):
        # Key: (A, B)
        # FD: A -> C (Partial dependency)
        candidate = KeyCandidate(
            columns=("A", "B"),
            tested_rows=100, duplicate_excess_rows=0, dup_pct=0, null_rows=0, null_pct=0
        )

        fd = FunctionalDependency(
            determinant=("A",),
            dependent="C",
            tested_rows=100, coverage_pct=100, total_groups=10,
            violating_groups=0, violating_groups_pct=0,
            violating_rows=0, violating_rows_pct=0, sample_violations=[]
        )

        analyzer = NormalizationAnalyzer(self.profile, [candidate], [fd])
        result = analyzer.analyze()

        self.assertEqual(len(result["second_nf_issues"]), 1)
        self.assertEqual(result["second_nf_issues"][0]["determinant"], ["A"])
        self.assertEqual(result["second_nf_issues"][0]["dependent"], "C")

    def test_analyze_3nf_violation(self):
        # Key: A
        # FD: B -> C (Transitive dependency, where B is not a key)
        candidate = KeyCandidate(
            columns=("A",),
            tested_rows=100, duplicate_excess_rows=0, dup_pct=0, null_rows=0, null_pct=0
        )

        fd = FunctionalDependency(
            determinant=("B",),
            dependent="C",
            tested_rows=100, coverage_pct=100, total_groups=10,
            violating_groups=0, violating_groups_pct=0,
            violating_rows=0, violating_rows_pct=0, sample_violations=[]
        )

        analyzer = NormalizationAnalyzer(self.profile, [candidate], [fd])
        result = analyzer.analyze()

        self.assertEqual(len(result["third_nf_issues"]), 1)
        self.assertEqual(result["third_nf_issues"][0]["determinant"], ["B"])
        self.assertEqual(result["third_nf_issues"][0]["dependent"], "C")

class TestProposalBuilder(unittest.TestCase):
    def test_build_proposal(self):
        profile = TableProfile(
            schema="dbo",
            table="orders",
            row_count=1000,
            sample_clause="",
            columns=[]
        )
        normalization = {
            "third_nf_issues": [
                {
                    "determinant": ["ZipCode"],
                    "dependent": "City",
                    "violating_rows_pct": 0.0
                }
            ]
        }

        builder = ProposalBuilder(profile, normalization)
        proposals = builder.build()

        self.assertEqual(len(proposals), 1)
        self.assertEqual(proposals[0].determinant, ("ZipCode",))
        self.assertEqual(proposals[0].dependents, ["City"])
        self.assertAlmostEqual(proposals[0].confidence, 1.0)

if __name__ == "__main__":
    unittest.main()
