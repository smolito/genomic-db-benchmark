"""
Base classes for database benchmarking to avoid circular imports.
"""

from datetime import datetime
from typing import List, Dict, Any, Callable


class BenchmarkResult:
    """Store results from a single benchmark run"""

    def __init__(
        self,
        database: str,
        query_type: str,
        response_time: float,
        rows_returned: int,
        cache_state: str,
    ):
        self.database = database
        self.query_type = query_type
        self.response_time = response_time  # milliseconds
        self.rows_returned = rows_returned
        self.cache_state = cache_state
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "query_type": self.query_type,
            "response_time_ms": self.response_time,
            "rows_returned": self.rows_returned,
            "cache_state": self.cache_state,
            "timestamp": self.timestamp,
        }


class DatabaseBenchmark:
    """Base class for database benchmarks"""

    def __init__(self, name: str):
        self.name = name
        self.client = None

    def connect(self):
        """Establish database connection"""
        raise NotImplementedError

    def disconnect(self):
        """Close database connection"""
        raise NotImplementedError

    # PARENT BLUEPRINT QUERIES

    # Q1: Lookup by Variant ID (Composite Key)
    def q1_variant_by_id(
        self, chromosome: str, position: int, ref: str, alt: str
    ) -> int:
        """Q1: Find variant by composite key (chr:pos:ref:alt), e.g., chr22:10736093:A:T"""
        raise NotImplementedError

    # Q2: Lookup by Genomic Position (Exact)
    def q2_variant_by_position(self, chromosome: str, position: int) -> int:
        """Q2: Find variant by exact genomic position, e.g., chr22:10736093"""
        raise NotImplementedError

    # Q3: Finding variant by external existing variation ID
    def q3_variant_by_rsid(self, rsid: str) -> int:
        """Q3: Find variant by rsID, e.g., rs1394819064"""
        raise NotImplementedError

    # Q4: All Variants in a Gene (by Symbol)
    def q4_variants_in_gene_all(self, gene: str) -> int:
        """Q4: All variants in a gene (by symbol), e.g., all BRCA1 variants"""
        raise NotImplementedError

    # Q5: All Variants in a Gene (by Symbol) return first 100
    def q5_variants_in_gene_limited(self, gene: str, limit: int = 100) -> int:
        """Q5: First N variants in a gene (by symbol), e.g., first 100 BRCA1 variants"""
        raise NotImplementedError

    # Q6: All variants in a genomic range - small range
    def q6_range_small(self, chromosome: str, start: int, end: int) -> int:
        """Q6: Variants in small genomic range (~4kb), e.g., chr22:10736093-10739993"""
        raise NotImplementedError

    # Q7: All variants in a genomic range - medium range
    def q7_range_medium(self, chromosome: str, start: int, end: int) -> int:
        """Q7: Variants in medium genomic range (~100kb)"""
        raise NotImplementedError

    # Q8: All variants in a genomic range - large range
    def q8_range_large(self, chromosome: str, start: int, end: int) -> int:
        """Q8: Variants in large genomic range (~10Mb)"""
        raise NotImplementedError

    # Q9: All variants in a Transcript
    def q9_transcript_variants(self, transcript: str) -> int:
        """Q9: All variants in a transcript, e.g., ENST00000615943"""
        raise NotImplementedError

    # Q10: Coding variants in a Transcript
    def q10_coding_variants(self, consequences: List[str], gene: str = None) -> int:
        """Q10: Coding variants with specific consequences, optionally filtered by gene"""
        raise NotImplementedError

    # Q11: All variants in a gene with quality filter
    def q11_gene_with_quality(self, gene: str, min_quality: float) -> int:
        """Q11: Variants in gene with quality filter (Quality > 30, PASS filter)"""
        raise NotImplementedError

    # Q12: All variants in a gene - only rare/novel variants
    def q12_gene_rare(self, gene: str, max_af: float = 0.01) -> int:
        """Q12: Rare/novel variants in gene (gnomAD AF < 0.01)"""
        raise NotImplementedError
