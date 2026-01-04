"""
postgresql implementation of the databasebenchmark class
optimized for network efficiency using server-side aggregation
"""

import psycopg2
from typing import List
from benchmark import DatabaseBenchmark


class PostgresBenchmark(DatabaseBenchmark):

    def __init__(
        self,
        host="localhost",
        port="5432",
        dbname="genomics",
        user="postgres",
        password="postgres_password",
    ):
        super().__init__("PostgreSQL")
        self.conn_params = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }
        self.conn = None
        self.cur = None

    def connect(self):
        self.conn = psycopg2.connect(**self.conn_params)
        self.cur = self.conn.cursor()

    def disconnect(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def _execute_count(self, query: str, params: tuple) -> int:
        """helper to execute count(*) queries to minimize network traffic"""
        self.cur.execute(query, params)
        return self.cur.fetchone()[0]

    # q1: lookup by variant id (composite key)
    def q1_variant_by_id(
        self, chromosome: str, position: int, ref: str, alt: str
    ) -> int:
        # for single row lookups, fetching is fine, but count is safer for consistency
        return self._execute_count(
            """
            SELECT COUNT(*) FROM variants 
            WHERE chrom = %s AND pos = %s AND ref = %s AND alt = %s
        """,
            (chromosome, position, ref, alt),
        )

    # q2: lookup by genomic position (exact)
    def q2_variant_by_position(self, chromosome: str, position: int) -> int:
        return self._execute_count(
            "SELECT COUNT(*) FROM variants WHERE chrom = %s AND pos = %s",
            (chromosome, position),
        )

    # q3: finding variant by external existing variation id
    def q3_variant_by_rsid(self, rsid: str) -> int:
        return self._execute_count(
            "SELECT COUNT(*) FROM variants WHERE rsid = %s", (rsid,)
        )

    # q4: all variants in a gene (by symbol)
    def q4_variants_in_gene_all(self, gene: str) -> int:
        return self._execute_count(
            """
            SELECT COUNT(*) FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.gene_symbol = %s
        """,
            (gene,),
        )

    # q5: all variants in a gene (by symbol) return first 100
    def q5_variants_in_gene_limited(self, gene: str, limit: int = 100) -> int:
        # limit queries must fetch data to actually test the limit clause performance
        # fetching 100 ids is negligible network overhead
        self.cur.execute(
            """
            SELECT v.id FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.gene_symbol = %s
            LIMIT %s
        """,
            (gene, limit),
        )
        return self.cur.rowcount

    # q6: all variants in a genomic range - small range
    def q6_range_small(self, chromosome: str, start: int, end: int) -> int:
        return self._execute_count(
            "SELECT COUNT(*) FROM variants WHERE chrom = %s AND pos >= %s AND pos <= %s",
            (chromosome, start, end),
        )

    # q7: all variants in a genomic range - medium range
    def q7_range_medium(self, chromosome: str, start: int, end: int) -> int:
        return self.q6_range_small(chromosome, start, end)

    # q8: all variants in a genomic range - large range
    def q8_range_large(self, chromosome: str, start: int, end: int) -> int:
        return self.q6_range_small(chromosome, start, end)

    # q9: all variants in a transcript
    def q9_transcript_variants(self, transcript: str) -> int:
        return self._execute_count(
            """
            SELECT COUNT(*) FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.transcript_id = %s
        """,
            (transcript,),
        )

    # q10: coding variants in a transcript
    def q10_coding_variants(self, consequences: List[str], gene: str = None) -> int:
        sql = """
            SELECT COUNT(DISTINCT v.id) FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.consequence = ANY(%s)
        """
        params = [consequences]
        if gene:
            sql += " AND a.gene_symbol = %s"
            params.append(gene)
        return self._execute_count(sql, tuple(params))

    # q11: all variants in a gene with quality filter
    def q11_gene_with_quality(self, gene: str, min_quality: float) -> int:
        return self._execute_count(
            """
            SELECT COUNT(*) FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.gene_symbol = %s AND v.qual > %s AND v.filter = 'PASS'
        """,
            (gene, min_quality),
        )

    # q12: all variants in a gene - only rare/novel variants
    def q12_gene_rare(self, gene: str, max_af: float = 0.01) -> int:
        return self._execute_count(
            """
            SELECT COUNT(*) FROM variants v
            JOIN annotations a ON v.id = a.variant_id
            WHERE a.gene_symbol = %s AND v.af < %s
        """,
            (gene, max_af),
        )
