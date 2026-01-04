#!/usr/bin/env python3
"""
Database Performance Benchmarking Tool for Genomic Variants

This script benchmarks various databases for storing and querying genomic variants.
It measures query response times, throughput, and other performance metrics.

Usage:
    python benchmark.py --iterations 100 --output results.csv
"""

import time
import json
import csv
import statistics
import argparse
from typing import List, Dict, Any, Callable
from base import DatabaseBenchmark, BenchmarkResult  # new import
from postgres_benchmark import PostgresBenchmark


def time_query(func: Callable, *args) -> tuple:
    """Time a query execution and return (response_time_ms, row_count)"""
    start = time.perf_counter()
    row_count = func(*args)
    end = time.perf_counter()
    response_time_ms = (end - start) * 1000
    return response_time_ms, row_count


def run_benchmark(
    benchmark: DatabaseBenchmark,
    query_name: str,
    query_func: Callable,
    args: tuple,
    iterations: int,
    cache_state: str,
) -> List[BenchmarkResult]:
    """Run a single benchmark query multiple times"""
    results = []

    for i in range(iterations):
        try:
            response_time, row_count = time_query(query_func, *args)
            result = BenchmarkResult(
                database=benchmark.name,
                query_type=query_name,
                response_time=response_time,
                rows_returned=row_count,
                cache_state=cache_state,
            )
            results.append(result)
        except Exception as e:
            print(f"Error in {benchmark.name} - {query_name} iteration {i+1}: {e}")
            # Continue with remaining iterations

    return results


def print_statistics(results: List[BenchmarkResult], query_name: str, db_name: str):
    """Print statistical summary of benchmark results"""
    if not results:
        print(f"\nNo results for {db_name} - {query_name}")
        return

    times = [r.response_time for r in results]

    print(f"\n{db_name} - {query_name}:")
    print(f"  Iterations: {len(times)}")
    print(f"  Mean: {statistics.mean(times):.2f} ms")
    print(f"  Median: {statistics.median(times):.2f} ms")
    print(f"  Min: {min(times):.2f} ms")
    print(f"  Max: {max(times):.2f} ms")
    print(
        f"  Std Dev: {statistics.stdev(times):.2f} ms"
        if len(times) > 1
        else "  Std Dev: N/A"
    )

    # Calculate percentiles
    sorted_times = sorted(times)
    p95_idx = int(len(sorted_times) * 0.95)
    p99_idx = int(len(sorted_times) * 0.99)
    print(f"  P95: {sorted_times[p95_idx]:.2f} ms")
    print(f"  P99: {sorted_times[p99_idx]:.2f} ms")


def save_results(all_results: List[BenchmarkResult], output_file: str):
    """Save results to CSV file"""
    with open(output_file, "w", newline="") as f:
        if all_results:
            writer = csv.DictWriter(f, fieldnames=all_results[0].to_dict().keys())
            writer.writeheader()
            for result in all_results:
                writer.writerow(result.to_dict())
        else:
            print("No results to save")

    print(f"\nResults saved to {output_file}")


def load_query_config(config_file: str = "query_config.json") -> dict:
    """Load query configuration from JSON file"""
    try:
        with open(config_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(
            f"Query configuration file '{config_file}' not found. Using default queries."
        )
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing query configuration: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark database performance for genomic variants"
    )
    parser.add_argument(
        "--iterations", type=int, default=100, help="Number of iterations per query"
    )
    parser.add_argument(
        "--warmup", type=int, default=10, help="Number of warmup iterations"
    )
    parser.add_argument(
        "--output", type=str, default="benchmark_results.csv", help="Output CSV file"
    )
    parser.add_argument(
        "--databases",
        type=str,
        default="all",
        help='Databases to test (comma-separated or "all")',
    )
    parser.add_argument(
        "--queries",
        type=str,
        default="all",
        help='Queries to run (comma-separated like "Q1,Q2,Q3" or "all")',
    )
    parser.add_argument(
        "--config",
        type=str,
        default="query_config.json",
        help="Path to query configuration JSON file",
    )

    args = parser.parse_args()

    # Initialize databases
    databases = []
    databases.append(PostgresBenchmark())

    if not databases:
        print("No database benchmark implementations available.")
        print(
            "Please implement a DatabaseBenchmark subclass and add it to the databases list."
        )
        return

    # Load query configuration
    config = load_query_config(args.config)

    # Build queries from configuration
    queries = []
    if config and "queries" in config:
        for query_id, query_def in config["queries"].items():
            method_name = query_def["method"]
            params = query_def["params"]
            description = query_def["description"]

            # Create lambda function that calls the method with params
            query_func = lambda db, m=method_name, p=params: getattr(db, m)(**p)
            queries.append((query_id, query_func, description))
    else:
        # Fallback to hardcoded queries if config not found
        print("Using default hardcoded queries...")
        queries = [
            # Q1: Lookup by Variant ID (Composite Key)
            (
                "Q1",
                lambda db: db.q1_variant_by_id("chr22", 10736093, "A", "T"),
                "Variant by ID (chr22:10736093:A:T)",
            ),
            # Q2: Lookup by Genomic Position (Exact)
            (
                "Q2",
                lambda db: db.q2_variant_by_position("chr22", 10736093),
                "Variant by Position (chr22:10736093)",
            ),
            # Q3: Finding variant by external existing variation ID
            (
                "Q3",
                lambda db: db.q3_variant_by_rsid("rs1394819064"),
                "Variant by rsID (rs1394819064)",
            ),
            # Q4: All Variants in a Gene (by Symbol)
            (
                "Q4",
                lambda db: db.q4_variants_in_gene_all("BRCA1"),
                "All Variants in Gene (BRCA1)",
            ),
            # Q5: All Variants in a Gene (by Symbol) return first 100
            (
                "Q5",
                lambda db: db.q5_variants_in_gene_limited("BRCA1", 100),
                "Gene Variants Limited (BRCA1, first 100)",
            ),
            # Q6: All variants in a genomic range - small range (~4kb)
            (
                "Q6",
                lambda db: db.q6_range_small("chr22", 10736093, 10739993),
                "Small Range (chr22:10736093-10739993)",
            ),
            # Q7: All variants in a genomic range - medium range (~100kb)
            (
                "Q7",
                lambda db: db.q7_range_medium("chr22", 10500000, 10600000),
                "Medium Range (chr22:10500000-10600000)",
            ),
            # Q8: All variants in a genomic range - large range (~10Mb)
            (
                "Q8",
                lambda db: db.q8_range_large("chr22", 10500000, 20500000),
                "Large Range (chr22:10500000-20500000)",
            ),
            # Q9: All variants in a Transcript
            (
                "Q9",
                lambda db: db.q9_transcript_variants("ENST00000615943"),
                "Transcript Variants (ENST00000615943)",
            ),
            # Q10: Coding variants
            (
                "Q10",
                lambda db: db.q10_coding_variants(
                    ["missense_variant", "frameshift_variant", "stop_gained"]
                ),
                "Coding Variants",
            ),
            # Q11: Gene with Quality Filter
            (
                "Q11",
                lambda db: db.q11_gene_with_quality("BRCA1", 30.0),
                "Gene with Quality (BRCA1, Q>30)",
            ),
            # Q12: Rare Variants
            (
                "Q12",
                lambda db: db.q12_gene_rare("BRCA1", 0.01),
                "Rare Variants (BRCA1, AF<0.01)",
            ),
        ]

    # Filter queries if specified
    if args.queries != "all":
        query_filter = set(args.queries.split(","))
        queries = [
            (qid, qfunc, qdesc) for qid, qfunc, qdesc in queries if qid in query_filter
        ]
        print(f"Running filtered queries: {query_filter}")

    all_results = []

    # Run benchmarks
    for db_benchmark in databases:
        print(f"\n{'=' * 80}")
        print(f"Benchmarking: {db_benchmark.name}")
        print(f"{'=' * 80}")

        try:
            db_benchmark.connect()
            print(f"Connected to {db_benchmark.name}")
        except Exception as e:
            print(f"Failed to connect to {db_benchmark.name}: {e}")
            continue

        for query_id, query_func, query_desc in queries:
            print(f"\nRunning {query_id}: {query_desc}...")

            # Warmup iterations
            if args.warmup > 0:
                print(f"  Warmup: {args.warmup} iterations...")
                for _ in range(args.warmup):
                    try:
                        query_func(db_benchmark)
                    except Exception as e:
                        print(f"  Warmup error: {e}")

            # Actual benchmark
            results = run_benchmark(
                benchmark=db_benchmark,
                query_name=query_id,
                query_func=lambda: query_func(db_benchmark),
                args=(),
                iterations=args.iterations,
                cache_state="warm",
            )

            all_results.extend(results)
            print_statistics(results, query_id, db_benchmark.name)

        try:
            db_benchmark.disconnect()
            print(f"\nDisconnected from {db_benchmark.name}")
        except Exception as e:
            print(f"Error disconnecting from {db_benchmark.name}: {e}")

    # Save results
    if all_results:
        save_results(all_results, args.output)
        print(f"\nTotal results collected: {len(all_results)}")
    else:
        print("\nNo results collected")


if __name__ == "__main__":
    main()
